/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/minio/dsync"
	"github.com/minio/minio/cmd/logger"
)

// lockInfo stores various info from the client for each lock that is requested.
type lockInfo struct {
	writer          bool      // Bool whether write or read lock.
	node            string    // Network address of client claiming lock.
	serviceEndpoint string    // RPC path of client claiming lock.
	uid             string    // UID to uniquely identify request of client.
	timestamp       time.Time // Timestamp set at the time of initialization.
	timeLastCheck   time.Time // Timestamp for last check of validity of lock.
}

func newWriteLockInfo(args dsync.LockArgs) lockInfo {
	return lockInfo{
		node:            args.ServerAddr,
		serviceEndpoint: args.ServiceEndpoint,
		uid:             args.UID,
		timestamp:       UTCNow(),
		timeLastCheck:   UTCNow(),
	}
}

func newReadLockInfo(args dsync.LockArgs) lockInfo {
	return lockInfo{
		writer:          true,
		node:            args.ServerAddr,
		serviceEndpoint: args.ServiceEndpoint,
		uid:             args.UID,
		timestamp:       UTCNow(),
		timeLastCheck:   UTCNow(),
	}
}

// isWriteLock - returns whether lockInfoList contains write lock or not.
func isWriteLock(lockInfoList []lockInfo) bool {
	return len(lockInfoList) == 1 && lockInfoList[0].writer
}

// localLocker implements Dsync.NetLocker
type localLocker struct {
	sync.Mutex
	serviceEndpoint string
	serverAddr      string
	lockMap         map[string][]lockInfo
}

// remove - removes lock info by uid from lockInfoList.  It also deletes entry from lock map when lock info list is empty.
func (locker *localLocker) remove(lockInfoList []lockInfo, name, uid string) bool {
	// Remove lock info by UID.
	for i, info := range lockInfoList {
		if info.uid != uid {
			continue
		}

		// If lockInfoList contains only one info, delete the entry from lock map
		// to avoid keeping empty lock info list for a name.
		if len(lockInfoList) == 1 {
			delete(locker.lockMap, name)
		} else {
			// Delete lockInfo by index.
			copy(lockInfoList[i:], lockInfoList[i+1:])
			lockInfoList[len(lockInfoList)-1] = lockInfo{} // Set empty for GC to free it up now.
			locker.lockMap[name] = lockInfoList[:len(lockInfoList)-1]
		}

		return true
	}

	return false
}

// Similar to removeEntry but only removes an entry only if the lock entry exists in map.
func (locker *localLocker) removeByName(name string, info lockInfo) {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	lockInfoList, found := locker.lockMap[name]
	if !found {
		return
	}

	if !locker.remove(lockInfoList, name, info.uid) {
		// remove() may fail if given info was removed by another goroutine.
		// Log this as error only for write locks.
		if info.writer {
			reqInfo := (&logger.ReqInfo{}).AppendTags("name", name)
			reqInfo.AppendTags("uid", info.uid)
			ctx := logger.SetReqInfo(context.Background(), reqInfo)
			logger.LogIf(ctx, errors.New("Lock maintenance failed to remove entry for write lock (should never happen)"))
		}
	}
}

func (l *localLocker) ServerAddr() string {
	return l.serverAddr
}

func (l *localLocker) ServiceEndpoint() string {
	return l.serviceEndpoint
}

func (locker *localLocker) Lock(args dsync.LockArgs) error {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	if _, locked := locker.lockMap[args.Resource]; locked {
		return fmt.Errorf("lock failed as resource %v is already locked", args.Resource)
	}

	locker.lockMap[args.Resource] = []lockInfo{newWriteLockInfo(args)}
	return nil
}

func (locker *localLocker) Unlock(args dsync.LockArgs) error {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	lockInfoList, found := locker.lockMap[args.Resource]

	if !found {
		return fmt.Errorf("resource %v was not locked to unlock", args.Resource)
	}

	if !isWriteLock(lockInfoList) {
		return fmt.Errorf("unlock failed as resource %v is read-locked %v times", args.Resource, len(lockInfoList))
	}

	if !locker.remove(lockInfoList, args.Resource, args.UID) {
		return fmt.Errorf("lock not found for resource %v and uid %v", args.Resource, args.UID)
	}

	return nil
}

func (locker *localLocker) RLock(args dsync.LockArgs) error {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	lockInfoList, found := locker.lockMap[args.Resource]
	if !found {
		locker.lockMap[args.Resource] = []lockInfo{newReadLockInfo(args)}
		return nil
	}

	if isWriteLock(lockInfoList) {
		return fmt.Errorf("read lock failed as resource %v is already locked", args.Resource)
	}

	locker.lockMap[args.Resource] = append(locker.lockMap[args.Resource], newReadLockInfo(args))
	return nil
}

func (locker *localLocker) RUnlock(args dsync.LockArgs) error {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	lockInfoList, found := locker.lockMap[args.Resource]

	if !found {
		return fmt.Errorf("resource %v was not locked to read-unlock", args.Resource)
	}

	if isWriteLock(lockInfoList) {
		return fmt.Errorf("read-unlock failed as resource %v is write-locked", args.Resource)
	}

	if !locker.remove(lockInfoList, args.Resource, args.UID) {
		return fmt.Errorf("read-lock not found for resource %v and uid %v", args.Resource, args.UID)
	}

	return nil
}

func (locker *localLocker) ForceUnlock(args dsync.LockArgs) error {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	if args.UID != "" {
		return fmt.Errorf("uid %v must be empty for resource %v ro force-unlock", args.UID, args.Resource)
	}

	delete(locker.lockMap, args.Resource)

	return nil
}

func (locker *localLocker) Expired(args dsync.LockArgs) bool {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	if lockInfoList, found := locker.lockMap[args.Resource]; found {
		for _, info := range lockInfoList {
			if info.uid == args.UID {
				return false
			}
		}
	}

	return true
}

type lockEntry struct {
	name     string
	lockInfo lockInfo
}

func (locker *localLocker) getLongLivedLocks(interval time.Duration) []lockEntry {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	entries := []lockEntry{}
	for name, lockInfoList := range locker.lockMap {
		for i := range lockInfoList {
			// Check whether enough time has gone by since last check
			if time.Since(lockInfoList[i].timeLastCheck) >= interval {
				entries = append(entries, lockEntry{name, lockInfoList[i]})
				lockInfoList[i].timeLastCheck = UTCNow()
			}
		}
	}

	return entries
}
