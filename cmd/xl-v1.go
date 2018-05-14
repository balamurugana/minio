/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bpool"
)

// XL constants.
const (
	// XL metadata file carries per object metadata.
	xlMetaJSONFile = "xl.json"
)

// xlObjects - Implements XL object layer.
type xlObjects struct {
	// name space mutex for object layer.
	nsMutex *nsLockMap

	// getDisks returns list of storageAPIs.
	getDisks func() []StorageAPI

	// Byte pools used for temporary i/o buffers.
	bp *bpool.BytePoolCap

	// TODO: Deprecated only kept here for tests, should be removed in future.
	storageDisks []StorageAPI

	// TODO: ListObjects pool management, should be removed in future.
	listPool *treeWalkPool
}

// list of all errors that can be ignored in tree walk operation in XL
var xlTreeWalkIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errVolumeNotFound, errFileNotFound)

// Shutdown function for object storage interface.
func (xl xlObjects) Shutdown(ctx context.Context) error {
	// Add any object layer shutdown activities here.
	closeStorageDisks(xl.getDisks())
	return nil
}

// Locking operations

// List namespace locks held in object layer
func (xl xlObjects) ListLocks(ctx context.Context, bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error) {
	xl.nsMutex.lockMapMutex.Lock()
	defer xl.nsMutex.lockMapMutex.Unlock()
	// Fetch current time once instead of fetching system time for every lock.
	timeNow := UTCNow()
	volumeLocks := []VolumeLockInfo{}

	for param, debugLock := range xl.nsMutex.debugLockMap {
		if param.volume != bucket {
			continue
		}
		// N B empty prefix matches all param.path.
		if !hasPrefix(param.path, prefix) {
			continue
		}

		volLockInfo := VolumeLockInfo{
			Bucket:                param.volume,
			Object:                param.path,
			LocksOnObject:         debugLock.counters.total,
			TotalBlockedLocks:     debugLock.counters.blocked,
			LocksAcquiredOnObject: debugLock.counters.granted,
		}
		// Filter locks that are held on bucket, prefix.
		for opsID, lockInfo := range debugLock.lockInfo {
			// filter locks that were held for longer than duration.
			elapsed := timeNow.Sub(lockInfo.since)
			if elapsed < duration {
				continue
			}
			// Add locks that are held for longer than duration.
			volLockInfo.LockDetailsOnObject = append(volLockInfo.LockDetailsOnObject,
				OpsLockState{
					OperationID: opsID,
					LockSource:  lockInfo.lockSource,
					LockType:    lockInfo.lType,
					Status:      lockInfo.status,
					Since:       lockInfo.since,
				})
			volumeLocks = append(volumeLocks, volLockInfo)
		}
	}
	return volumeLocks, nil
}

// Clear namespace locks held in object layer
func (xl xlObjects) ClearLocks(ctx context.Context, volLocks []VolumeLockInfo) error {
	// Remove lock matching bucket/prefix held longer than duration.
	for _, volLock := range volLocks {
		xl.nsMutex.ForceUnlock(volLock.Bucket, volLock.Object)
	}
	return nil
}

// StorageInfo - returns underlying storage statistics.
func (xl xlObjects) StorageInfo(ctx context.Context) StorageInfo {
	if info, err := globalStorageClients.GetStorageInfo(); err != nil {
		logger.LogIf(context.Background(), err)
		return StorageInfo{}
	}

	return *info
}
