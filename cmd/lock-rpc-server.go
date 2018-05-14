/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"math/rand"
	"path"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/dsync"
	"github.com/minio/minio/cmd/logger"
	xrpc "github.com/minio/minio/cmd/rpc"
	xnet "github.com/minio/minio/pkg/net"
)

const (
	// Lock rpc server endpoint.
	lockServiceSubPath = "/lock"

	// Lock rpc service name.
	lockServiceName = "Dsync"

	// Lock maintenance interval.
	lockMaintenanceInterval = 1 * time.Minute

	// Lock validity check interval.
	lockValidityCheckInterval = 2 * time.Minute
)

var lockServicePath = path.Join(minioReservedBucketPath, lockServiceSubPath)

// LockArgs represents arguments for any authenticated lock RPC call.
type LockArgs struct {
	AuthArgs
	LockArgs dsync.LockArgs
}

// lockRPCReceiver is type for RPC handlers
type lockRPCReceiver struct {
	ll localLocker
}

// Lock - rpc handler for (single) write lock operation.
func (l *lockRPCReceiver) Lock(args *LockArgs, reply *bool) (err error) {
	*reply, err = l.ll.Lock(args.LockArgs)
	return err
}

// Unlock - rpc handler for (single) write unlock operation.
func (l *lockRPCReceiver) Unlock(args *LockArgs, reply *bool) (err error) {
	*reply, err = l.ll.Unlock(args.LockArgs)
	return err
}

// RLock - rpc handler for read lock operation.
func (l *lockRPCReceiver) RLock(args *LockArgs, reply *bool) (err error) {
	*reply, err = l.ll.RLock(args.LockArgs)
	return err
}

// RUnlock - rpc handler for read unlock operation.
func (l *lockRPCReceiver) RUnlock(args *LockArgs, reply *bool) (err error) {
	*reply, err = l.ll.RUnlock(args.LockArgs)
	return err
}

// ForceUnlock - rpc handler for force unlock operation.
func (l *lockRPCReceiver) ForceUnlock(args *LockArgs, reply *bool) (err error) {
	*reply, err = l.ll.ForceUnlock(args.LockArgs)
	return err
}

// Expired - rpc handler for expired lock status.
func (l *lockRPCReceiver) Expired(args *LockArgs, reply *bool) error {
	*reply = l.ll.Expired(args.LockArgs)
	return nil
}

// lockMaintenance loops over locks that have been active for some time and checks back
// with the original server whether it is still alive or not
//
// Following logic inside ignores the errors generated for Dsync.Active operation.
// - server at client down
// - some network error (and server is up normally)
//
// We will ignore the error, and we will retry later to get a resolve on this lock
func (l *lockRPCReceiver) lockMaintenance(interval time.Duration) {
	entries := l.ll.getLongLivedLocks(interval)

	for _, entry := range entries {
		// Initialize client based on the long live locks.
		host, err := xnet.ParseHost(entry.lockInfo.node)
		logger.CriticalIf(context.Background(), err)
		rpcClient, err := NewLockRPCClient(host)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		// Call back to original server verify whether the lock is still active (based on name & uid)
		expired, _ := rpcClient.Expired(dsync.LockArgs{
			UID:      entry.lockInfo.uid,
			Resource: entry.name,
		})

		rpcClient.Close()

		if expired {
			// As lock is no longer active at server that originated this lock, remove it locally.
			l.ll.removeByName(entry.name, entry.lockInfo)
		}
	}
}

// Start lock maintenance from all lock servers.
func startLockMaintenance(lkSrv *lockRPCReceiver) {
	// Initialize a new ticker with a minute between each ticks.
	ticker := time.NewTicker(lockMaintenanceInterval)
	// Stop the timer upon service closure and cleanup the go-routine.
	ticker.Stop()

	// Start with random sleep time, so as to avoid "synchronous checks" between servers
	time.Sleep(time.Duration(rand.Float64() * float64(lockMaintenanceInterval)))
	for {
		// Verifies every minute for locks held more than 2minutes.
		select {
		case <-globalServiceDoneCh:
			return
		case <-ticker.C:
			lkSrv.lockMaintenance(lockValidityCheckInterval)
		}
	}
}

// NewLockRPCServer - returns new lock RPC server.
func NewLockRPCServer() (*xrpc.Server, error) {
	rpcServer := xrpc.NewServer()
	if err := rpcServer.RegisterName(lockServiceName, globalLockServer); err != nil {
		return nil, err
	}
	return rpcServer, nil
}

// Register distributed NS lock handlers.
func registerDistNSLockRouter(router *mux.Router) {
	rpcServer, err := NewLockRPCServer()
	logger.CriticalIf(context.Background(), err)

	// Start lock maintenance from all lock servers.
	go startLockMaintenance(globalLockServer)

	subrouter := router.PathPrefix(minioReservedBucketPath).Subrouter()
	subrouter.Path(lockServiceSubPath).Handler(rpcServer)
}
