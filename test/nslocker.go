package main

import (
	"fmt"
	"sync"
)

const (
	writeLock = false
	readLock  = true
)

func lockTypeString(lockType bool) string {
	if lockType == writeLock {
		return "write-lock"
	}

	return "read-lock"
}

type NSKey struct {
	BucketName string
	ObjectName string
}

type NSMutex struct {
	sync.RWMutex
	counter      int
	requestIDMap map[string]bool
}

type NSLocker struct {
	sync.Mutex
	nsMap map[NSKey]*NSMutex
}

func NewNSLocker() *NSLocker {
	return &NSLocker{
		nsMap: make(map[NSKey]*NSMutex),
	}
}

func (locker *NSLocker) getMutexToLock(requestID string, key NSKey, lockType bool) *NSMutex {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	mutex, mutexFound := locker.nsMap[key]
	if mutexFound {
		if storedLockType, requestIDFound := mutex.requestIDMap[requestID]; requestIDFound {
			panic(fmt.Sprintf("duplicate request ID %s with %v found for %v request",
				requestID, lockTypeString(storedLockType), lockTypeString(lockType)))
		}
	} else {
		mutex = &NSMutex{requestIDMap: make(map[string]bool)}
		locker.nsMap[key] = mutex
	}

	mutex.requestIDMap[requestID] = lockType
	mutex.counter++
	return mutex
}

func (locker *NSLocker) Lock(requestID, bucketName, objectName string) error {
	mutex := locker.getMutexToLock(requestID, NSKey{bucketName, objectName}, writeLock)
	mutex.Lock()
	return nil
}

func (locker *NSLocker) RLock(requestID, bucketName, objectName string) error {
	mutex := locker.getMutexToLock(requestID, NSKey{bucketName, objectName}, readLock)
	mutex.RLock()
	return nil
}

func (locker *NSLocker) getMutexToUnlock(requestID string, key NSKey) *NSMutex {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	if mutex, mutexFound := locker.nsMap[key]; mutexFound {
		if _, requestIDFound := mutex.requestIDMap[requestID]; requestIDFound {
			return mutex
		}
	}

	return nil
}

func (locker *NSLocker) unlock(requestID string, key NSKey, lockType bool) {
	mutex := locker.getMutexToUnlock(requestID, key)
	if mutex == nil {
		return
	}

	if lockType == writeLock {
		mutex.Unlock()
	} else {
		mutex.RUnlock()
	}

	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	delete(mutex.requestIDMap, requestID)
	if mutex.counter--; mutex.counter <= 0 {
		delete(locker.nsMap, key)
	}
}

func (locker *NSLocker) Unlock(requestID, bucketName, objectName string) error {
	locker.unlock(requestID, NSKey{bucketName, objectName}, writeLock)
	return nil
}

func (locker *NSLocker) RUnlock(requestID, bucketName, objectName string) error {
	locker.unlock(requestID, NSKey{bucketName, objectName}, readLock)
	return nil
}
