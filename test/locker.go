package main

import (
	"log"
	"sync"
)

type Locker interface {
	Lock(requestID, bucketName, objectName string) error
	RLock(requestID, bucketName, objectName string) error
	Unlock(requestID, bucketName, objectName string) error
	RUnlock(requestID, bucketName, objectName string) error
}

type Lockers struct {
	lockers     []Locker
	readQuorum  int
	writeQuorum int
}

func NewLockers(lockers []Locker, readQuorum, writeQuorum int) *Lockers {
	lockersCopy := make([]Locker, len(lockers))
	copy(lockersCopy, lockers)

	return &Lockers{
		lockers:     lockersCopy,
		readQuorum:  readQuorum,
		writeQuorum: writeQuorum,
	}
}

func (lockers *Lockers) Clone() *Lockers {
	return NewLockers(lockers.lockers, lockers.readQuorum, lockers.writeQuorum)
}

func (lockers *Lockers) Lock(requestID, bucketName, objectName string) error {
	errs := make([]error, len(lockers.lockers))
	var wg sync.WaitGroup
	for i := range lockers.lockers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if lockers.lockers[i] == nil {
				errs[i] = errNilLocker
				return
			}

			if errs[i] = lockers.lockers[i].Lock(requestID, bucketName, objectName); errs[i] != nil {
				lockers.lockers[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) >= lockers.writeQuorum {
		return nil
	}

	log.Println("Lockers.Lock() failed.", errs)
	return errWriteQuorum
}

func (lockers *Lockers) RLock(requestID, bucketName, objectName string) error {
	errs := make([]error, len(lockers.lockers))
	var wg sync.WaitGroup
	for i := range lockers.lockers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if lockers.lockers[i] == nil {
				errs[i] = errNilLocker
				return
			}

			if errs[i] = lockers.lockers[i].RLock(requestID, bucketName, objectName); errs[i] != nil {
				lockers.lockers[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) >= lockers.readQuorum {
		return nil
	}

	log.Println("Lockers.RLock() failed.", errs)
	return errReadQuorum
}

func (lockers *Lockers) Unlock(requestID, bucketName, objectName string) error {
	errs := make([]error, len(lockers.lockers))
	var wg sync.WaitGroup
	for i := range lockers.lockers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if lockers.lockers[i] == nil {
				errs[i] = errNilLocker
				return
			}

			if errs[i] = lockers.lockers[i].Unlock(requestID, bucketName, objectName); errs[i] != nil {
				lockers.lockers[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) >= lockers.writeQuorum {
		return nil
	}

	log.Println("Lockers.Unlock() failed.", errs)
	return errWriteQuorum
}

func (lockers *Lockers) RUnlock(requestID, bucketName, objectName string) error {
	errs := make([]error, len(lockers.lockers))
	var wg sync.WaitGroup
	for i := range lockers.lockers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if lockers.lockers[i] == nil {
				errs[i] = errNilLocker
				return
			}

			if errs[i] = lockers.lockers[i].RUnlock(requestID, bucketName, objectName); errs[i] != nil {
				lockers.lockers[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) >= lockers.readQuorum {
		return nil
	}

	log.Println("Lockers.RUnlock() failed.", errs)
	return errReadQuorum
}
