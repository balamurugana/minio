package backend

import (
	"errors"
	"io"
	"log"
	"sync"

	"github.com/balamurugana/minio/test/erasure"
)

var (
	errNilBackend  = errors.New("nil backend")
	ErrReadQuorum  = errors.New("read quorum error")
	ErrWriteQuorum = errors.New("write quorum error")
)

func getErrCount(errs []error, err error) int {
	counter := 0
	for i := range errs {
		if errs[i] == err {
			counter++
		}
	}

	return counter
}

type ErasureDisk struct {
	disks       []Backend
	dataCount   int
	parityCount int
	shardSize   int
	readQuorum  int
	writeQuorum int
	readClosers []io.ReadCloser
}

func NewErasureDisk(disks []Backend, dataCount, parityCount, shardSize, readQuorum, writeQuorum int) *ErasureDisk {
	disksCopy := make([]Backend, len(disks))
	copy(disksCopy, disks)

	return &ErasureDisk{
		disks:       disksCopy,
		dataCount:   dataCount,
		parityCount: parityCount,
		shardSize:   shardSize,
		readQuorum:  readQuorum,
		writeQuorum: writeQuorum,
	}
}

func (erasureDisk *ErasureDisk) Clone() *ErasureDisk {
	return NewErasureDisk(
		erasureDisk.disks,
		erasureDisk.dataCount,
		erasureDisk.parityCount,
		erasureDisk.shardSize,
		erasureDisk.readQuorum,
		erasureDisk.writeQuorum,
	)
}

func (erasureDisk *ErasureDisk) SetReadClosers(readClosers []io.ReadCloser) {
	erasureDisk.readClosers = readClosers
}

func (erasureDisk *ErasureDisk) DeleteBucket(requestID, bucketName string) error {
	errs := make([]error, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
				return
			}
			if errs[i] = erasureDisk.disks[i].DeleteBucket(requestID, bucketName); errs[i] != nil {
				erasureDisk.disks[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, ErrBucketNotFound) >= erasureDisk.writeQuorum {
		return ErrBucketNotFound
	}

	if getErrCount(errs, ErrBucketNotEmpty) >= erasureDisk.writeQuorum {
		return ErrBucketNotEmpty
	}

	if getErrCount(errs, nil) < erasureDisk.writeQuorum {
		log.Println("ErasureDisk.DeleteBucket() failed.", errs)
		return ErrWriteQuorum
	}

	return nil
}

func (erasureDisk *ErasureDisk) CloseDeleteBucket(requestID, bucketName string, undo bool) error {
	errs := make([]error, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
			} else {
				errs[i] = erasureDisk.disks[i].CloseDeleteBucket(requestID, bucketName, undo)
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) < erasureDisk.writeQuorum {
		log.Println("ErasureDisk.CloseDeleteBucket() failed.", errs)
		return ErrWriteQuorum
	}

	for i := range errs {
		if errs[i] != nil && erasureDisk.disks[i] != nil {
			log.Println(erasureDisk.disks[i], errs[i])
		}
	}

	return nil
}

func (erasureDisk *ErasureDisk) GetBucket(bucketName, prefix, startAfter string, maxKeys int) (keys, prefixes []string, marker string, err error) {
	var localDisk *Disk
	var isLocalDisk bool
	for _, disk := range erasureDisk.disks {
		if localDisk, isLocalDisk = disk.(*Disk); isLocalDisk {
			break
		}
	}

	if localDisk == nil {
		panic("local disk not found")
	}

	return localDisk.GetBucket(bucketName, prefix, startAfter, maxKeys)
}

func (erasureDisk *ErasureDisk) HeadBucket(bucketName string) (*BucketInfo, error) {
	errs := make([]error, len(erasureDisk.disks))
	bucketInfos := make([]*BucketInfo, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
				return
			}
			if bucketInfos[i], errs[i] = erasureDisk.disks[i].HeadBucket(bucketName); errs[i] != nil {
				erasureDisk.disks[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, ErrBucketNotFound) >= erasureDisk.readQuorum {
		return nil, ErrBucketNotFound
	}

	if getErrCount(errs, nil) < erasureDisk.readQuorum {
		log.Println("ErasureDisk.HeadBucket() failed.", errs)
		return nil, ErrReadQuorum
	}

	counters := make([]int, len(erasureDisk.disks))
	for i := 0; i < len(bucketInfos); i++ {
		if bucketInfos[i] == nil {
			continue
		}

		for j := i + 1; j < len(bucketInfos); j++ {
			if bucketInfos[j] != nil && bucketInfos[i].Equal(bucketInfos[j]) {
				counters[i]++
			}
		}
	}

	bestMatchIndex := 0
	max := 0
	for i := range counters {
		if max < counters[i] {
			max = counters[i]
			bestMatchIndex = i
		}
	}

	if max < erasureDisk.readQuorum {
		log.Printf("ErasureDisk.HeadBucket() failed. %+v\n", bucketInfos)
		return nil, ErrReadQuorum
	}

	return bucketInfos[bestMatchIndex], nil
}

func (erasureDisk *ErasureDisk) PutBucket(requestID, bucketName string) (err error) {
	errs := make([]error, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
				return
			}
			if errs[i] = erasureDisk.disks[i].PutBucket(requestID, bucketName); errs[i] != nil {
				erasureDisk.disks[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, ErrBucketAlreadyExists) >= erasureDisk.writeQuorum {
		return ErrBucketAlreadyExists
	}

	if getErrCount(errs, nil) < erasureDisk.writeQuorum {
		log.Println("ErasureDisk.PutBucket() failed.", errs)
		return ErrWriteQuorum
	}

	return nil
}

func (erasureDisk *ErasureDisk) ClosePutBucket(requestID, bucketName string, undo bool) error {
	errs := make([]error, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
			} else {
				errs[i] = erasureDisk.disks[i].ClosePutBucket(requestID, bucketName, undo)
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) < erasureDisk.writeQuorum {
		log.Println("ErasureDisk.ClosePutBucket() failed.", errs)
		return ErrWriteQuorum
	}

	for i := range errs {
		if errs[i] != nil && erasureDisk.disks[i] != nil {
			log.Println(erasureDisk.disks[i], errs[i])
		}
	}

	return nil
}

func (erasureDisk *ErasureDisk) DeleteObject(requestID, bucketName, objectName, versionID string) error {
	errs := make([]error, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
				return
			}
			if errs[i] = erasureDisk.disks[i].DeleteObject(requestID, bucketName, objectName, versionID); errs[i] != nil {
				erasureDisk.disks[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, ErrBucketNotFound) >= erasureDisk.writeQuorum {
		return ErrBucketNotFound
	}

	if getErrCount(errs, ErrObjectNotFound) >= erasureDisk.writeQuorum {
		return ErrObjectNotFound
	}

	if getErrCount(errs, nil) < erasureDisk.writeQuorum {
		log.Println("ErasureDisk.DeleteObject() failed.", errs)
		return ErrWriteQuorum
	}

	return nil
}

func (erasureDisk *ErasureDisk) CloseDeleteObject(requestID, bucketName, objectName, versionID string, undo bool) error {
	errs := make([]error, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
			} else {
				errs[i] = erasureDisk.disks[i].CloseDeleteObject(requestID, bucketName, objectName, versionID, undo)
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) < erasureDisk.writeQuorum {
		log.Println("ErasureDisk.CloseDeleteObject() failed.", errs)
		return ErrWriteQuorum
	}

	for i := range errs {
		if errs[i] != nil && erasureDisk.disks[i] != nil {
			log.Println(erasureDisk.disks[i], errs[i])
		}
	}
	return nil
}

func (erasureDisk *ErasureDisk) GetObject(bucketName, objectName, versionID string, offset, length int64) (io.ReadCloser, error) {
	if erasureDisk.readClosers == nil {
		return nil, errors.New("ErasureDisk.SetReadClosers() must be called before to get reply of ErasureDisk.GetObject()")
	}

	defer func() {
		erasureDisk.readClosers = nil
	}()

	errs := make([]error, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
				return
			}
			if erasureDisk.readClosers[i], errs[i] = erasureDisk.disks[i].GetObject(bucketName, objectName, versionID, offset, length); errs[i] != nil {
				erasureDisk.disks[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, ErrBucketNotFound) >= erasureDisk.readQuorum {
		return nil, ErrBucketNotFound
	}

	if getErrCount(errs, ErrObjectNotFound) >= erasureDisk.readQuorum {
		return nil, ErrObjectNotFound
	}

	if getErrCount(errs, nil) < erasureDisk.readQuorum {
		log.Println("ErasureDisk.GetObject() failed.", errs)
		return nil, ErrReadQuorum
	}

	return nil, nil
}

func (erasureDisk *ErasureDisk) HeadObject(bucketName, objectName, versionID string) (*ObjectInfo, error) {
	errs := make([]error, len(erasureDisk.disks))
	objectInfos := make([]*ObjectInfo, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
				return
			}
			if objectInfos[i], errs[i] = erasureDisk.disks[i].HeadObject(bucketName, objectName, versionID); errs[i] != nil {
				erasureDisk.disks[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, ErrBucketNotFound) >= erasureDisk.readQuorum {
		return nil, ErrBucketNotFound
	}

	if getErrCount(errs, ErrObjectNotFound) >= erasureDisk.readQuorum {
		return nil, ErrObjectNotFound
	}

	if getErrCount(errs, nil) < erasureDisk.readQuorum {
		log.Println("ErasureDisk.HeadObject() failed.", errs)
		return nil, ErrReadQuorum
	}

	counters := make([]int, len(erasureDisk.disks))
	for i := 0; i < len(objectInfos); i++ {
		if objectInfos[i] == nil {
			continue
		}

		for j := i + 1; j < len(objectInfos); j++ {
			if objectInfos[j] != nil && objectInfos[i].Equal(objectInfos[j]) {
				counters[i]++
			}
		}
	}

	bestMatchIndex := 0
	max := 0
	for i := range counters {
		if max < counters[i] {
			max = counters[i]
			bestMatchIndex = i
		}
	}

	if max < erasureDisk.readQuorum {
		log.Printf("ErasureDisk.HeadObject() failed. +v\n", objectInfos)
		return nil, ErrReadQuorum
	}

	return objectInfos[bestMatchIndex], nil
}

func (erasureDisk *ErasureDisk) erasureValues(readerSize int64) erasure.Values {
	return erasure.Compute(readerSize, erasureDisk.dataCount, erasureDisk.parityCount, erasureDisk.shardSize)
}

func (erasureDisk *ErasureDisk) PutObject(requestID, bucketName, objectName, versionID string, reader io.Reader, readerSize int64, metadata *ObjectInfo) error {
	count := len(erasureDisk.disks)
	values := erasureDisk.erasureValues(readerSize)

	metadata.DataCount = erasureDisk.dataCount
	metadata.ParityCount = erasureDisk.parityCount
	metadata.ShardSize = erasureDisk.shardSize
	metadata.ObjectSize = readerSize
	metadata.LastShardSize = values.LastShardSize
	metadata.ShardCount = values.ShardCount

	orderedDisks := make([]Backend, count)
	for i, order := range metadata.ShardOrder {
		orderedDisks[i] = erasureDisk.disks[order]
	}
	shardObjectSize := values.ShardObjectSize()

	pipeReaders := make([]*io.PipeReader, count)
	pipeWriters := make([]*io.PipeWriter, count)
	writers := make([]io.Writer, count)
	for i := 0; i < count; i++ {
		if erasureDisk.disks[i] != nil {
			pipeReaders[i], pipeWriters[i] = io.Pipe()
			writers[i] = pipeWriters[i]
		}
	}

	errs := make([]error, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
				return
			}

			objectInfo := *metadata
			objectInfo.ShardIndex = i
			if errs[i] = orderedDisks[i].PutObject(requestID, bucketName, objectName, versionID, pipeReaders[i], shardObjectSize, &objectInfo); errs[i] != nil {
				erasureDisk.disks[i] = nil
				pipeReaders[i].Close()
				pipeReaders[i] = nil
				pipeWriters[i].Close()
				pipeWriters[i] = nil
			}
		}(i)
	}

	encoder := erasure.NewEncoder(metadata.DataCount, metadata.ParityCount, metadata.ShardSize)
	err := encoder.CopyN(writers, reader, readerSize)
	for i := 0; i < count; i++ {
		if pipeWriters[i] != nil {
			pipeWriters[i].Close()
		}
	}

	wg.Wait()

	if getErrCount(errs, ErrBucketNotFound) >= erasureDisk.writeQuorum {
		return ErrBucketNotFound
	}

	if getErrCount(errs, nil) < erasureDisk.writeQuorum {
		log.Println("ErasureDisk.PutObject() failed.", errs)
		return ErrWriteQuorum
	}

	return err
}

func (erasureDisk *ErasureDisk) CommitPutObject(requestID, bucketName, objectName, versionID string) error {
	errs := make([]error, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
			} else {
				errs[i] = erasureDisk.disks[i].CommitPutObject(requestID, bucketName, objectName, versionID)
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, ErrBucketNotFound) >= erasureDisk.writeQuorum {
		return ErrBucketNotFound
	}

	if getErrCount(errs, ErrObjectNotFound) >= erasureDisk.writeQuorum {
		return ErrObjectNotFound
	}

	if getErrCount(errs, nil) < erasureDisk.writeQuorum {
		log.Println("ErasureDisk.CommitPutObject() failed.", errs)
		return ErrWriteQuorum
	}

	return nil
}

func (erasureDisk *ErasureDisk) ClosePutObject(requestID, bucketName, objectName, versionID string, undo bool) error {
	errs := make([]error, len(erasureDisk.disks))
	var wg sync.WaitGroup
	for i := range erasureDisk.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if erasureDisk.disks[i] == nil {
				errs[i] = errNilBackend
			} else {
				errs[i] = erasureDisk.disks[i].ClosePutObject(requestID, bucketName, objectName, versionID, undo)
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) >= erasureDisk.writeQuorum {
		return nil
	}

	for i := range errs {
		if errs[i] != nil && erasureDisk.disks[i] != nil {
			log.Println(erasureDisk.disks[i], errs[i])
		}
	}

	log.Println("ErasureDisk.ClosePutObject() failed.", errs)
	return ErrWriteQuorum
}
