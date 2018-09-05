package main

import (
	"errors"
	"io"
	"log"
	"math/rand"

	"github.com/balamurugana/minio/test/backend"
	"github.com/balamurugana/minio/test/erasure"
)

var (
	errNilBackend  = errors.New("nil backend")
	errNilLocker   = errors.New("nil locker")
	errQuorum      = errors.New("quorum error")
	errReadQuorum  = errors.New("read quorum error")
	errWriteQuorum = errors.New("write quorum error")
)

type S3Handler struct {
	backend backend.Backend
	locker  Locker
}

func NewS3ErasureHandler(erasureDisk *backend.ErasureDisk, lockers *Lockers) *S3Handler {
	return &S3Handler{
		backend: erasureDisk.Clone(),
		locker:  lockers.Clone(),
	}
}

func (handler *S3Handler) DeleteBucket(requestID, bucketName string) (err error) {
	if err = handler.locker.Lock(requestID, bucketName, ""); err != nil {
		return err
	}

	err = handler.backend.DeleteBucket(requestID, bucketName)
	handler.backend.CloseDeleteBucket(requestID, bucketName, err != nil)

	if uerr := handler.locker.Unlock(requestID, bucketName, ""); uerr != nil {
		switch err {
		case nil, backend.ErrBucketNotFound:
			log.Println(uerr)
		default:
			err = uerr
		}
	}

	return err
}

func (handler *S3Handler) GetBucket(requestID, bucketName, prefix, startAfter string, maxKeys int) (keys, prefixes []string, marker string, err error) {
	return handler.backend.(*backend.ErasureDisk).GetBucket(bucketName, prefix, startAfter, maxKeys)
}

func (handler *S3Handler) HeadBucket(requestID, bucketName string) (bucketInfo *backend.BucketInfo, err error) {
	if err = handler.locker.RLock(requestID, bucketName, ""); err != nil {
		return nil, err
	}

	bucketInfo, err = handler.backend.HeadBucket(bucketName)

	if uerr := handler.locker.RUnlock(requestID, bucketName, ""); uerr != nil {
		switch err {
		case nil, backend.ErrBucketNotFound:
			log.Println(uerr)
		default:
			err = uerr
		}
	}

	return bucketInfo, err
}

func (handler *S3Handler) PutBucket(requestID, bucketName string) (err error) {
	if err = handler.locker.Lock(requestID, bucketName, ""); err != nil {
		return err
	}

	err = handler.backend.PutBucket(requestID, bucketName)
	handler.backend.ClosePutBucket(requestID, bucketName, err != nil)

	if uerr := handler.locker.Unlock(requestID, bucketName, ""); uerr != nil {
		switch err {
		case nil, backend.ErrBucketNotFound:
			log.Println(uerr)
		default:
			err = uerr
		}
	}

	return err
}

func (handler *S3Handler) DeleteObject(requestID, bucketName, objectName, versionID string) (err error) {
	if err = handler.locker.Lock(requestID, bucketName, objectName); err != nil {
		return err
	}

	err = handler.backend.DeleteObject(requestID, bucketName, objectName, versionID)
	handler.backend.CloseDeleteObject(requestID, bucketName, objectName, versionID, err != nil)

	if uerr := handler.locker.Unlock(requestID, bucketName, objectName); uerr != nil {
		switch err {
		case nil, backend.ErrBucketNotFound:
			log.Println(uerr)
		default:
			err = uerr
		}
	}

	return err
}

func (handler *S3Handler) GetObject(requestID, bucketName, objectName, versionID string, offset, length int64) (rc io.ReadCloser, err error) {
	if err = handler.locker.RLock(requestID, bucketName, objectName); err != nil {
		return nil, err
	}

	isUnlocked := false
	defer func() {
		if isUnlocked {
			return
		}
		if uerr := handler.locker.RUnlock(requestID, bucketName, objectName); uerr != nil {
			switch err {
			case nil, backend.ErrBucketNotFound:
				log.Println(uerr)
			default:
				err = uerr
			}
		}
	}()

	objectInfo, err := handler.backend.HeadObject(bucketName, objectName, versionID)
	if err != nil {
		return nil, err
	}

	// 3. compute request offset/length
	if offset+length > objectInfo.ObjectSize {
		length = objectInfo.ObjectSize - offset
	}

	values := erasure.Values{
		ObjectSize:    objectInfo.ObjectSize,
		DataCount:     objectInfo.DataCount,
		ParityCount:   objectInfo.ParityCount,
		ShardSize:     objectInfo.ShardSize,
		LastShardSize: objectInfo.LastShardSize,
		ShardCount:    objectInfo.ShardCount,
	}
	sectionValues := values.SectionValues(offset, length)

	// 4. get data stream from cluster
	readClosers := make([]io.ReadCloser, objectInfo.DataCount+objectInfo.ParityCount)

	handler.backend.(*backend.ErasureDisk).SetReadClosers(readClosers)
	_, err = handler.backend.GetObject(bucketName, objectName, versionID, sectionValues.ShardOffset, sectionValues.ShardLength)

	if uerr := handler.locker.RUnlock(requestID, bucketName, objectName); uerr != nil {
		switch err {
		case nil, backend.ErrBucketNotFound:
			log.Println(uerr)
		default:
			err = uerr
		}
	}
	isUnlocked = true
	if err != nil {
		return nil, err
	}

	// 6. read data stream from cluster and write to client
	readers := make([]io.Reader, objectInfo.DataCount+objectInfo.ParityCount)
	for i, pos := range objectInfo.ShardOrder {
		readers[i] = readClosers[pos]
	}

	decoder := erasure.NewDecoder(
		objectInfo.DataCount,
		objectInfo.ParityCount,
		sectionValues.ShardSize,
		sectionValues.LastShardSize,
		sectionValues.ShardCount,
		sectionValues.BytesToSkip,
	)

	pipeReader, pipeWriter := io.Pipe()

	go func() {
		defer func() {
			pipeWriter.Close()
			for _, rc := range readClosers {
				if rc != nil {
					rc.Close()
				}
			}
		}()

		n, err := decoder.CopyN(pipeWriter, readers, length)
		if err != nil {
			if n == 0 {
				log.Printf("S3Handler.GetObject() failed. err = %+v\n", err)
			} else {
				log.Printf("WARNING: error in writing decoded data in S3Handler.GetObject(), %v\n", err)
			}

			return
		}

		if n != length {
			log.Printf("WARNING: short data written, expected; %v, got: %v\n", length, n)
		}
	}()

	return pipeReader, nil
}

func (handler *S3Handler) HeadObject(requestID, bucketName, objectName, versionID string) (objectInfo *backend.ObjectInfo, err error) {
	if err = handler.locker.RLock(requestID, bucketName, objectName); err != nil {
		return nil, err
	}

	objectInfo, err = handler.backend.HeadObject(bucketName, objectName, versionID)

	if uerr := handler.locker.RUnlock(requestID, bucketName, objectName); uerr != nil {
		switch err {
		case nil, backend.ErrBucketNotFound:
			log.Println(uerr)
		default:
			err = uerr
		}
	}

	return objectInfo, err
}

func (handler *S3Handler) PutObject(requestID, bucketName, objectName, versionID string, reader io.Reader, readerSize int64, metadata *backend.ObjectInfo) (err error) {
	metadata.ShardOrder = rand.Perm(4)

	if err = handler.backend.PutObject(requestID, bucketName, objectName, versionID, reader, readerSize, metadata); err != nil {
		return err
	}

	if err = handler.locker.Lock(requestID, bucketName, objectName); err != nil {
		return err
	}

	if err = handler.backend.CommitPutObject(requestID, bucketName, objectName, versionID); err != nil {
		return err
	}

	handler.backend.ClosePutObject(requestID, bucketName, objectName, versionID, err != nil)

	if uerr := handler.locker.Unlock(requestID, bucketName, objectName); uerr != nil {
		switch err {
		case nil, backend.ErrBucketNotFound:
			log.Println(uerr)
		default:
			err = uerr
		}
	}

	return err
}
