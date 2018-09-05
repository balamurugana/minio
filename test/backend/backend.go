package backend

import (
	"errors"
	"io"
	"reflect"
	"time"
)

var (
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrObjectNotFound      = errors.New("object not found")
)

type BucketInfo struct {
	Name    string
	ModTime time.Time
}

func (bucketInfo *BucketInfo) Equal(compBucketInfo *BucketInfo) bool {
	return bucketInfo.Name == compBucketInfo.Name
}

type ObjectInfo struct {
	BucketName      string `json:"bucketName"`
	ObjectName      string `json:"objectName"`
	ObjectVersionID string `json:"objectVersionId"`
	ObjectDataID    string `json:"objectDataId"`
	ObjectSize      int64  `json:"objectSize"`
	DataCount       int    `json:"dataCount"`
	ParityCount     int    `json:"parityCount"`
	ShardSize       int    `json:"shardSize"`
	LastShardSize   int    `json:"lastShardSize"`
	ShardCount      int    `json:"shardCount"`
	ShardOrder      []int  `json:"shardOrder"`
	ShardIndex      int    `json:"shardIndex"`
}

func (oi *ObjectInfo) Equal(coi *ObjectInfo) bool {
	if oi.BucketName != coi.BucketName {
		return false
	}

	if oi.ObjectName != coi.ObjectName {
		return false
	}

	if oi.ObjectVersionID != coi.ObjectVersionID {
		return false
	}

	if oi.ObjectDataID != coi.ObjectDataID {
		return false
	}

	if oi.ObjectSize != coi.ObjectSize {
		return false
	}

	if oi.DataCount != coi.DataCount {
		return false
	}

	if oi.ParityCount != coi.ParityCount {
		return false
	}

	if oi.ShardSize != coi.ShardSize {
		return false
	}

	if oi.LastShardSize != coi.LastShardSize {
		return false
	}

	if oi.ShardCount != coi.ShardCount {
		return false
	}

	return reflect.DeepEqual(oi.ShardOrder, coi.ShardOrder)
}

type Backend interface {
	DeleteBucket(requestID, bucketName string) error
	CloseDeleteBucket(requestID, bucketName string, undo bool) error
	GetBucket(bucketName, prefix, startAfter string, maxKeys int) (keys, prefixes []string, marker string, err error)
	HeadBucket(bucketName string) (*BucketInfo, error)
	PutBucket(requestID, bucketName string) (err error)
	ClosePutBucket(requestID, bucketName string, undo bool) error

	DeleteObject(requestID, bucketName, objectName, versionID string) error
	CloseDeleteObject(requestID, bucketName, objectName, versionID string, undo bool) error
	GetObject(bucketName, objectName, versionID string, offset, length int64) (io.ReadCloser, error)
	HeadObject(bucketName, objectName, versionID string) (*ObjectInfo, error)

	PutObject(requestID, bucketName, objectName, versionID string, reader io.Reader, readerSize int64, metadata *ObjectInfo) error
	CommitPutObject(requestID, bucketName, objectName, versionID string) error
	ClosePutObject(requestID, bucketName, objectName, versionID string, undo bool) error
}
