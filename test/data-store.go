package main

import (
	"io"
	"reflect"
	"time"
)

type BucketInfo struct {
	Name    string
	ModTime time.Time
}

type StageObjectArgs struct {
	BucketName    string `json:"bucketName"`
	ObjectName    string `json:"objectName"`
	ObjectSize    int64  `json:"objectSize"`
	DataCount     int    `json:"dataCount"`
	ParityCount   int    `json:"parityCount"`
	ShardSize     int    `json:"shardSize"`
	LastShardSize int    `json:"lastShardSize"`
	ShardCount    int    `json:"shardCount"`
	ShardOrder    []int  `json:"shardOrder"`
	ShardIndex    int    `json:"shardIndex"`
}

type ObjectInfo struct {
	BucketName    string `json:"bucketName"`
	ObjectName    string `json:"objectName"`
	ObjectSize    int64  `json:"objectSize"`
	DataCount     int    `json:"dataCount"`
	ParityCount   int    `json:"parityCount"`
	ShardSize     int    `json:"shardSize"`
	LastShardSize int    `json:"lastShardSize"`
	ShardCount    int    `json:"shardCount"`
	ShardOrder    []int  `json:"shardOrder"`
	ShardIndex    int    `json:"shardIndex"`
}

func (oi *ObjectInfo) Equal(coi *ObjectInfo) bool {
	if oi.BucketName != coi.BucketName {
		return false
	}

	if oi.ObjectName != coi.ObjectName {
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

type DataStore interface {
	DeleteBucket(bucketName string) error
	HeadBucket(bucketName string) (*BucketInfo, error)
	PutBucket(bucketName string) error

	DeleteObject(bucketName, objectName string) error
	GetObject(bucketName, objectName string, offset, length int64) (io.ReadCloser, error)
	HeadObject(bucketName, objectName string) (*ObjectInfo, error)
	SaveStageObject(args *StageObjectArgs, requestID string, reader io.Reader, readerSize int64) error
	CommitStageObject(requestID string) error
	CloseStageObject(bucketName, objectName, requestID string, undoCommit bool) error
}
