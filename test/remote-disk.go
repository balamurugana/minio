package main

import (
	"crypto/tls"
	"errors"
	"io"

	"github.com/balamurugana/minio/test/backend"
	xrpc "github.com/balamurugana/minio/test/rpc"
)

func getTypedError(err error) error {
	if err == nil {
		return nil
	}

	switch err.Error() {
	case backend.ErrBucketAlreadyExists.Error():
		return backend.ErrBucketAlreadyExists
	case backend.ErrBucketNotEmpty.Error():
		return backend.ErrBucketNotEmpty
	case backend.ErrBucketNotFound.Error():
		return backend.ErrBucketNotFound
	case backend.ErrObjectNotFound.Error():
		return backend.ErrObjectNotFound
	}

	return err
}

// DiskRPCClient - backend RPC client.
type DiskRPCClient struct {
	*RPCClient
}

func (client *DiskRPCClient) DeleteBucket(requestID, bucketName string) error {
	err := client.Call(diskDeleteBucket, &DeleteBucketArgs{RequestID: requestID, BucketName: bucketName}, &VoidReply{})
	return getTypedError(err)
}

func (client *DiskRPCClient) CloseDeleteBucket(requestID, bucketName string, undo bool) error {
	err := client.Call(diskCloseDeleteBucket, &CloseDeleteBucketArgs{RequestID: requestID, BucketName: bucketName, Undo: undo}, &VoidReply{})
	return getTypedError(err)
}

func (client *DiskRPCClient) GetBucket(bucketName, prefix, startAfter string, maxKeys int) (keys, prefixes []string, marker string, err error) {
	return nil, nil, "", errors.New("method unsupported")
}

func (client *DiskRPCClient) HeadBucket(bucketName string) (*backend.BucketInfo, error) {
	var bucketInfo backend.BucketInfo
	err := client.Call(diskHeadBucket, &HeadBucketArgs{BucketName: bucketName}, &bucketInfo)
	if err != nil {
		return nil, getTypedError(err)
	}
	return &bucketInfo, nil
}

func (client *DiskRPCClient) PutBucket(requestID, bucketName string) error {
	err := client.Call(diskPutBucket, &PutBucketArgs{RequestID: requestID, BucketName: bucketName}, &VoidReply{})
	return getTypedError(err)
}

func (client *DiskRPCClient) ClosePutBucket(requestID, bucketName string, undo bool) error {
	err := client.Call(diskClosePutBucket, &ClosePutBucketArgs{RequestID: requestID, BucketName: bucketName, Undo: undo}, &VoidReply{})
	return getTypedError(err)
}

func (client *DiskRPCClient) DeleteObject(requestID, bucketName, objectName, versionID string) error {
	err := client.Call(diskDeleteObject, &DeleteObjectArgs{RequestID: requestID, BucketName: bucketName, ObjectName: objectName, VersionID: versionID}, &VoidReply{})
	return getTypedError(err)
}

func (client *DiskRPCClient) CloseDeleteObject(requestID, bucketName, objectName, versionID string, undo bool) error {
	err := client.Call(diskCloseDeleteObject, &CloseDeleteObjectArgs{RequestID: requestID, BucketName: bucketName, ObjectName: objectName, VersionID: versionID, Undo: undo}, &VoidReply{})
	return getTypedError(err)
}

func (client *DiskRPCClient) GetObject(bucketName, objectName, versionID string, offset, length int64) (io.ReadCloser, error) {
	args := GetObjectArgs{
		BucketName: bucketName,
		ObjectName: objectName,
		VersionID:  versionID,
		Offset:     offset,
		Length:     length,
	}
	rc, err := client.CallWith(diskGetObject, &args, nil, &VoidReply{})
	if err != nil {
		if rc != nil {
			rc.Close()
			rc = nil
		}
	}

	return rc, getTypedError(err)
}

func (client *DiskRPCClient) HeadObject(bucketName, objectName, versionID string) (*backend.ObjectInfo, error) {
	var objectInfo backend.ObjectInfo
	err := client.Call(diskHeadObject, &HeadObjectArgs{BucketName: bucketName, ObjectName: objectName, VersionID: versionID}, &objectInfo)
	if err != nil {
		return nil, getTypedError(err)
	}
	return &objectInfo, nil
}

func (client *DiskRPCClient) PutObject(requestID, bucketName, objectName, versionID string, reader io.Reader, readerSize int64, metadata *backend.ObjectInfo) error {
	args := PutObjectArgs{
		RequestID:  requestID,
		BucketName: bucketName,
		ObjectName: objectName,
		VersionID:  versionID,
		ReaderSize: readerSize,
		Metadata:   metadata,
	}
	rc, err := client.CallWith(diskPutObject, &args, reader, &VoidReply{})
	if rc != nil {
		rc.Close()
	}
	return getTypedError(err)
}

func (client *DiskRPCClient) CommitPutObject(requestID, bucketName, objectName, versionID string) error {
	err := client.Call(diskCommitPutObject, &CommitPutObjectArgs{RequestID: requestID, BucketName: bucketName, ObjectName: objectName, VersionID: versionID}, &VoidReply{})
	return getTypedError(err)
}

func (client *DiskRPCClient) ClosePutObject(requestID, bucketName, objectName, versionID string, undo bool) error {
	err := client.Call(diskClosePutBucket, &ClosePutObjectArgs{RequestID: requestID, BucketName: bucketName, ObjectName: objectName, VersionID: versionID, Undo: undo}, &VoidReply{})
	return getTypedError(err)
}

func NewDiskRPCClient(serviceURL string, tlsConfig *tls.Config, rpcVersion RPCVersion) *DiskRPCClient {
	return &DiskRPCClient{NewRPCClient(serviceURL, tlsConfig, xrpc.DefaultRPCTimeout, globalRPCAPIVersion)}
}
