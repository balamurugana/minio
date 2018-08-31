package main

import (
	"crypto/tls"
	"io"

	xrpc "github.com/balamurugana/minio/test/rpc"
)

// BackendRPCClient - backend RPC client.
type BackendRPCClient struct {
	*RPCClient
}

func (client *BackendRPCClient) DeleteBucket(bucketName string) error {
	err := client.Call(backendServiceName+".DeleteBucket", &BucketNameArgs{BucketName: bucketName}, &VoidReply{})
	return getTypedError(err)
}

func (client *BackendRPCClient) HeadBucket(bucketName string) (*BucketInfo, error) {
	var bucketInfo BucketInfo
	err := client.Call(backendServiceName+".HeadBucket", &BucketNameArgs{BucketName: bucketName}, &bucketInfo)
	if err != nil {
		return nil, getTypedError(err)
	}
	return &bucketInfo, nil
}

func (client *BackendRPCClient) PutBucket(bucketName string) error {
	err := client.Call(backendServiceName+".PutBucket", &BucketNameArgs{BucketName: bucketName}, &VoidReply{})
	return getTypedError(err)
}

func (client *BackendRPCClient) DeleteObject(bucketName, objectName string) error {
	err := client.Call(backendServiceName+".DeleteObject", &ObjectNameArgs{BucketName: bucketName, ObjectName: objectName}, &VoidReply{})
	return getTypedError(err)
}

func (client *BackendRPCClient) GetObject(bucketName, objectName string, offset, length int64) (io.ReadCloser, error) {
	args := GetObjectArgs{
		BucketName: bucketName,
		ObjectName: objectName,
		Offset:     offset,
		Length:     length,
	}
	return client.CallWith(backendServiceName+".GetObject", &args, nil, &VoidReply{})
}

func (client *BackendRPCClient) HeadObject(bucketName, objectName string) (*ObjectInfo, error) {
	var objectInfo ObjectInfo
	err := client.Call(backendServiceName+".HeadObject", &ObjectNameArgs{BucketName: bucketName, ObjectName: objectName}, &objectInfo)
	if err != nil {
		return nil, getTypedError(err)
	}
	return &objectInfo, nil
}

func (client *BackendRPCClient) SaveStageObject(args *StageObjectArgs, requestID string, reader io.Reader, readerSize int64) error {
	rpcArgs := SaveStageObjectArgs{
		RequestID:       requestID,
		ReaderSize:      readerSize,
		StageObjectArgs: *args,
	}
	rc, err := client.CallWith(backendServiceName+".SaveStageObject", &rpcArgs, reader, &VoidReply{})
	if rc != nil {
		rc.Close()
	}
	return getTypedError(err)
}

func (client *BackendRPCClient) CommitStageObject(requestID string) error {
	err := client.Call(backendServiceName+".CommitStageObject", &CommitStageObjectArgs{RequestID: requestID}, &VoidReply{})
	return getTypedError(err)
}

func (client *BackendRPCClient) CloseStageObject(bucketName, objectName, requestID string, undoCommit bool) error {
	args := CloseStageObjectArgs{
		BucketName: bucketName,
		ObjectName: objectName,
		RequestID:  requestID,
		UndoCommit: undoCommit,
	}
	err := client.Call(backendServiceName+".CloseStageObject", &args, &VoidReply{})
	return getTypedError(err)
}

// NewBackendRPCClient - returns new backend RPC client.
func NewBackendRPCClient(serviceURL string, tlsConfig *tls.Config, rpcVersion RPCVersion) *BackendRPCClient {
	return &BackendRPCClient{NewRPCClient(serviceURL, tlsConfig, xrpc.DefaultRPCTimeout, globalRPCAPIVersion)}
}
