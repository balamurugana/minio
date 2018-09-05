package main

import (
	"crypto/tls"

	xrpc "github.com/balamurugana/minio/test/rpc"
)

type NSLockerRPCClient struct {
	*RPCClient
}

func (client *NSLockerRPCClient) Lock(requestID, bucketName, objectName string) error {
	return client.Call(nslockerLock, &LockRPCArgs{RequestID: requestID, BucketName: bucketName, ObjectName: objectName}, &VoidReply{})
}

func (client *NSLockerRPCClient) RLock(requestID, bucketName, objectName string) error {
	return client.Call(nslockerRLock, &LockRPCArgs{RequestID: requestID, BucketName: bucketName, ObjectName: objectName}, &VoidReply{})
}

func (client *NSLockerRPCClient) Unlock(requestID, bucketName, objectName string) error {
	return client.Call(nslockerUnlock, &LockRPCArgs{RequestID: requestID, BucketName: bucketName, ObjectName: objectName}, &VoidReply{})
}

func (client *NSLockerRPCClient) RUnlock(requestID, bucketName, objectName string) error {
	return client.Call(nslockerRUnlock, &LockRPCArgs{RequestID: requestID, BucketName: bucketName, ObjectName: objectName}, &VoidReply{})
}

func NewNSLockerRPCClient(serviceURL string, tlsConfig *tls.Config, rpcVersion RPCVersion) *NSLockerRPCClient {
	return &NSLockerRPCClient{NewRPCClient(serviceURL, tlsConfig, xrpc.DefaultRPCTimeout, globalRPCAPIVersion)}
}
