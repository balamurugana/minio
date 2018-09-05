package main

import (
	xrpc "github.com/balamurugana/minio/test/rpc"
)

const (
	nslockerServiceName = "NSLocker"
	nslockerLock        = nslockerServiceName + ".Lock"
	nslockerRLock       = nslockerServiceName + ".RLock"
	nslockerUnlock      = nslockerServiceName + ".Unlock"
	nslockerRUnlock     = nslockerServiceName + ".RUnlock"
)

type nslockerRPCReceiver struct {
	local *NSLocker
}

type LockRPCArgs struct {
	AuthArgs
	RequestID  string
	BucketName string
	ObjectName string
}

func (receiver *nslockerRPCReceiver) Lock(args *LockRPCArgs, reply *VoidReply) error {
	return receiver.local.Lock(args.RequestID, args.BucketName, args.ObjectName)
}

func (receiver *nslockerRPCReceiver) RLock(args *LockRPCArgs, reply *VoidReply) error {
	return receiver.local.RLock(args.RequestID, args.BucketName, args.ObjectName)
}

func (receiver *nslockerRPCReceiver) Unlock(args *LockRPCArgs, reply *VoidReply) error {
	return receiver.local.Unlock(args.RequestID, args.BucketName, args.ObjectName)
}

func (receiver *nslockerRPCReceiver) RUnlock(args *LockRPCArgs, reply *VoidReply) error {
	return receiver.local.RUnlock(args.RequestID, args.BucketName, args.ObjectName)
}

func NewNSLockerRPCServer(locker *NSLocker) *xrpc.Server {
	rpcServer := xrpc.NewServer()
	if err := rpcServer.RegisterName(nslockerServiceName, &nslockerRPCReceiver{locker}); err != nil {
		panic(err)
	}

	return rpcServer
}
