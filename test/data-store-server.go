package main

import (
	"io"

	xrpc "github.com/balamurugana/minio/test/rpc"
)

const backendServiceName = "Backend"

// backendRPCReceiver - Backend RPC receiver for backend RPC server
type backendRPCReceiver struct {
	local *Backend
}

type BucketNameArgs struct {
	AuthArgs
	BucketName string
}

func (receiver *backendRPCReceiver) DeleteBucket(args *BucketNameArgs, reply *VoidReply) error {
	return receiver.local.DeleteBucket(args.BucketName)
}

func (receiver *backendRPCReceiver) HeadBucket(args *BucketNameArgs, bucketInfo *BucketInfo) error {
	bi, err := receiver.local.HeadBucket(args.BucketName)
	if err == nil {
		*bucketInfo = *bi
	}

	return err
}

func (receiver *backendRPCReceiver) PutBucket(args *BucketNameArgs, reply *VoidReply) error {
	return receiver.local.PutBucket(args.BucketName)
}

type ObjectNameArgs struct {
	AuthArgs
	BucketName string
	ObjectName string
}

func (receiver *backendRPCReceiver) DeleteObject(args *ObjectNameArgs, reply *VoidReply) error {
	return receiver.local.DeleteObject(args.BucketName, args.ObjectName)
}

type GetObjectArgs struct {
	AuthArgs
	BucketName string
	ObjectName string
	Offset     int64
	Length     int64
}

func (receiver *backendRPCReceiver) GetObject(args *GetObjectArgs, reader io.Reader, reply *VoidReply) (io.ReadCloser, error) {
	return receiver.local.GetObject(args.BucketName, args.ObjectName, args.Offset, args.Length)
}

func (receiver *backendRPCReceiver) HeadObject(args *ObjectNameArgs, objectInfo *ObjectInfo) error {
	oi, err := receiver.local.HeadObject(args.BucketName, args.ObjectName)
	if err == nil {
		*objectInfo = *oi
	}

	return err
}

type SaveStageObjectArgs struct {
	AuthArgs
	RequestID       string
	ReaderSize      int64
	StageObjectArgs StageObjectArgs
}

func (receiver *backendRPCReceiver) SaveStageObject(args *SaveStageObjectArgs, reader io.Reader, reply *VoidReply) (io.ReadCloser, error) {
	return nil, receiver.local.SaveStageObject(&args.StageObjectArgs, args.RequestID, reader, args.ReaderSize)
}

type CommitStageObjectArgs struct {
	AuthArgs
	RequestID string
}

func (receiver *backendRPCReceiver) CommitStageObject(args *CommitStageObjectArgs, reply *VoidReply) error {
	return receiver.local.CommitStageObject(args.RequestID)
}

type CloseStageObjectArgs struct {
	AuthArgs
	BucketName string
	ObjectName string
	RequestID  string
	UndoCommit bool
}

func (receiver *backendRPCReceiver) CloseStageObject(args *CloseStageObjectArgs, reply *VoidReply) error {
	return receiver.local.CloseStageObject(args.BucketName, args.ObjectName, args.RequestID, args.UndoCommit)
}

// NewBackendRPCServer - returns new backend RPC server.
func NewBackendRPCServer(backend *Backend) *xrpc.Server {
	rpcServer := xrpc.NewServer()
	if err := rpcServer.RegisterName(backendServiceName, &backendRPCReceiver{backend}); err != nil {
		panic(err)
	}

	return rpcServer
}
