package main

import (
	"io"

	"github.com/balamurugana/minio/test/backend"
	xrpc "github.com/balamurugana/minio/test/rpc"
)

const (
	diskServiceName       = "Disk"
	diskDeleteBucket      = diskServiceName + ".DeleteBucket"
	diskCloseDeleteBucket = diskServiceName + ".CloseDeleteBucket"
	diskHeadBucket        = diskServiceName + ".HeadBucket"
	diskPutBucket         = diskServiceName + ".PutBucket"
	diskClosePutBucket    = diskServiceName + ".ClosePutBucket"
	diskDeleteObject      = diskServiceName + ".DeleteObject"
	diskCloseDeleteObject = diskServiceName + ".CloseDeleteObject"
	diskGetObject         = diskServiceName + ".GetObject"
	diskHeadObject        = diskServiceName + ".HeadObject"
	diskPutObject         = diskServiceName + ".PutObject"
	diskCommitPutObject   = diskServiceName + ".CommitPutObject"
	diskClosePutObject    = diskServiceName + ".ClosePutObject"
)

type diskRPCReceiver struct {
	local *backend.Disk
}

type DeleteBucketArgs struct {
	AuthArgs
	RequestID  string
	BucketName string
}

func (receiver *diskRPCReceiver) DeleteBucket(args *DeleteBucketArgs, reply *VoidReply) error {
	return receiver.local.DeleteBucket(args.RequestID, args.BucketName)
}

type CloseDeleteBucketArgs struct {
	AuthArgs
	RequestID  string
	BucketName string
	Undo       bool
}

func (receiver *diskRPCReceiver) CloseDeleteBucket(args *CloseDeleteBucketArgs, reply *VoidReply) error {
	return receiver.local.CloseDeleteBucket(args.RequestID, args.BucketName, args.Undo)
}

type HeadBucketArgs struct {
	AuthArgs
	BucketName string
}

func (receiver *diskRPCReceiver) HeadBucket(args *HeadBucketArgs, bucketInfo *backend.BucketInfo) error {
	bi, err := receiver.local.HeadBucket(args.BucketName)
	if err == nil {
		*bucketInfo = *bi
	}

	return err
}

type PutBucketArgs struct {
	AuthArgs
	RequestID  string
	BucketName string
}

func (receiver *diskRPCReceiver) PutBucket(args *PutBucketArgs, reply *VoidReply) error {
	return receiver.local.PutBucket(args.RequestID, args.BucketName)
}

type ClosePutBucketArgs struct {
	AuthArgs
	RequestID  string
	BucketName string
	Undo       bool
}

func (receiver *diskRPCReceiver) ClosePutBucket(args *ClosePutBucketArgs, reply *VoidReply) error {
	return receiver.local.ClosePutBucket(args.RequestID, args.BucketName, args.Undo)
}

type DeleteObjectArgs struct {
	AuthArgs
	RequestID  string
	BucketName string
	ObjectName string
	VersionID  string
}

func (receiver *diskRPCReceiver) DeleteObject(args *DeleteObjectArgs, reply *VoidReply) error {
	return receiver.local.DeleteObject(args.RequestID, args.BucketName, args.ObjectName, args.VersionID)
}

type CloseDeleteObjectArgs struct {
	AuthArgs
	RequestID  string
	BucketName string
	ObjectName string
	VersionID  string
	Undo       bool
}

func (receiver *diskRPCReceiver) CloseDeleteObject(args *CloseDeleteObjectArgs, reply *VoidReply) error {
	return receiver.local.CloseDeleteObject(args.RequestID, args.BucketName, args.ObjectName, args.VersionID, args.Undo)
}

type GetObjectArgs struct {
	AuthArgs
	BucketName string
	ObjectName string
	VersionID  string
	Offset     int64
	Length     int64
}

func (receiver *diskRPCReceiver) GetObject(args *GetObjectArgs, reader io.Reader, reply *VoidReply) (io.ReadCloser, error) {
	return receiver.local.GetObject(args.BucketName, args.ObjectName, args.VersionID, args.Offset, args.Length)
}

type HeadObjectArgs struct {
	AuthArgs
	BucketName string
	ObjectName string
	VersionID  string
}

func (receiver *diskRPCReceiver) HeadObject(args *HeadObjectArgs, objectInfo *backend.ObjectInfo) error {
	info, err := receiver.local.HeadObject(args.BucketName, args.ObjectName, args.VersionID)
	if err == nil {
		*objectInfo = *info
	}

	return err
}

type PutObjectArgs struct {
	AuthArgs
	RequestID  string
	BucketName string
	ObjectName string
	VersionID  string
	ReaderSize int64
	Metadata   *backend.ObjectInfo
}

func (receiver *diskRPCReceiver) PutObject(args *PutObjectArgs, reader io.Reader, reply *VoidReply) (io.ReadCloser, error) {
	err := receiver.local.PutObject(args.RequestID, args.BucketName, args.ObjectName, args.VersionID, reader, args.ReaderSize, args.Metadata)
	return nil, err
}

type CommitPutObjectArgs struct {
	AuthArgs
	RequestID  string
	BucketName string
	ObjectName string
	VersionID  string
}

func (receiver *diskRPCReceiver) CommitPutObject(args *CommitPutObjectArgs, reply *VoidReply) error {
	return receiver.local.CommitPutObject(args.RequestID, args.BucketName, args.ObjectName, args.VersionID)
}

type ClosePutObjectArgs struct {
	AuthArgs
	RequestID  string
	BucketName string
	ObjectName string
	VersionID  string
	Undo       bool
}

func (receiver *diskRPCReceiver) ClosePutObject(args *ClosePutObjectArgs, reply *VoidReply) error {
	return receiver.local.ClosePutObject(args.RequestID, args.BucketName, args.ObjectName, args.VersionID, args.Undo)
}

func NewDiskRPCServer(disk *backend.Disk) *xrpc.Server {
	rpcServer := xrpc.NewServer()
	if err := rpcServer.RegisterName(diskServiceName, &diskRPCReceiver{disk}); err != nil {
		panic(err)
	}

	return rpcServer
}
