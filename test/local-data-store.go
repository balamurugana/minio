package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	xos "github.com/balamurugana/minio/test/os"
)

var errBucketNotFound = errors.New("bucket not found")
var errBucketAlreadyExists = errors.New("bucket already exists")
var errObjectNotFound = errors.New("object not found")

func getTypedError(err error) error {
	if err == nil {
		return nil
	}

	switch err.Error() {
	case "bucket not found":
		return errBucketNotFound
	case "bucket already exists":
		return errBucketAlreadyExists
	case "object not found":
		return errObjectNotFound
	}

	return err
}

func getBucketInfo(bucketDir string) (os.FileInfo, error) {
	fi, err := os.Lstat(bucketDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errBucketNotFound
		}

		return nil, err
	}

	if !fi.IsDir() {
		return nil, errors.New("not a directory")
	}

	return fi, nil
}

func getObjectInfo(objectMetadataPath string, withStat bool, i interface{}) (os.FileInfo, error) {
	file, err := os.Open(objectMetadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errObjectNotFound
		}

		return nil, err
	}
	defer file.Close()

	if err = json.NewDecoder(file).Decode(i); err != nil {
		return nil, err
	}

	if !withStat {
		return nil, nil
	}

	fi, err := file.Stat()
	if err != nil {
		log.Printf("WARNING: unable to stat %v: %v\n", objectMetadataPath, err)
	}

	return fi, nil
}

type Backend struct {
	exportDir string
	tempDir   string
	transDir  string
}

func (b Backend) DeleteBucket(bucketName string) error {
	bucketDir := filepath.Join(b.exportDir, bucketName)

	err := os.Remove(bucketDir)
	if os.IsNotExist(err) {
		return errBucketNotFound
	}

	return err
}

func (b Backend) HeadBucket(bucketName string) (*BucketInfo, error) {
	bucketDir := filepath.Join(b.exportDir, bucketName)

	fi, err := getBucketInfo(bucketDir)
	if err != nil {
		return nil, err
	}

	return &BucketInfo{bucketName, fi.ModTime()}, nil
}

func (b Backend) PutBucket(bucketName string) error {
	bucketDir := filepath.Join(b.exportDir, bucketName)

	err := os.Mkdir(bucketDir, os.ModePerm)
	if os.IsExist(err) {
		return errBucketAlreadyExists
	}

	return err
}

func (b Backend) DeleteObject(bucketName, objectName string) error {
	bucketDir := filepath.Join(b.exportDir, bucketName)

	_, err := getBucketInfo(bucketDir)
	if err != nil {
		return err
	}

	objectDir := filepath.Join(bucketDir, objectName)
	parent, err := xos.RemoveAll(objectDir, bucketDir)
	if os.IsNotExist(err) {
		if parent == objectDir {
			return errObjectNotFound
		}

		log.Printf("WARNING: unable to remove parent directory %v: %v\n", parent, err)
		err = nil
	}

	return err
}

func (b Backend) GetObject(bucketName, objectName string, offset, length int64) (io.ReadCloser, error) {
	bucketDir := filepath.Join(b.exportDir, bucketName)

	_, err := getBucketInfo(bucketDir)
	if err != nil {
		return nil, err
	}

	objectDataPath := filepath.Join(bucketDir, objectName, "data")
	file, err := os.Open(objectDataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errObjectNotFound
		}

		return nil, err
	}

	return xos.NewSectionFileReader(file, offset, length), nil
}

func (b Backend) HeadObject(bucketName, objectName string) (*ObjectInfo, error) {
	bucketDir := filepath.Join(b.exportDir, bucketName)

	_, err := getBucketInfo(bucketDir)
	if err != nil {
		return nil, err
	}

	objectMetadataPath := filepath.Join(bucketDir, objectName, "metadata.json")

	var oi ObjectInfo
	if _, err = getObjectInfo(objectMetadataPath, true, &oi); err != nil {
		return nil, err
	}

	return &oi, nil
}

func (b Backend) SaveStageObject(args *StageObjectArgs, requestID string, reader io.Reader, readerSize int64) error {
	bucketDir := filepath.Join(b.exportDir, args.BucketName)
	requestIDDir := filepath.Join(b.tempDir, requestID)
	stageObjectMetadataPath := filepath.Join(requestIDDir, "metadata.json")
	stageObjectPath := filepath.Join(requestIDDir, "data")

	var err error
	defer func() {
		if err != nil {
			os.Remove(requestIDDir)
		}
	}()

	if _, err = getBucketInfo(bucketDir); err != nil {
		return err
	}

	if err = os.MkdirAll(requestIDDir, os.ModePerm); err != nil {
		return err
	}

	saveMetadata := func() error {
		file, err := os.OpenFile(stageObjectMetadataPath, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return err
		}
		defer file.Close()

		return json.NewEncoder(file).Encode(args)
	}

	if err = saveMetadata(); err != nil {
		return err
	}

	var file *os.File
	if file, err = os.OpenFile(stageObjectPath, os.O_RDWR|os.O_CREATE, 0755); err != nil {
		return err
	}
	defer file.Close()

	var n int64
	if n, err = io.Copy(file, reader); err != nil {
		return err
	}

	if n != readerSize {
		err = fmt.Errorf("insufficient data, expected: %v, got: %v", readerSize, n)
		return err
	}

	return nil
}

func (b Backend) CommitStageObject(requestID string) error {
	requestIDDir := filepath.Join(b.tempDir, requestID)
	stageObjectMetadataPath := filepath.Join(requestIDDir, "metadata.json")
	requestIDTransDir := filepath.Join(b.transDir, requestID)

	var err error
	defer func() {
		os.RemoveAll(requestIDDir)
	}()

	readMetadata := func() (*StageObjectArgs, error) {
		file, err := os.OpenFile(stageObjectMetadataPath, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		var args StageObjectArgs
		if err = json.NewDecoder(file).Decode(&args); err != nil {
			return nil, err
		}

		return &args, nil
	}

	args, err := readMetadata()
	if err != nil {
		return err
	}

	bucketDir := filepath.Join(b.exportDir, args.BucketName)
	if _, err = getBucketInfo(bucketDir); err != nil {
		return err
	}

	objectDir := filepath.Join(bucketDir, args.ObjectName)
	objectExists := true
	if err = os.Rename(objectDir, requestIDTransDir); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		objectExists = false
	}

	parentDir := ""
	if !objectExists {
		if parentDir, err = xos.MkdirAll(filepath.Dir(objectDir), os.ModePerm); err != nil {
			return err
		}
	}

	if err = os.Rename(requestIDDir, objectDir); err != nil {
		if objectExists {
			if rerr := os.Rename(requestIDTransDir, objectDir); rerr != nil {
				log.Printf("CRITICAL: reverting object path rename %v to %v failed %v", requestIDTransDir, objectDir, rerr)
			}
		} else {
			os.RemoveAll(parentDir)
		}
	}

	return err
}

func (b Backend) CloseStageObject(bucketName, objectName, requestID string, undoCommit bool) error {
	requestIDTransDir := filepath.Join(b.transDir, requestID)

	if undoCommit {
		objectDir := filepath.Join(b.exportDir, bucketName, objectName)
		return os.Rename(requestIDTransDir, objectDir)
	}

	os.RemoveAll(requestIDTransDir)
	return nil
}

func NewBackend(exportDir string) *Backend {
	return &Backend{
		exportDir: exportDir,
		tempDir:   filepath.Join(exportDir, "_", "tmp"),
		transDir:  filepath.Join(exportDir, "_", "trans"),
	}
}
