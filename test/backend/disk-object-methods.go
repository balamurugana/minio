package backend

import (
	"encoding/json"
	"io"
	"os"
	"path"
	"strings"

	"github.com/balamurugana/minio/test/datastore"
	xhash "github.com/balamurugana/minio/test/hash"
	xos "github.com/balamurugana/minio/test/os"
)

func (disk *Disk) objectInfo(metaFilename string) (*ObjectInfo, error) {
	file, err := os.Open(metaFilename)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrObjectNotFound
		}

		return nil, err
	}
	defer file.Close()

	var objectInfo ObjectInfo
	if err = json.NewDecoder(file).Decode(&objectInfo); err != nil {
		return nil, err
	}

	return &objectInfo, nil
}

func (disk *Disk) DeleteObject(requestID, bucketName, objectName, versionID string) error {
	if _, err := disk.headBucket(bucketName); err != nil {
		return err
	}

	metaFilename := disk.objectMetaFile(bucketName, objectName, versionID)
	if found := xos.Exist(metaFilename); !found {
		return ErrObjectNotFound
	}

	transDir := disk.transDir(requestID)
	if err := os.MkdirAll(transDir, os.ModePerm); err != nil {
		return err
	}

	return os.Rename(metaFilename, path.Join(transDir, disk.metaFilename(versionID)))
}

func (disk *Disk) CloseDeleteObject(requestID, bucketName, objectName, versionID string, undo bool) error {
	transDir := disk.transDir(requestID)
	backedMetaFilename := path.Join(transDir, disk.metaFilename(versionID))

	failure := func() error {
		metaFilename := disk.objectMetaFile(bucketName, objectName, versionID)
		if err := os.Rename(backedMetaFilename, metaFilename); err != nil {
			return err
		}
		os.RemoveAll(transDir)
		return nil
	}

	success := func() error {
		defer os.RemoveAll(transDir)

		// FIXME: refer comment in ClosePutObject().failure().
		if err := os.Remove(disk.objectDir(bucketName, objectName)); err != nil {
			perr, ok := err.(*os.PathError)
			if !ok {
				return err
			}

			if !strings.Contains(perr.Error(), "directory not empty") {
				return err
			}
		}

		objectInfo, err := disk.objectInfo(backedMetaFilename)
		if err != nil {
			// FIXME: this should not happen, hence log the error locally.
		}

		go func() {
			if err := disk.dataStore.Drop(objectInfo.ObjectDataID); err != nil {
				// FIXME: this should not happen, hence log the error locally.
			}
		}()

		return nil
	}

	if undo {
		return failure()
	}

	return success()
}

func (disk *Disk) GetObject(bucketName, objectName, versionID string, offset, length int64) (io.ReadCloser, error) {
	if _, err := disk.headBucket(bucketName); err != nil {
		return nil, err
	}

	objectInfo, err := disk.HeadObject(bucketName, objectName, versionID)
	if err != nil {
		return nil, err
	}

	return disk.dataStore.Get(objectInfo.ObjectDataID, "data.0", offset, length)
}

func (disk *Disk) HeadObject(bucketName, objectName, versionID string) (*ObjectInfo, error) {
	if _, err := disk.headBucket(bucketName); err != nil {
		return nil, err
	}

	metaFilename := disk.objectMetaFile(bucketName, objectName, versionID)
	return disk.objectInfo(metaFilename)
}

func (disk *Disk) PutObject(requestID, bucketName, objectName, versionID string, reader io.Reader, readerSize int64, metadata *ObjectInfo) (err error) {
	if _, err = disk.headBucket(bucketName); err != nil {
		return err
	}

	tempDir := disk.tempDir(requestID)
	if err = os.MkdirAll(tempDir, os.ModePerm); err != nil {
		return err
	}

	defer func() {
		os.RemoveAll(tempDir)
		if err != nil {
			os.RemoveAll(disk.dataDir(requestID))
		}
	}()

	dataFilename := path.Join(tempDir, "part.0")
	hasher, err := xhash.NewHighwayHash256(nil)
	if err != nil {
		return err
	}

	err = datastore.WriteDataFile(dataFilename, reader, readerSize, hasher, datastore.DefaultBlockSize)
	if err != nil {
		return err
	}

	metadataFilename := dataFilename + datastore.MetadataFileExt
	file, err := os.OpenFile(metadataFilename, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	metadata.BucketName = bucketName
	metadata.ObjectName = objectName
	metadata.ObjectVersionID = versionID
	metadata.ObjectDataID = requestID

	err = json.NewEncoder(file).Encode(metadata)
	file.Close()
	if err != nil {
		return err
	}

	return disk.dataStore.Put(requestID, "part.0", dataFilename, true, true)
}

func (d *Disk) CommitPutObject(requestID, bucketName, objectName, versionID string) (err error) {
	defer func() {
		if err != nil {
			os.RemoveAll(d.dataDir(requestID))
		}
	}()

	objectDir := d.objectDir(bucketName, objectName)
	metaFilename := d.objectMetaFile(bucketName, objectName, versionID)
	tempMetaFilename := path.Join(d.dataDir(requestID), d.metaFilename(""))

	if versionID != "" {
		if found := xos.Exist(objectDir); !found {
			if err = xos.MksubdirAll(d.objectsDir(bucketName), objectName); err != nil {
				return err
			}
		}

		return os.Rename(tempMetaFilename, metaFilename)
	}

	if found := xos.Exist(metaFilename); found {
		transDir := d.transDir(requestID)
		if err := os.Mkdir(transDir, os.ModePerm); err != nil {
			return err
		}

		if err = os.Rename(metaFilename, path.Join(transDir, d.metaFilename(""))); err != nil {
			return err
		}
	} else {
		if err = xos.MksubdirAll(d.objectsDir(bucketName), objectName); err != nil {
			return err
		}
	}

	return os.Rename(tempMetaFilename, metaFilename)
}

func (d *Disk) ClosePutObject(requestID, bucketName, objectName, versionID string, undo bool) error {
	restore := func() error {
		os.RemoveAll(d.dataDir(requestID))

		metaFilename := d.objectMetaFile(bucketName, objectName, versionID)

		if versionID == "" {
			transDir := d.transDir(requestID)
			backedMetaFilename := path.Join(transDir, d.metaFilename(""))
			if found := xos.Exist(backedMetaFilename); found {
				return os.Rename(backedMetaFilename, metaFilename)
			}
		}

		if err := os.Remove(metaFilename); err != nil {
			return err
		}

		//  FIXME:
		//  We should remove whatever directory tree we created in
		//  CommitPutObject() for this object.  But this is not possible due to
		//  limitation in cluster level namespace lock.
		//
		//  For example;
		//  * CommitPutObject(1) called for object a/b/c/d with namespace lock
		//    a/b/c/d which created directory tree a/b/c/d and returned.
		//
		//  * Just before calling this ClosePutObject() for object a/b/c/d with
		//    undo, another CommitPutObject(2) is called for object a/b/c with
		//    namespace lock a/b/c and CommitPutObject(2) also created directory
		//    tree a/b/c but not returned i.e. 'os.Rename(tempMetaFilename,
		//    metaFilename)' is about to execute.
		//
		//  * In this situation, if this CommitPutObject() with undo removes
		//    directory tree a/b/c/d entirely which will trigger
		//    CommitPutObject(2) to fail.  Not sure how to fix this problem ATM.
		//
		//  Hence we safely remove tail of object name.
		return os.Remove(d.objectDir(bucketName, objectName))
	}

	success := func() error {
		if versionID == "" {
			os.RemoveAll(d.transDir(requestID))
		}

		return nil
	}

	if undo {
		return restore()
	}

	return success()
}
