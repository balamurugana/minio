package backend

import (
	"path"

	"github.com/balamurugana/minio/test/datastore"
)

//
//  Disk backend directory layout.
//
//  ```bash
//  $ mkdir -p EXPORT_DIR/{buckets,data,tmp,trans}
//  $ mkdir -p EXPORT_DIR/buckets/BUCKET/objects/OBJECT
//  $ touch EXPORT_DIR/buckets/BUCKET/objects/OBJECT/meta.json
//  $ touch EXPORT_DIR/buckets/BUCKET/objects/OBJECT/meta.json.VERSION_ID
//  $ mkdir -p EXPORT_DIR/data/0000/ID
//  $ mkdir -p EXPORT_DIR/{tmp,trans}/ID
//  ```
//
//  ```bash
//  $ tree -F -n EXPORT_DIR
//  ```
//
//  <EXPORT_DIR>/
//  ├── buckets/
//  │   └── <BUCKET>/
//  │       └── objects/
//  │           └── <OBJECT>/
//  │               └── meta.json
//  │               └── meta.json.<VERSION_ID>
//  ├── data/
//  │   └── 0000/
//  │       └── <ID>/
//  ├── tmp/
//  │   └── <ID>/
//  └── trans/
//      └── <ID>/
//
//

func dataDir(exportDir string) string {
	return path.Join(exportDir, "data")
}

func tempDir(exportDir string) string {
	return path.Join(exportDir, "tmp")
}

func transDir(exportDir string) string {
	return path.Join(exportDir, "trans")
}

func bucketsDir(exportDir string) string {
	return path.Join(exportDir, "buckets")
}

type Disk struct {
	exportDir    string
	bucketsDir   string
	tempBaseDir  string
	transBaseDir string
	dataStoreDir string
	dataStore    *datastore.DataStore
}

func NewDisk(exportDir string) *Disk {
	return &Disk{
		exportDir:    exportDir,
		bucketsDir:   bucketsDir(exportDir),
		tempBaseDir:  tempDir(exportDir),
		transBaseDir: transDir(exportDir),
		dataStoreDir: dataDir(exportDir),
		dataStore:    datastore.New(dataDir(exportDir)),
	}
}

func (disk *Disk) metaFilename(versionID string) string {
	if versionID == "" {
		return "meta.json"
	}

	return "meta.json." + versionID
}

func (disk *Disk) tempDir(requestID string) string {
	return path.Join(disk.tempBaseDir, requestID)
}

func (disk *Disk) transDir(requestID string) string {
	return path.Join(disk.transBaseDir, requestID)
}

func (disk *Disk) bucketDir(bucketName string) string {
	return path.Join(disk.bucketsDir, bucketName)
}

func (disk *Disk) objectsDir(bucketName string) string {
	return path.Join(disk.bucketsDir, bucketName, "objects")
}

func (disk *Disk) objectDir(bucketName, objectName string) string {
	return path.Join(disk.bucketDir(bucketName), "objects", objectName)
}

func (disk *Disk) objectMetaFile(bucketName, objectName, versionID string) string {
	return path.Join(disk.objectDir(bucketName, objectName), disk.metaFilename(versionID))
}

func (disk *Disk) dataIndexDir(ID string) string {
	return path.Join(disk.dataStoreDir, ID[:4])
}

func (disk *Disk) dataDir(ID string) string {
	return path.Join(disk.dataIndexDir(ID), ID)
}
