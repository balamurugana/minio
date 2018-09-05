package datastore

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path"
	"sync"
)

const (
	MetadataFileExt  = ".meta.json"
	metadataFilename = "data.json"
)

var (
	ErrDataInUse         = errors.New("data in use")
	ErrDataAlreadyExists = errors.New("data already exists")
	ErrDataNotFound      = errors.New("data not found")
)

type DataStore struct {
	sync.RWMutex

	storeDir string

	dataReaders   map[*DataReader]struct{}
	usageMapMutex sync.RWMutex
	usageMap      map[string]int

	dropMapMutex sync.Mutex
	dropMap      map[string]string
}

func New(storeDir string) *DataStore {
	return &DataStore{
		storeDir:    storeDir,
		dataReaders: make(map[*DataReader]struct{}),
		usageMap:    make(map[string]int),
		dropMap:     make(map[string]string),
	}
}

func (ds *DataStore) incrementUsage(ID string, dataReader *DataReader) {
	ds.usageMapMutex.Lock()
	defer ds.usageMapMutex.Unlock()

	ds.usageMap[ID] = ds.usageMap[ID] + 1
	ds.dataReaders[dataReader] = struct{}{}
}

func (ds *DataStore) decrementUsage(ID string, dataReader *DataReader) {
	ds.usageMapMutex.Lock()
	defer ds.usageMapMutex.Unlock()

	ds.usageMap[ID] = ds.usageMap[ID] - 1
	if ds.usageMap[ID] <= 0 {
		delete(ds.usageMap, ID)
		delete(ds.dataReaders, dataReader)

		ds.dropMapMutex.Lock()
		tmpDir, ok := ds.dropMap[ID]
		ds.dropMapMutex.Unlock()

		if ok {
			ds.Lock()
			err := os.Rename(ds.dataDir(ID), tmpDir)
			ds.Unlock()

			if err != nil {
				log.Printf("Unable to drop data %v. %v", ID, err)
			}
		}
	}
}

func (ds *DataStore) indexDir(ID string) string {
	return path.Join(ds.storeDir, ID[:4])
}

func (ds *DataStore) dataDir(ID string) string {
	return path.Join(ds.indexDir(ID), ID)
}

func (ds *DataStore) dataFile(ID, name string) string {
	return path.Join(ds.dataDir(ID), name)
}

func (ds *DataStore) metadataFile(ID string) string {
	return path.Join(ds.dataDir(ID), metadataFilename)
}

func (ds *DataStore) PutMetadata(ID, srcFilename string) (err error) {
	ds.usageMapMutex.RLock()
	defer ds.usageMapMutex.RUnlock()
	if _, ok := ds.usageMap[ID]; ok {
		return ErrDataInUse
	}

	file, err := os.Open(srcFilename)
	if err != nil {
		return err
	}

	var metadata Metadata
	if err = json.NewDecoder(file).Decode(&metadata); err != nil {
		file.Close()
		return err
	}
	file.Close()

	indexDirCreated := false
	dataDirCreated := false

	metadataFilename := path.Join(ds.dataDir(ID), "data.json")

	ds.Lock()
	defer ds.Unlock()

	defer func() {
		if err == nil {
			return
		}

		if dataDirCreated {
			os.Remove(ds.dataDir(ID))
		}

		if indexDirCreated {
			os.Remove(ds.indexDir(ID))
		}
	}()

	if err = os.Mkdir(ds.indexDir(ID), os.ModePerm); err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	indexDirCreated = (err == nil)

	if err = os.Mkdir(ds.dataDir(ID), os.ModePerm); err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	dataDirCreated = (err == nil)

	if err = os.Rename(srcFilename, metadataFilename); err != nil {
		if os.IsExist(err) {
			return ErrDataAlreadyExists
		}

		return err
	}

	return nil
}

func (ds *DataStore) Put(ID, name, srcFilename string, checksummed, metadata bool) (err error) {
	ds.usageMapMutex.RLock()
	defer ds.usageMapMutex.RUnlock()
	if _, ok := ds.usageMap[ID]; ok {
		return ErrDataInUse
	}

	indexDirCreated := false
	dataDirCreated := false
	checksumFileCreated := false
	metadataFileCreated := false

	srcChecksumFilename := srcFilename + ChecksumFileExt
	srcMetadataFilename := srcFilename + MetadataFileExt

	dataFilename := ds.dataFile(ID, name)
	checksumFilename := dataFilename + ChecksumFileExt
	metadataFilename := dataFilename + MetadataFileExt

	ds.Lock()
	defer ds.Unlock()

	defer func() {
		if err == nil {
			return
		}

		if checksumFileCreated {
			os.Remove(checksumFilename)
		}

		if metadataFileCreated {
			os.Remove(metadataFilename)
		}

		if dataDirCreated {
			os.Remove(ds.dataDir(ID))
		}

		if indexDirCreated {
			os.Remove(ds.indexDir(ID))
		}
	}()

	if err = os.Mkdir(ds.indexDir(ID), os.ModePerm); err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	indexDirCreated = (err == nil)

	if err = os.Mkdir(ds.dataDir(ID), os.ModePerm); err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	dataDirCreated = (err == nil)

	if err = os.Rename(srcChecksumFilename, checksumFilename); err != nil {
		if os.IsExist(err) {
			return ErrDataAlreadyExists
		}

		return err
	}
	checksumFileCreated = true

	if err = os.Rename(srcMetadataFilename, metadataFilename); err != nil {
		if os.IsExist(err) {
			return ErrDataAlreadyExists
		}

		return err
	}
	metadataFileCreated = true

	if err = os.Rename(srcFilename, dataFilename); err != nil {
		if os.IsExist(err) {
			return ErrDataAlreadyExists
		}

		return err
	}

	return nil
}

func (ds *DataStore) Get(ID string, offset, length int64) (io.ReadCloser, error) {
	ds.RLock()
	defer ds.RUnlock()

	dataReader, err := NewDataReader(ID, offset, length, ds)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrDataNotFound
		}

		return nil, err
	}

	return dataReader, nil
}

func (ds *DataStore) Drop(ID, tmpDir string) (err error) {
	dropIfUnusaged := func() error {
		ds.usageMapMutex.RLock()
		defer ds.usageMapMutex.RUnlock()
		if _, ok := ds.usageMap[ID]; !ok {
			ds.Lock()
			err = os.Rename(ds.dataDir(ID), tmpDir)
			ds.Unlock()
		}

		return err
	}

	if err = dropIfUnusaged(); err != nil {
		return err
	}

	ds.dropMapMutex.Lock()
	if _, ok := ds.dropMap[ID]; !ok {
		ds.dropMap[ID] = tmpDir
	}
	ds.dropMapMutex.Unlock()

	return nil
}

func (ds *DataStore) Restore(ID, tmpDir string) (err error) {
	err = ErrDataNotFound

	ds.dropMapMutex.Lock()
	if _, ok := ds.dropMap[ID]; ok {
		err = os.Rename(tmpDir, ds.dataDir(ID))
	}
	ds.dropMapMutex.Unlock()

	return err
}
