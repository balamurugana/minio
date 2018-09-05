package main

import (
	"sync"

	"github.com/balamurugana/minio/test/uuid"
)

type RefValue struct {
	BucketName string
	ObjectName string
	Counter    int
}

type DataReaderMap struct {
	sync.RWMutex
	idMap map[uuid.UUID]*RefValue
}

func NewDataReaderMap() *DataReaderMap {
	return new(DataReaderMap)
}

func (m *DataReaderMap) Put(id uuid.UUID, bucketName, objectName string) {
	m.Lock()
	defer m.Unlock()

	refValue, ok := m.idMap[id]
	if !ok {
		refValue = new(RefValue)
	}
	refValue.Counter++
	m.idMap[id] = refValue
}

func (m *DataReaderMap) Del(id uuid.UUID) {
	m.Lock()
	defer m.Unlock()

	refValue, ok := m.idMap[id]
	if !ok {
		return
	}

	refValue.Counter--
	if refValue.Counter != 0 {
		m.idMap[id] = refValue
		return
	}

	delete(m.idMap, id)
	go func() {
		// FIXME:
		// 0. Lock cluster.
		// 1. Check whether <refValue.BucketName>/<refValue.ObjectName>/id.<id>
		//    is available in namespace
		// 2. Unlock cluster.
		// 3. If not found, remove data by <id>
	}()
}
