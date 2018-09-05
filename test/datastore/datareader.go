package datastore

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

const defaultIdleTimeout = time.Duration(15 * time.Minute)

var errClosedDataReader = errors.New("closed data reader")

type Metadata struct {
	DataFiles     []string
	DataFileSizes []int64
}

func (meta Metadata) SectionValues(offset, length int64) (startPartIndex, endPartIndex int, startPartOffset, endPartLength int64) {
	offsetFound := false

	size := int64(0)
	for i := range meta.DataFileSizes {
		if !offsetFound {
			size += meta.DataFileSizes[i]
			if size < offset {
				continue
			}

			offsetFound = true
			startPartIndex = i
			startPartOffset = offset - (size - meta.DataFileSizes[i])
			size = meta.DataFileSizes[i] - startPartOffset
		} else {
			size = meta.DataFileSizes[i]
		}

		if size >= length {
			endPartIndex = i
			endPartLength = length
			return
		}

		length -= size
	}

	return -1, -1, -1, -1
}

type DataReader struct {
	id string
	ds *DataStore

	metadata        *Metadata
	startPartIndex  int
	endPartIndex    int
	startPartOffset int64
	endPartLength   int64

	dataFile   *dataFileReader
	dataLength int64
	bytesRead  int64
	index      int

	closeMutex sync.Mutex
	idleTimer  *time.Timer
	isClosed   bool
}

func NewDataReader(ID string, offset, length int64, ds *DataStore) (reader *DataReader, err error) {
	metaFile, err := os.Open(ds.metadataFile(ID))
	if err != nil {
		return nil, err
	}

	var metadata Metadata
	err = json.NewDecoder(metaFile).Decode(&metadata)
	if err != nil {
		metaFile.Close()
		return nil, err
	}
	metaFile.Close()

	startPartIndex, endPartIndex, startPartOffset, endPartLength := metadata.SectionValues(offset, length)
	if startPartIndex < 0 {
		return nil, errors.New("invalid offset/length")
	}

	dr := &DataReader{
		metadata:        &metadata,
		startPartIndex:  startPartIndex,
		endPartIndex:    endPartIndex,
		startPartOffset: startPartOffset,
		endPartLength:   endPartLength,
		id:              ID,
		ds:              ds,
	}
	dr.idleTimer = time.AfterFunc(defaultIdleTimeout, func() { dr.Close() })

	ds.incrementUsage(ID, dr)

	return dr, nil
}

func (dr *DataReader) Read(p []byte) (n int, err error) {
	if dr.index > dr.endPartIndex {
		return 0, io.EOF
	}

	defer func() {
		if err != nil {
			dr.Close()
		}
	}()

	dr.idleTimer.Reset(defaultIdleTimeout)

	if dr.dataFile != nil {
		dataFile := dr.ds.dataFile(dr.id, dr.metadata.DataFiles[dr.index])

		dataOffset := int64(0)
		if dr.index == dr.startPartIndex {
			dataOffset = dr.startPartOffset
		}

		dr.dataLength = dr.metadata.DataFileSizes[dr.index]
		if dr.index == dr.endPartIndex {
			dr.dataLength = dr.endPartLength
		}

		if dr.dataFile, err = newDataFileReader(dataFile, dataOffset, dr.dataLength); err != nil {
			return 0, err
		}
	}

	n, err = dr.dataFile.Read(p)
	dr.bytesRead += int64(n)
	if dr.bytesRead == dr.dataLength {
		dr.dataFile.Close()
		dr.index++
		dr.dataFile = nil
	}

	return n, err
}

func (dr *DataReader) Close() (err error) {
	dr.closeMutex.Lock()
	defer dr.closeMutex.Unlock()

	if dr.isClosed {
		return errClosedDataReader
	}

	dr.idleTimer.Stop()
	err = dr.dataFile.Close()
	dr.isClosed = true
	dr.ds.decrementUsage(dr.id, dr)

	return err
}
