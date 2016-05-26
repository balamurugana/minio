package storage

import (
	"errors"
	"io"
	"minio2/pkg/mio"

	"github.com/klauspost/reedsolomon"
)

const DefaultStripeSize = 256 * 1024

type ErasureDisk struct {
	diskList    []*Disk
	stripeCount int
	parityCount int
	stripeSize  int
	reedSolomon reedsolomon.Encoder
}

func NewErasureDisk(parityCount, stripeSize int, roots ...string) (*ErasureDisk, error) {
	diskList := make([]*Disk, len(roots))

	var errs []error
	for i := range roots {
		if disk, err := NewDisk(roots[i]); err != nil {
			errs = append(errs, err)
		} else {
			diskList[i] = disk
		}
	}

	if len(errs) >= parityCount {
		return nil, errors.New("Error occured beyond acceptable range")
	}

	stripeCount := len(roots) - parityCount
	if encoder, err := reedsolomon.New(stripeCount, parityCount); err == nil {
		return &ErasureDisk{diskList, stripeCount, parityCount, stripeSize, encoder}, nil
	} else {
		return nil, err
	}
}

func (ed *ErasureDisk) CreateFolder(name string, metadata io.Reader) error {
	var errs []error
	for i := range ed.diskList {
		if err := ed.diskList[i].CreateFolder(name, metadata); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) >= ed.parityCount {
		return errors.New("Error occured beyond acceptable range")
	}

	return nil
}

func (ed *ErasureDisk) DeleteFolder(name string) error {
	var errs []error
	for i := range ed.diskList {
		if err := ed.diskList[i].DeleteFolder(name); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) >= ed.parityCount {
		return errors.New("Error occured beyond acceptable range")
	}

	return nil
}

func (ed *ErasureDisk) GetFolderInfo(name string) (*FolderInfo, error) {
	var folderInfos []*FolderInfo
	var errs []error
	for i := range ed.diskList {
		if folderInfo, err := ed.diskList[i].GetFolderInfo(name); err != nil {
			errs = append(errs, err)
		} else {
			folderInfos = append(folderInfos, folderInfo)
		}
	}

	if len(errs) >= ed.parityCount {
		return nil, errors.New("Error occured beyond acceptable range")
	}

	return folderInfos[0], nil
}

func (ed *ErasureDisk) ListFolders() ([]FolderInfo, error) {
	var folderInfos [][]FolderInfo
	var errs []error
	for i := range ed.diskList {
		if folderInfo, err := ed.diskList[i].ListFolders(); err != nil {
			errs = append(errs, err)
		} else {
			folderInfos = append(folderInfos, folderInfo)
		}
	}

	if len(errs) >= ed.parityCount {
		return nil, errors.New("Error occured beyond acceptable range")
	}

	return folderInfos[0], nil
}

func (ed *ErasureDisk) GetFolderMetaData(name string) (map[string]string, error) {
	var metadataList []map[string]string
	var errs []error
	for i := range ed.diskList {
		if metadata, err := ed.diskList[i].GetFolderMetaData(name); err != nil {
			errs = append(errs, err)
		} else {
			metadataList = append(metadataList, metadata)
		}
	}

	if len(errs) >= ed.parityCount {
		return nil, errors.New("Error occured beyond acceptable range")
	}

	return metadataList[0], nil
}

func (ed *ErasureDisk) UpdateFolderMetaData(name string, metadata map[string]string) error {
	var errs []error
	for i := range ed.diskList {
		if err := ed.diskList[i].UpdateFolderMetaData(name, metadata); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) >= ed.parityCount {
		return errors.New("Error occured beyond acceptable range")
	}

	return nil
}

func (ed *ErasureDisk) Create(name string, dir string, filenames string, metadata io.Reader) error {
	return errors.New("Not implemented")
}

func (ed *ErasureDisk) Store(name string, metadata io.Reader) (wac mio.WriteAbortCloser, err error) {
	writers := make([]mio.WriteAbortCloser, len(ed.diskList))
	var errs []error
	defer func() {
		if len(errs) >= ed.parityCount {
			for i := range ed.diskList {
				if writers[i] != nil {
					writers[i].Abort()
				}
			}
		}
	}()

	for i := range ed.diskList {
		if writers[i], err = ed.diskList[i].Store(name, metadata); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) >= ed.parityCount {
		return nil, errors.New("Error occured beyond acceptable range")
	}

	wac = mio.NewErasureWriter(ed.reedSolomon, ed.stripeSize, ed.parityCount, writers)
	return
}

func (ed *ErasureDisk) Get(name string, offset int64, length int64) (rc io.ReadCloser, err error) {
	readers := make([]io.ReadCloser, len(ed.diskList))
	var errs []error
	defer func() {
		if len(errs) >= ed.parityCount {
			for i := range ed.diskList {
				if readers[i] != nil {
					readers[i].Close()
				}
			}
		}
	}()

	blockSize := ed.stripeSize * ed.stripeCount

	var stripeOffset, stripeLength int64
	var bytesToSkip int
	if offset > 0 {
		blockToSkip := offset / int64(blockSize)
		bytesToSkip = int(offset % int64(blockSize))
		stripeOffset = blockToSkip * int64(ed.stripeSize)
	}

	if length > 0 {
		blockToEnd := length / int64(blockSize)
		if length%int64(blockSize) != 0 {
			blockToEnd++
		}
		stripeLength = blockToEnd * int64(ed.stripeSize)
	}

	for i := range ed.diskList {
		if readers[i], err = ed.diskList[i].Get(name, stripeOffset, stripeLength); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) >= ed.parityCount {
		return nil, errors.New("Error occured beyond acceptable range")
	}

	rc = mio.NewErasureReader(ed.reedSolomon, ed.stripeSize, ed.parityCount, readers, bytesToSkip)
	if length > 0 {
		rc = mio.LimitReadCloser(rc, length)
	}

	return
}

func (ed *ErasureDisk) List() ([]FileInfo, error) {
	return nil, errors.New("Not implemented")
}

func (ed *ErasureDisk) GetMetaData(name string) (map[string]string, error) {
	return nil, errors.New("Not implemented")
}

func (ed *ErasureDisk) UpdateMetaData(name string, metadata map[string]string) error {
	return errors.New("Not implemented")
}
