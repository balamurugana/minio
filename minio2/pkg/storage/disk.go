package storage

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"minio2/pkg/mio"
	"minio2/pkg/safe"
	"os"
	"path/filepath"
	"syscall"
)

type Disk struct {
	root string
}

func NewDisk(root string) (*Disk, error) {
	return &Disk{root}, nil
}

func (disk *Disk) CreateFolder(name string, metadata io.Reader) (err error) {
	folder := filepath.Join(disk.root, name)
	defer func() {
		if err != nil {
			if pathErr, ok := err.(*os.PathError); ok {
				if rerr := disk.remove(filepath.Dir(pathErr.Path)); rerr != nil {
					log.Println(rerr)
				}
			}
		}
	}()
	err = os.MkdirAll(folder, os.ModePerm)
	return
}

func (disk *Disk) remove(name string) (err error) {
	if name == disk.root {
		return
	}

	if err = os.Remove(name); err != nil {
		return
	}

	return disk.remove(filepath.Dir(name))
}

func (disk *Disk) DeleteFolder(name string) (err error) {
	folder := filepath.Join(disk.root, name)
	if err = os.Remove(folder); err != nil {
		return
	}

	if rerr := disk.remove(filepath.Dir(folder)); rerr != nil {
		log.Println(rerr)
	}

	return
}

func (disk *Disk) GetFolderInfo(name string) (*FolderInfo, error) {
	folder := filepath.Join(disk.root, name)
	info, err := os.Stat(folder)
	if err != nil {
		return nil, err
	}

	return &FolderInfo{Name: name, BirthTime: info.ModTime()}, nil
}

func (disk *Disk) ListFolders() ([]FolderInfo, error) {
	files, err := ioutil.ReadDir(disk.root)
	if err != nil {
		return nil, err
	}

	var folderInfos []FolderInfo
	for _, file := range files {
		if file.IsDir() {
			folderInfos = append(folderInfos, FolderInfo{Name: file.Name(), BirthTime: file.ModTime()})
		}
	}

	return folderInfos, nil
}

func (disk *Disk) GetFolderMetaData(name string) (map[string]string, error) {
	folder := filepath.Join(disk.root, name)
	info, err := os.Stat(folder)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return nil, &os.PathError{"statdir", folder, syscall.ENOTDIR}
	}

	metadata := make(map[string]string)
	metadata["modTime"] = info.ModTime().String()

	return metadata, nil
}

func (disk *Disk) UpdateFolderMetaData(name string, metadata map[string]string) error {
	folder := filepath.Join(disk.root, name)
	info, err := os.Stat(folder)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return &os.PathError{"statdir", folder, syscall.ENOTDIR}
	}

	return nil
}

func (disk *Disk) Create(name string, dir string, filenames string, metadata io.Reader) error {
	return errors.New("Not implemented")
}

func (disk *Disk) Store(name string, metadata io.Reader) (mio.WriteAbortCloser, error) {
	file := filepath.Join(disk.root, name)
	return safe.CreateFile(file)
}

func (disk *Disk) Get(name string, offset int64, length int64) (rc io.ReadCloser, err error) {
	file := filepath.Join(disk.root, name)

	var fh *os.File
	if fh, err = os.Open(file); err != nil {
		return
	}

	defer func() {
		if err != nil && fh != nil {
			if cerr := fh.Close(); cerr != nil {
				log.Println(cerr)
			}
		}
	}()

	if _, err = fh.Seek(offset, 1); err != nil {
		return
	}

	rc = fh
	if length > 0 {
		rc = mio.LimitReadCloser(fh, length)
	}

	return
}

func (disk *Disk) List() ([]FileInfo, error) {
	return nil, errors.New("Not implemented")
}

func (disk *Disk) GetMetaData(name string) (map[string]string, error) {
	return nil, errors.New("Not implemented")
}

func (disk *Disk) UpdateMetaData(name string, metadata map[string]string) error {
	return errors.New("Not implemented")
}
