package storage

import (
	"io"
	"minio2/pkg/mio"
	"time"
)

type Folder interface {
	// Folder operations.
	CreateFolder(name string, metadata io.Reader) error
	DeleteFolder(name string) error
	GetFolderInfo(name string) (*FolderInfo, error)
	ListFolders() ([]FolderInfo, error)
	GetFolderMetaData(name string) (map[string]string, error)
	UpdateFolderMetaData(name string, metadata map[string]string) error
}

type File interface {
	// File operations.
	Create(name string, dir string, filenames string, metadata io.Reader) error
	Store(name string, metadata io.Reader) (mio.WriteAbortCloser, error)
	Get(name string, offset int64, length int64) (io.ReadCloser, error)
	List() ([]FileInfo, error)
	GetMetaData(name string) (map[string]string, error)
	UpdateMetaData(name string, metadata map[string]string) error
}

type Storage interface {
	Folder
	File
}

type FolderInfo struct {
	Name      string
	BirthTime time.Time
}

type FileInfo struct {
	Name    string
	ModTime time.Time
	Size    int64
}
