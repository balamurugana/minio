package os

import (
	"io"
	"os"
	"path/filepath"
)

func MkdirAll(name string, perm os.FileMode) (parent string, err error) {
	parent = name
	for {
		if err = os.Mkdir(parent, perm); err == nil {
			break
		}

		if os.IsExist(err) {
			err = nil
			break
		}

		if !os.IsNotExist(err) {
			return "", err
		}

		parent = filepath.Dir(parent)
	}

	if parent != name {
		err = os.MkdirAll(name, perm)
	}

	return parent, err
}

func RemoveAll(name, root string) (parent string, err error) {
	if err = os.RemoveAll(name); err != nil {
		return name, err
	}

	for {
		if parent = filepath.Dir(name); parent == root {
			break
		}

		if err = os.Remove(parent); err != nil {
			break
		}

		parent = filepath.Dir(parent)
	}

	if !os.IsNotExist(err) {
		return parent, err
	}

	return "", nil
}

type SectionFileReader struct {
	file   *os.File
	reader *io.SectionReader
}

func (sf *SectionFileReader) Read(p []byte) (int, error) {
	return sf.reader.Read(p)
}

func (sf *SectionFileReader) Close() error {
	return sf.file.Close()
}

func NewSectionFileReader(file *os.File, offset, length int64) *SectionFileReader {
	return &SectionFileReader{
		file:   file,
		reader: io.NewSectionReader(file, offset, length),
	}
}
