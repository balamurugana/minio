package os

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

// Available functions:
// func Exist(name string) bool

func CopyFile(srcFilename, destFilename string) (err error) {
	var srcFile *os.File
	var destFile *os.File
	destFileCreated := false

	defer func() {
		if srcFile != nil {
			srcFile.Close()
		}

		if destFile != nil {
			destFile.Close()
		}

		if err != nil && destFileCreated {
			os.Remove(destFilename)
		}
	}()

	if srcFile, err = os.Open(srcFilename); err != nil {
		return err
	}

	fi, err := srcFile.Stat()
	if err != nil {
		return err
	}

	if !fi.Mode().IsRegular() {
		return fmt.Errorf("source %v is not regular file", srcFilename)
	}

	if destFile, err = os.OpenFile(destFilename, os.O_WRONLY|os.O_CREATE, os.ModePerm); err != nil {
		return err
	}
	destFileCreated = true

	_, err = io.Copy(destFile, srcFile)
	return err
}

func MksubdirAll(base, name string) error {
	for _, subDir := range strings.Split(name, "/") {
		if subDir == "" {
			continue
		}

		base = path.Join(base, subDir)
		if err := os.Mkdir(base, os.ModePerm); err != nil && !os.IsExist(err) {
			return err
		}
	}

	return nil
}

func MkdirAllX(name string, perm os.FileMode) (parent string, err error) {
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

		parent = path.Dir(parent)
	}

	if parent != name {
		err = os.MkdirAll(name, perm)
	}

	return parent, err
}

func RemoveAllX(name, root string) (parent string, err error) {
	if err = os.Remove(name); err != nil {
		return name, err
	}

	for {
		if parent = path.Dir(name); parent == root {
			break
		}

		if err = os.Remove(parent); err != nil {
			break
		}

		parent = path.Dir(parent)
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
