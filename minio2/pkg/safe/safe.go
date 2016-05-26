package safe

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type SafeFile struct {
	name    string
	tmpfile *os.File
	closed  bool
	aborted bool
}

func (file *SafeFile) SetName(newname string) {
	file.name = newname
}

func (file *SafeFile) Write(p []byte) (n int, err error) {
	defer func() {
		if err != nil {
			if rerr := os.Remove(file.tmpfile.Name()); rerr != nil {
				log.Fatal(rerr)
			}
		}
	}()

	n, err = file.tmpfile.Write(p)
	return
}

func (file *SafeFile) Close() (err error) {
	defer func() {
		if err != nil {
			if rerr := os.Remove(file.tmpfile.Name()); rerr != nil {
				log.Fatal(rerr)
			}
		}
	}()

	if file.aborted || file.closed {
		return
	}

	if err = file.tmpfile.Close(); err != nil {
		return
	}

	err = os.Rename(file.tmpfile.Name(), file.name)

	file.closed = true
	return
}

func (file *SafeFile) Abort() (err error) {
	if file.aborted || file.closed {
		return
	}

	err = file.tmpfile.Close()
	rerr := os.Remove(file.tmpfile.Name())

	if err != nil {
		if rerr != nil {
			log.Fatal(rerr)
		}
		return
	}

	err = rerr
	if err == nil {
		file.aborted = true
	}

	return
}

func CreateFile(name string) (*SafeFile, error) {
	dname, fname := filepath.Split(name)
	if len(dname) == 0 {
		dname = "."
	}

	tmpfile, err := ioutil.TempFile(dname, fname+".safe.")
	if err != nil {
		return nil, err
	}

	return &SafeFile{name: name, tmpfile: tmpfile}, nil
}
