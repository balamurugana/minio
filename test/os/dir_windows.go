// +build windows

package os

import (
	"os"
	"syscall"
)

func Readdirnames(name string, picker Picker) error {
	d, err := os.Open(name)
	if err != nil {
		return err
	}
	defer d.Close()

	handle := syscall.Handle(d.Fd())

	data := &syscall.Win32finddata{}
	for {
		err := syscall.FindNextFile(handle, data)
		if err != nil {
			if err != syscall.ERROR_NO_MORE_FILES {
				return &os.PathError{
					Op:   "FindNextFile",
					Path: name,
					Err:  err,
				}
			}

			break
		}

		filename := syscall.UTF16ToString(data.FileName[:])
		if filename == "." || filename == ".." {
			continue
		}

		mode := os.ModeType
		switch {
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0:
			mode = os.ModeDir
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0:
			mode = os.ModeSymlink
		}

		if picker(filename, mode) {
			break
		}
	}

	return nil
}
