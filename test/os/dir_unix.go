// +build darwin dragonfly freebsd linux nacl netbsd openbsd solaris

package os

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

const blockSize = 8192

func parseDirents(buf []byte, picker Picker) (stop bool) {
	n := len(buf)
	var dirent *syscall.Dirent
	for i := 0; i < n; i += int(dirent.Reclen) {
		dirent = (*syscall.Dirent)(unsafe.Pointer(&buf[i]))

		// Zero Reclen is valid in Linux; EOF for others.
		if dirent.Reclen == 0 && runtime.GOOS != "linux" {
			return true
		}

		if absent(dirent) {
			continue
		}

		nameBytes := (*[blockSize]byte)(unsafe.Pointer(&dirent.Name[0]))
		nameLen := n - i // Guessed max length to avoid buffer overrun.
		for j := 0; j < nameLen; j++ {
			if nameBytes[j] == 0 { // Null termination from c-world.
				nameLen = j
				break
			}
		}
		name := string(nameBytes[0:nameLen])
		if name == "." || name == ".." {
			continue
		}

		mode := os.ModeType
		switch dirent.Type {
		case syscall.DT_BLK:
			mode = os.ModeDevice
		case syscall.DT_CHR:
			mode = os.ModeCharDevice
		case syscall.DT_DIR:
			mode = os.ModeDir
		case syscall.DT_FIFO:
			mode = os.ModeNamedPipe
		case syscall.DT_LNK:
			mode = os.ModeSymlink
		case syscall.DT_REG:
			mode = os.FileMode(0)
		case syscall.DT_SOCK:
			mode = os.ModeSocket
		}

		if picker(name, mode) {
			return true
		}
	}

	return false
}

func Readdirnames(name string, picker Picker) error {
	buf := make([]byte, blockSize)

	d, err := os.Open(name)
	if err != nil {
		return err
	}
	defer d.Close()

	fd := int(d.Fd())

	for {
		n, err := syscall.ReadDirent(fd, buf)
		if err != nil {
			return err
		}
		if n <= 0 {
			break
		}

		if parseDirents(buf[:n], picker) {
			break
		}
	}

	return nil
}
