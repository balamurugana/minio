// +build darwin linux

package os

import "syscall"

func absent(dirent *syscall.Dirent) bool {
	return dirent.Ino == 0
}
