// +build openbsd netbsd freebsd dragonfly

package os

import "syscall"

func absent(dirent *syscall.Dirent) bool {
	return dirent.Fileno == 0
}
