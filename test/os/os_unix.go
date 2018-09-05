// +build darwin dragonfly freebsd linux nacl netbsd openbsd solaris

package os

import "syscall"

func Exist(name string) bool {
	return syscall.Access(name, syscall.F_OK) == nil
}
