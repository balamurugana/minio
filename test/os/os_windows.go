// +build windows

package os

import (
	"syscall"
	"unsafe"
)

var (
	shlwapi         = syscall.NewLazyDLL("Shlwapi.dll")
	pathFileExistsA = shlwapi.NewProc("PathFileExistsA")
)

func Exist(name string) bool {
	r1, _, _ := pathFileExistsA.Call(uintptr(unsafe.Pointer(name)))
	return r1 == 1
}
