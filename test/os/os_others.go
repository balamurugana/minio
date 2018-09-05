// +build plan9

package os

import "os"

func Exist(name string) bool {
	_, err := os.Lstat(name)
	return err == nil
}
