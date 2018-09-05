// +build plan9

package os

import "os"

func Readdirnames(name string, picker Picker) error {
	d, err := os.Open(name)
	if err != nil {
		return err
	}
	defer d.Close()

	names, err := d.Readdirnames(0)
	for _, filename := range names {
		if picker(filename, os.ModeType) {
			break
		}
	}

	return err
}
