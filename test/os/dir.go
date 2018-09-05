package os

import "os"

type Picker func(name string, mode os.FileMode) (stop bool)

// Available functions:
// func Readdirnames(name string, picker Picker) error
