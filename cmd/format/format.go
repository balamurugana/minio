package format

import (
	"fmt"
)

const (
	// fsType - FS format type
	fsType = "fs"

	// xlType - XL format type
	xlType = "xl"

	// V1 - version 1
	V1 = "1"

	// V2 - version 2
	V2 = "2"

	// V3 - version 3
	V3 = "3"
)

type formatV1 struct {
	Version string `json:"version"` // Format version "1".
	Format  string `json:"format"`  // Format type "fs" or "xl".
}

func (f formatV1) validate(formatType string) error {
	if f.Version != V1 {
		return fmt.Errorf("unknown format version %v", f.Version)
	}

	if f.Format != formatType {
		return fmt.Errorf("%v found for %v format", f.Format, formatType)
	}

	return nil
}

func (f formatV1) validateFS() error {
	return f.validate(fsType)
}

func (f formatV1) validateXL() error {
	return f.validate(xlType)
}
