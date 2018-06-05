package format

import "fmt"

// fsv1 - FS version 1.
type fsv1 struct {
	Version string `json:"version"` // FS version "1".
}

func (fs fsv1) Validate() error {
	if fs.Version != V1 {
		return fmt.Errorf("unknown FS version %v", fs.Version)
	}

	return nil
}

// FSV1 - Format FS version 1.
type FSV1 struct {
	formatV1
	FS fsv1 `json:"fs"`
}

func (f FSV1) Validate() error {
	if err := f.validateFS(); err != nil {
		return err
	}

	return f.FS.Validate()
}
