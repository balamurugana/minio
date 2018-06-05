package format

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// fsv2 - FS version 2; version bump to indicate multipart backend namespace change to
// .minio.sys/multipart/SHA256(BUCKET/OBJECT)/UPLOADID/{fs.json, part.1, part.2, ...}
type fsv2 struct {
	Version string `json:"version"` // FS version "2".
}

func (fs fsv2) Validate() error {
	if fs.Version != V2 {
		return fmt.Errorf("unknown FS version %v", fs.Version)
	}

	return nil
}

// FSV2 - Format FS version 2.
type FSV2 struct {
	formatV1
	FS fsv2 `json:"fs"`
}

func (f FSV2) Validate() error {
	if err := f.validateFS(); err != nil {
		return err
	}

	return f.FS.Validate()
}

// Save - saves FS format into given filename.
func (f FSV2) Save(filename string) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filename, data, os.ModePerm)
}

// NewFSV2 - creates new format FS version 2.
func NewFSV2() *FSV2 {
	return &FSV2{
		formatV1: formatV1{
			Version: V1,
			Format:  fsType,
		},
		FS: fsv2{
			Version: V2,
		},
	}
}
