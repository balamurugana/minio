package format

import (
	"encoding/json"
	"fmt"

	"github.com/skyrings/skyring-common/tools/uuid"
)

// xlv1 - XL version 1.
type xlv1 struct {
	Version string `json:"version"` // XL version "1".
	Disk    string `json:"disk"`    // This disk UUID.
	JBOD    JBOD   `json:"jbod"`    // List of disk UUID.
}

func (xl xlv1) Validate() error {
	if xl.Version != V1 {
		return fmt.Errorf("unknown XL version %v", xl.Version)
	}

	if _, err := uuid.Parse(xl.Disk); err != nil {
		return fmt.Errorf("disk %v: %v", xl.Disk, err)
	}

	return xl.JBOD.Validate()
}

// XLV1 - Format XL version 1.
type XLV1 struct {
	formatV1
	XL xlv1 `json:"xl"`
}

func (f XLV1) Validate() error {
	if err := f.validateXL(); err != nil {
		return err
	}

	return f.XL.Validate()
}

func (f XLV1) MarshalJSON() ([]byte, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	type subXLV1 XLV1
	return json.Marshal(subXLV1(f))
}
