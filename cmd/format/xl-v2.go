package format

import (
	"encoding/json"
	"fmt"

	"github.com/skyrings/skyring-common/tools/uuid"
)

const distributionAlgo = "CRCMOD"

// xlv2 - XL version 2.
type xlv2 struct {
	Version          string  `json:"version"`          // XL version "2".
	This             string  `json:"this"`             // This disk UUID.
	Sets             JBODSet `json:"sets"`             // Set of disk UUID.
	DistributionAlgo string  `json:"distributionAlgo"` // Algorithm "CRCMOD".
}

func (xl xlv2) Validate() error {
	if xl.Version != V2 {
		return fmt.Errorf("unknown XL version %v", xl.Version)
	}

	if _, err := uuid.Parse(xl.This); err != nil {
		return fmt.Errorf("This disk %v: %v", xl.This, err)
	}

	if err := xl.Sets.Validate(); err != nil {
		return err
	}

	if xl.Sets.Index(xl.This) == -1 {
		return fmt.Errorf("This disk %v not found in sets", xl.This)
	}

	if xl.DistributionAlgo != distributionAlgo {
		return fmt.Errorf("unknown DistributionAlgo value %v", xl.DistributionAlgo)
	}

	return nil
}

// XLV2 - Format XL version 2.
type XLV2 struct {
	formatV1
	XL xlv2 `json:"xl"`
}

func (f XLV2) Validate() error {
	if err := f.validateXL(); err != nil {
		return err
	}

	return f.XL.Validate()
}

func (f XLV2) MarshalJSON() ([]byte, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	type subXLV2 XLV2
	return json.Marshal(subXLV2(f))
}
