package format

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"

	"github.com/skyrings/skyring-common/tools/uuid"
)

// xlv3 - XL version 3; version bump to indicate multipart backend namespace change to
// .minio.sys/multipart/SHA256(BUCKET/OBJECT)/UPLOADID/{xl.json, part.1, part.2, ...}
type xlv3 struct {
	Version          string  `json:"version"`          // XL version "3".
	This             string  `json:"this"`             // This disk UUID.
	Sets             JBODSet `json:"sets"`             // Set of disk UUID.
	DistributionAlgo string  `json:"distributionAlgo"` // Algorithm "CRCMOD".
}

func (xl xlv3) Validate() error {
	if xl.Version != V3 {
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

// XLV3 - Format XL version 3.
type XLV3 struct {
	formatV1
	XL xlv3 `json:"xl"`
}

func (f XLV3) Validate() error {
	if err := f.validateXL(); err != nil {
		return err
	}

	return f.XL.Validate()
}

func (f XLV3) MarshalJSON() ([]byte, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	type subXLV3 XLV3
	return json.Marshal(subXLV3(f))
}

// Index - returns index of This disk's index in Sets.
func (f XLV3) Index() int {
	return f.XL.Sets.Index(f.XL.This)
}

// IsEmpty - checks whether format is empty or not.
func (f XLV3) IsEmpty() bool {
	return reflect.DeepEqual(f, XLV3{})
}

// Match - checks whether two XL match each other.
func (f XLV3) Match(f2 XLV3) bool {
	if f.Version != f2.Version {
		return false
	}

	if f.Format != f2.Format {
		return false
	}

	if f.XL.Version != f.XL.Version {
		return false
	}

	// Two XL should not have same 'This'.
	if f.XL.This == f2.XL.This {
		return false
	}

	if !reflect.DeepEqual(f.XL.Sets, f2.XL.Sets) {
		return false
	}

	return f.XL.DistributionAlgo == f2.XL.DistributionAlgo
}

// Save - saves XL format into given filename.
func (f XLV3) Save(filename string) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filename, data, os.ModePerm)
}

// newXLV3 - creates new format XL version 3.
func newXLV3(this string, sets []JBOD) *XLV3 {
	return &XLV3{
		formatV1: formatV1{
			Version: V1,
			Format:  xlType,
		},
		XL: xlv3{
			Version:          V3,
			This:             this,
			Sets:             sets,
			DistributionAlgo: distributionAlgo,
		},
	}
}
