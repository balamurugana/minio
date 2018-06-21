package format

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/minio/minio-go/pkg/set"
	"github.com/skyrings/skyring-common/tools/uuid"
)

func mustGetNewUUID() string {
	uuid, err := uuid.New()
	if err != nil {
		panic(err)
	}

	return uuid.String()
}

type JBOD []string

func (jbod JBOD) Validate() error {
	if len(jbod) == 0 {
		return fmt.Errorf("empty JBOD")
	}

	size := len(jbod)
	if size%2 != 0 || size < 2 || size > 16 {
		return fmt.Errorf("unsupported JBOD size %v", size)
	}

	s := set.NewStringSet()
	for i := range jbod {
		if _, err := uuid.Parse(jbod[i]); err != nil {
			return fmt.Errorf("JBOD disk %v: %v", jbod[i], err)
		}

		if s.Contains(jbod[i]) {
			return fmt.Errorf("duplicate JBOD disk %v", jbod[i])
		}

		s.Add(jbod[i])
	}

	return nil
}

func (jbod JBOD) MarshalJSON() ([]byte, error) {
	if err := jbod.Validate(); err != nil {
		return nil, err
	}

	return json.Marshal([]string(jbod))
}

type JBODSet []JBOD

func (sets JBODSet) Count() int {
	return len(sets)
}

func (sets JBODSet) Size() int {
	return len(sets[0])
}

func (sets JBODSet) Index(disk string) int {
	for i := range sets {
		for j := range sets[i] {
			if disk == sets[i][j] {
				return i*len(sets[i]) + j
			}
		}
	}

	return -1
}

func (sets JBODSet) Validate() error {
	if len(sets) == 0 {
		return fmt.Errorf("empty JBOD set")
	}

	size := len(sets[0])
	if size%2 != 0 || size < 2 || size > 16 {
		return fmt.Errorf("unsupported JBOD set size %v", size)
	}

	s := set.NewStringSet()
	for i := range sets {
		if size != len(sets[i]) {
			return fmt.Errorf("JBOD[%v] size %v is not same as JBOD set size %v", i+1, len(sets[i]), size)
		}

		for j := range sets[i] {
			if _, err := uuid.Parse(sets[i][j]); err != nil {
				return fmt.Errorf("JBOD disk %v: %v", sets[i][j], err)
			}

			if s.Contains(sets[i][j]) {
				return fmt.Errorf("duplicate JBOD disk %v", sets[i][j])
			}

			s.Add(sets[i][j])
		}
	}

	return nil
}

func (sets JBODSet) MarshalJSON() ([]byte, error) {
	if err := sets.Validate(); err != nil {
		return nil, err
	}

	type subJBODSet JBODSet
	return json.Marshal(subJBODSet(sets))
}

func newJBODSet(setCount, setSize int) (JBODSet, error) {
	if setCount < 1 {
		return nil, fmt.Errorf("invalid set count %v", setCount)
	}

	if setSize%2 != 0 || setSize < 2 || setSize > 16 {
		return nil, fmt.Errorf("unsupported set size %v", setSize)
	}

	jbodSet := make([]JBOD, setCount)
	for i := 0; i < setCount; i++ {
		jbod := JBOD(make([]string, setSize))
		for j := 0; j < setSize; j++ {
			jbod[i] = mustGetNewUUID()
		}
		jbodSet[i] = jbod
	}

	return JBODSet(jbodSet), nil
}

// LoadXL - loads XL format from given filename and returns XL, migration flag and error.
func LoadXL(filename string) (*XLV3, bool, error) {
	var f XLV3

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return &f, false, nil
		}

		return nil, false, err
	}

	if err = json.Unmarshal(data, &f); err != nil {
		return nil, false, err
	}

	if err = f.validateXL(); err != nil {
		return nil, false, err
	}

	switch f.XL.Version {
	case V1:
		var f1 XLV1
		if err = json.Unmarshal(data, &f1); err != nil {
			return nil, false, err
		}

		if err = f1.Validate(); err != nil {
			return nil, false, err
		}

		return newXLV3(f1.XL.Disk, []JBOD{f1.XL.JBOD}), true, nil
	case V2:
		var f2 XLV2
		if err = json.Unmarshal(data, &f2); err != nil {
			return nil, false, err
		}

		if err = f2.Validate(); err != nil {
			return nil, false, err
		}

		return newXLV3(f2.XL.This, f2.XL.Sets), true, nil
	case V3:
		if err = f.Validate(); err != nil {
			return nil, false, err
		}

		return &f, false, nil
	}

	return nil, false, fmt.Errorf("unknown XL version %v", f.XL.Version)
}

func GenerateXL(setCount, setSize int) []*XLV3 {
	sets, err := newJBODSet(setCount, setSize)
	if err != nil {
		panic(err)
	}

	formats := make([]*XLV3, setCount*setSize)
	for i := 0; i < setCount; i++ {
		for j := 0; j < setSize; j++ {
			formats[i*setSize+j] = newXLV3(sets[i][j], sets)
		}
	}

	return formats
}

func GenerateXLs(sets JBODSet) []*XLV3 {
	if err := sets.Validate(); err != nil {
		panic(err)
	}

	setCount := sets.Count()
	setSize := sets.Size()

	formats := make([]*XLV3, setCount*setSize)
	for i := 0; i < setCount; i++ {
		for j := 0; j < setSize; j++ {
			formats[i*setSize+j] = newXLV3(sets[i][j], sets)
		}
	}

	return formats
}
