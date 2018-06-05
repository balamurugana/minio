package format

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
)

// ParseFS - loads FS format from given filename and returns FS, migration flag and error.
func ParseFS(reader io.Reader) (*FSV2, bool, error) {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, false, err
	}

	var f FSV2
	if err = json.Unmarshal(data, &f); err != nil {
		return nil, false, err
	}

	if err = f.validateFS(); err != nil {
		return nil, false, err
	}

	switch f.FS.Version {
	case V1:
		var f1 FSV1
		if err = json.Unmarshal(data, &f1); err != nil {
			return nil, false, err
		}

		if err = f1.Validate(); err != nil {
			return nil, false, err
		}

		return NewFSV2(), true, nil
	case V2:
		if err = f.Validate(); err != nil {
			return nil, false, err
		}

		return &f, false, nil
	}

	return nil, false, fmt.Errorf("unknown FS version %v", f.FS.Version)
}
