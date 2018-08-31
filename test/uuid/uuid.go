package uuid

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

// UUID is 128bits = 16bytes
type UUID [16]byte

func (uuid UUID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
}

func (uuid UUID) TrimmedString() string {
	return fmt.Sprintf("%x%x%x%x%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
}

func (uuid UUID) IsZero() bool {
	var zeroUUID UUID
	return Equal(zeroUUID, uuid)
}

func (uuid UUID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + uuid.String() + `"`), nil
}

func (uuid *UUID) UnmarshalJSON(b []byte) error {
	u, err := Parse(string(b))
	if err != nil {
		return err
	}

	copy(uuid[:], u[:])
	return nil
}

func New() *UUID {
	uuid := new(UUID)

	n, err := io.ReadFull(rand.Reader, uuid[:])
	if err != nil {
		panic(err)
	}
	if n != len(uuid) {
		panic(fmt.Errorf("insufficient random data (expected: %d, read: %d)", len(uuid), n))
	}

	// variant bits; for more info
	// see https://www.ietf.org/rfc/rfc4122.txt section 4.1.1
	uuid[8] = uuid[8]&0x3f | 0x80
	// version 4 (pseudo-random); for more info
	// see https://www.ietf.org/rfc/rfc4122.txt section 4.1.3
	uuid[6] = uuid[6]&0x0f | 0x40

	return uuid
}

func NewString() string {
	return New().TrimmedString()
}

func Equal(uuid1 UUID, uuid2 UUID) bool {
	for i, v := range uuid1 {
		if v != uuid2[i] {
			return false
		}
	}

	return true
}

func Parse(s string) (*UUID, error) {
	// the string format should be either in
	// xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (or)
	// xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

	// If the uuid is marshaled by us we add " " around the uuid.
	// while parsing this, we have to remove the " " around the
	// uuid. So we check if uuid has " " around it, if yes we remove
	// it.

	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		s = s[1 : len(s)-1]
	}

	uuid := new(UUID)
	if len(s) == 36 {
		if ba, err := hex.DecodeString(s[0:8]); err == nil {
			copy(uuid[:4], ba)
		} else {
			return nil, err
		}
		if ba, err := hex.DecodeString(s[9:13]); err == nil {
			copy(uuid[4:], ba)
		} else {
			return nil, err
		}
		if ba, err := hex.DecodeString(s[14:18]); err == nil {
			copy(uuid[6:], ba)
		} else {
			return nil, err
		}
		if ba, err := hex.DecodeString(s[19:23]); err == nil {
			copy(uuid[8:], ba)
		} else {
			return nil, err
		}
		if ba, err := hex.DecodeString(s[24:]); err == nil {
			copy(uuid[10:], ba)
		} else {
			return nil, err
		}
	} else if len(s) == 32 {
		if ba, err := hex.DecodeString(s); err == nil {
			copy(uuid[:], ba)
		} else {
			return nil, err
		}
	} else {
		return nil, errors.New("unknown UUID string " + s)
	}

	return uuid, nil
}
