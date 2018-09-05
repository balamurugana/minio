package hash

import (
	"encoding/hex"

	"github.com/minio/highwayhash"
)

var defaultHighwayHashKey = []byte{
	1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 13, 14, 15, 16,
	17, 18, 19, 20, 21, 22, 23, 24,
	25, 26, 27, 28, 29, 30, 31, 32,
}

type HighwayHash256 struct {
	key []byte
}

func NewHighwayHash256(key []byte) (*HighwayHash256, error) {
	if key == nil {
		key = defaultHighwayHashKey
	} else if _, err := highwayhash.New(key); err != nil {
		return nil, err
	}

	return &HighwayHash256{key}, nil
}

func (hasher *HighwayHash256) Name() string {
	return HighwayHash256Algorithm
}

func (hasher *HighwayHash256) HashKey() string {
	return hex.EncodeToString(hasher.key)
}

func (hasher *HighwayHash256) HashLength() int {
	return 64
}

func (hasher *HighwayHash256) Sum(data []byte) string {
	checksum := highwayhash.Sum(data, hasher.key)
	return hex.EncodeToString(checksum[:])
}
