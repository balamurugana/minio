package hash

import "errors"

type Hash interface {
	Name() string
	HashKey() string
	HashLength() int
	Sum([]byte) string
}

const (
	HighwayHash256Algorithm = "HighwayHash256"
	SHA256Algorithm         = "SHA256"
)

var ErrUnknownAlgorithm = errors.New("unknown algorithm")

func NewHashByName(name string, key []byte) (Hash, error) {
	switch name {
	case HighwayHash256Algorithm:
		return NewHighwayHash256(key)
	case SHA256Algorithm:
		return NewSHA256(), nil
	}

	return nil, ErrUnknownAlgorithm
}
