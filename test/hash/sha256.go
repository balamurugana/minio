package hash

import (
	"crypto/sha256"
	"encoding/hex"
)

type SHA256 struct {
}

func NewSHA256() *SHA256 {
	return &SHA256{}
}

func (hasher *SHA256) Name() string {
	return SHA256Algorithm
}

func (hasher *SHA256) HashKey() string {
	return ""
}

func (hasher *SHA256) HashLength() int {
	return 64
}

func (hasher *SHA256) Sum(data []byte) string {
	checksum := sha256.Sum256(data)
	return hex.EncodeToString(checksum[:])
}
