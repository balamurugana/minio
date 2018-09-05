package datastore

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"

	xhash "github.com/balamurugana/minio/test/hash"
)

const ChecksumFileExt = ".checksum"

func checksumFile(name string) string {
	return name + ChecksumFileExt
}

type ChecksumHeader struct {
	HashName   string `json:"HashName"`
	HashKey    string `json:"haskKey"`
	HashLength int    `json:"hashLength"`
	BlockSize  int    `json:"blockSize"`
	BlockCount int    `json:"blockCount"`
	DataLength int64  `json:"dataLength"`
}

type ChecksumFile struct {
	file         *os.File
	header       *ChecksumHeader
	hasher       xhash.Hash
	headerLength int
	buf          []byte
}

func (csFile *ChecksumFile) readHeader() (int, error) {
	var data []byte
	for {
		buf := make([]byte, 1024)
		n, err := io.ReadFull(csFile.file, buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			return 0, err
		}

		data = append(data, buf[:n]...)
		if i := bytes.IndexRune(data, '\n'); i >= 0 {
			data = data[:i+1]
			break
		}
	}

	if _, err := csFile.file.Seek(int64(len(data)), os.SEEK_SET); err != nil {
		return 0, err
	}

	var header ChecksumHeader
	if err := json.Unmarshal(data, &header); err != nil {
		return 0, err
	}

	key, err := hex.DecodeString(header.HashKey)
	if err != nil {
		return 0, err
	}

	hasher, err := xhash.NewHashByName(header.HashName, key)
	if err != nil {
		return 0, err
	}

	csFile.hasher = hasher
	csFile.header = &header

	return len(data), nil
}

func (csFile *ChecksumFile) SectionValues(offset, length int64) (startBlock, bytesToSkip, requiredBlocks, blockSize, blockCount int, err error) {
	if offset+length > csFile.header.DataLength {
		err = errors.New("requested length from offset is beyond file size")
		return
	}

	startBlock = int(offset / int64(csFile.header.BlockSize))
	if int64(startBlock*csFile.header.BlockSize) < offset {
		bytesToSkip = int(offset - int64(startBlock*csFile.header.BlockSize))
	}

	requiredBlocks = int(length / int64(csFile.header.BlockSize))
	if int64(requiredBlocks*csFile.header.BlockSize) < length {
		requiredBlocks++
	}

	return startBlock, bytesToSkip, requiredBlocks, csFile.header.BlockSize, csFile.header.BlockCount, nil
}

func (csFile *ChecksumFile) Read() (string, error) {
	if csFile.buf == nil {
		csFile.buf = make([]byte, csFile.header.HashLength+1)
	}

	_, err := io.ReadFull(csFile.file, csFile.buf)
	if err != nil {
		return "", err
	}

	return string(csFile.buf[:csFile.header.HashLength]), nil
}

func (csFile *ChecksumFile) Write(checksum string) error {
	if len(checksum) != csFile.header.HashLength {
		return errors.New("invalid checksum")
	}

	_, err := csFile.file.WriteString(checksum + "\n")
	return err
}

func (csFile *ChecksumFile) WriteSum(data []byte) error {
	_, err := csFile.file.WriteString(csFile.hasher.Sum(data) + "\n")
	return err
}

func (csFile *ChecksumFile) Checksum(data []byte) string {
	return csFile.hasher.Sum(data)
}

func (csFile *ChecksumFile) Seek(checksumIndex int) error {
	pos := int64((checksumIndex * (csFile.header.HashLength + 1)) + csFile.headerLength)
	_, err := csFile.file.Seek(pos, os.SEEK_SET)
	return err
}

func (csFile *ChecksumFile) Close() error {
	return csFile.file.Close()
}

func ChecksumFileWriter(filename string, hasher xhash.Hash, blockSize, blockCount int, dataLength int64) (*ChecksumFile, error) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			file.Close()
			os.Remove(filename)
		}
	}()

	header := ChecksumHeader{
		HashName:   hasher.Name(),
		HashKey:    hasher.HashKey(),
		HashLength: hasher.HashLength(),
		BlockSize:  blockSize,
		BlockCount: blockCount,
		DataLength: dataLength,
	}

	if err = json.NewEncoder(file).Encode(&header); err != nil {
		return nil, err
	}

	return &ChecksumFile{
		file:   file,
		header: &header,
		hasher: hasher,
	}, nil
}

func ChecksumFileReader(filename string) (*ChecksumFile, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			file.Close()
		}
	}()

	checksumFile := &ChecksumFile{file: file}
	headerLength, err := checksumFile.readHeader()
	if err != nil {
		return nil, err
	}

	checksumFile.headerLength = headerLength
	return checksumFile, nil
}
