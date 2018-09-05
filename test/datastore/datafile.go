package datastore

import (
	"fmt"
	"io"
	"math"
	"os"

	xhash "github.com/balamurugana/minio/test/hash"
)

const DefaultBlockSize = 1024 * 1024 // 1 MiB

func WriteDataFile(name string, reader io.Reader, length int64, hasher xhash.Hash, blockSize int) (err error) {
	lastReadSize := blockSize
	blockCount := math.MaxUint32 // Assumed value if length is -1
	if length > 0 {
		blockCount = int(length / int64(blockSize))
		if int64(blockCount*blockSize) < length {
			lastReadSize = int(length - int64(blockCount*blockSize))
			blockCount++
		}
	}

	checksumFilename := checksumFile(name)
	defer func() {
		if err != nil {
			os.Remove(name)
			os.Remove(checksumFilename)
		}
	}()

	dataFile, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer dataFile.Close()

	checksumFile, err := ChecksumFileWriter(checksumFilename, hasher, blockSize, blockCount, length)
	if err != nil {
		return err
	}
	defer checksumFile.Close()

	if length > 0 {
		reader = io.LimitReader(reader, length)
	}

	buf := make([]byte, blockSize)
	n := 0
	for i := 0; i < blockCount; i++ {
		if i == blockCount-1 {
			buf = buf[:lastReadSize]
		}

		if n, err = io.ReadFull(reader, buf); err != nil {
			if length >= 0 {
				return err
			}

			if err != io.ErrUnexpectedEOF {
				return err
			}

			blockCount = i + 1
			buf = buf[:n]
		}

		if _, err = dataFile.Write(buf); err != nil {
			return err
		}

		if err = checksumFile.WriteSum(buf); err != nil {
			return err
		}
	}

	return nil
}

type dataFileReader struct {
	dataFile *os.File
	offset   int64
	length   int64

	checksumFile *ChecksumFile
	blockIndex   int
	blockSize    int64
	blockCount   int
	buf          []byte
	boff         int
	blen         int
}

func newDataFileReader(name string, offset, length int64) (reader *dataFileReader, err error) {
	checksumFile, err := ChecksumFileReader(checksumFile(name))
	if err != nil {
		return nil, err
	}

	var dataFile *os.File

	defer func() {
		if err == nil {
			return
		}
		checksumFile.Close()
		if dataFile != nil {
			dataFile.Close()
		}
	}()

	startBlock, bytesToSkip, _, blockSize, blockCount, err := checksumFile.SectionValues(offset, length)
	if err != nil {
		return nil, err
	}

	if err = checksumFile.Seek(startBlock); err != nil {
		return nil, err
	}

	if dataFile, err = os.Open(name); err != nil {
		return nil, err
	}

	if _, err = dataFile.Seek(int64(startBlock*blockSize), os.SEEK_SET); err != nil {
		return nil, err
	}

	return &dataFileReader{
		dataFile: dataFile,
		offset:   int64(bytesToSkip),
		length:   length,

		checksumFile: checksumFile,
		blockIndex:   startBlock,
		blockSize:    int64(blockSize),
		blockCount:   blockCount,
	}, nil
}

func (dr *dataFileReader) readBlock() (n int, err error) {
	expectedChecksum, err := dr.checksumFile.Read()
	if err != nil {
		return 0, err
	}

	n, err = io.ReadFull(dr.dataFile, dr.buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return 0, err
	}

	if err == io.ErrUnexpectedEOF && dr.blockIndex != dr.blockCount-1 {
		return 0, err
	}

	dr.boff = 0
	dr.blen = n
	dr.blockIndex++

	checksum := dr.checksumFile.Checksum(dr.buf[dr.boff:dr.blen])
	if checksum != expectedChecksum {
		return 0, fmt.Errorf("checksum mismatch; expected: %v, got: %v", expectedChecksum, checksum)
	}

	return n, nil
}

func (dr *dataFileReader) Read(p []byte) (n int, err error) {
	if dr.length <= 0 {
		return 0, io.EOF
	}

	defer func() {
		if err != nil {
			dr.Close()
		}
	}()

	if dr.buf == nil {
		dr.buf = make([]byte, dr.blockSize)
		dr.boff = 0
		dr.blen = 0
	}

	clen := 0
	if dr.blen > 0 {
		if dr.length < int64(dr.blen) {
			dr.blen = int(dr.length)
		}

		clen = copy(p, dr.buf[dr.boff:dr.boff+dr.blen])
		dr.length -= int64(clen)
		dr.boff += clen
		dr.blen -= clen
	}

	coff := 0
	for len(p) > clen && dr.length > 0 {
		if n, err = dr.readBlock(); err != nil {
			return 0, err
		}

		// Here dr.boff = 0 and dr.blen = n.

		if dr.offset > 0 {
			dr.boff = int(dr.offset)
			if dr.blen = n - dr.boff; dr.blen < 0 {
				dr.blen = 0
			}
			dr.offset = 0
		}

		if dr.length < int64(dr.blen) {
			dr.blen = int(dr.length)
		}
		dr.length -= int64(dr.blen)

		coff = clen
		clen = copy(p[coff:], dr.buf[dr.boff:dr.boff+dr.blen])
		if dr.boff += clen; dr.boff > n {
			dr.boff = 0
		}
		if dr.blen -= clen; dr.blen < 0 {
			dr.blen = 0
		}

		coff += clen
	}

	return clen, nil
}

func (dr *dataFileReader) Close() (err error) {
	dr.checksumFile.Close()
	return dr.dataFile.Close()
}
