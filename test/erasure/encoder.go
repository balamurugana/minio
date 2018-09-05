package erasure

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
)

type Encoder struct {
	encoder     reedsolomon.Encoder
	dataCount   int
	parityCount int
	shardSize   int
}

func (e *Encoder) CopyN(writers []io.Writer, reader io.Reader, length int64) error {
	if len(writers) != e.dataCount+e.parityCount {
		return fmt.Errorf("insufficient writers %v found, expected %v", len(writers), e.dataCount+e.parityCount)
	}

	blockSize := int64(e.shardSize * e.dataCount)
	blockCount := length / blockSize
	if blockCount*blockSize < length {
		blockCount++
	}

	var shards [][]byte
	shardSize := e.shardSize
	lastShardSize := e.shardSize
	dataCount := e.dataCount
	availableSize := length

	readShards := func() error {
		for i := 0; i < dataCount; i++ {
			if i == dataCount-1 {
				shardSize = lastShardSize
			}

			n, err := io.ReadFull(reader, shards[i][:shardSize])
			if err != nil {
				return err
			}

			availableSize -= int64(n)
		}

		return nil
	}

	tmpWriters := make([]io.Writer, len(writers))
	copy(tmpWriters, writers)
	writers = tmpWriters
	errs := make([]error, len(writers))

	writeShards := func() error {
		var wg sync.WaitGroup
		for i := 0; i < len(writers); i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				if writers[i] == nil {
					errs[i] = errors.New("nil writer")
				} else {
					_, errs[i] = writers[i].Write(shards[i])
				}
			}(i)
		}
		wg.Wait()

		errCount := 0
		for i := 0; i < len(errs); i++ {
			if errs[i] != nil {
				errCount++
				writers[i] = nil
			}
		}

		if errCount > e.parityCount-1 {
			return fmt.Errorf("too many write errors. %v", errs)
		}

		return nil
	}

	shardCount := int(blockCount)
	for i := 0; i < shardCount; i++ {
		if i == shardCount-1 && availableSize < blockSize {
			availableSize := int(availableSize)

			shardSize = availableSize / e.dataCount
			if shardSize*e.dataCount < availableSize {
				shardSize++
			}

			lastShardSize = shardSize
			dataCount = availableSize / shardSize

			if dataCount*shardSize < availableSize {
				lastShardSize = availableSize - (dataCount * shardSize)
				dataCount++
			}

			if shards != nil {
				for i := 0; i < e.dataCount+e.parityCount; i++ {
					shards[i] = shards[i][:shardSize]
				}

				for i := lastShardSize; i < shardSize; i++ {
					shards[e.dataCount-1][i] = 0
				}

				for d := dataCount; d < e.dataCount; d++ {
					for i := 0; i < shardSize; i++ {
						shards[d][i] = 0
					}
				}
			}
		}

		if shards == nil {
			shards = make([][]byte, e.dataCount+e.parityCount)
			for i := 0; i < e.dataCount+e.parityCount; i++ {
				shards[i] = make([]byte, shardSize)
			}
		}

		if err := readShards(); err != nil {
			return err
		}

		if err := e.encoder.Encode(shards); err != nil {
			return err
		}

		if err := writeShards(); err != nil {
			return err
		}
	}

	return nil
}

func NewEncoder(dataCount, parityCount, shardSize int) *Encoder {
	encoder, err := reedsolomon.New(dataCount, parityCount)
	if err != nil {
		panic(err)
	}

	return &Encoder{
		encoder:     encoder,
		dataCount:   dataCount,
		parityCount: parityCount,
		shardSize:   shardSize,
	}
}
