package erasure

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
)

type Decoder struct {
	encoder       reedsolomon.Encoder
	dataCount     int
	parityCount   int
	shardSize     int
	lastShardSize int
	shardCount    int
	bytesToSkip   int
}

func (d *Decoder) CopyN(writer io.Writer, readers []io.Reader, length int64) (int64, error) {
	if len(readers) != d.dataCount+d.parityCount {
		return 0, fmt.Errorf("insufficient readers %v found, expected %v", len(readers), d.dataCount+d.parityCount)
	}

	tmpReaders := make([]io.Reader, len(readers))
	copy(tmpReaders, readers)
	readers = tmpReaders

	var shards [][]byte
	errs := make([]error, len(readers))

	readShards := func() (bool, error) {
		var wg sync.WaitGroup
		for i := 0; i < len(readers); i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				if readers[i] == nil {
					errs[i] = errors.New("nil reader")
				} else {
					_, errs[i] = io.ReadFull(readers[i], shards[i])
				}
			}(i)
		}
		wg.Wait()

		errCount := 0
		reconstruct := false
		for i := 0; i < len(errs); i++ {
			if errs[i] != nil {
				errCount++
				readers[i] = nil
				// make it to zero length. cap is intact.
				shards[i] = shards[i][:0]
				if i < d.dataCount {
					reconstruct = true
				}
			}
		}

		if errCount > d.parityCount+1 {
			return false, fmt.Errorf("too many read errors. %v", errs)
		}

		return reconstruct, nil
	}

	bytesToWrite := length
	writeShards := func() error {
		for i := 0; i < d.dataCount; i++ {
			start := 0
			if d.bytesToSkip > 0 {
				if len(shards[i]) < d.bytesToSkip {
					d.bytesToSkip -= len(shards[i])
					continue
				}

				start = d.bytesToSkip
				d.bytesToSkip = 0
			}

			end := len(shards[i][start:])
			if end > int(bytesToWrite) {
				end = int(bytesToWrite)
			}

			n, err := writer.Write(shards[i][start:][:end])
			if err != nil {
				return err
			}

			bytesToWrite -= int64(n)
		}

		return nil
	}

	for i := 0; i < d.shardCount; i++ {
		if i == d.shardCount-1 {
			if shards != nil && d.shardSize != d.lastShardSize {
				for i := 0; i < d.dataCount+d.parityCount; i++ {
					shards[i] = shards[i][:d.lastShardSize]
				}
			}

			d.shardSize = d.lastShardSize
		}

		if shards == nil {
			shards = make([][]byte, d.dataCount+d.parityCount)
			for i := 0; i < d.dataCount+d.parityCount; i++ {
				shards[i] = make([]byte, d.shardSize)
			}
		}

		reconstruct, err := readShards()
		if err != nil {
			return length - bytesToWrite, err
		}

		if reconstruct {
			if err := d.encoder.ReconstructData(shards); err != nil {
				return length - bytesToWrite, err
			}
		}

		if err = writeShards(); err != nil {
			return length - bytesToWrite, err
		}
	}

	return length - bytesToWrite, nil
}

func NewDecoder(dataCount, parityCount, shardSize, lastShardSize, shardCount, bytesToSkip int) *Decoder {
	encoder, err := reedsolomon.New(dataCount, parityCount)
	if err != nil {
		panic(err)
	}

	return &Decoder{
		encoder:       encoder,
		dataCount:     dataCount,
		parityCount:   parityCount,
		shardSize:     shardSize,
		lastShardSize: lastShardSize,
		shardCount:    shardCount,
		bytesToSkip:   bytesToSkip,
	}
}
