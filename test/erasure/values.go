package erasure

type ShardSectionValues struct {
	ShardOffset   int64
	ShardLength   int64
	ShardSize     int
	LastShardSize int
	ShardCount    int
	BytesToSkip   int
}

type Values struct {
	ObjectSize    int64
	DataCount     int
	ParityCount   int
	ShardSize     int
	LastShardSize int
	ShardCount    int
}

func (v Values) ShardObjectSize() int64 {
	return int64(v.LastShardSize + v.ShardSize*(v.ShardCount-1))
}

func (v Values) SectionValues(offset, length int64) ShardSectionValues {
	shardObjectSize := int64(v.LastShardSize) + int64(v.ShardSize*(v.ShardCount-1))

	if offset > v.ObjectSize {
		return ShardSectionValues{ShardOffset: shardObjectSize}
	}

	if offset+length > v.ObjectSize {
		length = v.ObjectSize - offset
	}

	blockSize := int64(v.ShardSize * v.DataCount)

	skipShardCount := int(offset / blockSize)
	bytesToSkip := int(offset - int64(skipShardCount)*blockSize)

	shardCount := int(length / blockSize)
	if int64(shardCount)*blockSize < length {
		shardCount++
	}

	shardSize := v.ShardSize
	lastShardSize := v.ShardSize
	if skipShardCount+shardCount == v.ShardCount {
		lastShardSize = v.LastShardSize
		if shardCount == 1 {
			shardSize = v.LastShardSize
		}
	}

	shardOffset := int64(skipShardCount * shardSize)
	shardLength := int64(shardCount * shardSize)
	if shardOffset+shardLength > shardObjectSize {
		shardLength = shardObjectSize - shardOffset
	}

	return ShardSectionValues{
		ShardOffset:   shardOffset,
		ShardLength:   shardLength,
		ShardSize:     shardSize,
		LastShardSize: lastShardSize,
		ShardCount:    shardCount,
		BytesToSkip:   bytesToSkip,
	}
}

func Compute(objectSize int64, dataCount, parityCount, shardSize int) Values {
	blockSize := shardSize * dataCount

	lastShardSize := shardSize
	computeLastChunk := func(chunkSize int) {
		lastShardSize = chunkSize / dataCount
		if lastShardSize*dataCount < chunkSize {
			lastShardSize++
		}
	}

	shardCount := 1
	if objectSize > int64(blockSize) {
		shardCount = int(objectSize / int64(blockSize))
		if int64(shardCount*blockSize) < objectSize {
			computeLastChunk(int(objectSize - int64(shardCount*blockSize)))
			shardCount++
		}
	} else {
		computeLastChunk(int(objectSize))
		shardSize = lastShardSize
	}

	return Values{
		ObjectSize:    objectSize,
		DataCount:     dataCount,
		ParityCount:   parityCount,
		ShardSize:     shardSize,
		LastShardSize: lastShardSize,
		ShardCount:    shardCount,
	}
}
