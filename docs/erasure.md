# Erasure
## Encoding
Data is encoded by splitting it into fixed block size.  Each block is further split into fixed number of stripe size.  Each stripe data is placed into data shards and parity shards are calculated.  Then each stripe data and parity data are written to individual disks with metadata.  Below is a diagram for six disks case.
```
          +-----------+-----------+----------~
  Data    |  Block 1  |  Block 2  |    ...   ~
          +-----------+-----------+----------~
               |||         |||
               vvv         vvv
          +-----------+-----------+----------~
  Disk1   |  Stripe 1 |  Stripe 1 |    ...   ~
          +-----------+-----------+----------~
  Disk2   |  Stripe 2 |  Stripe 2 |    ...   ~
          +-----------+-----------+----------~
  Disk3   |  Stripe 3 |  Stripe 3 |    ...   ~
          +-----------+-----------+----------~
  Disk4   |  Parity 1 |  Parity 1 |    ...   ~
          +-----------+-----------+----------~
  Disk5   |  Parity 2 |  Parity 2 |    ...   ~
          +-----------+-----------+----------~
  Disk6   |  Parity 3 |  Parity 3 |    ...   ~
          +-----------+-----------+----------~
```

### Metadata format of Data
```go
type ErasureDataInfo struct {
	ID            string   `json:"id"`
	Size          uint64   `json:"size"`
	BlockSize     uint     `json:"blockSize"`
	LastBlockSize uint     `json:"lastBlockSize"`
	BlockCount    uint     `json:"blockCount"`
	DataCount     uint     `json:"dataCount"`
	ParityCount   uint     `json:"parityCount"`
	DataSpace     []string `json:"dataSpace"` // len(DataSpace) == DataCount+ParityCount
}
```

### Metadata format of Stripe Data
```go
type StripeDataInfo struct {
	StripeSize     uint `json:"stripeSize"`
	LastStripeSize uint `json:"lastStripeSize"`
	StripeCount    uint `json:"stripeCount"`
	StripeIndex    uint `json:"stripeIndex"`
}
```

## Decoding

