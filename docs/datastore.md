# DataSpace

```go
type DataSpace interface {
	InitUpload(uploadID string, metadata map[string][]string) error
	UploadPart(uploadID string, partNumber uint, data io.Reader, size uint64) (etag string, err error)
	UploadPartCopy(uploadID string, partNumber uint, srcID string, offset, length uint64) (etag string, err error)
	CompleteUpload(ID, uploadID string, parts []Part, metadata map[string][]string) error
	AbortUpload(uploadID string) error
	ListParts(uploadID string) ([]Part, error)

	Put(ID string, data io.Reader, size uint64, metadata map[string][]string) error
	Get(ID string, offset, length uint64) (io.ReadCloser, map[string][]string, error)
	GetMetadata(ID string) (map[string][]string, error)
	Delete(ID string) error
	Copy(ID, srcID string, offset, length uint64, metadata map[string][]string) error
}
```

## Wormer DataSpace
A lock-free WORM storage is interface compatiable to DataSpace. Every upload uses `tmp` directory as interim storage and every `Delete()` is staged and actual removal is done once all `Get()` are finished.

```
Wormer
|-- data/
|   `-- <INDEX>/
|       `-- <ID>/
|           |-- data.json
|           |-- part.<N>
|           `-- part.<N>.checksum
|-- uploads/
|   `-- <INDEX>/
|       `-- <UPLOAD_ID>/
|           |-- parts.json
|           |-- part.<N>
|           `-- part.<N>.checksum
`-- tmp/
```

* `INDEX` is first two bytes (Most Significant Byte) of `ID` or `UPLOAD_ID`.
* All parts stored under `ID`/`UPLOAD_ID` are checksummed.

### Format of data.json
```go
type Part struct {
	ETag   string `json:"etag"`
	Number uint   `json:"number"`
	Size   uint64 `json:"size"`
}

type DataInfo struct {
	ETag  string `json:"etag"`
	Key   string `json:"key"`
	Parts []Part `json:"parts"`
	Size  uint64 `json:"size"`
}
```
Example:
```json
{
    "etag": "akinkekhybbe",
    "key": "mybucket/path/to/myobject",
    "parts": [
        {
            "etag": "poumnasdfk",
            "number": 2,
            "size": 8762812
        }
    ],
    "size": 8762812
}
```

### Checksum file format
```
{Single line JSON of checksum header}\n
<Block-1 checksum>\n
<Block-2 checksum>\n
<Block-3 checksum>\n
...
...
<Block-N checksum>\n
```

#### Format of checksum header
```go
type ChecksumHeader struct {
	HashName   string  `json:"hashName"`
	HashKey    string  `json:"haskKey"`
	HashLength uint    `json:"hashLength"`
	BlockSize  uint    `json:"blockSize"`
	BlockCount uint    `json:"blockCount"`
	DataLength uint64  `json:"dataLength"`
}
```
Example:
```json
{"hashName":"HighwayHash256","hashKey":"","hashLength":32,"blockSize":10485760,"blockCount":84,"dataLength":871265537}
```

## Format of parts.json
```go
type Parts map[uint]Part
```

Example:
```json
{
    "2": {
        "etag": "poumnasdfk",
        "number": 2,
        "size": 8762812
    }
}
```

# S3 Namespace format
```
NameSpace
|-- buckets/
|   `-- <BUCKET>/
|       |-- metadata-files
|       `-- objects/
|           `-- <OBJECT>/
|               |-- default.json
|               `-- <ID>.json
`-- tmp/
```

`default.json` points to one of `<ID>.json`.


## Format of <ID>.json
```json
{
	"default": true,
	"size": SIZE,
	"createdAt": "TIME",
	"contentType": "content-type",
	"metadata": { METADATA }
}
```
