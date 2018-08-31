package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"sync"

	"github.com/balamurugana/minio/test/erasure"
	"github.com/balamurugana/minio/test/uuid"
	"github.com/gorilla/mux"
)

// defaultBlockSize is exactly 10,450,440 bytes; approximately 9.97 MiB.
// Number 360360 is used because it is divisible by any number between 1 and 15.
const defaultBlockSize = 360360 * 29

var defaultShardSize = []int{
	0,
	defaultBlockSize / 1,
	defaultBlockSize / 2,
	defaultBlockSize / 3,
	defaultBlockSize / 4,
	defaultBlockSize / 5,
	defaultBlockSize / 6,
	defaultBlockSize / 7,
	defaultBlockSize / 8,
	defaultBlockSize / 9,
	defaultBlockSize / 10,
	defaultBlockSize / 11,
	defaultBlockSize / 12,
	defaultBlockSize / 13,
	defaultBlockSize / 14,
	defaultBlockSize / 15,
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
//
func deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]

	// FIXME: lock cluster wide

	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range dataStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = dataStores[i].DeleteObject(bucketName, objectName)
		}(i)
	}
	wg.Wait()

	// FIXME: unlock cluster wide

	if getErrCount(errs, nil) > 2 {
		w.WriteHeader(http.StatusOK)
		return
	}

	if getErrCount(errs, errBucketNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: delete object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, errObjectNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: delete object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, "FAILED: delete object handler\n")
	fmt.Fprintf(w, "errs = %+v\n", errs)
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
//
func getObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]

	offset := int64(2)
	length := int64(5)

	// 1. FIXME: lock cluster wide

	// 2. get object info from cluster
	ois := make([]*ObjectInfo, 4)
	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range dataStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ois[i], errs[i] = dataStores[i].HeadObject(bucketName, objectName)
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, errBucketNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: get object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, errObjectNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: get object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, nil) <= 2 {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "FAILED: get object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
	}

	counters := make([]int, 4)
	for i := 0; i < len(ois); i++ {
		if ois[i] == nil {
			continue
		}

		for j := i + 1; j < len(ois); j++ {
			if ois[j] != nil && ois[i].Equal(ois[j]) {
				counters[i]++
			}
		}
	}

	bestMatchIndex := 0
	max := 0
	for i := range counters {
		if max < counters[i] {
			max = counters[i]
			bestMatchIndex = i
		}
	}

	if max < 2 {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "FAILED: get object handler\n")
		fmt.Fprintf(w, "no qourum match of object info found\n")
		fmt.Fprintf(w, "objectInfos = %+v\n", ois)
	}

	// 3. compute request offset/length
	objectInfo := ois[bestMatchIndex]
	if offset+length > objectInfo.ObjectSize {
		length = objectInfo.ObjectSize - offset
	}

	values := erasure.Values{
		ObjectSize:    objectInfo.ObjectSize,
		DataCount:     objectInfo.DataCount,
		ParityCount:   objectInfo.ParityCount,
		ShardSize:     objectInfo.ShardSize,
		LastShardSize: objectInfo.LastShardSize,
		ShardCount:    objectInfo.ShardCount,
	}
	sectionValues := values.SectionValues(offset, length)

	// 4. get data stream from cluster
	readClosers := make([]io.ReadCloser, 4)
	defer func() {
		for _, rc := range readClosers {
			if rc != nil {
				rc.Close()
			}
		}
	}()

	for i := range dataStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			readClosers[i], errs[i] = dataStores[i].GetObject(bucketName, objectName, sectionValues.ShardOffset, sectionValues.ShardLength)
		}(i)
	}
	wg.Wait()

	// 5. FIXME: unlock cluster wide

	if getErrCount(errs, errBucketNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: get object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, errObjectNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: get object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, nil) <= 2 {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "FAILED: get object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
	}

	// 6. read data stream from cluster and write to client
	readers := make([]io.Reader, 4)
	for i, pos := range ois[bestMatchIndex].ShardOrder {
		readers[i] = readClosers[pos]
	}

	decoder := erasure.NewDecoder(
		objectInfo.DataCount,
		objectInfo.ParityCount,
		sectionValues.ShardSize,
		sectionValues.LastShardSize,
		sectionValues.ShardCount,
		sectionValues.BytesToSkip,
	)

	n, err := decoder.CopyN(w, readers, length)
	if err != nil {
		if n == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "FAILED: get object handler\n")
			fmt.Fprintf(w, "err = %+v\n", err)
		} else {
			log.Printf("WARNING: error in writing decoded data in get object handler, %v\n", err)
		}

		return
	}

	if n != length {
		log.Printf("WARNING: short data written, expected; %v, got: %v\n", length, n)
	}
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
//
func headObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]

	ois := make([]*ObjectInfo, 4)
	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range dataStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ois[i], errs[i] = dataStores[i].HeadObject(bucketName, objectName)
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) > 2 {
		w.WriteHeader(http.StatusOK)
		return
	}

	if getErrCount(errs, errBucketNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: head object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, errObjectNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: head object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, "FAILED: head object handler\n")
	fmt.Fprintf(w, "errs = %+v\n", errs)
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
//
func postObjectHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "this is post object handler\n")
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
//
func putObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]

	ui64, err := strconv.ParseUint(r.Header.Get("content-length"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "FAILED: put object handler\n")
		fmt.Fprintf(w, "invalid content-length  %+v\n", err)
		return
	}
	if ui64 > math.MaxInt64 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "FAILED: put object handler\n")
		fmt.Fprintf(w, "content-length overflow %+v\n", ui64)
		return
	}

	objectSize := int64(ui64)
	values := erasure.Compute(objectSize, 2, 2, defaultShardSize[2])

	shardOrder := rand.Perm(4)
	orderedDataStores := make([]DataStore, 4)
	for i, order := range shardOrder {
		orderedDataStores[i] = dataStores[order]
	}

	requestID := uuid.NewString()
	stageObjectArgs := StageObjectArgs{
		BucketName:    bucketName,
		ObjectName:    objectName,
		ObjectSize:    objectSize,
		DataCount:     values.DataCount,
		ParityCount:   values.ParityCount,
		ShardSize:     values.ShardSize,
		LastShardSize: values.LastShardSize,
		ShardCount:    values.ShardCount,
		ShardOrder:    shardOrder,
	}
	shardObjectSize := values.ShardObjectSize()

	pipeReaders := make([]*io.PipeReader, 4)
	pipeWriters := make([]*io.PipeWriter, 4)
	for i := 0; i < 4; i++ {
		pipeReaders[i], pipeWriters[i] = io.Pipe()
	}

	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range orderedDataStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := stageObjectArgs
			args.ShardIndex = i
			errs[i] = orderedDataStores[i].SaveStageObject(&args, requestID, pipeReaders[i], shardObjectSize)
			pipeReaders[i].Close()
		}(i)
	}

	encoder := erasure.NewEncoder(2, 2, defaultShardSize[2])
	writers := make([]io.Writer, 4)
	for i := 0; i < 4; i++ {
		writers[i] = pipeWriters[i]
	}
	err = encoder.CopyN(writers, r.Body, objectSize)
	for i := 0; i < 4; i++ {
		pipeWriters[i].Close()
	}

	wg.Wait()

	if getErrCount(errs, errBucketNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: put object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, errObjectNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: put object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, nil) <= 2 {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "FAILED: put object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	// FIXME: lock cluster wide

	for i := range orderedDataStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = orderedDataStores[i].CommitStageObject(requestID)
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, errBucketNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: put object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, errObjectNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: put object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, nil) <= 2 {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "FAILED: put object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	for i := range orderedDataStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = orderedDataStores[i].CloseStageObject(bucketName, objectName, requestID, false)
		}(i)
	}
	wg.Wait()

	// FIXME: unlock cluster wide

	if getErrCount(errs, nil) > 2 {
		w.WriteHeader(http.StatusOK)
		return
	}

	if getErrCount(errs, errBucketNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: put object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	if getErrCount(errs, errObjectNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "FAILED: put object handler\n")
		fmt.Fprintf(w, "errs = %+v\n", errs)
		return
	}

	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, "FAILED: put object handler\n")
	fmt.Fprintf(w, "errs = %+v\n", errs)
}
