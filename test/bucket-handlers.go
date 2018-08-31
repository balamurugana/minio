package main

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

func getErrCount(errs []error, err error) int {
	counter := 0
	for i := range errs {
		if errs[i] == err {
			counter++
		}
	}

	return counter
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
//
func deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]

	// FIXME: lock cluster wide

	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range dataStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = dataStores[i].DeleteBucket(bucketName)
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
		return
	}

	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, "FAILED: delete bucket handler\n")
	fmt.Fprintf(w, "errs = %+v\n", errs)
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
//
func headBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]

	bis := make([]*BucketInfo, 4)
	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range dataStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bis[i], errs[i] = dataStores[i].HeadBucket(bucketName)
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) > 2 {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "bis = %+v\n", bis)
		return
	}

	if getErrCount(errs, errBucketNotFound) > 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, "FAILED: head bucket handler\n")
	fmt.Fprintf(w, "errs = %+v\n", errs)
	fmt.Fprintf(w, "bis = %+v\n", bis)
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
//
func putBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]

	// FIXME: lock cluster wide

	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range dataStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = dataStores[i].PutBucket(bucketName)
		}(i)
	}
	wg.Wait()

	// FIXME: unlock cluster wide

	if getErrCount(errs, nil) > 2 {
		w.WriteHeader(http.StatusOK)
		return
	}

	if getErrCount(errs, errBucketAlreadyExists) > 2 {
		w.WriteHeader(http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, "FAILED: put bucket handler\n")
	fmt.Fprintf(w, "errs = %+v\n", errs)
}
