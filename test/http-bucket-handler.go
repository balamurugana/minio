package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/balamurugana/minio/test/backend"
	"github.com/balamurugana/minio/test/uuid"
	"github.com/gorilla/mux"
)

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
//
func deleteBucketHTTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	requestID := uuid.NewString()

	s3Handler := NewS3ErasureHandler(erasureDisk, erasureLockers)

	err := s3Handler.DeleteBucket(requestID, bucketName)

	switch err {
	case nil:
		w.WriteHeader(http.StatusOK)
	case backend.ErrBucketNotFound:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "delete bucket failed for bucket %v; %v\n", bucketName, err)
	default:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "delete bucket failed for bucket %v; %v\n", bucketName, err)
	}
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
// https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html
//
func getBucketHTTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	requestID := uuid.NewString()

	s3Handler := NewS3ErasureHandler(erasureDisk, erasureLockers)
	keys, prefixes, marker, err := s3Handler.GetBucket(requestID, bucketName, "", "", 1000)

	switch err {
	case nil:
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "keys = %v\nprefixes = %vmarker = %v\n", keys, prefixes, marker)
	case backend.ErrBucketNotFound:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "get bucket failed for bucket %v; %v\n", bucketName, err)
	default:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "get bucket failed for bucket %v; %v\n", bucketName, err)
	}
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
//
func headBucketHTTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	requestID := uuid.NewString()

	s3Handler := NewS3ErasureHandler(erasureDisk, erasureLockers)
	bucketInfo, err := s3Handler.HeadBucket(requestID, bucketName)
	log.Printf("headBucketHTTPHandler returns %+v\n", bucketInfo)

	switch err {
	case nil:
		w.WriteHeader(http.StatusOK)
	case backend.ErrBucketNotFound:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "head bucket failed for bucket %v; %v\n", bucketName, err)
	default:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "head bucket failed for bucket %v; %v\n", bucketName, err)
	}
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
//
func putBucketHTTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	requestID := uuid.NewString()

	s3Handler := NewS3ErasureHandler(erasureDisk, erasureLockers)

	err := s3Handler.PutBucket(requestID, bucketName)

	switch err {
	case nil:
		w.WriteHeader(http.StatusOK)
	case backend.ErrBucketAlreadyExists:
		w.WriteHeader(http.StatusConflict)
		fmt.Fprintf(w, "put bucket failed for bucket %v; %v\n", bucketName, err)
	default:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "put bucket failed for bucket %v; %v\n", bucketName, err)
	}

	return
}
