package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"

	"github.com/balamurugana/minio/test/backend"
	"github.com/balamurugana/minio/test/uuid"
	"github.com/gorilla/mux"
)

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
//
func deleteObjectHTTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]
	versionID := ""
	requestID := uuid.NewString()

	s3Handler := NewS3ErasureHandler(erasureDisk, erasureLockers)

	err := s3Handler.DeleteObject(requestID, bucketName, objectName, versionID)

	switch err {
	case nil:
		w.WriteHeader(http.StatusOK)
	case backend.ErrBucketNotFound:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "head object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	case backend.ErrObjectNotFound:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "head object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	default:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "head object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	}
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
//
func getObjectHTTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]
	versionID := ""
	requestID := uuid.NewString()

	offset := int64(2)
	length := int64(5)

	s3Handler := NewS3ErasureHandler(erasureDisk, erasureLockers)

	rc, err := s3Handler.GetObject(requestID, bucketName, objectName, versionID, offset, length)

	switch err {
	case nil:
		w.WriteHeader(http.StatusOK)
		if _, err = io.Copy(w, rc); err != nil {
			fmt.Fprintf(w, "get object failed to write to response for bucket %v, object %v; %v\n", bucketName, objectName, err)
		}
		rc.Close()
	case backend.ErrBucketNotFound:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "head object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	case backend.ErrObjectNotFound:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "head object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	default:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "head object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	}
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
//
func headObjectHTTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]
	versionID := ""
	requestID := uuid.NewString()

	s3Handler := NewS3ErasureHandler(erasureDisk, erasureLockers)

	objectInfo, err := s3Handler.HeadObject(requestID, bucketName, objectName, versionID)
	log.Printf("headObjectHTTPHandler returns %+v\n", objectInfo)

	switch err {
	case nil:
		w.WriteHeader(http.StatusOK)
	case backend.ErrBucketNotFound:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "head object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	case backend.ErrObjectNotFound:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "head object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	default:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "head object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	}
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
//
func postObjectHTTPHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "this is post object handler\n")
}

//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
//
func putObjectHTTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucketName"]
	objectName := vars["objectName"]
	versionID := ""

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

	readerSize := int64(ui64)

	requestID := uuid.NewString()

	s3Handler := NewS3ErasureHandler(erasureDisk, erasureLockers)

	objectInfo := backend.ObjectInfo{
		BucketName:      bucketName,
		ObjectName:      objectName,
		ObjectVersionID: versionID,
	}

	err = s3Handler.PutObject(requestID, bucketName, objectName, versionID, r.Body, readerSize, &objectInfo)

	switch err {
	case nil:
		w.WriteHeader(http.StatusOK)
	case backend.ErrBucketNotFound:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "put object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	default:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "put object failed for bucket %v, object %v; %v\n", bucketName, objectName, err)
	}
}
