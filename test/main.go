package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	xrpc "github.com/balamurugana/minio/test/rpc"
	"github.com/gorilla/mux"
)

var dataStores = make([]DataStore, 4)

func rootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "this is root handler\n")
}

func rpcHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "this is rpc handler\n")
}

func main() {
	rand.Seed(time.Now().UnixNano())

	var port string
	var dataStoreServer *xrpc.Server
	switch os.Args[1] {
	case "1":
		port = "9001"

		localDataStore := NewBackend("/var/tmp/d1")

		dataStores[0] = localDataStore
		dataStores[1] = NewBackendRPCClient("http://localhost:9002/_", nil, globalRPCAPIVersion)
		dataStores[2] = NewBackendRPCClient("http://localhost:9003/_", nil, globalRPCAPIVersion)
		dataStores[3] = NewBackendRPCClient("http://localhost:9004/_", nil, globalRPCAPIVersion)

		dataStoreServer = NewBackendRPCServer(localDataStore)
	case "2":
		port = "9002"

		localDataStore := NewBackend("/var/tmp/d2")

		dataStores[0] = NewBackendRPCClient("http://localhost:9001/_", nil, globalRPCAPIVersion)
		dataStores[1] = localDataStore
		dataStores[2] = NewBackendRPCClient("http://localhost:9003/_", nil, globalRPCAPIVersion)
		dataStores[3] = NewBackendRPCClient("http://localhost:9004/_", nil, globalRPCAPIVersion)

		dataStoreServer = NewBackendRPCServer(localDataStore)
	case "3":
		port = "9003"

		localDataStore := NewBackend("/var/tmp/d3")

		dataStores[0] = NewBackendRPCClient("http://localhost:9001/_", nil, globalRPCAPIVersion)
		dataStores[1] = NewBackendRPCClient("http://localhost:9002/_", nil, globalRPCAPIVersion)
		dataStores[2] = localDataStore
		dataStores[3] = NewBackendRPCClient("http://localhost:9004/_", nil, globalRPCAPIVersion)

		dataStoreServer = NewBackendRPCServer(localDataStore)
	case "4":
		port = "9004"

		localDataStore := NewBackend("/var/tmp/d4")

		dataStores[0] = NewBackendRPCClient("http://localhost:9001/_", nil, globalRPCAPIVersion)
		dataStores[1] = NewBackendRPCClient("http://localhost:9002/_", nil, globalRPCAPIVersion)
		dataStores[2] = NewBackendRPCClient("http://localhost:9003/_", nil, globalRPCAPIVersion)
		dataStores[3] = localDataStore

		dataStoreServer = NewBackendRPCServer(localDataStore)
	default:
		panic("unknown os.Args[1]")
	}

	router := mux.NewRouter()

	router.HandleFunc("/", http.HandlerFunc(rootHandler))

	router.Path("/_").HandlerFunc(dataStoreServer.ServeHTTP)
	router.PathPrefix("/_/").HandlerFunc(rpcHandler)

	bucketRouter := router.PathPrefix("/{bucketName}").Subrouter()
	objectRouter := bucketRouter.Path("/{objectName:.+}").Subrouter()

	objectRouter.Methods("DELETE").HandlerFunc(deleteObjectHandler)
	objectRouter.Methods("GET").HandlerFunc(getObjectHandler)
	objectRouter.Methods("HEAD").HandlerFunc(headObjectHandler)
	objectRouter.Methods("POST").HandlerFunc(postObjectHandler)
	objectRouter.Methods("PUT").HandlerFunc(putObjectHandler)

	bucketRouter.Methods("DELETE").HandlerFunc(deleteBucketHandler)
	bucketRouter.Methods("HEAD").HandlerFunc(headBucketHandler)
	bucketRouter.Methods("PUT").HandlerFunc(putBucketHandler)

	http.Handle("/", router)
	log.Println("Listening on", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
