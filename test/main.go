package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/balamurugana/minio/test/backend"
	xrpc "github.com/balamurugana/minio/test/rpc"
	"github.com/gorilla/mux"
)

func parseArgs() (port string, backends []backend.Backend, lockers []Locker, diskRPCServer, lockRPCServer *xrpc.Server) {
	if len(os.Args) < 5 {
		fmt.Printf("usage: %v <INDEX> ENDPOINTS ...\n", os.Args[0])
		fmt.Printf("example:\n")
		fmt.Printf("on node1: %v 0 http://node1:9000/mnt/d1 http://node2:9000/mnt/d1 http://node3:9000/mnt/d1 http://node4:9000/mnt/d1\n", os.Args[0])
		fmt.Printf("on node2: %v 1 http://node1:9000/mnt/d1 http://node2:9000/mnt/d1 http://node3:9000/mnt/d1 http://node4:9000/mnt/d1\n", os.Args[0])
		fmt.Printf("on node3: %v 2 http://node1:9000/mnt/d1 http://node2:9000/mnt/d1 http://node3:9000/mnt/d1 http://node4:9000/mnt/d1\n", os.Args[0])
		fmt.Printf("on node4: %v 3 http://node1:9000/mnt/d1 http://node2:9000/mnt/d1 http://node3:9000/mnt/d1 http://node4:9000/mnt/d1\n", os.Args[0])
		os.Exit(1)
	}

	localIndex, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}

	endpoints := os.Args[2:]

	backends = make([]backend.Backend, len(endpoints))
	lockers = make([]Locker, len(endpoints))

	for i, rawurl := range endpoints {
		u, err := url.Parse(rawurl)
		if err != nil {
			panic(err)
		}

		if i == localIndex {
			port = strings.Split(u.Host, ":")[1]

			localDisk := backend.NewDisk(u.Path)
			localLocker := NewNSLocker()

			backends[i] = localDisk
			lockers[i] = localLocker
			diskRPCServer = NewDiskRPCServer(localDisk)
			lockRPCServer = NewNSLockerRPCServer(localLocker)
		} else {
			u.Path = "_"
			backends[i] = NewDiskRPCClient(u.String(), nil, globalRPCAPIVersion)
			u.Path = "_/lock"
			lockers[i] = NewNSLockerRPCClient(u.String(), nil, globalRPCAPIVersion)
		}
	}

	return
}

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

var erasureDisk *backend.ErasureDisk
var erasureLockers *Lockers

func getErrCount(errs []error, err error) int {
	counter := 0
	for i := range errs {
		if errs[i] == err {
			counter++
		}
	}

	return counter
}

func rootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "this is root handler\n")
}

func rpcHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "this is rpc handler\n")
}

func main() {
	port, backends, lockers, diskRPCServer, lockRPCServer := parseArgs()

	rand.Seed(time.Now().UnixNano())

	dataCount := len(backends) / 2
	parityCount := len(backends) / 2
	readQuorum := dataCount
	writeQuorum := readQuorum + 1
	erasureDisk = backend.NewErasureDisk(backends, dataCount, parityCount, defaultShardSize[dataCount], readQuorum, writeQuorum)
	erasureLockers = NewLockers(lockers, readQuorum, writeQuorum)

	router := mux.NewRouter()

	router.HandleFunc("/", http.HandlerFunc(rootHandler))

	router.Path("/_").HandlerFunc(diskRPCServer.ServeHTTP)
	router.Path("/_/lock").HandlerFunc(lockRPCServer.ServeHTTP)
	// router.PathPrefix("/_/").HandlerFunc(rpcHandler)

	bucketRouter := router.PathPrefix("/{bucketName}").Subrouter()
	objectRouter := bucketRouter.Path("/{objectName:.+}").Subrouter()

	objectRouter.Methods("DELETE").HandlerFunc(deleteObjectHTTPHandler)
	objectRouter.Methods("GET").HandlerFunc(getObjectHTTPHandler)
	objectRouter.Methods("HEAD").HandlerFunc(headObjectHTTPHandler)
	objectRouter.Methods("POST").HandlerFunc(postObjectHTTPHandler)
	objectRouter.Methods("PUT").HandlerFunc(putObjectHTTPHandler)

	bucketRouter.Methods("DELETE").HandlerFunc(deleteBucketHTTPHandler)
	bucketRouter.Methods("GET").HandlerFunc(getBucketHTTPHandler)
	bucketRouter.Methods("HEAD").HandlerFunc(headBucketHTTPHandler)
	bucketRouter.Methods("PUT").HandlerFunc(putBucketHTTPHandler)

	http.Handle("/", router)
	log.Println("Listening on", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
