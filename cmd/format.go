package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"sync"
)

var formatDoneCh chan struct{}
var formatDoneChMutex sync.Mutex

func getFormatDoneCh() chan struct{} {
	formatDoneChMutex.Lock()
	defer formatDoneChMutex.Unlock()

	if formatDoneCh == nil {
		formatDoneCh = make(chan struct{})
	}

	return formatDoneCh
}

func closeFormatDoneCh() {
	formatDoneChMutex.Lock()
	defer formatDoneChMutex.Unlock()

	if formatDoneCh != nil {
		close(formatDoneCh)
		formatDoneCh = nil
	}
}

type FormatArgs struct {
	GOOS      string
	GOARCH    string
	Endpoints []string
}

func (a FormatArgs) Equal(b FormatArgs) bool {
	if a.GOOS != b.GOOS {
		fmt.Printf("OS differs; this: %v, remote: %v", a.GOOS, b.GOOS)
		return false
	}

	if a.GOARCH != b.GOARCH {
		fmt.Printf("OS architecture differs; this: %v, remote: %v", a.GOARCH, b.GOARCH)
		return false
	}

	if !reflect.DeepEqual(a.Endpoints, b.Endpoints) {
		fmt.Printf("OS architecture differs; this: %v, remote: %v", a.GOARCH, b.GOARCH)
		return false
	}

	return true
}

func readFormatHandler(w http.ResponseWriter, r *http.Request) {
	var args FormatArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	if args.GOOS != runtime.GOOS {
		fmt.Printf("OS differs; this: %v, remote: %v", args.GOOS, runtime.GOOS)
		w.WriteHeader(http.StatusBadRequest)
	}

	if args.GOARCH != runtime.GOARCH {
		fmt.Printf("OS architecture differs; this: %v, remote: %v", args.GOARCH, runtime.GOARCH)
		w.WriteHeader(http.StatusBadRequest)
	}

	if !reflect.DeepEqual(args.Endpoints, globalEndpoints) {
		fmt.Printf("endpoints differs; this: %v, remote: %v", args.Endpoints, globalEndpoints)
		w.WriteHeader(http.StatusBadRequest)
	}

	data := []byte("{}")
	if format != nil {
		var err error
		if data, err = json.Marshal(format); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}

	w.Write(data)
}
