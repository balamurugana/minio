package rpc

import (
	"io"
	"io/ioutil"
	"sync/atomic"
)

type DrainReader struct {
	rc       io.ReadCloser
	isClosed int32
}

func NewDrainReader(rc io.ReadCloser) *DrainReader {
	return &DrainReader{rc: rc}
}

func (dr *DrainReader) Read(p []byte) (n int, err error) {
	return dr.rc.Read(p)
}

func (dr *DrainReader) Close() (err error) {
	if atomic.SwapInt32(&dr.isClosed, 1) == 0 {
		go func() {
			io.Copy(ioutil.Discard, dr.rc)
			dr.rc.Close()
		}()
	}

	return nil
}
