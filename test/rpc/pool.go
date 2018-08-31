package rpc

import (
	"bytes"
	"sync"
)

var bufPool = newPool()

type pool struct {
	p *sync.Pool
}

func (p pool) Get() *bytes.Buffer {
	return p.p.Get().(*bytes.Buffer)
}

func (p pool) Put(buf *bytes.Buffer) {
	buf.Reset()
	p.p.Put(buf)
}

func newPool() pool {
	return pool{p: &sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}}
}
