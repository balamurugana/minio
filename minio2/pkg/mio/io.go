package mio

import (
	"bytes"
	"errors"
	"io"

	"github.com/klauspost/reedsolomon"
)

type Aborter interface {
	Abort() error
}

type WriteAbortCloser interface {
	io.WriteCloser
	Aborter
}

type LimitedReadCloser struct {
	reader io.Reader
	closer io.Closer
}

func (l *LimitedReadCloser) Read(p []byte) (n int, err error) {
	return l.reader.Read(p)
}

func (l *LimitedReadCloser) Close() error {
	return l.closer.Close()
}

func LimitReadCloser(rc io.ReadCloser, n int64) io.ReadCloser {
	return &LimitedReadCloser{io.LimitReader(rc, n), rc}
}

type ErasureWriter struct {
	reedSolomon reedsolomon.Encoder
	stripeSize  int
	stripeCount int
	parityCount int
	writers     []WriteAbortCloser
	blockSize   int
	buf         []byte
	aborted     bool
	closed      bool
}

func NewErasureWriter(reedSolomon reedsolomon.Encoder, stripeSize int, parityCount int, writers []WriteAbortCloser) *ErasureWriter {
	stripeCount := len(writers) - parityCount
	return &ErasureWriter{
		reedSolomon: reedSolomon,
		stripeSize:  stripeSize,
		stripeCount: stripeCount,
		parityCount: parityCount,
		writers:     writers,
		blockSize:   stripeSize * stripeCount,
	}
}

func (w *ErasureWriter) write(data []byte) (err error) {
	if len(data)%w.stripeCount != 0 {
		return errors.New("unaligned data found")
	}
	stripeSize := len(data) / w.stripeCount

	dataShards := make([][]byte, w.stripeCount)
	for i := 0; i < w.stripeCount; i++ {
		dataShards[i] = data[i*stripeSize : (i+1)*stripeSize]
	}

	parityShards := make([][]byte, w.parityCount)
	for i := 0; i < w.parityCount; i++ {
		parityShards[i] = make([]byte, stripeSize)
	}

	var shards [][]byte
	shards = append(shards, dataShards...)
	shards = append(shards, parityShards...)

	if err = w.reedSolomon.Encode(shards); err != nil {
		return err
	}

	var errs []error
	for i := range shards {
		if w.writers[i] == nil {
			errs = append(errs, errors.New("closed or nil writer"))
			continue
		}
		if _, err = w.writers[i].Write(shards[i]); err != nil {
			errs = append(errs, err)
			w.writers[i].Abort()
			w.writers[i] = nil
		}
	}

	if len(errs) >= w.parityCount {
		return errors.New("Too many errors in writing data stripes")
	}

	return nil
}

func (w *ErasureWriter) Write(p []byte) (n int, err error) {
	if w.aborted || w.closed {
		err = errors.New("closed writer")
		return
	}

	w.buf = append(w.buf, p...)
	bufSize := len(w.buf)

	if bufSize >= w.blockSize {
		data := w.buf[:w.blockSize]
		w.buf = append([]byte{}, w.buf[w.blockSize:]...)
		err = w.write(data)

	}

	if err != nil {
		w.buf = []byte{}
	} else {
		n = len(p)
	}

	return
}

func (w *ErasureWriter) Close() (err error) {
	if w.aborted || w.closed {
		return
	}

	defer func() {
		if err != nil {
			for i := range w.writers {
				if w.writers[i] != nil {
					w.writers[i].Abort()
				}
			}
		} else {
			for i := range w.writers {
				if w.writers[i] != nil {
					w.writers[i].Close()
				}
			}
		}
	}()

	dataLen := len(w.buf)
	if dataLen > 0 {
		data := w.buf
		w.buf = []byte{}

		if dataLen%w.stripeCount != 0 {
			stripeSize := 1 + dataLen/w.stripeCount
			blockSize := stripeSize * w.stripeCount
			padding := blockSize - dataLen
			data = append(data, make([]byte, padding)...)
		}

		err = w.write(data)
	}

	w.closed = true
	return
}

func (w *ErasureWriter) Abort() (err error) {
	if w.aborted || w.closed {
		return nil
	}

	defer func() {
		for i := range w.writers {
			if w.writers[i] != nil {
				w.writers[i].Abort()
			}
		}
	}()

	w.aborted = true
	w.buf = []byte{}

	return nil
}

type ErasureReader struct {
	reedSolomon reedsolomon.Encoder
	stripeSize  int
	stripeCount int
	parityCount int
	readers     []io.ReadCloser
	buf         []byte
	blockSize   int
	bytesToSkip int
}

func NewErasureReader(reedSolomon reedsolomon.Encoder, stripeSize int, parityCount int, readers []io.ReadCloser, bytesToSkip int) *ErasureReader {
	stripeCount := len(readers) - parityCount
	return &ErasureReader{
		reedSolomon: reedSolomon,
		stripeSize:  stripeSize,
		stripeCount: stripeCount,
		parityCount: parityCount,
		readers:     readers,
		blockSize:   stripeSize * stripeCount,
		bytesToSkip: bytesToSkip,
	}
}

func (r *ErasureReader) read() (p []byte, err error) {
	defer func() {
		if err != nil {
			for i := range r.readers {
				if r.readers[i] != nil {
					r.readers[i].Close()
					r.readers[i] = nil
				}
			}
		}
	}()

	shards := make([][]byte, r.stripeCount+r.parityCount)
	buf := make([]byte, r.stripeSize)
	var errs []error
	var eofs []bool
	var n int
	for i := range shards {
		if r.readers[i] == nil {
			errs = append(errs, errors.New("closed or nil reader"))
			continue
		}

		n, err = io.ReadFull(r.readers[i], buf)
		if err == io.ErrUnexpectedEOF {
			err = nil
		}
		if err == io.EOF {
			r.readers[i].Close()
			r.readers[i] = nil
			eofs = append(eofs, true)
			continue
		}
		if err != nil {
			r.readers[i] = nil
			errs = append(errs, err)
			continue
		}

		shards[i] = append([]byte{}, buf[0:n]...)
	}

	if len(errs) >= r.parityCount {
		err = errors.New("Too many errors in reading data stripes")
		return
	}

	if len(eofs) >= r.stripeCount {
		err = io.EOF
		return
	}

	ok := false
	if ok, err = r.reedSolomon.Verify(shards); err != nil {
		return
	}
	if !ok {
		if err = r.reedSolomon.Reconstruct(shards); err != nil {
			return
		}
		if ok, err = r.reedSolomon.Verify(shards); err != nil {
			return
		}
		if !ok {
			err = errors.New("corrupted data")
			return
		}
	}

	var bbuf bytes.Buffer
	if err = r.reedSolomon.Join(&bbuf, shards, len(shards[0])*r.stripeCount); err != nil {
		return
	}

	p = bbuf.Bytes()
	if r.bytesToSkip > 0 {
		p = p[r.bytesToSkip:]
		r.bytesToSkip = 0
	}

	return
}

func (r *ErasureReader) Read(p []byte) (n int, err error) {
	plen := len(p)
	if len(r.buf) >= plen {
		copy(p, r.buf[:plen])
		n = plen
		r.buf = append([]byte{}, r.buf[plen:]...)
		return
	}

	copy(p, r.buf)
	n = len(r.buf)
	r.buf = []byte{}
	bytesToRead := plen - n

	var block []byte
	for block, err = r.read(); bytesToRead > 0 && err == nil; block, err = r.read() {
		if bytesToRead > len(block) {
			copy(p[n:], block)
			n += len(block)
			bytesToRead = plen - n
			continue
		} else {
			copy(p[n:], block[:bytesToRead])
			n += bytesToRead
			r.buf = block[bytesToRead:]
			bytesToRead = plen - n
			break
		}
	}

	if err != nil && bytesToRead != plen {
		err = io.ErrUnexpectedEOF
	}

	return
}

func (r *ErasureReader) Close() (err error) {
	var errs []error
	for i := range r.readers {
		if r.readers[i] != nil {
			if err = r.readers[i].Close(); err != nil {
				r.readers[i] = nil
				errs = append(errs, err)
			}
		}
	}

	if len(errs) >= r.parityCount {
		err = errors.New("Too many errors in reading data stripes")
		return
	}

	err = nil
	return
}
