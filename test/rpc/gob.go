package rpc

import "io"

type gobReader struct {
	io.ReadCloser
	buf []byte
	pos int
}

func (r *gobReader) Read(p []byte) (n int, err error) {
	if r.pos == len(r.buf) {
		return r.ReadCloser.Read(p)
	}

	n = copy(p, r.buf[r.pos:])
	r.pos += n
	return n, nil
}

func (r *gobReader) ReadByte() (byte, error) {
	if r.pos == len(r.buf) {
		n, err := io.ReadFull(r.ReadCloser, r.buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			return 0, err
		}

		r.buf = r.buf[:n]
		r.pos = 0
	}

	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func newGobReader(rc io.ReadCloser) *gobReader {
	return &gobReader{
		ReadCloser: rc,
		buf:        make([]byte, 1024),
		pos:        1024,
	}
}
