/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package http

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

var sslRequiredErrMsg = []byte("HTTP/1.0 403 Forbidden\r\n\r\nSSL required")
var httpRequiredErrMsg = []byte("HTTP/1.0 403 Forbidden\r\n\r\nPlain-Text required")

// The value chosen below is longest word chosen
// from all the http verbs comprising of
// "PRI", "OPTIONS", "GET", "HEAD", "POST",
// "PUT", "DELETE", "TRACE", "CONNECT".
const (
	maxHTTPMethodLen = 7
)

// HTTP2 PRI method.
var httpMethodPRI = "PRI"

var defaultHTTP2Methods = []string{
	httpMethodPRI,
}

var defaultHTTP1Methods = []string{
	http.MethodOptions,
	http.MethodGet,
	http.MethodHead,
	http.MethodPost,
	http.MethodPut,
	http.MethodDelete,
	http.MethodTrace,
	http.MethodConnect,
}

type acceptResult struct {
	conn net.Conn
	err  error
}

// httpListener - HTTP listener capable of handling multiple server addresses.
type httpListener struct {
	mutex                  sync.Mutex         // to guard Close() method.
	tcpListeners           []*net.TCPListener // underlaying TCP listeners.
	acceptCh               chan acceptResult  // channel where all TCP listeners write accepted connection.
	doneChs                []chan struct{}    // done channels for each goroutine for TCP listeners.
	tlsConfig              *tls.Config        // TLS configuration
	tcpKeepAliveTimeout    time.Duration
	readTimeout            time.Duration
	writeTimeout           time.Duration
	updateBytesReadFunc    func(int)                           // function to be called to update bytes read in bufConn.
	updateBytesWrittenFunc func(int)                           // function to be called to update bytes written in bufConn.
	errorLogFunc           func(error, string, ...interface{}) // function to be called on errors.
}

// start - starts separate goroutine for each TCP listener.  A valid insecure/TLS HTTP new connection is passed to httpListener.acceptCh.
func (listener *httpListener) start() {
	listener.acceptCh = make(chan acceptResult)

	// Closure to send acceptResult to acceptCh.
	// It returns true if the result is sent else false if returns when doneCh is closed.
	send := func(result acceptResult, doneCh <-chan struct{}) bool {
		select {
		case listener.acceptCh <- result:
			// Successfully written to acceptCh
			return true
		case <-doneCh:
			// As stop signal is received, close accepted connection.
			if result.conn != nil {
				result.conn.Close()
			}
			return false
		}
	}

	// Guess TLS protocol returns true if HTTP1.1 or
	// HTTP2 methods are not found in the input buffer.
	//
	// This function if it returns true doesn't mean
	// buffer has indeed TLS data, which needs
	// to be further validated by using tls handshake.
	guessTLSProtocol := func(buf []byte) bool {
		// Check for HTTP2 methods first.
		for _, m := range defaultHTTP2Methods {
			if strings.HasPrefix(string(buf), m) {
				return false
			}
		}

		// Check for HTTP1 methods.
		for _, m := range defaultHTTP1Methods {
			if strings.HasPrefix(string(buf), m) {
				return false
			}
		}

		return true
	}

	// Closure to accept single connection.
	acceptTCP := func(tcpConn *net.TCPConn, doneCh <-chan struct{}) {
		// Tune accepted TCP connection.
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(listener.tcpKeepAliveTimeout)

		bufconn := newBufConn(tcpConn, listener.readTimeout, listener.writeTimeout,
			listener.updateBytesReadFunc, listener.updateBytesWrittenFunc)

		// Peek bytes of maximum length of all HTTP methods.
		data, err := bufconn.Peek(maxHTTPMethodLen)
		if err != nil {
			if listener.errorLogFunc != nil {
				listener.errorLogFunc(err,
					"Error in reading from new connection %s at server %s",
					bufconn.RemoteAddr(), bufconn.LocalAddr())
			}
			bufconn.Close()
			return
		}

		if !guessTLSProtocol(data) {
			// Return bufconn if read data is a valid HTTP method.
			if listener.tlsConfig != nil {
				// As TLS is configured and we got plain text HTTP request,
				// return 403 (forbidden) error.
				bufconn.Write(sslRequiredErrMsg)
				bufconn.Close()
				return
			}
			send(acceptResult{bufconn, nil}, doneCh)
			return
		}

		if listener.tlsConfig == nil {
			// Client is TLS and server is not TLS configured.
			// Can't send any error here, since client will not
			// be able to interpret it on an attempted TLS conn.
			if listener.errorLogFunc != nil {
				listener.errorLogFunc(errors.New(""),
					"Server %s not configured to support TLS request from %s",
					bufconn.LocalAddr(), bufconn.RemoteAddr())
			}
			bufconn.Close()
			return
		}

		// As the listener is configured with TLS, try to do TLS
		// handshake, drop the connection if it fails.
		tlsConn := tls.Server(bufconn, listener.tlsConfig)
		if err = tlsConn.Handshake(); err != nil {
			if listener.errorLogFunc != nil {
				listener.errorLogFunc(err,
					"TLS handshake failed with new connection %s at server %s",
					bufconn.RemoteAddr(), bufconn.LocalAddr())
			}
			bufconn.Close()
			return
		}

		// Accept the new TLS connection.
		send(acceptResult{newBufConn(tlsConn,
			listener.readTimeout,
			listener.writeTimeout,
			listener.updateBytesReadFunc,
			listener.updateBytesWrittenFunc), nil}, doneCh)
	}

	// Closure to handle new connections till done channel is not closed.
	handleConnection := func(tcpListener *net.TCPListener, doneCh <-chan struct{}) {
		for {
			tcpConn, err := tcpListener.AcceptTCP()
			if err != nil {
				// Returns when send fails.
				if !send(acceptResult{nil, err}, doneCh) {
					return
				}
				continue
			}
			go acceptTCP(tcpConn, doneCh)
		}
	}

	// Start separate goroutine for each TCP listener to handle connection.
	for _, tcpListener := range listener.tcpListeners {
		doneCh := make(chan struct{})
		go handleConnection(tcpListener, doneCh)
		listener.doneChs = append(listener.doneChs, doneCh)
	}
}

// Accept - reads from httpListener.acceptCh for one of previously accepted TCP connection and returns the same.
func (listener *httpListener) Accept() (conn net.Conn, err error) {
	result, ok := <-listener.acceptCh
	if ok {
		return result.conn, result.err
	}

	return nil, io.EOF
}

// Close - closes underneath all TCP listeners.
func (listener *httpListener) Close() (err error) {
	listener.mutex.Lock()
	defer listener.mutex.Unlock()
	if listener.doneChs == nil {
		return nil
	}

	wg := &sync.WaitGroup{}
	for i := range listener.tcpListeners {
		wg.Add(1)
		go func(tcpListener *net.TCPListener, doneCh chan<- struct{}) {
			tcpListener.Close()
			close(doneCh)
			wg.Done()
		}(listener.tcpListeners[i], listener.doneChs[i])
	}
	wg.Wait()

	listener.doneChs = nil
	return nil
}

// Addr - net.Listener interface compatible method returns net.Addr.  In case of multiple TCP listeners, it returns '0.0.0.0' as IP address.
func (listener *httpListener) Addr() (addr net.Addr) {
	addr = listener.tcpListeners[0].Addr()
	if len(listener.tcpListeners) == 1 {
		return addr
	}

	tcpAddr := addr.(*net.TCPAddr)
	if ip := net.ParseIP("0.0.0.0"); ip != nil {
		tcpAddr.IP = ip
	}

	addr = tcpAddr
	return addr
}

// Addrs - returns all address information of TCP listeners.
func (listener *httpListener) Addrs() (addrs []net.Addr) {
	for i := range listener.tcpListeners {
		addrs = append(addrs, listener.tcpListeners[i].Addr())
	}

	return addrs
}

// newHTTPListener - creates new httpListener object which is interface compatible to net.Listener.
// httpListener is capable to
// * listen to multiple addresses
// * controls incoming connections only doing HTTP protocol
func newHTTPListener(serverAddrs []string,
	tlsConfig *tls.Config,
	tcpKeepAliveTimeout time.Duration,
	readTimeout time.Duration,
	writeTimeout time.Duration,
	updateBytesReadFunc func(int),
	updateBytesWrittenFunc func(int),
	errorLogFunc func(error, string, ...interface{})) (listener *httpListener, err error) {

	var tcpListeners []*net.TCPListener
	// Close all opened listeners on error
	defer func() {
		if err == nil {
			return
		}

		for _, tcpListener := range tcpListeners {
			// Ignore error on close.
			tcpListener.Close()
		}
	}()

	for _, serverAddr := range serverAddrs {
		var l net.Listener
		if l, err = net.Listen("tcp", serverAddr); err != nil {
			return nil, err
		}

		tcpListener, ok := l.(*net.TCPListener)
		if !ok {
			return nil, errors.New("unexpected assertion failure to get net.TCPListener")
		}

		tcpListeners = append(tcpListeners, tcpListener)
	}

	listener = &httpListener{
		tcpListeners:           tcpListeners,
		tlsConfig:              tlsConfig,
		tcpKeepAliveTimeout:    tcpKeepAliveTimeout,
		readTimeout:            readTimeout,
		writeTimeout:           writeTimeout,
		updateBytesReadFunc:    updateBytesReadFunc,
		updateBytesWrittenFunc: updateBytesWrittenFunc,
		errorLogFunc:           errorLogFunc,
	}
	listener.start()

	return listener, nil
}
