/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"crypto/tls"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	serverShutdownPoll = 500 * time.Millisecond
)

// Current number of concurrent http requests
var globalActiveReqs int32
var globalInShutdown int32

// Server - the main mux server
type Server struct {
	*http.Server

	// Time to wait before forcing server shutdown
	gracefulTimeout time.Duration
}

// NewServer constructor to create a Server
func NewServer(addr string, handler http.Handler) *Server {
	m := &Server{
		Server: &http.Server{
			Addr:    addr,
			Handler: handler,
			TLSConfig: &tls.Config{
				// Causes servers to use Go's default ciphersuite preferences,
				// which are tuned to avoid attacks. Does nothing on clients.
				PreferServerCipherSuites: true,
				// Set minimum version to TLS 1.2
				MinVersion: tls.VersionTLS12,
			}, // Always instantiate.
		},
		// Wait for 5 seconds for new incoming connnections, otherwise
		// forcibly close them during graceful stop or restart.
		gracefulTimeout: 5 * time.Second,
	}

	// Returns configured HTTP server.
	return m
}

type connRequestHandler struct {
	handler http.Handler
}

func setConnRequestHandler(h http.Handler) http.Handler {
	return connRequestHandler{handler: h}
}

func (c connRequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt32(&globalInShutdown) == 1 {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	// Execute registered handlers, update currentReqs to keep
	// track of concurrent requests processing on the server
	atomic.AddInt32(&globalActiveReqs, 1)
	defer atomic.AddInt32(&globalActiveReqs, -1)

	c.handler.ServeHTTP(w, r)
}

// ListenAndServe - serve HTTP requests with protocol multiplexing support
// TLS is actived when certFile and keyFile parameters are not empty.
func (m *Server) ListenAndServe() (err error) {
	go handleServiceSignals()

	certFile, keyFile := getPublicCertFile(), getPrivateKeyFile()
	if globalIsSSL {
		return m.ListenAndServeTLS(certFile, keyFile)
	}

	return m.Server.ListenAndServe()
}

// Close initiates the graceful shutdown
func (m *Server) Close() error {
	atomic.AddInt32(&globalInShutdown, 1)
	defer atomic.AddInt32(&globalInShutdown, -1)

	// Starting graceful shutdown. Check if all requests are finished
	// in regular interval or force the shutdown
	ticker := time.NewTicker(serverShutdownPoll)
	defer ticker.Stop()
	for {
		select {
		case <-time.After(m.gracefulTimeout):
			return nil
		case <-ticker.C:
			if atomic.LoadInt32(&globalActiveReqs) <= 0 {
				return nil
			}
		}
	}
}
