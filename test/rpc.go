package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	xrpc "github.com/balamurugana/minio/test/rpc"
)

// DefaultSkewTime - skew time is 15 minutes between peer nodes.
const DefaultSkewTime = 15 * time.Minute

var errRPCRetry = fmt.Errorf("rpc: retry error")

func isNetError(err error) bool {
	if err == nil {
		return false
	}

	if uerr, isURLError := err.(*url.Error); isURLError {
		if uerr.Timeout() {
			return true
		}

		err = uerr.Err
	}

	_, isNetOpError := err.(*net.OpError)
	return isNetOpError
}

var globalRPCAPIVersion = RPCVersion{1, 0, 0}

// RPCVersion - RPC semantic version based on semver 2.0.0 https://semver.org/.
type RPCVersion struct {
	Major uint64
	Minor uint64
	Patch uint64
}

// Compare - compares given version with this version.
func (v RPCVersion) Compare(o RPCVersion) int {
	compare := func(v1, v2 uint64) int {
		if v1 == v2 {
			return 0
		}

		if v1 > v2 {
			return 1
		}

		return -1
	}

	if r := compare(v.Major, o.Major); r != 0 {
		return r
	}

	if r := compare(v.Minor, o.Minor); r != 0 {
		return r
	}

	return compare(v.Patch, o.Patch)
}

func (v RPCVersion) String() string {
	return fmt.Sprintf("%v.%v.%v", v.Major, v.Minor, v.Patch)
}

// AuthArgs - base argument for any RPC call for authentication.
type AuthArgs struct {
	RequestTime time.Time
	RPCVersion  RPCVersion
}

// Authenticate - checks if given arguments are valid to allow RPC call.
// This is xrpc.Authenticator type and is called in RPC server.
func (args AuthArgs) Authenticate() error {
	// Check whether request time is within acceptable skew time.
	utcNow := time.Now().UTC()
	if args.RequestTime.Sub(utcNow) > DefaultSkewTime || utcNow.Sub(args.RequestTime) > DefaultSkewTime {
		return fmt.Errorf("client time %v is too apart with server time %v", args.RequestTime, utcNow)
	}

	if globalRPCAPIVersion.Compare(args.RPCVersion) != 0 {
		return fmt.Errorf("version mismatch. expected: %v, received: %v", globalRPCAPIVersion, args.RPCVersion)
	}

	return nil
}

// SetAuthArgs - sets given authentication arguments to this args. This is called in RPC client.
func (args *AuthArgs) SetAuthArgs(authArgs AuthArgs) {
	*args = authArgs
}

// VoidReply - void (empty) RPC reply.
type VoidReply struct{}

// RPCClient - base RPC client.
type RPCClient struct {
	sync.RWMutex
	RPCVersion  RPCVersion
	rpcClient   *xrpc.Client
	retryTicker *time.Ticker
}

func (client *RPCClient) setRetryTicker(ticker *time.Ticker) {
	if ticker == nil {
		client.RLock()
		isNil := client.retryTicker == nil
		client.RUnlock()
		if isNil {
			return
		}
	}

	client.Lock()
	defer client.Unlock()

	if client.retryTicker != nil {
		client.retryTicker.Stop()
	}

	client.retryTicker = ticker
}

// CallWith - calls servicemethod on remote server with reader.
func (client *RPCClient) CallWith(serviceMethod string, args interface {
	SetAuthArgs(args AuthArgs)
}, reader io.Reader, reply interface{}) (body io.ReadCloser, err error) {
	lockedCall := func() (io.ReadCloser, error) {
		client.RLock()
		retryTicker := client.retryTicker
		client.RUnlock()
		if retryTicker != nil {
			select {
			case <-retryTicker.C:
			default:
				return nil, errRPCRetry
			}
		}

		// Make RPC call.
		args.SetAuthArgs(AuthArgs{time.Now().UTC(), client.RPCVersion})
		return client.rpcClient.Call(serviceMethod, args, reader, reply)
	}

	call := func() (io.ReadCloser, error) {
		body, err = lockedCall()

		if err == errRPCRetry {
			return nil, err
		}

		if isNetError(err) {
			client.setRetryTicker(time.NewTicker(xrpc.DefaultRPCTimeout))
		} else {
			client.setRetryTicker(nil)
		}

		return body, err
	}

	// If authentication error is received, retry the same call only once
	// with new authentication token.
	if body, err = call(); err == nil {
		return body, nil
	}
	if err.Error() != "authentication error" {
		return nil, err
	}

	return call()
}

// Call - calls servicemethod on remote server.
func (client *RPCClient) Call(serviceMethod string, args interface {
	SetAuthArgs(args AuthArgs)
}, reply interface{}) (err error) {
	body, err := client.CallWith(serviceMethod, args, nil, reply)
	if body != nil {
		body.Close()
	}

	return err
}

// Close - closes underneath RPC client.
func (client *RPCClient) Close() error {
	client.Lock()
	defer client.Unlock()

	return client.rpcClient.Close()
}

// NewRPCClient - returns new RPC client.
func NewRPCClient(serviceURL string, tlsConfig *tls.Config, timeout time.Duration, rpcVersion RPCVersion) *RPCClient {
	return &RPCClient{
		RPCVersion: rpcVersion,
		rpcClient:  xrpc.NewClient(serviceURL, tlsConfig, timeout),
	}
}
