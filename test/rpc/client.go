package rpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
)

// DefaultRPCTimeout - default RPC timeout is one minute.
const DefaultRPCTimeout = 1 * time.Minute

// Client - http based RPC client.
type Client struct {
	httpClient *http.Client
	serviceURL string
}

// Call - calls service method on RPC server.
func (client *Client) Call(serviceMethod string, args interface{}, reader io.Reader, reply interface{}) (io.ReadCloser, error) {
	replyKind := reflect.TypeOf(reply).Kind()
	if replyKind != reflect.Ptr {
		return nil, fmt.Errorf("rpc reply must be a pointer type, but found %v", replyKind)
	}

	argsBuf := bufPool.Get()
	defer bufPool.Put(argsBuf)
	if err := gob.NewEncoder(argsBuf).Encode(args); err != nil {
		return nil, err
	}

	callRequest := CallRequest{
		Method:   serviceMethod,
		ArgBytes: argsBuf.Bytes(),
	}

	requestBuf := bufPool.Get()
	defer bufPool.Put(requestBuf)
	if err := gob.NewEncoder(requestBuf).Encode(callRequest); err != nil {
		return nil, err
	}

	var body io.Reader = requestBuf
	if reader != nil {
		body = io.MultiReader(body, reader)
	}

	response, err := client.httpClient.Post(client.serviceURL, "", body)
	response.Body = NewDrainReader(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, fmt.Errorf("%v rpc call failed with error code %v", serviceMethod, response.StatusCode)
	}

	var callResponse CallResponse
	if err := gob.NewDecoder(newGobReader(response.Body)).Decode(&callResponse); err != nil {
		response.Body.Close()
		return nil, err
	}

	if callResponse.Error != "" {
		response.Body.Close()
		return nil, errors.New(callResponse.Error)
	}

	return response.Body, gob.NewDecoder(bytes.NewReader(callResponse.ReplyBytes)).Decode(reply)
}

// Close - does nothing and presents for interface compatibility.
func (client *Client) Close() error {
	return nil
}

func newCustomDialContext(timeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout:   timeout,
			KeepAlive: timeout,
			DualStack: true,
		}

		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		return xhttp.NewTimeoutConn(conn, timeout, timeout), nil
	}
}

// NewClient - returns new RPC client.
func NewClient(serviceURL string, tlsConfig *tls.Config, timeout time.Duration) *Client {
	return &Client{
		httpClient: &http.Client{
			// Transport is exactly same as Go default in https://golang.org/pkg/net/http/#RoundTripper
			// except custom DialContext and TLSClientConfig.
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				DialContext:           newCustomDialContext(timeout),
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				TLSClientConfig:       tlsConfig,
			},
		},
		serviceURL: serviceURL,
	}
}
