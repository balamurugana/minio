package rpc

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestClientCall(t *testing.T) {
	rpcServer := NewServer()
	if err := rpcServer.RegisterName("Arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcServer.ServeHTTP(w, r)
	}))
	defer httpServer.Close()

	rpcClient := NewClient(httpServer.URL, nil, DefaultRPCTimeout)

	var reply int
	var boolReply bool
	var intArg int

	testCases := []struct {
		serviceMethod string
		args          interface{}
		reply         interface{}
		expectErr     bool
	}{
		{"Arith.Multiply", Args{7, 8}, &reply, false},
		{"Arith.Multiply", &Args{7, 8}, &reply, false},
		// rpc reply must be a pointer type but found int error.
		{"Arith.Multiply", &Args{7, 8}, reply, true},
		// gob: type mismatch in decoder: want struct type rpc.Args; got non-struct error.
		{"Arith.Multiply", intArg, &reply, true},
		// gob: decoding into local type *bool, received remote type int error.
		{"Arith.Multiply", &Args{7, 8}, &boolReply, true},
	}

	for i, testCase := range testCases {
		body, err := rpcClient.Call(testCase.serviceMethod, testCase.args, nil, testCase.reply)
		if body != nil {
			body.Close()
		}

		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestClientCallInputStream(t *testing.T) {
	rpcServer := NewServer()
	if err := rpcServer.RegisterName("Arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcServer.ServeHTTP(w, r)
	}))
	defer httpServer.Close()

	rpcClient := NewClient(httpServer.URL, nil, DefaultRPCTimeout)

	testCases := []struct {
		serviceMethod string
		args          interface{}
		reader        io.Reader
		expectedReply int
		expectErr     bool
	}{
		{"Arith.MultiplyInputStream", Args{7, 8}, strings.NewReader("multiply reader request"), 7 * 8, false},
		{"Arith.MultiplyInputStream", &Args{7, 8}, strings.NewReader("multiply reader request"), 7 * 8, false},
		// error "expected: message: multiply reader request, got: foo bar"
		{"Arith.MultiplyInputStream", Args{7, 8}, strings.NewReader("foo bar"), 0, true},
	}

	for i, testCase := range testCases {
		var reply int
		body, err := rpcClient.Call(testCase.serviceMethod, testCase.args, testCase.reader, &reply)
		if body != nil {
			body.Close()
		}

		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if reply != testCase.expectedReply {
				t.Fatalf("case %v: reply: expected: %v, got: %v", i+1, testCase.expectedReply, reply)
			}
		}
	}
}

func TestClientCallOutputStream(t *testing.T) {
	rpcServer := NewServer()
	if err := rpcServer.RegisterName("Arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcServer.ServeHTTP(w, r)
	}))
	defer httpServer.Close()

	rpcClient := NewClient(httpServer.URL, nil, DefaultRPCTimeout)

	var buf bytes.Buffer
	for i := 0; i < 3*5; i++ {
		buf.Write([]byte("reply"))
	}

	testCases := []struct {
		serviceMethod  string
		args           interface{}
		expectedReply  int
		expectedResult string
		expectErr      bool
	}{
		{"Arith.MultiplyOutputStream", Args{3, 5}, 3 * 5, buf.String(), false},
		{"Arith.MultiplyOutputStream", &Args{3, 5}, 3 * 5, buf.String(), false},
	}

	for i, testCase := range testCases {
		var reply int
		body, err := rpcClient.Call(testCase.serviceMethod, testCase.args, nil, &reply)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if reply != testCase.expectedReply {
				t.Fatalf("case %v: reply: expected: %v, got: %v", i+1, testCase.expectedReply, reply)
			}

			result, err := ioutil.ReadAll(body)
			if err != nil {
				t.Fatalf("case %v: unexpected error %v", i+1, err)
			}

			if string(result) != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, string(result))
			}

			body.Close()
		}
	}
}

func TestClientCallIOStream(t *testing.T) {
	rpcServer := NewServer()
	if err := rpcServer.RegisterName("Arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcServer.ServeHTTP(w, r)
	}))
	defer httpServer.Close()

	rpcClient := NewClient(httpServer.URL, nil, DefaultRPCTimeout)

	var buf bytes.Buffer
	for i := 0; i < 3*5; i++ {
		buf.Write([]byte("foo bar"))
	}

	testCases := []struct {
		serviceMethod  string
		args           interface{}
		reader         io.Reader
		expectedReply  int
		expectedResult []byte
		expectErr      bool
	}{
		{"Arith.MultiplyIOStream", Args{3, 5}, strings.NewReader("foo bar"), 3 * 5, buf.Bytes(), false},
		{"Arith.MultiplyIOStream", &Args{3, 5}, strings.NewReader("foo bar"), 3 * 5, buf.Bytes(), false},
	}

	for i, testCase := range testCases {
		var reply int
		body, err := rpcClient.Call(testCase.serviceMethod, testCase.args, testCase.reader, &reply)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if reply != testCase.expectedReply {
				t.Fatalf("case %v: reply: expected: %v, got: %v", i+1, testCase.expectedReply, reply)
			}

			result, err := ioutil.ReadAll(body)
			if err != nil {
				t.Fatalf("case %v: unexpected error %v", i+1, err)
			}

			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}

			body.Close()
		}
	}
}
