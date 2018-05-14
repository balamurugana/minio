package cmd

import (
	"context"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/quorum"
)

type PeerClients struct {
	RPCClients     []*PeerRPCClient
	MaxExecTime    time.Duration
	MaxSuccessWait time.Duration
}

func (clients *PeerClients) writeQuorumCall(functions []quorum.Func) error {
	return callFunctions(functions, len(functions), clients.MaxExecTime, clients.MaxSuccessWait, false)
}

// GetPeerRPCClient - returns PeerRPCClient of addr.
func (clients *PeerClients) GetPeerRPCClient(host string) *PeerRPCClient {
	for _, rpcClient := range clients.RPCClients {
		if url := rpcClient.ServiceURL(); url.Host == host {
			return rpcClient
		}
	}

	return nil
}

// DeleteBucket - calls delete bucket RPC.
func (clients *PeerClients) DeleteBucket(bucketName string) error {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.RPCClients[i].DeleteBucket(bucketName); err != nil {
					errch <- quorum.Error{Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// SetBucketPolicy - calls set bucket policy RPC.
func (clients *PeerClients) SetBucketPolicy(bucketName string, bucketPolicy *policy.Policy) error {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.RPCClients[i].SetBucketPolicy(bucketName, bucketPolicy); err != nil {
					errch <- quorum.Error{Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// RemoveBucketPolicy - calls remove bucket policy RPC.
func (clients *PeerClients) RemoveBucketPolicy(bucketName string) error {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.RPCClients[i].RemoveBucketPolicy(bucketName); err != nil {
					errch <- quorum.Error{Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// PutBucketNotification - calls put bukcet notification RPC.
func (clients *PeerClients) PutBucketNotification(bucketName string, rulesMap event.RulesMap) error {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.RPCClients[i].PutBucketNotification(bucketName, rulesMap); err != nil {
					errch <- quorum.Error{Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// ListenBucketNotification - calls listen bucket notification RPC.
func (clients *PeerClients) ListenBucketNotification(bucketName string, eventNames []event.Name,
	pattern string, targetID event.TargetID, addr xnet.Host) error {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.RPCClients[i].ListenBucketNotification(bucketName, eventNames, pattern, targetID, addr); err != nil {
					errch <- quorum.Error{Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// SetCredentials - calls set credentials RPC.
func (clients *PeerClients) SetCredentials(credentials auth.Credentials) error {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.RPCClients[i].SetCredentials(credentials); err != nil {
					errch <- quorum.Error{Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// NewPeerClients - creates peer clients for given endpoint list.
func NewPeerClients(endpoints EndpointList) *PeerClients {
	rpcClients := []*PeerRPCClient{}
	for _, hostStr := range GetRemotePeers(endpoints) {
		host, err := xnet.ParseHost(hostStr)
		logger.CriticalIf(context.Background(), err)
		rpcClient, err := NewPeerRPCClient(host)
		logger.CriticalIf(context.Background(), err)
		rpcClients = append(rpcClients, rpcClient)
	}

	return &PeerClients{
		RPCClients:     rpcClients,
		MaxExecTime:    30 * time.Second,
		MaxSuccessWait: 1 * time.Second,
	}
}
