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

package cmd

import (
	"crypto/x509"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/minio/minio-go/pkg/set"
)

// SetupType - enum for setup type.
type SetupType int

const (
	// FSSetupType - FS setup type enum.
	FSSetupType SetupType = 1

	// XLSetupType - XL setup type enum.
	XLSetupType = 2

	// DistXLSetupType - Distributed XL setup type enum.
	DistXLSetupType = 3
)

func (setupType SetupType) String() string {
	switch setupType {
	case FSSetupType:
		return "FS"
	case XLSetupType:
		return "XL"
	case DistXLSetupType:
		return "DistXL"
	}

	return ""
}

// Setup minio setup information
type Setup struct {
	mutex       *sync.Mutex
	serverAddr  string
	endpoints   EndpointList
	setupType   SetupType
	configDir   string
	profileMode string
	cred        credential
	profiler    interface {
		Stop()
	}
	isCacheDisabled   bool
	isBrowserDisabled bool

	serverConfig *serverConfigV14New

	publicCrtFile  string
	privateKeyFile string
	publicCerts    []*x509.Certificate
	rootCAs        *x509.CertPool
	secureConn     bool
}

// NewSetup - creates new setup based on given args.
func NewSetup(serverAddr string, args ...string) (setup Setup, err error) {
	// Check whether serverAddr is valid for this host.
	if err = CheckLocalServerAddr(serverAddr); err != nil {
		return setup, err
	}

	isServerAddrEmpty := (serverAddr == "")
	// Normalize server address.
	serverAddrHost, serverAddrPort := mustSplitHostPort(serverAddr)
	serverAddr = net.JoinHostPort(serverAddrHost, serverAddrPort)

	// For single arg, return FS setup.
	if len(args) == 1 {
		var endpoint Endpoint
		if endpoint, err = NewEndpoint(args[0]); err != nil {
			return setup, err
		} else if endpoint.Type() != PathEndpointType {
			return setup, fmt.Errorf("FS: Use Path style endpoint")
		}

		var endpoints EndpointList
		endpoints = append(endpoints, endpoint)

		return Setup{
			mutex:      &sync.Mutex{},
			serverAddr: serverAddr,
			endpoints:  endpoints,
			setupType:  FSSetupType,
		}, nil
	}

	// Convert args to endpoints
	endpoints, err := NewEndpointList(args...)
	if err != nil {
		return setup, err
	}

	// Return XL setup when all endpoints are path style.
	if endpoints[0].Type() == PathEndpointType {
		return Setup{
			mutex:      &sync.Mutex{},
			serverAddr: serverAddr,
			endpoints:  endpoints,
			setupType:  XLSetupType,
		}, nil
	}

	// Here all endpoints are URL style.

	// Error out if same path is exported by different ports on same server.
	{
		hostPathMap := make(map[string]set.StringSet)
		for _, endpoint := range endpoints {
			hostPort := endpoint.URL.Host
			path := endpoint.URL.Path
			host, _ := mustSplitHostPort(hostPort)

			pathSet, ok := hostPathMap[host]
			if !ok {
				pathSet = set.NewStringSet()
			}

			if pathSet.Contains(path) {
				return setup, fmt.Errorf("Same path can not be served from different port")
			}

			pathSet.Add(path)
			hostPathMap[host] = pathSet
		}
	}

	// Normalized args is used below if URL style endpoint is used for XL.
	newArgs := make([]string, len(args))

	// Get unique hosts.
	sset := set.NewStringSet()
	for i, endpoint := range endpoints {
		sset.Add(endpoint.URL.Host)
		newArgs[i] = endpoint.URL.Path
	}
	uniqueHosts := sset.ToSlice()

	// URL style endpoints are used for XL.
	if len(uniqueHosts) == 1 {
		endpointHost := uniqueHosts[0]
		portFound := true
		if _, _, err = net.SplitHostPort(endpointHost); err != nil && strings.Contains(err.Error(), "missing port in address") {
			portFound = false
		}
		err = nil

		host, port := mustSplitHostPort(endpointHost)
		endpointHost = net.JoinHostPort(host, port)
		if err = CheckLocalServerAddr(endpointHost); err != nil {
			if err.Error() == "host in server address should be this server" {
				return setup, fmt.Errorf("no endpoint found for this host")
			}

			return setup, err
		}

		if isServerAddrEmpty {
			serverAddr = endpointHost
		} else if portFound {
			// As serverAddr is given, serverAddr and endpoint should have same port.
			if serverAddrPort != port {
				return setup, fmt.Errorf("server address and endpoint have different ports")
			}
		}

		endpoints, _ = NewEndpointList(newArgs...)
		return Setup{
			mutex:      &sync.Mutex{},
			serverAddr: serverAddr,
			endpoints:  endpoints,
			setupType:  XLSetupType,
		}, nil
	}

	isAllEndpointLocalHost := true
	localHostPort := ""
	uniqueLocalHostSet := set.NewStringSet()
	{
		localIPs := mustGetLocalIP4()
		// Check whether at least one local endpoint should be present.
		for i, hostPort := range uniqueHosts {
			host, port := mustSplitHostPort(hostPort)
			hostIPs, err := getHostIP4(host)
			if err != nil {
				return setup, err
			}

			if !localIPs.Intersection(hostIPs).IsEmpty() {
				uniqueLocalHostSet.Add(hostPort)
				if i == 0 {
					localHostPort = port
				} else if localHostPort != port {
					isAllEndpointLocalHost = false
				}
			} else {
				isAllEndpointLocalHost = false
			}
		}
	}
	uniqueLocalHosts := uniqueLocalHostSet.ToSlice()

	// Error out if no endpoint for this server.
	if len(uniqueLocalHosts) == 0 {
		return setup, fmt.Errorf("no endpoint found for this host")
	}

	// If isAllSameLocalHost is true, then the setup is XL.
	if isAllEndpointLocalHost {
		// TODO: In this case, we bind to 0.0.0.0 ie to all interfaces.
		// The actual way to do is bind to only IPs in uniqueLocalHosts.
		endpoints, _ = NewEndpointList(newArgs...)
		serverAddr = net.JoinHostPort("", localHostPort)

		return Setup{
			mutex:      &sync.Mutex{},
			serverAddr: serverAddr,
			endpoints:  endpoints,
			setupType:  XLSetupType,
		}, nil
	}

	// This is Distribute setup.
	if len(uniqueLocalHosts) == 1 {
		host, port := mustSplitHostPort(uniqueLocalHosts[0])
		if isServerAddrEmpty {
			serverAddr = net.JoinHostPort(host, port)
		} else {
			// As serverAddr is given, serverAddr and endpoint should have same port.
			if serverAddrPort != port {
				return setup, fmt.Errorf("server address and endpoint have different ports")
			}
		}
	} else {
		// If length of uniqueLocalHosts is more than one,
		// server address should be present with same port with the same or empty hostname.
		if isServerAddrEmpty {
			return setup, fmt.Errorf("for more than one endpoints for local host with different port, server address must be provided")
		}

		found := false
		for _, host := range uniqueLocalHosts {
			_, port := mustSplitHostPort(host)
			if serverAddrPort == port {
				found = true
				break
			}
		}

		if !found {
			return setup, fmt.Errorf("port in server address does not match with local endpoints")
		}
	}

	for _, endpoint := range endpoints {
		if uniqueLocalHostSet.Contains(endpoint.URL.Host) {
			endpoint.IsLocal = true
		}
	}

	return Setup{
		mutex:      &sync.Mutex{},
		serverAddr: serverAddr,
		endpoints:  endpoints,
		setupType:  DistXLSetupType,
	}, nil
}
