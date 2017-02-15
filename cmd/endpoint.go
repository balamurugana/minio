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
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"

	"github.com/minio/minio-go/pkg/set"
)

// IsValidDistribution - checks whether given count is a valid distribution for erasure coding.
func IsValidDistribution(count int) bool {
	return (count >= 4 && count <= 16 && count%2 == 0)
}

// CheckLocalServerAddr - checks if serverAddr is valid and local host.
func CheckLocalServerAddr(serverAddr string) error {
	host, port, err := net.SplitHostPort(serverAddr)
	if err != nil {
		return err
	}

	// Check whether port is a valid port number.
	p, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("invalid port number")
	} else if p < 1 {
		return fmt.Errorf("port number should be greater than zero")
	}

	if host != "" {
		localIPs := mustGetLocalIP4()
		hostIPs, err := getHostIP4(host)
		if err != nil {
			return err
		}

		if localIPs.Intersection(hostIPs).IsEmpty() {
			return fmt.Errorf("host in server address should be this server")
		}
	}

	return nil
}

// IsEmptyPath - check whether given path is not empty.
func IsEmptyPath(path string) bool {
	return path == "" || path == "." || path == "/" || path == `\`
}

// EndpointType - enum for endpoint type.
type EndpointType int

const (
	// PathEndpointType - path style endpoint type enum.
	PathEndpointType EndpointType = 1

	// URLEndpointType - URL style endpoint type enum.
	URLEndpointType = 2
)

// Endpoint - any type of endpoint.
type Endpoint struct {
	Value   string
	URL     *url.URL
	IsLocal bool
}

// Type - returns type of endpoint.
func (endpoint Endpoint) Type() EndpointType {
	if endpoint.URL != nil {
		return URLEndpointType
	}

	return PathEndpointType
}

// NewEndpoint - returns new endpoint based on given arguments.
func NewEndpoint(arg string) (Endpoint, error) {
	if IsEmptyPath(arg) {
		return Endpoint{}, fmt.Errorf("Empty or root endpoint is not supported")
	}

	u, err := url.Parse(arg)
	if err == nil && u.Host != "" {
		// URL style of endpoint.
		if !((u.Scheme == "http" || u.Scheme == "https") &&
			u.User == nil && u.Opaque == "" && u.ForceQuery == false && u.RawQuery == "" && u.Fragment == "") {
			return Endpoint{}, fmt.Errorf("Unknown endpoint format")
		}

		host, port, err := splitHostPort(u.Host)
		if !(err == nil && host != "") {
			return Endpoint{}, fmt.Errorf("Invalid host in endpoint format")
		}

		if IsEmptyPath(u.Path) {
			return Endpoint{}, fmt.Errorf("Empty or root path is not supported in URL endpoint")
		}

		u.Host = net.JoinHostPort(host, port)
		arg = u.String()
	} else {
		u = nil
	}

	return Endpoint{
		Value: arg,
		URL:   u,
	}, nil
}

// EndpointList - list of same type of endpoint.
type EndpointList []Endpoint

func (endpoints EndpointList) changeScheme(scheme string) {
	for _, endpoint := range endpoints {
		if endpoint.Type() == URLEndpointType {
			endpoint.URL.Scheme = scheme
		}
	}
}

// SetSSL - sets scheme to https
func (endpoints EndpointList) SetSSL() {
	endpoints.changeScheme("https")
}

// SetNonSSL - sets scheme to http
func (endpoints EndpointList) SetNonSSL() {
	endpoints.changeScheme("http")
}

// GetRemoteHosts - get remote hosts.
func (endpoints EndpointList) GetRemoteHosts() []string {
	hostSet := set.NewStringSet()
	for _, endpoint := range endpoints {
		if endpoint.Type() == URLEndpointType && !endpoint.IsLocal {
			// TODO: fix remote host duplication ie example.org and 93.184.216.34 points to same remote host.
			hostSet.Add(endpoint.URL.Host)
		}
	}

	return hostSet.ToSlice()
}

// NewEndpointList - returns new endpoint list based on input args.
func NewEndpointList(args ...string) (endpoints EndpointList, err error) {
	// Check whether given args contain duplicates.
	if uniqueArgs := set.CreateStringSet(args...); len(uniqueArgs) != len(args) {
		return nil, fmt.Errorf("duplicate endpoints found")
	}

	// Check whether no. of args are valid for XL distribution.
	if !IsValidDistribution(len(args)) {
		return nil, fmt.Errorf("total endpoints %d found. For XL/Distribute, it should be 4, 6, 8, 10, 12, 14 or 16", len(args))
	}

	sort.Strings(args)

	var endpointType EndpointType
	var scheme string

	// Loop through args and adds to endpoint list.
	for i, arg := range args {
		endpoint, err := NewEndpoint(arg)
		if err != nil {
			return nil, fmt.Errorf("unknown endpoint format %s", arg)
		}

		// All endpoints have to be same type and scheme if applicable.
		if i == 0 {
			endpointType = endpoint.Type()
			if endpoint.URL != nil {
				scheme = endpoint.URL.Scheme
			}
		} else if endpoint.Type() != endpointType {
			return nil, fmt.Errorf("mixed style endpoints are not supported")
		} else if endpoint.URL != nil && scheme != endpoint.URL.Scheme {
			return nil, fmt.Errorf("mixed scheme is not supported")
		}

		endpoints = append(endpoints, endpoint)
	}

	return endpoints, nil
}
