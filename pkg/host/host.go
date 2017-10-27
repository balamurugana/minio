package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// Host - holds network host and its port.
type Host struct {
	Host      string
	Port      uint16
	isPortSet bool
}

// IsEmpty - returns whether Host is empty or not
func (hp Host) IsEmpty() bool {
	return hp.Host == ""
}

// String - returns string representation of Host.
func (hp Host) String() string {
	if !hp.isPortSet {
		return hp.Host
	}

	return fmt.Sprintf("%v:%v", hp.Host, hp.Port)
}

// MarshalJSON - converts Host into JSON data
func (hp Host) MarshalJSON() ([]byte, error) {
	return json.Marshal(hp.String())
}

// UnmarshalJSON - parses data into Host.
func (hp *Host) UnmarshalJSON(data []byte) (err error) {
	var s string
	if err = json.Unmarshal(data, &s); err != nil {
		return err
	}

	var h *Host
	if h, err = Parse(s); err != nil {
		return err
	}

	*hp = *h
	return nil
}

// Parse - parses string into Host
func Parse(s string) (h *Host, err error) {
	isValidHost := func(host string) bool {
		if host == "" {
			return false
		}

		if ip := net.ParseIP(host); ip != nil {
			return true
		}

		// host is not a valid IPv4 or IPv6 address
		// host may be a hostname
		// refer https://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_host_names
		// why checks are done like below
		if len(host) < 1 || len(host) > 253 {
			return false
		}

		for _, label := range strings.Split(host, ".") {
			if len(label) < 1 || len(label) > 63 {
				return false
			}

			hostLabelRegexp, err := regexp.Compile("^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$")
			if err != nil {
				panic(err)
			}

			if !hostLabelRegexp.MatchString(label) {
				return false
			}
		}

		return true
	}

	isPortSet := true
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		if !strings.Contains(err.Error(), "missing port in address") {
			return nil, err
		}

		host = s
		portStr = ""
		isPortSet = false
	}

	port := 0
	if isPortSet {
		if port, err = strconv.Atoi(portStr); err != nil {
			return nil, errors.New("invalid port number")
		}

		if port < 0 && port > 65535 {
			return nil, errors.New("port number out of range")
		}
	}

	if !isValidHost(host) {
		return nil, errors.New("invalid hostname")
	}

	return &Host{
		Host:      host,
		Port:      uint16(port),
		isPortSet: isPortSet,
	}, nil
}
