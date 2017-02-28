package cmd

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"path/filepath"
)

func parsePublicCertFile(certFile string) (certs []*x509.Certificate, err error) {
	var bytes []byte

	if bytes, err = ioutil.ReadFile(certFile); err != nil {
		return certs, err
	}

	// Parse all certs in the chain.
	var block *pem.Block
	var cert *x509.Certificate
	current := bytes
	for len(current) > 0 {
		if block, current = pem.Decode(current); block == nil {
			err = fmt.Errorf("Could not read PEM block from %s", certFile)
			return certs, err
		}

		if cert, err = x509.ParseCertificate(block.Bytes); err != nil {
			return certs, err
		}

		certs = append(certs, cert)
	}

	return certs, err
}

func getRootCAs(certsCAsDir string) (*x509.CertPool, error) {
	// Get all CA file names.
	var caFiles []string
	fis, _ := ioutil.ReadDir(certsCAsDir)
	for _, fi := range fis {
		caFiles = append(caFiles, filepath.Join(certsCAsDir, fi.Name()))
	}

	if len(caFiles) == 0 {
		return nil, nil
	}

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		// In some systems like Windows, system cert pool is not supported.
		// Hence we create a new cert pool.
		rootCAs = x509.NewCertPool()
	}

	// Load custom root CAs for client requests
	for _, caFile := range caFiles {
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return rootCAs, err
		}

		rootCAs.AppendCertsFromPEM(caCert)
	}

	return rootCAs, nil
}
