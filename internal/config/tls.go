package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {

	tlsConfig := &tls.Config{}

	// client or server has a certificate. The client ensures that the cert comes from a server it requested. The server vice versa
	// basically both client and server needs these
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	// in a way should we not throw an error if this is empty?
	// because then server can't authenticate the client and vice versa
	if cfg.CAFile != "" {
		// reads the ca file and puts it into a cert pool. A cert pool is a collection
		// of certificates that you trust (root ca's)
		pemBytes, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		crtPool := x509.NewCertPool()
		ok := crtPool.AppendCertsFromPEM(pemBytes)
		if !ok {
			return nil, fmt.Errorf("oh god couldn't load the CA file")
		}
		// if server then set the expected client's cert to be signed by this CA
		// then set the policy for authenticating as needing to verify client certificate
		if cfg.Server {

			tlsConfig.ClientCAs = crtPool
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			// if not a server then the client needs to add the root ca file
			tlsConfig.RootCAs = crtPool
		}
		// this basically for client.
		tlsConfig.ServerName = cfg.ServerAddress
	}

	return tlsConfig, nil
}
