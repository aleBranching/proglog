package loadbalance_test

import (
	"net"
	"net/url"
	"reflect"
	"testing"

	"github.com/aleBranching/proglog/internal/config"
	"github.com/aleBranching/proglog/internal/loadbalance"
	"github.com/aleBranching/proglog/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/aleBranching/proglog/api/v1"
)

func TestResolver(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("oh no")
	}

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CaFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})

	if err != nil {
		t.Fatal("oh no")
	}
	serverCreds := credentials.NewTLS(tlsConfig)

	srv, err := server.NewGRPCServer(&server.Config{GetServerer: &getServers{}}, grpc.Creds(serverCreds))
	if err != nil {
		t.Fatal("oh no")
	}

	// sets up a server with mock GetServerer
	go srv.Serve(l)

	conn := &clientConn{}
	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CaFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})

	if err != nil {
		t.Fatal("oh no")
	}
	clientCreds := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{
		DialCreds: clientCreds,
	}
	r := &loadbalance.Resolver{}
	// url , err := url.Parse(l.Addr().Network())
	// if err != nil {
	// 	t.Fatal("oh no")
	// }
	url := url.URL{
		Path: l.Addr().String(),
	}
	_, err = r.Build(
		resolver.Target{
			// Endpoint: l.Addr().String(),
			URL: url,
		},
		conn,
		opts,
	)
	if err != nil {
		t.Fatal("oh no")
	}

	wantState := resolver.State{
		Addresses: []resolver.Address{{
			Addr:       "localhost:9001",
			Attributes: attributes.New("is_leader", true),
		}, {

			Addr:       "localhost:9002",
			Attributes: attributes.New("is_leader", false),
		}},
	}

	if !reflect.DeepEqual(wantState, conn.state) {
		t.Fatal("they don't match")
	}

	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})

	if !reflect.DeepEqual(wantState, conn.state) {
		t.Fatal("they don't match")
	}
}

// Mocks

type getServers struct{}

func (s *getServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{
		{
			Id:       "leader",
			RpcAddr:  "localhost:9001",
			IsLeader: true,
		},
		{
			Id:      "follower",
			RpcAddr: "localhost:9002",
		},
	}, nil
}

type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}
func (c *clientConn) ReportError(err error)               {}
func (c *clientConn) NewAddress(addrs []resolver.Address) {}
func (c *clientConn) NewServiceConfig(config string)      {}

func (c *clientConn) ParseServiceConfig(config string) *serviceconfig.ParseResult {
	return nil
}
