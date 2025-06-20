package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/aleBranching/proglog/internal/auth"
	"github.com/aleBranching/proglog/internal/discovery"
	"github.com/aleBranching/proglog/internal/log"
	"github.com/aleBranching/proglog/internal/server"
	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	// for serf
	BindAddr string
	// for grpc
	RCPPort         int
	NodeName        string
	StartJoindAddrs []string
	ACLModelFile    string
	ACLPolicyFile   string
	// raft
	Bootstrap bool
}
type Agent struct {
	Config

	// Different services
	mux        cmux.CMux
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership
	// replicator *log.Replicator

	// shutdown stuff
	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func (c Config) RCPAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RCPPort), nil
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go a.serve()
	return a, nil
}

func (a *Agent) setupMux() error {
	rcpAddr := fmt.Sprintf(":%d", a.Config.RCPPort)
	ln, err := net.Listen("tcp", rcpAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)
	return nil
}

func (a *Agent) setupLogger() error {
	// logger, err
	return nil

}

// Before mux
// func (a *Agent) setupLog() error {
// 	var err error

// 	a.log, err = log.NewLog(a.Config.DataDir, log.Config{})
// 	return err
// }

func (a *Agent) setupLog() error {
	raftLn := a.mux.Match(func(r io.Reader) bool {
		b := make([]byte, 1)
		if _, err := r.Read(b); err != nil {
			return false
		}
		return bytes.Equal(b, []byte{byte(log.RaftRCP)})
	})

	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(raftLn, a.Config.ServerTLSConfig, a.Config.PeerTLSConfig)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap
	var err error
	a.log, err = log.NewDistributedLog(a.Config.DataDir, logConfig)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		return a.log.WaitForLeader(3 * time.Second)
	}
	return nil
}

func (a *Agent) setupServer() error {
	authorizer, err := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)
	if err != nil {
		return err
	}
	serverConfig := &server.Config{
		CommitLog:   a.log,
		Authorizer:  authorizer,
		GetServerer: a.log,
	}

	var opts []grpc.ServerOption

	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	// rcpAddr, err := a.RCPAddr()
	// if err != nil {
	// 	return err
	// }

	grpcLn := a.mux.Match(cmux.Any())
	// grpcLn, err := net.Listen("tcp", rcpAddr)
	// if err != nil {
	// 	return err
	// }

	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}

	}()
	return err
}

// before consensus
// func (a *Agent) setupMembership() error {
// 	rcpAddr, err := a.Config.RCPAddr()
// 	if err != nil {
// 		return err
// 	}
// 	var opts []grpc.DialOption
// 	if a.Config.PeerTLSConfig != nil {
// 		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(a.Config.PeerTLSConfig)))
// 	}

// 	conn, err := grpc.Dial(rcpAddr, opts...)
// 	if err != nil {
// 		return err
// 	}
// 	client := api.NewLogClient(conn)
// 	a.replicator = &log.Replicator{
// 		DialOptions: opts,
// 		LocalServer: client,
// 	}
// 	a.membership, err = discovery.New(a.replicator, discovery.Config{
// 		NodeName: a.Config.NodeName,
// 		BindAddr: a.Config.BindAddr,
// 		Tags: map[string]string{
// 			"rpc_addr": rcpAddr,
// 		},
// 		StartJoinAddrs: a.Config.StartJoindAddrs,
// 	})

// 	return err

// }
func (a *Agent) setupMembership() error {
	rcpAddr, err := a.Config.RCPAddr()
	if err != nil {
		return err
	}
	a.membership, err = discovery.New(a.log, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rcpAddr,
		},
		StartJoinAddrs: a.Config.StartJoindAddrs,
	})

	return err
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)
	shutdown := []func() error{
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
