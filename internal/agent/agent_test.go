package agent_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	api "github.com/aleBranching/proglog/api/v1"
	"github.com/aleBranching/proglog/internal/agent"
	"github.com/aleBranching/proglog/internal/config"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestAgent(t *testing.T) {

	ServerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CaFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	if err != nil {
		t.Fatal("aaa")
	}

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CaFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	if err != nil {
		t.Fatal("aaa")
	}

	var agents []*agent.Agent

	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		BindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rcpPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		if err != nil {
			t.Fatal("aaa")
		}

		var StartJoinAddrs []string
		if i != 0 {
			StartJoinAddrs = append(StartJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoindAddrs: StartJoinAddrs,
			BindAddr:        BindAddr,
			RCPPort:         rcpPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: ServerTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		if err != nil {
			t.Fatal("aaa")
		}

		agents = append(agents, agent)
	}
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			if err != nil {
				t.Fatal("bbbb")
			}
			err = os.RemoveAll(agent.Config.DataDir)
			if err != nil {
				t.Fatal("bbbb")
			}

		}
	}()
	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{Record: &api.Record{Value: []byte("oh no")}},
	)
	if err != nil {
		t.Fatal("cccc")
	}
	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset},
	)
	if err != nil {
		t.Fatal("cccc")
	}
	if !bytes.Equal(consumeResponse.Record.Value, []byte("oh no")) {
		t.Fatal("oh no it no match`")
	}
	time.Sleep(3 * time.Second)

	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset},
	)
	if err != nil {
		t.Fatal("cccc")
	}
	if !bytes.Equal(consumeResponse.Record.Value, []byte("oh no")) {
		t.Fatal("oh no it no match`")
	}
}

func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RCPAddr()
	if err != nil {
		t.Fatal("aaaa")
	}
	cc, err := grpc.Dial(fmt.Sprintf("%s", rpcAddr), opts...)
	if err != nil {
		t.Fatal("aaaa")
	}
	client := api.NewLogClient(cc)
	return client
}
