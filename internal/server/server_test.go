package server

import (
	"bytes"
	"context"
	"net"
	"os"
	"testing"

	api "github.com/aleBranching/proglog/api/v1"
	"github.com/aleBranching/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	scenarios := map[string]func(t *testing.T, client api.LogClient, config *Config){
		"Simple Produce, read": testProduceConsume,
		"past boundaty ":       testConsumePastBoundary,
		"test consume twice":   testConsumeTwice,
		//"stream read":          testProduceConsumeStream,
	}

	for scenario, fn := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			client, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, cfg)

		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("no")
	}
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	//client
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	dir, err := os.MkdirTemp("", "server-test")
	if err != nil {
		t.Fatal("no")
	}
	clog, err := log.NewLog(dir, log.Config{})
	if err != nil {
		t.Fatal("didnt")
	}

	cfg = &Config{CommitLog: clog}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg)
	if err != nil {
		t.Fatal("no")
	}
	go func() {
		server.Serve(l)
	}()
	client = api.NewLogClient(cc)
	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}

}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {

	ctx := context.Background()

	want := &api.Record{Value: []byte("hello world")}
	wantRequest := &api.ProduceRequest{Record: want}

	produceResponse, err := client.Produce(ctx, wantRequest)
	if err != nil {
		t.Fatal(err)
	}
	if produceResponse.Offset != uint64(0) {
		t.Fatal("didn't work")
	}
	consumeResponse, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(consumeResponse.Record.Value, wantRequest.Record.Value) {
		t.Fatal("didn't work")
	}

	if want.Offset != consumeResponse.Record.Offset {
		t.Fatal("din't work")
	}

}
func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})

	if err != nil {
		t.Fatal(err)
	}

	consResponse, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})

	if consResponse != nil {
		t.Fatal("there was a response ? ")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err : %v , want: %v", got, want)
	}

}
func testConsumeTwice(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want1 := []byte("Heloo world")
	want2 := []byte("Heloo world2")

	var err error
	_, err = client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: want1}})

	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: want2}})

	if err != nil {
		t.Fatal(err)
	}

	consResponse, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(consResponse.Record.Value, want1) {
		t.Fatal("none match")
	}
	consResponse2, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(consResponse2.Record.Value, want2) {
		t.Fatal("none match")
	}
	got := status.Code(err)
	want := status.Code(nil)
	if got != want {
		t.Fatalf("got err : %v , want: %v", got, want)
	}

}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}
	{
		stream, err := client.ProduceStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			if err != nil {
				t.Fatal(err)
			}
			prs, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}

			if prs.Offset != uint64(offset) {
				t.Fatalf("offset got %v want %v", prs.Offset, offset)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			prs, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(prs.Record.Value, record.Value) {
				t.Fatalf("offset got %v want %v", prs.Record.Value, record.Value)
			}
		}
	}
}
