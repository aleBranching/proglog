package server

import (
	"bytes"
	"context"
	"net"
	"os"
	"testing"
	"time"

	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	// "time"

	api "github.com/aleBranching/proglog/api/v1"
	"github.com/aleBranching/proglog/internal/auth"
	"github.com/aleBranching/proglog/internal/config"
	"github.com/aleBranching/proglog/internal/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	// "go.opentelemetry.io/otel"
	// "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	// "go.opentelemetry.io/otel/sdk/resource"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdkTrace "go.opentelemetry.io/otel/sdk/trace"

	// semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	stdLog "log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	scenarios := map[string]func(t *testing.T, rootClient api.LogClient, nobodyClient api.LogClient, config *Config){
		"Simple Produce, read": testProduceConsume,
		"past boundaty ":       testConsumePastBoundary,
		"test consume twice":   testConsumeTwice,
		"stream read":          testProduceConsumeStream,
		"unauthorized":         testUnauthorizedProduceConsume,
	}

	for scenario, fn := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, cfg)

		})
	}
}

// func initOpenTelemetry(ctx context.Context) func() {
// 	// Create OTLP gRPC exporter (change endpoint as needed)

// 	// exporter, err := otlptracegrp.New(ctx,
// 	// 	otlptracegrpc.WithInsecure(), // remove for TLS
// 	// 	otlptracegrpc.WithEndpoint("localhost:4317"), // default for OTLP/gRPC
// 	// )

// 	// exporter, err := stdouttrace.New(
// 	// 	stdouttrace.WithPrettyPrint(),
// 	// )
// 	f, err := os.Create("traces.json") // Overwrites each run; use os.OpenFile to append if needed
// 	if err != nil {
// 		stdLog.Fatalf("failed to create trace output file: %v", err)
// 	}

// 	exporter, err := stdouttrace.New(
// 		stdouttrace.WithWriter(f),
// 		stdouttrace.WithPrettyPrint(),
// 	)

// 	if err != nil {
// 		stdLog.Fatalf("failed to create exporter: %v", err)
// 	}

// 	// Set up trace provider
// 	tp := sdkTrace.NewTracerProvider(
// 		sdkTrace.WithBatcher(exporter),
// 		sdkTrace.WithResource(resource.NewWithAttributes(
// 			semconv.SchemaURL,
// 			// semconv.ServiceName("proglog-server"),
// 			semconv.ServiceNameKey.String("proglog-server"),
// 		)),
// 	)

// 	otel.SetTracerProvider(tp)

// 	return func() {
// 		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
// 		defer cancel()
// 		if err := tp.Shutdown(ctx); err != nil {
// 			stdLog.Fatalf("failed to shutdown trace provider: %v", err)
// 		}
// 	}
// }

func initOpenTelemetry(ctx context.Context) func() {
	// Create file for traces
	// f, err := os.Create("traces.json")
	// if err != nil {
	// 	stdLog.Fatalf("failed to create trace output file: %v", err)
	// }

	// traceExporter, err := stdouttrace.New(
	// 	stdouttrace.WithWriter(f),
	// 	stdouttrace.WithPrettyPrint(),
	// )
	useGrpc := false
	var err error
	var traceExporter sdkTrace.SpanExporter
	if useGrpc {
		traceExporter, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint("localhost:4317"),
		)
		if err != nil {
			stdLog.Fatalf("failed to create trace exporter: %v", err)
		}
	} else {
		f, _ := os.Create("traces.json")
		traceExporter, _ = stdouttrace.New(stdouttrace.WithWriter(f), stdouttrace.WithPrettyPrint())
	}

	// traceExporter, err := otlptracegrpc.New(ctx,
	// 	otlptracegrpc.WithInsecure(),                 // disable TLS for local dev
	// 	otlptracegrpc.WithEndpoint("localhost:4317"), // default OTLP endpoint for Jaeger
	// )

	// Create file for metrics
	metricFile, err := os.Create("metrics.json")
	if err != nil {
		stdLog.Fatalf("failed to create metrics output file: %v", err)
	}

	metricExporter, err := stdoutmetric.New(
		stdoutmetric.WithWriter(metricFile),
		stdoutmetric.WithPrettyPrint(),
	)
	if err != nil {
		stdLog.Fatalf("failed to create metric exporter: %v", err)
	}

	resource := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("proglog-server"),
	)

	// Set up Tracer Provider
	tp := sdkTrace.NewTracerProvider(
		sdkTrace.WithBatcher(traceExporter),
		sdkTrace.WithResource(resource),
	)
	otel.SetTracerProvider(tp)

	// Set up Meter Provider
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(resource),
	)
	otel.SetMeterProvider(mp)

	// Enable runtime metrics (CPU, memory, GC)
	if err := otelruntime.Start(otelruntime.WithMinimumReadMemStatsInterval(time.Second)); err != nil {
		stdLog.Fatalf("failed to start runtime instrumentation: %v", err)
	}

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			stdLog.Fatalf("failed to shutdown trace provider: %v", err)
		}
		if err := mp.Shutdown(ctx); err != nil {
			stdLog.Fatalf("failed to shutdown meter provider: %v", err)
		}
	}
}

func setupTest(t *testing.T, fn func(*Config)) (rootClient api.LogClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {

	ctx := context.Background()

	teleShut := initOpenTelemetry(ctx)
	initLatencyMetric()
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("no")
	}

	// client
	// if only a ca file is provided a client can ensure that the server is who they say they are
	// server doesn't know who we are

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {

		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CAFile:   config.CaFile,
			CertFile: crtPath,
			KeyFile:  keyPath,
			Server:   false,
		})
		if err != nil {
			t.Fatal("couldn't setup tls")
		}
		clientCreds := credentials.NewTLS(clientTLSConfig)

		opts := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}

		cc, err := grpc.Dial(l.Addr().String(), opts...)
		if err != nil {
			t.Fatal("no")
		}
		client := api.NewLogClient(cc)

		return cc, client, opts
	}

	var rootCon *grpc.ClientConn
	rootCon, rootClient, _ = newClient(config.RootClientCertFile, config.RootClientKeyFile)

	var nobodyCon *grpc.ClientConn
	nobodyCon, nobodyClient, _ = newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	// server

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		// if I don't have a ca file  and server false here it will still work
		CAFile:        config.CaFile,
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	if err != nil {
		t.Fatal("oh no")
	}
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	if err != nil {
		t.Fatal("no")
	}
	clog, err := log.NewLog(dir, log.Config{})
	if err != nil {
		t.Fatal("didnt")
	}

	authorizer, err := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	if err != nil {
		t.Fatal("couldn't set up auth")
	}

	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	if err != nil {
		t.Fatal("no")
	}
	go func() {
		server.Serve(l)
	}()
	return rootClient, nobodyClient, cfg, func() {
		time.Sleep(time.Second * 3)
		teleShut()
		server.Stop()
		// cc.Close()
		rootCon.Close()
		nobodyCon.Close()
		l.Close()
		clog.Remove()
	}

}

// INSECURE SETUP

// func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, cfg *Config, teardown func()) {
// 	t.Helper()
// 	l, err := net.Listen("tcp", ":0")
// 	if err != nil {
// 		t.Fatal("no")
// 	}
// 	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
// 	//client
// 	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
// 	if err != nil {
// 		t.Fatal("no")
// 	}
// 	dir, err := os.MkdirTemp("", "server-test")
// 	if err != nil {
// 		t.Fatal("no")
// 	}
// 	clog, err := log.NewLog(dir, log.Config{})
// 	if err != nil {
// 		t.Fatal("didnt")
// 	}

// 	cfg = &Config{CommitLog: clog}

// 	if fn != nil {
// 		fn(cfg)
// 	}

// 	server, err := NewGRPCServer(cfg)
// 	if err != nil {
// 		t.Fatal("no")
// 	}
// 	go func() {
// 		server.Serve(l)
// 	}()
// 	client = api.NewLogClient(cc)
// 	return client, cfg, func() {
// 		server.Stop()
// 		cc.Close()
// 		l.Close()
// 		clog.Remove()
// 	}

// }

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {

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
func testUnauthorizedProduceConsume(t *testing.T, _, client api.LogClient, config *Config) {

	ctx := context.Background()

	want := &api.Record{Value: []byte("hello world")}
	wantRequest := &api.ProduceRequest{Record: want}

	produceResponse, err := client.Produce(ctx, wantRequest)
	if produceResponse != nil {
		t.Fatal("produce response should be nil")
	}

	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code %d want %d", gotCode, wantCode)
	}

	consumeResponse, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})

	if consumeResponse != nil {
		t.Fatal("consume response should be nil")
	}

	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code %d want %d", gotCode, wantCode)
	}

}
func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
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
func testConsumeTwice(t *testing.T, client, _ api.LogClient, config *Config) {
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

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
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
