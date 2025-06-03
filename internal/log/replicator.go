package log

import (
	"context"
	"log"
	"sync"

	api "github.com/aleBranching/proglog/api/v1"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	// logger api.LogClient
	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	if _, ok := r.servers[name]; ok {
		// We are already replicating
		return nil
	}
	// What is the benefit of making this a channel of struct
	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.LogError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	// We create a client here in order to read the data of another server
	client := api.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})

	if err != nil {
		r.LogError(err, "failed to consume", addr)
		// forgot this initially
		return
	}
	records := make(chan *api.Record)

	go func() {
		for {
			recv, err := stream.Recv()

			if err != nil {
				r.LogError(err, "failed to receive", addr)
				return
			}

			records <- recv.Record
		}
	}()

	for {
		select {
		case <-r.close:
			return
			// Reading from a closed channel succeeds immediately, returning the zero value of the underlying type
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record})
			if err != nil {
				r.LogError(err, "failed to produce", addr)
				return
			}
		}
	}
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}

	close(r.servers[name])
	delete(r.servers, name)

	return nil
}

func (r *Replicator) init() {
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	// if r.logger

	if r.close == nil {
		r.close = make(chan struct{})
	}

}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

func (r *Replicator) LogError(err error, msg, addr string) {
	log.Println(msg, "addr: ", addr, err)
}
