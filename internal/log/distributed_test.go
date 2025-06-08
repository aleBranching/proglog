package log_test

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	api "github.com/aleBranching/proglog/api/v1"
	"google.golang.org/grpc/status"

	"github.com/aleBranching/proglog/internal/log"
	"github.com/hashicorp/raft"
	"github.com/travisjeffery/go-dynaport"
)

func eventually(t *testing.T, condFunc func() bool) {
	t.Helper()

	res := false
	for i := 0; i < 10; i++ {
		time.Sleep(50 * time.Millisecond)
		// how many joined
		res = condFunc()

		if res {
			break
		}

	}
	if !res {
		t.Fatalf("It failed")
	}

}

func TestMultipleNodes(t *testing.T) {

	var logs []*log.DistributedLog

	nodeCount := 3
	ports := dynaport.Get(nodeCount)
	for i := 0; i < nodeCount; i++ {
		dataDir, err := os.MkdirTemp("", "distributed-log-test")
		if err != nil {
			t.Fatal("oh no")
		}
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))

		if err != nil {
			t.Fatalf("oh no")
		}
		config := log.Config{}
		config.Raft.StreamLayer = log.NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond
		if i == 0 {
			config.Raft.Bootstrap = true
		}
		l, err := log.NewDistributedLog(dataDir, config)
		if err != nil {
			t.Fatal("oh no")
		}
		if i != 0 {
			err = logs[0].Join(fmt.Sprintf("%d", i), ln.Addr().String())
			if err != nil {
				t.Fatal("oh no")
			}
		} else {
			err = l.WaitForLeader(3 * time.Second)
			if err != nil {
				t.Fatal("oh no")
			}
		}

		logs = append(logs, l)
	}
	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	for _, record := range records {
		off, err := logs[0].Append(record)
		if err != nil {
			t.Fatal("oh no")
		}

		eventually(t, func() bool {

			for j := 0; j < nodeCount; j++ {
				got, err := logs[j].Read(off)
				if err != nil {
					return false
				}
				record.Offset = off
				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}
			return true
		})

	}

	err := logs[0].Leave("1")
	if err != nil {
		t.Fatal("oh no")
	}
	time.Sleep(50 * time.Millisecond)
	off, err := logs[0].Append(&api.Record{
		Value: []byte("third"),
	})
	if err != nil {
		t.Fatal("oh no")
	}
	time.Sleep(50 * time.Millisecond)
	record, err := logs[1].Read(off)

	st, ok := status.FromError(err)
	// quick dirty hack
	if !ok || st.Code() != 404 {
		t.Fatal("oh no")
	}

	if record != nil {
		t.Fatal("oh no")
	}

	record, err = logs[2].Read(off)

	if err != nil {
		t.Fatal("oh no")
	}

	if !bytes.Equal([]byte("third"), record.Value) {
		t.Fatal("oh no")
	}
	if off != record.Offset {
		t.Fatal("aaaa")
	}

}
