package discovery_test

import (
	"fmt"
	"testing"
	"time"

	. "github.com/aleBranching/proglog/internal/discovery"
	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/go-dynaport"
)

func eventually(t *testing.T, condFunc func() bool) {
	t.Helper()

	res := false
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
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

func TestMembersip(t *testing.T) {
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	eventually(t, func() bool {
		// how many joined
		return len(handler.joins) == 2 &&
			// how many members
			len(m[0].Members()) == 3 &&
			// how many left
			len(handler.leaves) == 0
	})

	fmt.Println("joins", len(handler.joins))
	err := m[2].Leave()
	if err != nil {
		t.Fatal("oh  no", err)
	}
	fmt.Println("joins", len(handler.joins))

	eventually(t, func() bool {
		// fmt.Println("joins", len(handler.joins))
		// fmt.Println("status", m[0].Members()[2].Status)
		// fmt.Println("leaves", len(handler.leaves))
		return len(handler.joins) == 2 &&
			len(m[0].Members()) == 3 &&
			serf.StatusLeft == m[0].Members()[2].Status &&
			len(handler.leaves) == 1
	})
	if fmt.Sprintf("%d", 2) != <-handler.leaves {
		t.Fatalf("the right one didn't leave")
	}

}

func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		"rpc_addr": addr,
	}

	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}
	h := &handler{}
	if len(members) != 0 {
		c.StartJoinAddrs = []string{members[0].BindAddr}
	} else {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	}

	m, err := New(h, c)
	if err != nil {
		t.Fatalf("failed to create a new instance")
	}
	members = append(members, m)
	return members, h

}

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil

}
func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}
