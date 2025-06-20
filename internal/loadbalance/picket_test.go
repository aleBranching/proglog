package loadbalance_test

import (
	"reflect"
	"testing"

	"github.com/aleBranching/proglog/internal/loadbalance"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

func TestPickerNoSubConnAvailable(t *testing.T) {
	picker := &loadbalance.Picker{}
	for _, method := range []string{
		"/log.vX.Log/Produce",
		"/log.vX.Log/Consume",
	} {
		info := balancer.PickInfo{
			FullMethodName: method,
		}

		result, err := picker.Pick(info)
		if err == nil {
			t.Fatal("oh no")
		}
		if result.SubConn != nil {
			t.Fatal("oh no")
		}
	}
}

func TestPickerProducesToLeader(t *testing.T) {
	picker, subConns := setupTest()
	info := balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Produce",
	}
	for i := 0; i < 5; i++ {
		gotPick, err := picker.Pick(info)
		if err != nil {
			t.Fatal("oh no")
		}
		if !reflect.DeepEqual(subConns[0], gotPick.SubConn) {
			t.Fatal("they are not equal")
		}
	}
}

func TestPickerConsumeFromFollowers(t *testing.T) {
	picker, subConns := setupTest()

	info := balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Consume",
	}

	for i := 0; i < 5; i++ {
		gotPick, err := picker.Pick(info)
		if err != nil {
			t.Fatal("oh no")
		}
		if !reflect.DeepEqual(subConns[i%2+1], gotPick.SubConn) {
			t.Fatal("oh no")
		}
	}
}

func setupTest() (*loadbalance.Picker, []*subConn) {
	var subConns []*subConn
	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}
	for i := 0; i < 3; i++ {
		sc := &subConn{}
		addr := resolver.Address{
			Attributes: attributes.New("is_leader", i == 0),
		}
		sc.UpdateAddresses([]resolver.Address{addr})
		buildInfo.ReadySCs[sc] = base.SubConnInfo{Address: addr}
		subConns = append(subConns, sc)
	}
	picker := &loadbalance.Picker{}
	picker.Build(buildInfo)
	return picker, subConns
}

type subConn struct {
	addrs []resolver.Address
	balancer.SubConn
}

func (s *subConn) UpdateAddresses(addrs []resolver.Address) {
	s.addrs = addrs
}
