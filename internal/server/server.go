package server

import (
	"context"
	api "github.com/aleBranching/proglog/api/v1"
)

type Config struct {
	CommitLog CommitLog
}
type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(off uint64) (*api.Record, error)
}

// compile time check
// this syntax is type conversion same as uint32(2)
var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}

	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil

}
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, err
}

func (s *grpcServer) ProduceStream(req *api.ConsumeRequest)
