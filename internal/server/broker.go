package server

import (
	"context"
	"log"
	"net"

	"github.com/ahmed0427/logbroker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ahmed0427/logbroker/internal/storage"
)

type BrokerServer struct {
	storage *storage.LogStorage
	pb.UnimplementedBrokerServiceServer
}

func NewBrokerServer(s *storage.LogStorage) *BrokerServer {
	return &BrokerServer{storage: s}
}

func (s *BrokerServer) Produce(ctx context.Context,
	req *pb.ProduceRequest) (*pb.ProduceResponse, error) {

	offsets := make([]uint64, 0, len(req.Records))

	for _, r := range req.Records {
		offset, err := s.storage.Produce(req.Topic, req.Partition, r.Key, r.Value)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s", err.Error())
		}
		offsets = append(offsets, offset)
	}
	return &pb.ProduceResponse{Offsets: offsets}, nil
}

func (s *BrokerServer) Consume(ctx context.Context,
	req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {

	entries, err := s.storage.Consume(req.Topic, req.Partition, req.Offset, req.MaxBytes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err.Error())
	}

	resp := &pb.ConsumeResponse{
		Entries: make([]*pb.LogEntry, 0, len(entries)),
	}

	for _, ent := range entries {
		resp.Entries = append(resp.Entries, &pb.LogEntry{
			Offset:    ent.Offset,
			Timestamp: ent.Timestamp,
			Key:       ent.Key,
			Value:     ent.Value,
		})
	}

	return resp, nil
}

func RunBrokerServer(storage *storage.LogStorage, port string) error {
	l, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBrokerServiceServer(grpcServer, NewBrokerServer(storage))

	log.Printf("gRPC server listening on %s", port)
	return grpcServer.Serve(l)
}
