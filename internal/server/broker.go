package server

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/ahmed0427/logbroker/internal/storage"
	"github.com/ahmed0427/logbroker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	if req.Acks == pb.Acks_ACKS_LEADER {
		partition, err := s.storage.GetPartition(req.Topic, req.Partition)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get partition: %s", err.Error())
		}
		if err := partition.Flush(); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to flush: %s", err.Error())
		}
	}

	return &pb.ProduceResponse{Offsets: offsets}, nil
}

func (s *BrokerServer) Consume(
	req *pb.ConsumeRequest,
	stream grpc.ServerStreamingServer[pb.LogEntry],
) error {

	offset := req.Offset

	for {
		entries, err := s.storage.Consume(
			req.Topic,
			req.Partition,
			offset,
			req.MaxBytes,
		)
		if err != nil {
			return status.Errorf(codes.Internal, "consume failed: %s", err.Error())
		}

		if len(entries) == 0 {
			select {
			case <-stream.Context().Done():
				return nil
			case <-time.After(75 * time.Millisecond):
				continue
			}
		}

		for _, e := range entries {
			if err := stream.Send(&pb.LogEntry{
				Offset:    e.Offset,
				Timestamp: e.Timestamp,
				Key:       e.Key,
				Value:     e.Value,
			}); err != nil {
				return err
			}
			offset = e.Offset + 1
		}
	}
}

func (s *BrokerServer) CreateTopic(ctx context.Context,
	req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic name cannot be empty")
	}

	if req.NumPartitions == 0 {
		req.NumPartitions = 1 // Default to 1 partition
	}

	// Initialize all partitions for the topic
	for i := uint32(0); i < req.NumPartitions; i++ {
		_, err := s.storage.GetPartition(req.Name, i)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get partition failed: %s", err.Error())
		}
	}

	return &pb.CreateTopicResponse{}, nil
}

func (s *BrokerServer) ListTopics(ctx context.Context,
	req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {

	topicMetadata := s.storage.StorageMetadata()

	topics := make([]*pb.TopicMetadata, len(topicMetadata))

	for i, tm := range topicMetadata {
		topics[i] = &pb.TopicMetadata{
			Name:       tm.Name,
			Partitions: tm.Partitions,
		}
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func RunBrokerServer(storage *storage.LogStorage, port string) error {
	l, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(unaryErrorInterceptor),
		grpc.StreamInterceptor(streamErrorInterceptor),
	)
	pb.RegisterBrokerServiceServer(grpcServer, NewBrokerServer(storage))

	log.Printf("gRPC server listening on %s", port)
	return grpcServer.Serve(l)
}

// Error interceptors for consistent error handling
func unaryErrorInterceptor(ctx context.Context, req interface{},
	info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	resp, err := handler(ctx, req)
	if err != nil {
		log.Printf("Unary call %s failed: %v", info.FullMethod, err)
		return nil, err
	}
	return resp, nil
}

func streamErrorInterceptor(srv interface{}, ss grpc.ServerStream,
	info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	err := handler(srv, ss)
	if err != nil {
		log.Printf("Stream call %s failed: %v", info.FullMethod, err)
		return err
	}
	return nil
}
