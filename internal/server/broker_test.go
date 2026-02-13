package server

import (
	"context"
	"testing"

	"github.com/ahmed0427/logbroker/internal/storage"
	"github.com/ahmed0427/logbroker/pb"
)

func TestProduceConsumeGRPC(t *testing.T) {
	storage := storage.NewLogStorage(t.TempDir(), 1024, 10)
	server := NewBrokerServer(storage)

	// Produce
	pubResp, err := server.Produce(context.Background(), &pb.ProduceRequest{
		Topic:     "test",
		Partition: 0,
		Records: []*pb.Record{
			&pb.Record{
				Key:   []byte("key1"),
				Value: []byte("value1"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Consume
	subResp, err := server.Consume(context.Background(), &pb.ConsumeRequest{
		Topic:     "test",
		Partition: 0,
		Offset:    pubResp.Offsets[0],
		MaxBytes:  1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(subResp.Entries) != 1 || string(subResp.Entries[0].Value) != "value1" {
		t.Fatalf("expected value1, got %+v", subResp.Entries)
	}
}
