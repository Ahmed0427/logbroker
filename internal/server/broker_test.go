package server

import (
	"context"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahmed0427/logbroker/internal/storage"
	"github.com/ahmed0427/logbroker/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var l *bufconn.Listener

func setupTestServer(t *testing.T) (pb.BrokerServiceClient, func()) {
	tmpDir, err := os.MkdirTemp("", "broker-test-*")
	require.NoError(t, err)

	storage, err := storage.NewLogStorage(tmpDir, 1024, 10)
	if err != nil {
		t.Fatalf("NewLogStorage: %v", err)
	}

	l = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	pb.RegisterBrokerServiceServer(s, NewBrokerServer(storage))

	go func() {
		if err := s.Serve(l); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return l.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := pb.NewBrokerServiceClient(conn)

	cleanup := func() {
		conn.Close()
		s.Stop()
		os.RemoveAll(tmpDir)
	}

	return client, cleanup
}

func TestBrokerServerProduceAndConsume(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	produceResp, err := client.Produce(ctx, &pb.ProduceRequest{
		Topic:     "test-topic",
		Partition: 0,
		Records: []*pb.Record{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		},
		Acks: pb.Acks_ACKS_LEADER,
	})
	require.NoError(t, err)
	assert.Len(t, produceResp.Offsets, 2)
	assert.Equal(t, uint64(0), produceResp.Offsets[0])
	assert.Equal(t, uint64(1), produceResp.Offsets[1])

	consumeClient, err := client.Consume(ctx, &pb.ConsumeRequest{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		MaxBytes:  1024,
	})
	require.NoError(t, err)

	msg, err := consumeClient.Recv()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), msg.Offset)
	assert.Equal(t, []byte("key1"), msg.Key)
	assert.Equal(t, []byte("value1"), msg.Value)

	msg, err = consumeClient.Recv()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), msg.Offset)
	assert.Equal(t, []byte("key2"), msg.Key)
	assert.Equal(t, []byte("value2"), msg.Value)

	ctxTimeout, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
	defer cancel()
	consumeClient, err = client.Consume(ctxTimeout, &pb.ConsumeRequest{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    2,
		MaxBytes:  1024,
	})
	require.NoError(t, err)

	_, err = consumeClient.Recv()
	isDeadlineExceeded := strings.Contains(err.Error(), context.DeadlineExceeded.Error())
	assert.Equal(t, isDeadlineExceeded, true)
}

func TestBrokerServerTopicManagement(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{
		Name:          "new-topic",
		NumPartitions: 3,
	})
	require.NoError(t, err)

	// produce to each partition
	for i := uint32(0); i < 3; i++ {
		produceResp, err := client.Produce(ctx, &pb.ProduceRequest{
			Topic:     "new-topic",
			Partition: i,
			Records: []*pb.Record{
				{Key: []byte("key"), Value: []byte("value")},
			},
		})
		require.NoError(t, err)
		assert.Len(t, produceResp.Offsets, 1)
	}

	listResp, err := client.ListTopics(ctx, &pb.ListTopicsRequest{})
	require.NoError(t, err)
	var found bool
	for _, topic := range listResp.Topics {
		if topic.Name == "new-topic" {
			assert.ElementsMatch(t, topic.Partitions, []uint32{0, 1, 2})
			found = true
		}
	}
	require.True(t, found, "new-topic should be listed")
}

func TestHighConcurrency(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	topic := "concurrency-test"
	numProducers := 10
	numMessages := 100

	var wg sync.WaitGroup
	errCh := make(chan error, numProducers*numMessages)

	for i := 0; i < numProducers; i++ {
		wg.Add(1)

		go func(producerID int) {
			defer wg.Done()

			for j := 0; j < numMessages; j++ {
				_, err := client.Produce(ctx, &pb.ProduceRequest{
					Topic:     topic,
					Partition: uint32(producerID % 5),
					Records: []*pb.Record{
						{
							Key:   []byte("key"),
							Value: []byte("value"),
						},
					},
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	// Verify all messages are consumable
	for p := 0; p < 5; p++ {
		ctxTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		stream, err := client.Consume(ctxTimeout, &pb.ConsumeRequest{
			Topic:     topic,
			Partition: uint32(p),
			Offset:    0,
			MaxBytes:  1024 * 1024,
		})
		require.NoError(t, err)

		count := 0
		for {
			_, err := stream.Recv()
			if err != nil {
				break
			}
			count++
		}

		require.Equal(t, numMessages*numProducers/5, count)
		cancel()
	}
}
