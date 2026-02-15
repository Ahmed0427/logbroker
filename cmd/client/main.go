package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ahmed0427/logbroker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("server", "localhost:50051", "broker address")
	timeout := flag.Duration("timeout", 150*time.Millisecond, "request timeout ms")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Usage:")
		fmt.Println("  create   -topic <name> -partitions <n>")
		fmt.Println("  produce  -topic <name> -partition <id> [-acks leader|none]")
		fmt.Println("  consume  -topic <name> -partition <id> [-offset <n>]")
		fmt.Println("  list")
		os.Exit(1)
	}

	conn, err := grpc.Dial(*serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connection failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)
	cmd := flag.Arg(0)

	switch cmd {
	case "create":
		runCreate(client, *timeout)
	case "list":
		runList(client, *timeout)
	case "produce":
		runProduce(client, *timeout)
	case "consume":
		runConsume(client)
	default:
		log.Fatalf("unknown command: %s", cmd)
	}
}

func runCreate(client pb.BrokerServiceClient, timeout time.Duration) {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	topic := fs.String("topic", "", "topic name")
	partitions := fs.Uint("partitions", 1, "number of partitions")
	fs.Parse(flag.Args()[1:])

	if *topic == "" {
		log.Fatal("topic required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{
		Name:          *topic,
		NumPartitions: uint32(*partitions),
	})
	if err != nil {
		log.Fatalf("create failed: %v", err)
	}

	log.Printf("Topic '%s' created with %d partitions", *topic, *partitions)
}

func runList(client pb.BrokerServiceClient, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := client.ListTopics(ctx, &pb.ListTopicsRequest{})
	if err != nil {
		log.Fatalf("list failed: %v", err)
	}

	for _, t := range resp.Topics {
		log.Printf("Topic: %s | Partitions: %v", t.Name, t.Partitions)
	}
}

func runProduce(client pb.BrokerServiceClient, timeout time.Duration) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	topic := fs.String("topic", "", "topic name")
	partition := fs.Uint("partition", 0, "partition id")
	acks := fs.String("acks", "leader", "acks: leader|none")
	fs.Parse(flag.Args()[1:])

	if *topic == "" {
		log.Fatal("topic required")
	}

	ackMode := pb.Acks_ACKS_LEADER
	if *acks == "none" {
		ackMode = pb.Acks_ACKS_NONE
	}

	reader := bufio.NewReader(os.Stdin)
	log.Println("Enter messages (Ctrl+C to exit):")

	for {
		fmt.Print("> ")
		text, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		resp, err := client.Produce(ctx, &pb.ProduceRequest{
			Topic:     *topic,
			Partition: uint32(*partition),
			Records: []*pb.Record{
				{Value: []byte(text)},
			},
			Acks: ackMode,
		})
		cancel()

		if err != nil {
			log.Printf("produce error: %v", err)
			continue
		}

		log.Printf("Produced at offset %d", resp.Offsets[0])
	}
}

func runConsume(client pb.BrokerServiceClient) {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	topic := fs.String("topic", "", "topic name")
	partition := fs.Uint("partition", 0, "partition id")
	offset := fs.Uint64("offset", 0, "starting offset")
	group := fs.String("group", "", "consumer group")
	fs.Parse(flag.Args()[1:])

	if *topic == "" {
		log.Fatal("topic required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("Shutting down consumer...")
		cancel()
	}()

	stream, err := client.Consume(ctx, &pb.ConsumeRequest{
		Topic:         *topic,
		Partition:     uint32(*partition),
		Offset:        *offset,
		ConsumerGroup: *group,
		MaxBytes:      1024 * 1024,
	})
	if err != nil {
		log.Fatalf("consume failed: %v", err)
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("receive error: %v", err)
		}

		fmt.Printf("[offset=%d] %s\n", msg.Offset, string(msg.Value))
	}
}
