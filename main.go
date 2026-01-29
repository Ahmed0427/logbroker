package main

import "fmt"

func main() {
	storage := NewLogStorage("/tmp/kafka_data", 1024)

	fmt.Println("Producing messages...")
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("message-%d: This is test data", i))

		offset, err := storage.Produce("test-topic", 0, key, value)
		if err != nil {
			fmt.Printf("Error producing message: %v\n", err)
			continue
		}
		fmt.Printf("Produced message at offset %d\n", offset)
	}

	fmt.Println("\nConsuming messages...")
	entries, err := storage.Consume("test-topic", 0, 0, 2048)
	if err != nil {
		fmt.Printf("Error consuming messages: %v\n", err)
		return
	}

	for _, entry := range entries {
		fmt.Printf("Offset: %d, Key: %s, Value: %s\n",
			entry.Offset, string(entry.Key), string(entry.Value))
	}

	hwm, err := storage.GetHighWatermark("test-topic", 0)
	if err != nil {
		fmt.Printf("Error getting high watermark: %v\n", err)
		return
	}
	fmt.Printf("\nHigh watermark: %d\n", hwm)

	if err := storage.Close(); err != nil {
		fmt.Printf("Error closing storage: %v\n", err)
		return
	}
	fmt.Println("\nStorage closed successfully")
}
