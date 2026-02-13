package main

import (
	"fmt"
	"log"

	"github.com/ahmed0427/logbroker/internal/storage"
)

func main() {
	// tmpDir, err := os.MkdirTemp("", "kafka-storage-test-*")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	s := storage.NewLogStorage("kafka-storage-test-1279150422", 1024*1024, 100)

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		offset, err := s.Produce("test-topic", 0, key, value)
		fmt.Println(offset)
		if err != nil {
			log.Fatal(err)
		}
	}
	entries, err := s.Consume("test-topic", 0, 0, 1024*1024)
	if err != nil {
		log.Fatal(err)
	}
	for _, ent := range entries {
		fmt.Printf("%s: %s\n", string(ent.Key), string(ent.Value))
	}
}
