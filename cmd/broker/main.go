package main

import (
	"log"
	"logbroker/internal/server"
	"logbroker/internal/storage"
)

func main() {
	s := storage.NewLogStorage("/tmp/logdata", 1024, 10)
	if err := server.RunBrokerServer(s, ":50051"); err != nil {
		log.Fatal(err)
	}
}
