package main

import (
	"flag"
	"log"
	"os"

	"github.com/ahmed0427/logbroker/internal/server"
	"github.com/ahmed0427/logbroker/internal/storage"
)

func main() {
	port := flag.String("port", "50051", "port to listen on")
	baseDir := flag.String("dir", "", "storage base directory")
	flag.Parse()

	if *baseDir == "" {
		flag.Usage()
		os.Exit(1)
	}

	s, err := storage.NewLogStorage(*baseDir, 4*1024*1024, 256)
	if err != nil {
		log.Fatalf("NewLogStorage: %v", err)
	}
	if err := server.RunBrokerServer(s, ":"+*port); err != nil {
		log.Fatalf("RunBrokerServer: %v", err)
	}
}
