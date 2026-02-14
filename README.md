# LogBroker

A minimal distributed log broker written in Go using gRPC.  
Supports partitioned topics, streaming consumption, and persistent storage.

## Goals

- Simple Kafka-style log broker
- Clear storage model
- Deterministic partition behavior
- Clean, testable architecture

## Architecture

```
Client (gRPC)
      │
      ▼
BrokerService
      │
      ▼
 LogStorage
      │
      ▼
 Partitions → Segments
```

- **Server layer**: Implements gRPC service
- **Storage layer**: Manages partitions and segments
- **Segments**: Fixed-size append-only files

## Project Structure

```
api/        → protobuf definitions
cmd/        → broker entrypoint
internal/   → server + storage implementation
pb/         → generated protobuf code
scripts/    → proto generation script
```

## API Overview

Defined in `api/broker.proto`.

### Produce

Append records to a topic partition.

```proto
rpc Produce(ProduceRequest) returns (ProduceResponse);

```

Supports:

- Multiple records per request
- Configurable acknowledgments (`ACKS_NONE`, `ACKS_LEADER`)

---

### Consume (Streaming)

Stream records starting from a given offset.

```proto
rpc Consume(ConsumeRequest) returns (stream LogEntry);
```

- Server-side streaming
- Offset-based reads
- Max bytes per batch

### Create Topic

```proto
rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse);
```

Creates a topic with N partitions.

### List Topics

```proto
rpc ListTopics(ListTopicsRequest) returns (ListTopicsResponse);
```

Returns topic metadata:

```proto
message TopicMetadata {
  string name = 1;
  repeated uint32 partitions = 2;
}
```

## Running

### Generate protobuf

```bash
./scripts/generate.sh
```

### Start broker

```bash
go run ./cmd/broker
```

## Testing

Run all tests:

```bash
go test ./internal/storage
go test ./internal/server
```

Includes:

- Storage tests
- Partition & segment tests
- High concurrency tests
- gRPC server tests

## Storage Model

On disk:

```
<baseDir>/
├── <topic>-0/
│   ├── 00000000000000000000.log
│   ├── 00000000000000000000.index
│   ├── 00000000000000000032.log
│   └── 00000000000000000032.index
├── <topic>-1/
│   ├── 00000000000000000000.log
│   └── 00000000000000000000.index
└── <another-topic>-0/
    ├── 00000000000000000000.log
    └── 00000000000000000000.index
```

Each partition directory contains log segments and index.

Partition naming format:

```
<topic>-<partitionID>
```

## Future Improvements

- Consumer groups
- Replication
- Leader election
- Metrics
