package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type TopicMetadata struct {
	Name       string
	Partitions []uint32
}

type LogStorage struct {
	BaseDirectory       string
	SegmentSize         uint32
	MaxNumberOfSegments uint32
	partitions          map[string]*LogPartition
	lock                sync.RWMutex
}

func NewLogStorage(
	baseDirectory string,
	segmentSize uint32,
	maxNumberOfSegments uint32,
) (*LogStorage, error) {

	if err := os.MkdirAll(baseDirectory, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir base dir: %w", err)
	}

	entries, err := os.ReadDir(baseDirectory)
	if err != nil {
		return nil, fmt.Errorf("read base dir: %w", err)
	}

	partitions := make(map[string]*LogPartition)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()

		idx := strings.LastIndex(name, "-")
		if idx == -1 {
			return nil, fmt.Errorf("bad partition dir %q", name)
		}

		topic := name[:idx]
		partStr := name[idx+1:]
		if topic == "" {
			return nil, fmt.Errorf("bad partition dir %q", name)
		}

		id, err := strconv.ParseUint(partStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("bad partition id %q", name)
		}

		fullPath := filepath.Join(baseDirectory, name)

		partitions[name], err = NewLogPartition(
			uint32(id),
			fullPath,
			segmentSize,
			maxNumberOfSegments,
		)

		if err != nil {
			return nil, err
		}
	}

	return &LogStorage{
		BaseDirectory:       baseDirectory,
		SegmentSize:         segmentSize,
		MaxNumberOfSegments: maxNumberOfSegments,
		partitions:          partitions,
	}, nil
}

func (s *LogStorage) GetPartition(topic string, partitionID uint32) (*LogPartition, error) {
	key := fmt.Sprintf("%s-%d", topic, partitionID)

	s.lock.Lock()
	defer s.lock.Unlock()

	if p, exists := s.partitions[key]; exists {
		return p, nil
	}

	partitionDir := filepath.Join(s.BaseDirectory, key)

	p, err := NewLogPartition(
		partitionID,
		partitionDir,
		s.SegmentSize,
		s.MaxNumberOfSegments,
	)
	if err != nil {
		return nil, err
	}

	s.partitions[key] = p
	return p, nil
}

func (s *LogStorage) Produce(topic string, partitionID uint32,
	key, value []byte) (uint64, error) {

	p, err := s.GetPartition(topic, partitionID)
	if err != nil {
		return 0, err
	}

	offset, err := p.Append(key, value)
	return offset, err
}

func (s *LogStorage) Consume(topic string, partitionID uint32,
	offset uint64, maxBytes uint32) ([]*LogEntry, error) {

	p, err := s.GetPartition(topic, partitionID)
	if err != nil {
		return nil, err
	}

	return p.ReadRange(offset, maxBytes)
}

func (s *LogStorage) GetHighWatermark(topic string, partition uint32) (uint64, error) {
	p, err := s.GetPartition(topic, partition)
	if err != nil {
		return 0, err
	}

	return p.GetLogEndOffset(), nil
}

func (s *LogStorage) StorageMetadata() []*TopicMetadata {
	s.lock.RLock()
	defer s.lock.RUnlock()

	topicMap := make(map[string][]uint32)

	for key := range s.partitions {
		// key format: "<topic>-<partitionID>"
		idx := strings.LastIndex(key, "-")
		if idx == -1 {
			panic(fmt.Sprintf("invalid partition key format %q", key))
		}

		topic := key[:idx]
		partitionID := key[idx+1:]

		id, err := strconv.Atoi(partitionID)
		if err != nil {
			panic(fmt.Sprintf("invalid partition ID in key %q: %v", key, err))
		}

		topicMap[topic] = append(topicMap[topic], uint32(id))
	}

	metadata := make([]*TopicMetadata, 0, len(topicMap))

	for topic, partitions := range topicMap {
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i] < partitions[j]
		})
		metadata = append(metadata, &TopicMetadata{
			Name:       topic,
			Partitions: partitions,
		})
	}

	return metadata
}

func (s *LogStorage) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	var errs []error
	for i, partition := range s.partitions {
		if err := partition.Close(); err != nil {
			errs = append(errs, err)
		}
		s.partitions[i] = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing storage: %v", errs)
	}
	return nil
}
