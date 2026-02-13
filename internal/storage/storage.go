package storage

import (
	"fmt"
	"path/filepath"
	"sync"
)

type LogStorage struct {
	BaseDirectory       string
	SegmentSize         uint32
	MaxNumberOfSegments uint32
	partitions          map[string]*LogPartition
	lock                sync.RWMutex
}

func NewLogStorage(baseDirectory string, segmentSize,
	maxNumberOfSegments uint32) *LogStorage {

	return &LogStorage{
		BaseDirectory:       baseDirectory,
		SegmentSize:         segmentSize,
		MaxNumberOfSegments: maxNumberOfSegments,
		partitions:          make(map[string]*LogPartition),
	}
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
		uint32(partitionID),
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
