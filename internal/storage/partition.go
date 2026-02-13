package storage

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type LogPartition struct {
	directory           string
	partitionID         uint32
	segmentSize         uint32
	segments            []*LogSegment
	nextOffset          uint64
	maxNumberOfSegments uint32
	lock                sync.RWMutex
}

func NewLogPartition(id uint32, dir string, segmentSize, maxNumberOfSegments uint32) (*LogPartition, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create partition dir: %w", err)
	}

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read partition dir: %w", err)
	}

	segments := []*LogSegment{}

	for _, entry := range dirEntries {
		err = checkPartitionDirEntry(entry.Name(), entry.IsDir())
		if err != nil {
			return nil, fmt.Errorf("corrupted partition dir: %w", err)
		}
		if strings.HasSuffix(entry.Name(), ".log") {
			var baseOffset uint64
			if n, err := fmt.Sscanf(entry.Name(), "%020d.log", &baseOffset); n != 1 || err != nil {
				return nil, fmt.Errorf("invalid segment filename: %s", entry.Name())
			}
			segment, err := NewLogSegment(dir, baseOffset, segmentSize)
			if err != nil {
				return nil, fmt.Errorf("failed to create segment: %w", err)
			}
			segments = append(segments, segment)
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].baseOffset < segments[j].baseOffset
	})

	// only last segment is active
	var nextOffset uint64 = 0
	for i, seg := range segments {
		if i == len(segments)-1 {
			lastOffset, err := seg.RebuildIndex()
			if err != nil {
				return nil, fmt.Errorf("failed to rebuild active segment index: %w", err)
			}
			nextOffset = lastOffset + 1
			break
		}
		if err := seg.Seal(); err != nil {
			return nil, fmt.Errorf("failed to seal old segment: %w", err)
		}
	}

	return &LogPartition{
		partitionID:         id,
		directory:           dir,
		segmentSize:         segmentSize,
		segments:            segments,
		nextOffset:          nextOffset,
		maxNumberOfSegments: maxNumberOfSegments,
	}, nil
}

func (p *LogPartition) Append(key, value []byte) (uint64, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	offset := p.nextOffset
	entry := &LogEntry{
		Offset:    offset,
		Timestamp: uint64(time.Now().UnixNano()),
		Key:       key,
		Value:     value,
	}

	if len(p.segments) == 0 {
		segment, err := NewLogSegment(p.directory, 0, p.segmentSize)
		if err != nil {
			return 0, err
		}
		p.segments = append(p.segments, segment)
	}

	activeSegment := p.segments[len(p.segments)-1]

	appended, err := activeSegment.Append(entry)
	if err != nil {
		return 0, err
	}

	if !appended {
		if err = activeSegment.Seal(); err != nil {
			return 0, err
		}
		newSegment, err := NewLogSegment(p.directory, offset, p.segmentSize)
		if err != nil {
			return 0, err
		}

		p.segments = append(p.segments, newSegment)
		if appended, err := newSegment.Append(entry); !appended || err != nil {
			return 0, fmt.Errorf("failed to append to the new segment: %v", err)
		}
	}

	if len(p.segments) > int(p.maxNumberOfSegments) {
		p.segments[0].Remove()
		p.segments = p.segments[1:]
	}

	p.nextOffset++
	return offset, nil
}

func (p *LogPartition) Read(offset uint64) (*LogEntry, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if len(p.segments) == 0 {
		return nil, fmt.Errorf("partition empty")
	}
	if offset >= p.nextOffset {
		return nil, fmt.Errorf("offset >= p.nextOffset")
	}
	i := p.findSegmentIndex(offset)
	return p.segments[i].Read(offset)
}

func (p *LogPartition) ReadRange(startOffset uint64, maxBytes uint32) ([]*LogEntry, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if len(p.segments) == 0 {
		return nil, fmt.Errorf("partition empty")
	}

	var entries []*LogEntry
	var bytesRead uint32

	if startOffset >= p.nextOffset {
		return entries, nil
	}

	segmentIdx := p.findSegmentIndex(startOffset)

	for i := segmentIdx; i < len(p.segments); i++ {
		segment := p.segments[i]
		segmentEntries, err := segment.ReadRange(startOffset, maxBytes-bytesRead)
		if err != nil {
			return entries, err
		}

		entries = append(entries, segmentEntries...)
		startOffset += uint64(len(segmentEntries))

		for _, entry := range segmentEntries {
			bytesRead += uint32(len(entry.Key) + len(entry.Value))
		}

		if bytesRead >= maxBytes {
			break
		}
	}

	return entries, nil
}

func (p *LogPartition) findSegmentIndex(offset uint64) int {
	result := 0
	left, right := 0, len(p.segments)-1
	for left <= right {
		mid := (left + right) / 2
		if p.segments[mid].baseOffset <= offset {
			result = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return result
}

func (p *LogPartition) GetLogEndOffset() uint64 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.nextOffset
}

func (p *LogPartition) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	var errs []error
	for _, seg := range p.segments {
		if err := seg.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	p.segments = nil
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

func checkPartitionDirEntry(name string, isDir bool) error {
	err := fmt.Errorf("partition is corrupt")

	if isDir {
		return err
	}

	base, ext, ok := strings.Cut(name, ".")
	if !ok {
		return err
	}

	if len(base) != 20 || !isAllDigits(base) {
		return err
	}

	switch ext {
	case "log", "index":
		return nil
	default:
		return err
	}
}

func isAllDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
