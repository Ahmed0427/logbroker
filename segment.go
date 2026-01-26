package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type LogSegment struct {
	directory          string
	baseOffset         uint64
	maxSize            uint32
	currentSize        uint32
	sizeSinceLastIndex uint32
	logFile            *os.File
	logPath            string
	indexFile          *os.File
	indexPath          string
	isSealed           bool
}

func NewLogSegment(dir string, baseOffset uint64, maxSize uint32) (*LogSegment, error) {
	fileName := fmt.Sprintf("%020d", baseOffset)
	logPath := filepath.Join(dir, fileName+".log")
	indexPath := filepath.Join(dir, fileName+".index")

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		logFile.Close()
		return nil, err
	}

	currentSize, err := logFile.Seek(0, io.SeekEnd)
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, err
	}

	return &LogSegment{
		directory:   dir,
		baseOffset:  baseOffset,
		maxSize:     maxSize,
		currentSize: uint32(currentSize),
		logFile:     logFile,
		logPath:     logPath,
		indexFile:   indexFile,
		indexPath:   indexPath,
	}, nil
}

func (s *LogSegment) Append(entry *LogEntry) error {
	if s.isSealed {
		return fmt.Errorf("segment sealed: cannot append to %s", s.logFile.Name())
	}

	entryData, err := entry.Encode()
	if err != nil {
		return fmt.Errorf("encode entry: %w", err)
	}

	if s.currentSize+uint32(len(entryData)) > s.maxSize {
		return fmt.Errorf("segment full: entry size %d exceeds remaining %d bytes",
			len(entryData), s.maxSize-s.currentSize)
	}

	entryPos := s.currentSize
	_ = entryPos

	if _, err := s.logFile.Write(entryData); err != nil {
		return fmt.Errorf("write entry: %w", err)
	}

	s.currentSize += uint32(len(entryData))
	s.sizeSinceLastIndex += uint32(len(entryData))

	if s.sizeSinceLastIndex >= 4096 {
		indexEntryData := [8]byte{}
		binary.BigEndian.PutUint32(indexEntryData[:], uint32(entry.offset-s.baseOffset))
		binary.BigEndian.PutUint32(indexEntryData[4:], entryPos)
		if _, err := s.indexFile.Write(indexEntryData[:]); err != nil {
			return fmt.Errorf("write index entry: %w", err)
		}
		s.sizeSinceLastIndex = 0
	}

	return nil
}

func (s *LogSegment) lookupOffset(targetOffset uint64) (uint32, error) {
	if targetOffset < s.baseOffset {
		return 0, fmt.Errorf("target offset < base offset")
	}

	indexData, err := os.ReadFile(s.indexPath)
	if err != nil {
		return 0, fmt.Errorf("can't read index file: %w", err)
	}

	if len(indexData) == 0 {
		return 0, nil
	}

	if len(indexData)%8 != 0 {
		return 0, fmt.Errorf("corrupted index file: %w", err)
	}

	var targetPos int32 = -1
	l, r := 0, (len(indexData)/8)-1

	for l <= r {
		mid := (l + r) / 2
		entryData := indexData[mid*8 : (mid*8)+8]

		offset := binary.BigEndian.Uint32(entryData)
		pos := binary.BigEndian.Uint32(entryData[4:])

		if uint64(offset) > targetOffset-s.baseOffset {
			r = mid - 1
		} else {
			targetPos = int32(pos)
			l = mid + 1
		}
	}

	if targetPos == -1 {
		return 0, fmt.Errorf("offset not found in index")
	}

	return uint32(targetPos), nil
}

func (s *LogSegment) Read(targetOffset uint64) (*LogEntry, error) {
	if targetOffset < s.baseOffset {
		return nil, fmt.Errorf("target offset < base offset")
	}

	entryPos, err := s.lookupOffset(targetOffset)
	if err != nil {
		return nil, fmt.Errorf("index lookup: %w", err)
	}

	if _, err := s.logFile.Seek(int64(entryPos), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek start: %w", err)
	}

	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(s.logFile, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}

		entryOffset := binary.BigEndian.Uint64(header[offsetPos:timestampPos])
		timestamp := binary.BigEndian.Uint64(header[timestampPos:valueLenPos])
		keyLen := binary.BigEndian.Uint32(header[keyLenPos:valueLenPos])
		valueLen := binary.BigEndian.Uint32(header[valueLenPos:headerSize])

		if entryOffset == targetOffset {
			key := make([]byte, keyLen)
			value := make([]byte, valueLen)

			if _, err := io.ReadFull(s.logFile, key); err != nil {
				return nil, fmt.Errorf("read key: %w", err)
			}
			if _, err := io.ReadFull(s.logFile, value); err != nil {
				return nil, fmt.Errorf("read value: %w", err)
			}

			return &LogEntry{
				offset:    entryOffset,
				timestamp: timestamp,
				key:       key,
				value:     value,
			}, nil
		}

		if _, err := s.logFile.Seek(int64(keyLen+valueLen), io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("skip entry: %w", err)
		}
	}

	return nil, fmt.Errorf("offset %d: entry not found in segment", targetOffset)
}

func (s *LogSegment) Seal() error {
	s.isSealed = true
	return s.Close()
}

func (s *LogSegment) Close() error {
	var errs []error

	if s.logFile != nil {
		if err := s.logFile.Close(); err != nil {
			if !errors.Is(err, os.ErrClosed) {
				errs = append(errs, fmt.Errorf("closing log file: %w", err))
			}
		}
		s.logFile = nil
	}

	if s.indexFile != nil {
		if err := s.indexFile.Close(); err != nil {
			if !errors.Is(err, os.ErrClosed) {
				errs = append(errs, fmt.Errorf("closing index file: %w", err))
			}
		}
		s.indexFile = nil
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
