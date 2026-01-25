package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type LogSegment struct {
	directory   string
	baseOffset  uint64
	maxSize     int64
	currentSize int64
	logFile     *os.File
	logPath     string
	isSealed    bool
}

func NewLogSegment(dir string, baseOffset uint64, maxSize int64) (*LogSegment, error) {
	fileName := fmt.Sprintf("%020d", baseOffset)
	logPath := filepath.Join(dir, fileName+".log")

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	currentSize, err := logFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	return &LogSegment{
		directory:   dir,
		baseOffset:  baseOffset,
		maxSize:     maxSize,
		currentSize: currentSize,
		logFile:     logFile,
		logPath:     logPath,
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

	if s.currentSize+int64(len(entryData)) > s.maxSize {
		return fmt.Errorf("segment full: entry size %d exceeds remaining %d bytes",
			len(entryData), s.maxSize-s.currentSize)
	}

	if _, err := s.logFile.Write(entryData); err != nil {
		return fmt.Errorf("write entry: %w", err)
	}

	s.currentSize += int64(len(entryData))
	return nil
}

func (s *LogSegment) Read(targetOffset uint64) (*LogEntry, error) {
	if targetOffset < s.baseOffset {
		return nil, fmt.Errorf("target offset < base offset")
	}

	if _, err := s.logFile.Seek(0, io.SeekStart); err != nil {
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
				Offset:    entryOffset,
				Timestamp: timestamp,
				Key:       key,
				Value:     value,
			}, nil
		}

		if _, err := s.logFile.Seek(int64(keyLen+valueLen), io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("skip entry: %w", err)
		}
	}

	return nil, fmt.Errorf("offset %d: entry not found in segment", targetOffset)
}
