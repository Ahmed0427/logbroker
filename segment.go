package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const indexInterval = 4096

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
	indexCache         []byte
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

	indexCache, err := os.ReadFile(indexPath)
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
		indexCache:  indexCache,
	}, nil
}

func (s *LogSegment) appendToIndex(offset uint64, pos uint32) error {
	indexEntryData := [8]byte{}
	binary.BigEndian.PutUint32(indexEntryData[:], uint32(offset-s.baseOffset))
	binary.BigEndian.PutUint32(indexEntryData[4:], pos)
	if _, err := s.indexFile.Write(indexEntryData[:]); err != nil {
		return fmt.Errorf("write index entry: %w", err)
	}
	s.indexCache = append(s.indexCache, indexEntryData[:]...)
	s.sizeSinceLastIndex = 0
	return nil
}

func (s *LogSegment) Append(entry *LogEntry) (bool, error) {
	if s.isSealed {
		return false, nil
	}

	entryData, err := entry.Encode()
	if err != nil {
		return false, fmt.Errorf("encode entry: %w", err)
	}

	if s.currentSize+uint32(len(entryData)) > s.maxSize {
		return false, nil
	}

	entryPos := s.currentSize
	_ = entryPos

	if _, err := s.logFile.Write(entryData); err != nil {
		return false, fmt.Errorf("write entry: %w", err)
	}

	s.currentSize += uint32(len(entryData))
	s.sizeSinceLastIndex += uint32(len(entryData))

	if s.sizeSinceLastIndex >= indexInterval {
		if err = s.appendToIndex(entry.offset, entryPos); err != nil {
			// rollback file
			if trErr := s.logFile.Truncate(int64(entryPos)); trErr != nil {
				return false, fmt.Errorf("index failed (%v) and truncate failed (%v)", err, trErr)
			}

			// rollback memory state
			s.currentSize = entryPos
			s.sizeSinceLastIndex -= uint32(len(entryData))

			return false, fmt.Errorf("append to index: %w", err)
		}
	}
	return true, nil
}

func (s *LogSegment) lookupOffset(targetOffset uint64) (uint32, error) {
	if targetOffset < s.baseOffset {
		return 0, fmt.Errorf("target offset < base offset")
	}

	indexData := s.indexCache[:]

	if len(indexData) == 0 {
		return 0, nil
	}

	if len(indexData)%8 != 0 {
		return 0, fmt.Errorf("corrupted index file")
	}

	var targetPos uint32 = 0
	l, r := 0, (len(indexData)/8)-1

	for l <= r {
		mid := (l + r) / 2
		entryData := indexData[mid*8 : (mid*8)+8]

		offset := binary.BigEndian.Uint32(entryData)
		pos := binary.BigEndian.Uint32(entryData[4:])

		if uint64(offset) > targetOffset-s.baseOffset {
			r = mid - 1
		} else {
			targetPos = pos
			l = mid + 1
		}
	}

	return targetPos, nil
}

func (s *LogSegment) Read(targetOffset uint64) (*LogEntry, error) {
	if targetOffset < s.baseOffset {
		return nil, fmt.Errorf("target offset < base offset")
	}

	entryPos, err := s.lookupOffset(targetOffset)
	if err != nil {
		return nil, fmt.Errorf("index lookup: %w", err)
	}

	currentPos := int64(entryPos)

	header := make([]byte, headerSize)

	for {
		if _, err := s.logFile.ReadAt(header, currentPos); err != nil {
			if err != io.EOF {
				return nil, fmt.Errorf("read header at %d: %w", currentPos, err)
			}
			break
		}

		entryOffset := binary.BigEndian.Uint64(header[offsetPos:timestampPos])
		timestamp := binary.BigEndian.Uint64(header[timestampPos:valueLenPos])
		keyLen := binary.BigEndian.Uint32(header[keyLenPos:valueLenPos])
		valueLen := binary.BigEndian.Uint32(header[valueLenPos:headerSize])

		if entryOffset == targetOffset {
			keyValue := make([]byte, keyLen+valueLen)

			off := currentPos + int64(headerSize)

			if _, err := s.logFile.ReadAt(keyValue, off); err != nil {
				return nil, fmt.Errorf("read key: %w", err)
			}

			key := keyValue[:keyLen]
			value := keyValue[keyLen:]

			return &LogEntry{
				offset:    entryOffset,
				timestamp: timestamp,
				key:       key,
				value:     value,
			}, nil
		}

		currentPos += int64(headerSize + keyLen + valueLen)

		if currentPos >= int64(s.currentSize) {
			break
		}
	}

	return nil, fmt.Errorf("offset %d: entry not found in segment", targetOffset)
}

func (s *LogSegment) ReadRange(startOffset uint64, maxSize int) ([]*LogEntry, error) {
	entries := []*LogEntry{}
	if maxSize == 0 {
		return entries, nil
	}

	currentSize := 0
	for {
		entry, err := s.Read(startOffset)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
		currentSize += len(entry.key) + len(entry.value)
		if currentSize >= maxSize {
			break
		}
		startOffset++
	}
	return entries, nil
}

func (s *LogSegment) Seal() error {
	s.isSealed = true
	var errs []error
	err := s.logFile.Sync()
	if err != nil {
		errs = append(errs, fmt.Errorf("syncing log file: %w", err))
	}
	err = s.indexFile.Sync()
	if err != nil {
		errs = append(errs, fmt.Errorf("syncing index file: %w", err))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *LogSegment) Close() error {
	var errs []error
	err := s.Seal()
	if err != nil {
		errs = append(errs, fmt.Errorf("sealing the segment: %w", err))
	}

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

func (s *LogSegment) RebuildIndex() (uint64, error) {
	if s.indexFile != nil {
		s.indexFile.Close()
	}
	os.Remove(s.indexPath)

	var err error
	s.indexFile, err = os.OpenFile(s.indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return 0, err
	}

	header := make([]byte, headerSize)
	var lastOffset uint64
	var currentPos uint32

	for {
		if _, err = s.logFile.ReadAt(header, int64(currentPos)); err != nil {
			if err != io.EOF {
				return 0, fmt.Errorf("read header at %d: %w", currentPos, err)
			}
			break
		}

		lastOffset = binary.BigEndian.Uint64(header[offsetPos:timestampPos])
		keyLen := binary.BigEndian.Uint32(header[keyLenPos:valueLenPos])
		valueLen := binary.BigEndian.Uint32(header[valueLenPos:headerSize])

		entrySize := headerSize + keyLen + valueLen

		s.sizeSinceLastIndex += entrySize
		if s.sizeSinceLastIndex > indexInterval {
			if err = s.appendToIndex(lastOffset, currentPos); err != nil {
				return 0, fmt.Errorf("failed to append to index: %w", err)
			}
		}

		currentPos += uint32(entrySize)
		if currentPos >= s.currentSize {
			break
		}
	}

	return lastOffset, nil
}
