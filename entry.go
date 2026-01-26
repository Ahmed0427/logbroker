package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	offsetPos    = 0
	timestampPos = 8
	keyLenPos    = 16
	valueLenPos  = 20
	headerSize   = 24
)

type LogEntry struct {
	offset    uint64
	timestamp uint64
	key       []byte
	value     []byte
}

func (e *LogEntry) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	order := binary.BigEndian

	if err := binary.Write(buf, order, e.offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, order, e.timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, order, uint32(len(e.key))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, order, uint32(len(e.value))); err != nil {
		return nil, err
	}

	buf.Write(e.key)
	buf.Write(e.value)

	return buf.Bytes(), nil
}

func DecodeLogEntry(data []byte) (*LogEntry, error) {
	reader := bytes.NewReader(data)
	e := new(LogEntry)
	order := binary.BigEndian

	// Read fixed-size fields
	if err := binary.Read(reader, order, &e.offset); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, order, &e.timestamp); err != nil {
		return nil, err
	}

	var keyLen, valLen uint32
	if err := binary.Read(reader, order, &keyLen); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, order, &valLen); err != nil {
		return nil, err
	}

	e.key = make([]byte, keyLen)
	if _, err := io.ReadFull(reader, e.key); err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	e.value = make([]byte, valLen)
	if _, err := io.ReadFull(reader, e.value); err != nil {
		return nil, fmt.Errorf("failed to read value: %w", err)
	}

	return e, nil
}
