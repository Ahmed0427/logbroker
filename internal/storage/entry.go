package storage

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
	Offset    uint64
	Timestamp uint64
	Key       []byte
	Value     []byte
}

func (e *LogEntry) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	order := binary.BigEndian

	if err := binary.Write(buf, order, e.Offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, order, e.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, order, uint32(len(e.Key))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, order, uint32(len(e.Value))); err != nil {
		return nil, err
	}

	buf.Write(e.Key)
	buf.Write(e.Value)

	return buf.Bytes(), nil
}

func DecodeLogEntry(data []byte) (*LogEntry, error) {
	reader := bytes.NewReader(data)
	e := new(LogEntry)
	order := binary.BigEndian

	// Read fixed-size fields
	if err := binary.Read(reader, order, &e.Offset); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, order, &e.Timestamp); err != nil {
		return nil, err
	}

	var keyLen, valLen uint32
	if err := binary.Read(reader, order, &keyLen); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, order, &valLen); err != nil {
		return nil, err
	}

	e.Key = make([]byte, keyLen)
	if _, err := io.ReadFull(reader, e.Key); err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	e.Value = make([]byte, valLen)
	if _, err := io.ReadFull(reader, e.Value); err != nil {
		return nil, fmt.Errorf("failed to read value: %w", err)
	}

	return e, nil
}
