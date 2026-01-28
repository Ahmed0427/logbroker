package main

import (
	"testing"
)

func TestCheckPartitionDirEntry(t *testing.T) {
	tests := []struct {
		name    string
		isDir   bool
		wantErr bool
	}{
		{"00000000000000000001.log", false, false},
		{"00000000000000000001.index", false, false},
		{"short.log", false, true},                // Length != 20
		{"00000000000000000001.txt", false, true}, // Invalid extension
		{"000000000000000abcde.log", false, true}, // Not all digits
		{"00000000000000000001.log", true, true},  // Is a directory
	}

	for _, tt := range tests {
		err := checkPartitionDirEntry(tt.name, tt.isDir)
		if (err != nil) != tt.wantErr {
			t.Errorf("checkPartitionDirEntry(%s) error = %v, wantErr %v",
				tt.name, err, tt.wantErr)
		}
	}
}

func TestLogPartitionComprehensive(t *testing.T) {
	t.Run("EmptyReadReturnsError", func(t *testing.T) {
		p, _ := NewLogPartition(1, t.TempDir(), 1024)
		defer p.Close()

		_, err := p.Read(0)
		if err == nil {
			t.Error("expected error when reading from empty partition, got nil")
		}
	})

	t.Run("BoundaryOffsets", func(t *testing.T) {
		p, _ := NewLogPartition(1, t.TempDir(), 1024)
		defer p.Close()

		p.Append([]byte("k1"), []byte("v1")) // Offset 0

		// Test exact match
		if _, err := p.Read(0); err != nil {
			t.Errorf("failed to read existing offset 0: %v", err)
		}

		// Test out of bounds (higher than nextOffset)
		if _, err := p.Read(1); err == nil {
			t.Error("expected error reading out-of-bounds offset 1")
		}
	})

	t.Run("RollingAndRecovery", func(t *testing.T) {
		dir := t.TempDir()
		// Set size small enough so two appends trigger a roll
		segmentSize := uint32(50)
		p1, _ := NewLogPartition(1, dir, segmentSize)

		// Fill and Roll
		for i := 0; i < 5; i++ {
			p1.Append([]byte("key"), make([]byte, 30))
		}

		if len(p1.segments) < 2 {
			t.Errorf("expected rolling, but have only %d segments", len(p1.segments))
		}

		lastOffset := p1.nextOffset - 1
		p1.Close()

		// Recovery Check
		p2, err := NewLogPartition(1, dir, segmentSize)
		if err != nil {
			t.Fatalf("recovery failed: %v", err)
		}
		defer p2.Close()

		if p2.nextOffset != lastOffset+1 {
			t.Errorf("offset mismatch after recovery: expected %d, got %d",
				lastOffset+1, p2.nextOffset)
		}

		// Verify data integrity after recovery
		if _, err := p2.Read(0); err != nil {
			t.Errorf("could not read offset 0 after recovery: %v", err)
		}
	})
}
