package storage

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
		p, _ := NewLogPartition(1, t.TempDir(), 1024, 10)
		defer p.Close()

		_, err := p.Read(0)
		if err == nil {
			t.Error("expected error when reading from empty partition, got nil")
		}
	})

	t.Run("BoundaryOffsets", func(t *testing.T) {
		p, _ := NewLogPartition(1, t.TempDir(), 1024, 10)
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
		// be carefule cuz the size isn't only key and value length but also some metadata
		segmentSize := uint32(60)
		p1, err := NewLogPartition(1, dir, segmentSize, 40)
		if err != nil {
			t.Errorf("%v", err)
		}

		for i := 0; i < 20; i++ {
			p1.Append([]byte("key"), make([]byte, 30))
		}

		if len(p1.segments) < 2 {
			t.Errorf("expected rolling, but have only %d segments", len(p1.segments))
		}

		lastOffset := p1.nextOffset - 1
		p1.Close()

		// Recovery Check
		p2, err := NewLogPartition(1, dir, segmentSize, 10)
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

	t.Run("RollingAndRecovery", func(t *testing.T) {
		dir := t.TempDir()

		// Set size small enough so two appends trigger a roll
		// be carefule cuz the size isn't only key and value length but also some metadata
		segmentSize := uint32(60)
		p, err := NewLogPartition(1, dir, segmentSize, 10)
		if err != nil {
			t.Errorf("%v", err)
		}

		for i := 0; i < 30; i++ {
			p.Append([]byte("key"), make([]byte, 30))
		}

		if len(p.segments) != int(p.maxNumberOfSegments) {
			t.Errorf("expected %d segment, but have %d segments",
				p.maxNumberOfSegments, len(p.segments))
		}
	})
}

func TestLogPartitionReadRange(t *testing.T) {
	dir := t.TempDir()
	// Set segment size small to force multiple files
	// Each entry (key+val) is ~20 bytes, so 50 bytes rolls every ~2.5 entries
	p, _ := NewLogPartition(1, dir, 50, 20)
	defer p.Close()

	// 1. Setup: Write 10 entries across roughly 4 segments
	for i := 0; i < 10; i++ {
		_, err := p.Append([]byte{byte(i)}, []byte("data")) // ~5 bytes + overhead
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run("ReadAcrossBoundaries", func(t *testing.T) {
		// Start at offset 2, ask for enough bytes to get 5 entries
		// This should start in Segment 1 and end in Segment 2 or 3
		entries, err := p.ReadRange(2, 1000)
		if err != nil {
			t.Fatalf("ReadRange failed: %v", err)
		}

		if len(entries) != 8 { // Offsets 2 through 9
			t.Errorf("Expected 8 entries, got %d", len(entries))
		}

		if entries[0].Offset != 2 || entries[len(entries)-1].Offset != 9 {
			t.Errorf("Range bounds mismatch: start %d, end %d",
				entries[0].Offset, entries[len(entries)-1].Offset)
		}
	})

	t.Run("RespectMaxBytes", func(t *testing.T) {
		// Ask for offset 0, but only 10 bytes of data
		// Since each entry's value "data" is 4 bytes and key is 1,
		// it should return exactly 2 entries.
		maxBytes := 10
		entries, err := p.ReadRange(0, maxBytes)
		if err != nil {
			t.Fatal(err)
		}

		if len(entries) > 2 {
			t.Errorf("ReadRange exceeded maxBytes: got %d entries", len(entries))
		}
	})

	t.Run("OutOfBoundsStart", func(t *testing.T) {
		entries, err := p.ReadRange(100, 100)
		if len(entries) > 0 || err != nil {
			t.Error("Expected error for out-of-bounds startOffset, got nil")
		}
	})
}
