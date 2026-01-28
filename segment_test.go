package main

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestLogSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const maxSize uint32 = 1024

	t.Run("Append and Read multiple entries", func(t *testing.T) {
		segment, err := NewLogSegment(dir, 0, maxSize)
		if err != nil {
			t.Fatalf("failed to create segment: %v", err)
		}
		defer segment.logFile.Close()

		testEntries := []*LogEntry{
			{offset: 100, timestamp: 1000, key: []byte("color"), value: []byte("red")},
			{offset: 101, timestamp: 1001, key: []byte("size"), value: []byte("large")},
			{offset: 102, timestamp: 1002, key: []byte("mode"), value: []byte("silent")},
		}

		for _, e := range testEntries {
			if err := segment.Append(e); err != nil {
				t.Fatalf("failed to append entry %d: %v", e.offset, err)
			}
		}

		targets := []struct {
			offset   uint64
			expected string
		}{
			{101, "large"},
			{102, "silent"},
			{100, "red"},
		}

		for _, tc := range targets {
			entry, err := segment.Read(tc.offset)
			if err != nil {
				t.Errorf("Read(%d) failed: %v", tc.offset, err)
				continue
			}
			if !bytes.Equal(entry.value, []byte(tc.expected)) {
				t.Errorf("Read(%d): expected value %s, got %s",
					tc.offset, tc.expected, string(entry.value))
			}
		}
	})

	t.Run("Entry Not Found", func(t *testing.T) {
		segment, _ := NewLogSegment(dir, 200, maxSize)
		_, err := segment.Read(999)
		if err == nil {
			t.Error("expected error for non-existent offset, got nil")
		}
	})

	t.Run("Respect Max Size", func(t *testing.T) {
		smallSegment, _ := NewLogSegment(dir, 300, 30)
		entry := &LogEntry{offset: 300, key: []byte("key"),
			value: []byte("too long value for small segment")}

		err := smallSegment.Append(entry)
		if err == nil {
			t.Error("expected error due to maxSize limit, but append succeeded")
		}
	})

	t.Run("Sealed Segment", func(t *testing.T) {
		segment, _ := NewLogSegment(dir, 400, maxSize)
		segment.isSealed = true

		entry := &LogEntry{offset: 400, key: []byte("k"), value: []byte("v")}
		err := segment.Append(entry)
		if err == nil || err.Error() == "" {
			t.Error("expected error appending to sealed segment")
		}
	})
}

func TestLogSegmentPersistence(t *testing.T) {
	dir, _ := os.MkdirTemp("", "persist_test")
	defer os.RemoveAll(dir)

	const maxSize = 1024 * 1024
	entry := &LogEntry{offset: 100, key: []byte("key1"), value: []byte("val1")}

	seg1, _ := NewLogSegment(dir, 0, maxSize)
	seg1.Append(entry)
	err := seg1.Close()
	if err != nil {
		t.Fatalf("seg1.Close() failed: %v", err)
	}

	// reopen
	seg2, err := NewLogSegment(dir, 0, maxSize)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer seg2.logFile.Close()

	if seg2.currentSize == 0 {
		t.Error("expected recovered size > 0, got 0")
	}

	readBack, err := seg2.Read(100)
	if err != nil || string(readBack.value) != "val1" {
		t.Errorf("failed to read back persistent data: %v", err)
	}
}

func TestLogSegmentSparseIndexScanning(t *testing.T) {
	dir, _ := os.MkdirTemp("", "sparse_test")
	defer os.RemoveAll(dir)

	segment, _ := NewLogSegment(dir, 0, 4*1024*1024)
	defer segment.logFile.Close()

	largeVal := bytes.Repeat([]byte("x"), 1000)

	for i := uint64(0); i < 100; i++ {
		segment.Append(&LogEntry{
			offset: i,
			key:    []byte(fmt.Sprintf("%d", i)),
			value:  largeVal,
		})
	}

	_, err := segment.RebuildIndex()
	if err != nil {
		t.Errorf("%v", err)
	}

	for i := uint64(0); i < 100; i += 10 {
		ent, err := segment.Read(i)
		if err != nil {
			t.Fatalf("Failed to find offset %d in sparse index: %v", i, err)
		}
		if ent.offset != i {
			t.Errorf("Expected offset %d, got %d", i, ent.offset)
		}
		if !bytes.Equal(ent.value, largeVal) {
			t.Errorf("Unexpected value")
		}
	}

	for i := 0; i < 100; i += 10 {
		// key length = 1
		maxSize := (len(largeVal) + 1) * i
		ents, err := segment.ReadRange(0, maxSize)
		if err != nil {
			t.Fatalf("Failed to read rand from offset 0: %v", err)
		}
		if len(ents) != i {
			t.Fatalf("Expected to read %d entries, got: %d", i+1, len(ents))
		}
		for j := 0; j < len(ents); j++ {
			if string(ents[j].key) != fmt.Sprintf("%d", j) {
				t.Fatalf("Expected to read %d entries, got: %s", j, string(ents[j].key))
			}
		}
	}
}

func TestLogSegmentCorruptedIndex(t *testing.T) {
	dir, _ := os.MkdirTemp("", "corrupt_test")
	defer os.RemoveAll(dir)

	segment, _ := NewLogSegment(dir, 0, 1024)

	segment.indexCache = append(segment.indexCache, []byte{1, 2, 3}...)

	_, err := segment.lookupOffset(10)
	if err == nil {
		t.Error("expected error for corrupted index size, got nil")
	}
}

func BenchmarkLogSegmentAppend(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_append")
	defer os.RemoveAll(dir)

	segment, _ := NewLogSegment(dir, 0, 1024*1024*1024) // 1GB
	entry := &LogEntry{key: []byte("test-key"), value: []byte("test-value")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.offset = uint64(i)
		err := segment.Append(entry)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogSegmentRead(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_read")
	defer os.RemoveAll(dir)

	segment, _ := NewLogSegment(dir, 0, 1024*1024*1024)

	// Pre-fill
	for i := 0; i < 1000; i++ {
		segment.Append(&LogEntry{offset: uint64(i), key: []byte("k"), value: []byte("v")})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Read a random offset within the 1000 entries
		_, err := segment.Read(uint64(i % 1000))
		if err != nil {
			b.Fatal(err)
		}
	}
}
