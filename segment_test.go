package main

import (
	"bytes"
	"os"
	"testing"
)

func TestLogSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const maxSize int64 = 1024

	t.Run("Append and Read multiple entries", func(t *testing.T) {
		segment, err := NewLogSegment(dir, 0, maxSize)
		if err != nil {
			t.Fatalf("failed to create segment: %v", err)
		}
		defer segment.logFile.Close()

		testEntries := []*LogEntry{
			{Offset: 100, Timestamp: 1000, Key: []byte("color"), Value: []byte("red")},
			{Offset: 101, Timestamp: 1001, Key: []byte("size"), Value: []byte("large")},
			{Offset: 102, Timestamp: 1002, Key: []byte("mode"), Value: []byte("silent")},
		}

		for _, e := range testEntries {
			if err := segment.Append(e); err != nil {
				t.Fatalf("failed to append entry %d: %v", e.Offset, err)
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
			if !bytes.Equal(entry.Value, []byte(tc.expected)) {
				t.Errorf("Read(%d): expected value %s, got %s",
					tc.offset, tc.expected, string(entry.Value))
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
		entry := &LogEntry{Offset: 300, Key: []byte("key"),
			Value: []byte("too long value for small segment")}

		err := smallSegment.Append(entry)
		if err == nil {
			t.Error("expected error due to maxSize limit, but append succeeded")
		}
	})

	t.Run("Sealed Segment", func(t *testing.T) {
		segment, _ := NewLogSegment(dir, 400, maxSize)
		segment.isSealed = true

		entry := &LogEntry{Offset: 400, Key: []byte("k"), Value: []byte("v")}
		err := segment.Append(entry)
		if err == nil || err.Error() == "" {
			t.Error("expected error appending to sealed segment")
		}
	})
}
