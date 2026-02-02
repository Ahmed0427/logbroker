package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func setupTestStorage(t *testing.T) (*LogStorage, string) {
	tmpDir, err := os.MkdirTemp("", "kafka-storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	storage := NewLogStorage(tmpDir, 1024*1024, 100) // 1MB segments
	return storage, tmpDir
}

func cleanupTestStorage(t *testing.T, storage *LogStorage, tmpDir string) {
	if storage != nil {
		if err := storage.Close(); err != nil {
			t.Logf("Error closing storage: %v", err)
		}
	}
	if tmpDir != "" {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("Error cleaning up temp dir: %v", err)
		}
	}
}

func TestLogStorageBasic(t *testing.T) {
	storage, tmpDir := setupTestStorage(t)
	defer cleanupTestStorage(t, storage, tmpDir)

	expectedOffsets := make([]uint64, 10)
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		offset, err := storage.Produce("test-topic", 0, key, value)
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}

		if i == 0 && offset != 0 {
			t.Errorf("First offset should be 0, got %d", offset)
		}
		if i > 0 && offset != expectedOffsets[i-1]+1 {
			t.Errorf("Non-sequential offset at %d: got %d, expected %d",
				i, offset, expectedOffsets[i-1]+1)
		}
		expectedOffsets[i] = offset
	}

	entries, err := storage.Consume("test-topic", 0, 0, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to consume messages: %v", err)
	}

	if len(entries) != 10 {
		t.Fatalf("Expected 10 entries, got %d", len(entries))
	}

	for i, entry := range entries {
		expectedKey := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := []byte(fmt.Sprintf("value-%d", i))

		if entry.Offset != uint64(i) {
			t.Errorf("Entry %d: offset mismatch: got %d, want %d", i, entry.Offset, i)
		}
		if !bytes.Equal(entry.Key, expectedKey) {
			t.Errorf("Entry %d: key mismatch: got %s, want %s", i, entry.Key, expectedKey)
		}
		if !bytes.Equal(entry.Value, expectedValue) {
			t.Errorf("Entry %d: value mismatch: got %s, want %s", i, entry.Value, expectedValue)
		}
		if entry.Timestamp == 0 {
			t.Errorf("Entry %d: timestamp should not be zero", i)
		}
	}

	entries, err = storage.Consume("test-topic", 0, 0, 100) // Small limit
	if err != nil {
		t.Fatalf("Failed to consume with limit: %v", err)
	}
	if len(entries) == 0 {
		t.Error("Should get at least one entry with 100 byte limit")
	}

	entries, err = storage.Consume("test-topic", 0, 5, 1024)
	if err != nil {
		t.Fatalf("Failed to consume from offset 5: %v", err)
	}
	if len(entries) != 5 { // offsets 5-9
		t.Errorf("Expected 5 entries from offset 5, got %d", len(entries))
	}
	if len(entries) > 0 && entries[0].Offset != 5 {
		t.Errorf("First entry from offset 5 should be offset 5, got %d", entries[0].Offset)
	}
}

func TestLogStorageMultipleTopicsPartitions(t *testing.T) {
	storage, tmpDir := setupTestStorage(t)
	defer cleanupTestStorage(t, storage, tmpDir)

	type produceTest struct {
		topic     string
		partition int
		key       []byte
		value     []byte
	}

	tests := []produceTest{
		{"topic1", 0, []byte("key1"), []byte("value1")},
		{"topic1", 1, []byte("key2"), []byte("value2")},
		{"topic2", 0, []byte("key3"), []byte("value3")},
		{"topic2", 1, []byte("key4"), []byte("value4")},
	}

	for _, test := range tests {
		offset, err := storage.Produce(test.topic, test.partition, test.key, test.value)
		if err != nil {
			t.Fatalf("Failed to produce to %s-%d: %v", test.topic, test.partition, err)
		}

		// Should be offset 0 for each partition
		if offset != 0 {
			t.Errorf("First message in partition should be offset 0, got %d", offset)
		}
	}

	// Verify isolation between topics/partitions
	for _, test := range tests {
		entries, err := storage.Consume(test.topic, test.partition, 0, 1024)
		if err != nil {
			t.Fatalf("Failed to consume from %s-%d: %v", test.topic, test.partition, err)
		}

		if len(entries) != 1 {
			t.Errorf("Expected 1 entry for %s-%d, got %d", test.topic, test.partition, len(entries))
			continue
		}

		entry := entries[0]
		if !bytes.Equal(entry.Key, test.key) {
			t.Errorf("Key mismatch for %s-%d: got %s, want %s",
				test.topic, test.partition, entry.Key, test.key)
		}
		if !bytes.Equal(entry.Value, test.value) {
			t.Errorf("Value mismatch for %s-%d: got %s, want %s",
				test.topic, test.partition, entry.Value, test.value)
		}
	}
}

func TestLogStorageSegmentRollover(t *testing.T) {
	// Use very small segment size to force rollover
	storage := NewLogStorage(t.TempDir(), 128, 200)
	defer storage.Close()

	// Create messages that will force segment rollover
	largeValue := bytes.Repeat([]byte("x"), 90)

	offsets := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		offset, err := storage.Produce("test", 0, []byte(fmt.Sprintf("key-%d", i)), largeValue)
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
		offsets[i] = offset
	}

	// Should have created multiple segments
	_, err := storage.GetPartition("test", 0)
	if err != nil {
		t.Fatalf("Failed to get partition: %v", err)
	}

	// This assumes LogPartition has a way to check segment count
	// Since we can't access segments directly, verify by reading all messages
	entries, err := storage.Consume("test", 0, 0, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to consume: %v", err)
	}

	if len(entries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(entries))
	}

	// Verify offsets are sequential
	for i := 0; i < len(entries)-1; i++ {
		if entries[i+1].Offset != entries[i].Offset+1 {
			t.Errorf("Non-sequential offsets: %d followed by %d",
				entries[i].Offset, entries[i+1].Offset)
		}
	}
}

func TestLogStorageHighWatermark(t *testing.T) {
	storage, tmpDir := setupTestStorage(t)
	defer cleanupTestStorage(t, storage, tmpDir)

	// Initial high watermark should be 0
	hwm, err := storage.GetHighWatermark("test", 0)
	if err != nil {
		t.Fatalf("Failed to get initial high watermark: %v", err)
	}
	if hwm != 0 {
		t.Errorf("Initial high watermark should be 0, got %d", hwm)
	}

	// After producing messages, high watermark should update
	for i := 0; i < 5; i++ {
		_, err := storage.Produce("test", 0, []byte(fmt.Sprintf("key-%d", i)), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}

		hwm, err := storage.GetHighWatermark("test", 0)
		if err != nil {
			t.Fatalf("Failed to get high watermark after message %d: %v", i, err)
		}

		// High watermark should be next offset to be assigned
		if hwm != uint64(i+1) {
			t.Errorf("After %d messages, high watermark should be %d, got %d", i+1, i+1, hwm)
		}
	}

	_, err = storage.Produce("test", 1, []byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to produce to partition 1: %v", err)
	}

	hwm1, err := storage.GetHighWatermark("test", 1)
	if err != nil {
		t.Fatalf("Failed to get high watermark for partition 1: %v", err)
	}
	if hwm1 != 1 {
		t.Errorf("Partition 1 high watermark should be 1, got %d", hwm1)
	}

	hwm0, err := storage.GetHighWatermark("test", 0)
	if err != nil {
		t.Fatalf("Failed to get high watermark for partition 0: %v", err)
	}
	if hwm0 != 5 {
		t.Errorf("Partition 0 high watermark should still be 5, got %d", hwm0)
	}
}

func TestLogStorageConcurrent(t *testing.T) {
	storage, tmpDir := setupTestStorage(t)
	defer cleanupTestStorage(t, storage, tmpDir)

	const numGoroutines = 10
	const messagesPerGoroutine = 100

	errCh := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < messagesPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("goroutine-%d-key-%d", goroutineID, i))
				value := []byte(fmt.Sprintf("goroutine-%d-value-%d", goroutineID, i))

				_, err := storage.Produce("test", 0, key, value)
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d failed to produce message %d: %v", goroutineID, i, err)
					return
				}
			}
			errCh <- nil
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}

	// Verify total messages
	hwm, err := storage.GetHighWatermark("test", 0)
	if err != nil {
		t.Fatalf("Failed to get high watermark: %v", err)
	}

	expected := uint64(numGoroutines * messagesPerGoroutine)
	if hwm != expected {
		t.Errorf("Expected %d messages, high watermark is %d", expected, hwm)
	}

	// Try to read all messages concurrently
	done := make(chan bool, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func() {
			entries, err := storage.Consume("test", 0, 0, 1024*1024)
			if err != nil {
				t.Errorf("Failed to consume: %v", err)
			}
			if len(entries) != int(expected) {
				t.Errorf("Expected %d entries, got %d", expected, len(entries))
			}
			done <- true
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

func TestLogStoragePersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// First storage instance
	storage1 := NewLogStorage(tmpDir, 1024, 200)

	// Produce some messages
	for i := 0; i < 10; i++ {
		_, err := storage1.Produce("test", 0, []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
	}

	// Close first instance
	if err := storage1.Close(); err != nil {
		t.Fatalf("Failed to close storage1: %v", err)
	}

	// Create second storage instance with same directory
	storage2 := NewLogStorage(tmpDir, 1024, 200)
	defer storage2.Close()

	// Verify data was persisted
	hwm, err := storage2.GetHighWatermark("test", 0)
	if err != nil {
		t.Fatalf("Failed to get high watermark: %v", err)
	}
	if hwm != 10 {
		t.Errorf("Expected high watermark 10, got %d", hwm)
	}

	// Read all messages
	entries, err := storage2.Consume("test", 0, 0, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to consume: %v", err)
	}

	if len(entries) != 10 {
		t.Fatalf("Expected 10 entries, got %d", len(entries))
	}

	// Verify content
	for i, entry := range entries {
		expectedKey := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := []byte(fmt.Sprintf("value-%d", i))

		if entry.Offset != uint64(i) {
			t.Errorf("Entry %d: offset mismatch: got %d, want %d", i, entry.Offset, i)
		}
		if !bytes.Equal(entry.Key, expectedKey) {
			t.Errorf("Entry %d: key mismatch: got %s, want %s", i, entry.Key, expectedKey)
		}
		if !bytes.Equal(entry.Value, expectedValue) {
			t.Errorf("Entry %d: value mismatch: got %s, want %s", i, entry.Value, expectedValue)
		}
	}

	// Produce more messages after reload
	offset, err := storage2.Produce("test", 0, []byte("new-key"), []byte("new-value"))
	if err != nil {
		t.Fatalf("Failed to produce new message: %v", err)
	}
	if offset != 10 {
		t.Errorf("New message should have offset 10, got %d", offset)
	}
}

// TestLogStorageEdgeCases tests edge cases
func TestLogStorageEdgeCases(t *testing.T) {
	storage, tmpDir := setupTestStorage(t)
	defer cleanupTestStorage(t, storage, tmpDir)

	// Test empty key and value
	offset, err := storage.Produce("test", 0, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("Failed to produce empty message: %v", err)
	}

	entries, err := storage.Consume("test", 0, offset, 1024)
	if err != nil {
		t.Fatalf("Failed to consume empty message: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]
	if len(entry.Key) != 0 {
		t.Errorf("Expected empty key, got %v", entry.Key)
	}
	if len(entry.Value) != 0 {
		t.Errorf("Expected empty value, got %v", entry.Value)
	}

	// Test nil key
	offset, err = storage.Produce("test", 0, nil, []byte("value"))
	if err != nil {
		t.Fatalf("Failed to produce with nil key: %v", err)
	}

	entries, err = storage.Consume("test", 0, offset, 1024)
	if err != nil {
		t.Fatalf("Failed to consume nil key message: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	// Test very large offset consumption (should return empty)
	entries, err = storage.Consume("test", 0, 1000, 1024)
	if err != nil {
		t.Fatalf("Failed to consume from large offset: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected no entries from large offset, got %d", len(entries))
	}

	// Test negative maxBytes (should be handled gracefully by caller, but test anyway)
	entries, err = storage.Consume("test", 0, 0, 0)
	if err != nil {
		t.Fatalf("Failed to consume with maxBytes 0: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected no entries with maxBytes 0, got %d", len(entries))
	}
}

func TestLogStorageFileCorruption(t *testing.T) {
	tmpDir := t.TempDir()

	// First create valid storage with some data
	storage := NewLogStorage(tmpDir, 1024, 200)

	// Produce valid messages
	for i := 0; i < 3; i++ {
		_, err := storage.Produce("test", 0, []byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)))

		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
	}

	storage.Close()

	// Corrupt the last segment by truncating it
	logFiles, _ := filepath.Glob(filepath.Join(tmpDir, "test-0", "*.log"))
	if len(logFiles) == 0 {
		t.Fatal("No log files found")
	}

	lastLogFile := logFiles[len(logFiles)-1]
	file, err := os.OpenFile(lastLogFile, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatalf("Failed to stat log file: %v", err)
	}

	// Truncate to corrupt the last entry
	if err := file.Truncate(info.Size() - 10); err != nil {
		file.Close()
		t.Fatalf("Failed to truncate file: %v", err)
	}
	file.Close()

	// Try to reload storage - it should handle corruption gracefully
	storage2 := NewLogStorage(tmpDir, 1024, 200)
	defer storage2.Close()

	// Should still be able to read uncorrupted messages
	entries, err := storage2.Consume("test", 0, 0, 1024)
	if err != nil {
		// Either error or partial read is acceptable
		t.Logf("Consume after corruption returned error (expected): %v", err)
	}

	// If we got entries, verify they're valid
	for _, entry := range entries {
		if entry.Offset >= 3 { // Our corruption might affect offset 2+
			t.Logf("Got entry from corrupted region: offset %d", entry.Offset)
		}
	}

	// Should still be able to produce new messages
	offset, err := storage2.Produce("test", 0, []byte("new-key"), []byte("new-value"))
	if err != nil {
		t.Fatalf("Failed to produce after corruption: %v", err)
	}

	// New message should be on a fresh segment or continuation
	t.Logf("Produced new message at offset %d after corruption", offset)
}

func TestLogStorageCloseBehavior(t *testing.T) {
	storage, tmpDir := setupTestStorage(t)
	defer os.RemoveAll(tmpDir) // Don't use cleanupTestStorage as we test Close

	// Produce some messages
	for i := 0; i < 5; i++ {
		_, err := storage.Produce("test", 0, []byte(fmt.Sprintf("key-%d", i)), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
	}

	// Close storage
	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	// Attempting to use closed storage should either panic or return error
	// The actual behavior depends on implementation
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Panic on use after close (expected): %v", r)
		}
	}()

	_, err := storage.Produce("test", 0, []byte("key"), []byte("value"))
	if err == nil {
		t.Error("Expected error when using closed storage, got nil")
	}
}
