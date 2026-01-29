package main

import (
	"reflect"
	"testing"
)

func TestLogEntry(t *testing.T) {
	validEnt := &LogEntry{Key: []byte("Hi"), Value: []byte("There")}
	validData, _ := validEnt.Encode()

	tests := []struct {
		name        string
		input       *LogEntry
		corruptData []byte
		wantErr     bool
	}{
		{
			name: "Success: Standard entry",
			input: &LogEntry{
				Offset:    12345,
				Timestamp: 1672531200,
				Key:       []byte("user_1"),
				Value:     []byte("active"),
			},
			wantErr: false,
		},
		{
			name: "Success: Empty key and value",
			input: &LogEntry{
				Offset:    0,
				Timestamp: 0,
				Key:       []byte(""),
				Value:     []byte(""),
			},
			wantErr: false,
		},
		{
			name:        "Error: Short data (header too small)",
			corruptData: []byte{0x00, 0x01, 0x02, 0x03},
			wantErr:     true,
		},
		{
			name:        "Error: Corrupted length (missing last byte)",
			corruptData: validData[:len(validData)-1],
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.input != nil {
				data, err := tt.input.Encode()
				if (err != nil) != tt.wantErr {
					t.Fatalf("Encode() error = %v, wantErr %v", err, tt.wantErr)
				}

				decoded, err := DecodeLogEntry(data)
				if (err != nil) != tt.wantErr {
					t.Fatalf("DecodeLogEntry() error = %v, wantErr %v", err, tt.wantErr)
				}

				if err == nil && !reflect.DeepEqual(tt.input, decoded) {
					t.Errorf("Roundtrip mismatch!\nGot:  %+v\nWant: %+v", decoded, tt.input)
				}
			} else {
				_, err := DecodeLogEntry(tt.corruptData)
				if (err != nil) != tt.wantErr {
					t.Errorf("DecodeLogEntry() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}
