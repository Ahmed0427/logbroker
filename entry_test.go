package main

import (
	"reflect"
	"testing"
)

func TestLogEntry(t *testing.T) {
	validEnt := &LogEntry{key: []byte("Hi"), value: []byte("There")}
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
				offset:    12345,
				timestamp: 1672531200,
				key:       []byte("user_1"),
				value:     []byte("active"),
			},
			wantErr: false,
		},
		{
			name: "Success: Empty key and value",
			input: &LogEntry{
				offset:    0,
				timestamp: 0,
				key:       []byte(""),
				value:     []byte(""),
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
