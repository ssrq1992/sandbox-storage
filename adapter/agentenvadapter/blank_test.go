package agentenvadapter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

type failingZeroReader struct{}

func (failingZeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), errors.New("device read failure")
}

func TestCheckZeroSizeRequiresWholeDevice(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
		size uint64
		ok   bool
	}{
		{"empty-capacity", nil, 0, false},
		{"blank", make([]byte, 4096), 4096, true},
		{"short", make([]byte, 4095), 4096, false},
		{"grown", make([]byte, 4097), 4096, false},
		{"unknown-data", append(make([]byte, 4095), 1), 4096, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkZeroSize(context.Background(), bytes.NewReader(tc.data), tc.size)
			if (err == nil) != tc.ok {
				t.Fatalf("error=%v, want success=%v", err, tc.ok)
			}
		})
	}
	if err := checkZeroSize(context.Background(), bytes.NewReader(nil), 4096); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short read: %v", err)
	}
	if err := checkZeroSize(context.Background(), failingZeroReader{}, 4096); err == nil {
		t.Fatal("read failure accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := checkZeroSize(ctx, bytes.NewReader(make([]byte, 4096)), 4096); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
