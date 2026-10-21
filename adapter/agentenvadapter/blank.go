package agentenvadapter

import (
	"context"
	"errors"
	"io"
)

// Initialization requires an actually empty fresh device, not merely the
// absence of a blkid-recognized filesystem. This one-time scan is not on reuse.
func checkZero(ctx context.Context, r io.Reader) error {
	b := make([]byte, 1<<20)
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		n, e := r.Read(b)
		for _, v := range b[:n] {
			if v != 0 {
				return errors.New("new-volume initialization refused: device contains nonzero data")
			}
		}
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
}

// checkZeroSize checks the complete advertised device and rejects both short
// reads and unexpected growth. A truncated stream is not an empty volume.
func checkZeroSize(ctx context.Context, r io.Reader, size uint64) error {
	if size == 0 || size > uint64(^uint64(0)>>1) {
		return errors.New("invalid block capacity")
	}
	limited := &io.LimitedReader{R: r, N: int64(size)}
	if err := checkZero(ctx, limited); err != nil {
		return err
	}
	if limited.N != 0 {
		return io.ErrUnexpectedEOF
	}
	var extra [1]byte
	n, err := r.Read(extra[:])
	if n != 0 {
		return errors.New("device grew during initialization check")
	}
	if err != io.EOF {
		if err == nil {
			return io.ErrNoProgress
		}
		return err
	}
	return nil
}
