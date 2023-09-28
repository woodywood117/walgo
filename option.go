package wal

import (
	"errors"
	"time"
)

var ErrIndexAndTimestampSet = errors.New("index and timestamp cannot both be set")

type Option func(*Wal)

// WithMaxSegmentSize sets the maximum size of a segment in bytes
func WithMaxSegmentSize(maxSegmentSize uint64) Option {
	return func(wal *Wal) {
		wal.config.MaxSegmentSize = maxSegmentSize
	}
}

// WithMaxSegmentCount sets the maximum number of segments
func WithMaxSegmentCount(maxSegmentCount uint64) Option {
	return func(wal *Wal) {
		wal.config.MaxSegmentCount = maxSegmentCount
	}
}

// WithExpiration sets the expiration time of messages in the Wal and the interval at which to check for expired segments
func WithExpiration(limit, interval time.Duration) Option {
	return func(wal *Wal) {
		wal.config.ExpirationTime = limit
		wal.config.ExpirationInterval = interval
	}
}

type ReaderOption func(*Reader) error

// WithIndex sets the starting index of the Reader. It will try to seek to the message with that index.
// If it is set to before the earliest index, it will be set to the earliest index.
// If it is set to after the latest index, it will be set to the latest index.
// It will fail if the WithTimestamp option is also set.
func WithIndex(index uint64) ReaderOption {
	return func(reader *Reader) error {
		reader.index = index

		if !reader.timestamp.IsZero() {
			return ErrIndexAndTimestampSet
		}
		return nil
	}
}

// WithTimestamp sets the starting timestamp of the Reader. It will seek for the first message after that timestamp.
// If it is set to after the latest timestamp, it will be set to the latest timestamp.
// It will fail if the WithIndex option is also set.
func WithTimestamp(timestamp time.Time) ReaderOption {
	return func(reader *Reader) error {
		reader.timestamp = timestamp

		if reader.index != 0 {
			return ErrIndexAndTimestampSet
		}
		return nil
	}
}
