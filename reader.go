package wal

import (
	"slices"
	"time"
)

type Reader struct {
	index     uint64
	timestamp time.Time

	wal *Wal

	current *segment
}

func (r *Reader) Next() (*Entry, error) {
	if r.current == nil {
		return nil, ErrNoSegmentsFound
	}

	if r.current.file == nil {
		err := r.current.open()
		if err != nil {
			return nil, err
		}
	}

	if r.index > r.current.lastIndex {
		sindex := slices.IndexFunc(r.wal.segments, func(s *segment) bool {
			return s.path == r.current.path
		})
		if sindex == -1 {
			return nil, ErrNoSegmentsFound
		} else if sindex+1 >= len(r.wal.segments) {
			return nil, ErrNoSegmentsFound
		}

		err := r.current.close()
		if err != nil {
			return nil, err
		}

		r.current, err = loadSegment(r.wal.segments[sindex+1].path)
		if err != nil {
			return nil, err
		}

		err = r.current.open()
		if err != nil {
			return nil, err
		}
	}

	entry, err := readEntry(r.current.file)
	if err != nil {
		return nil, err
	}

	r.index = entry.Index + 1
	return entry, nil
}

func (r *Reader) Close() error {
	if r.current != nil {
		return r.current.close()
	}
	return nil
}
