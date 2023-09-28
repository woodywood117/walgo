package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

var ErrNoSegmentsFound = errors.New("no segments found")
var ErrFileAlreadyOpen = errors.New("file already open")

type segment struct {
	firstIndex uint64
	lastIndex  uint64

	firstTimestamp time.Time
	lastTimestamp  time.Time

	path       string
	file       *os.File
	fileLength uint64
}

// createSegment creates a new segment file in the given directory
// the name of the segment file is of the format `{index}.wal` where the index is zero-padded to the length of the max index
func createSegment(dir string, index uint64) (*segment, error) {
	// Create segment file
	fname := fmt.Sprintf("%020d.wal", index)
	fpath := filepath.Join(dir, fname)
	file, err := os.Create(fpath)
	if err != nil {
		return nil, err
	}

	return &segment{
		firstIndex: index,
		lastIndex:  index,
		path:       fpath,
		file:       file,
	}, nil
}

// Will load the segment file at the given path, doesn't keep the file open
func loadSegment(path string) (*segment, error) {
	// Open segment file
	file, err := os.OpenFile(path, os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var firstTimestamp time.Time
	m, err := readEntry(file)
	if err == io.EOF {
	} else if err != nil {
		return nil, err
	} else {
		firstTimestamp = m.Timestamp
	}

	// Get the length of the file
	length, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	name := filepath.Base(path)
	var current uint64
	_, err = fmt.Sscanf(name, "%d.wal", &current)
	if err != nil {
		return nil, err
	}

	var lastIndex = current
	var lastTimestamp = firstTimestamp
	if length > 0 {
		m, err := readPreviousEntry(file)
		if err != nil {
			return nil, err
		}
		lastIndex = m.Index
		lastTimestamp = m.Timestamp
	}

	return &segment{
		firstIndex:     current,
		lastIndex:      lastIndex,
		firstTimestamp: firstTimestamp,
		lastTimestamp:  lastTimestamp,
		path:           path,
		fileLength:     uint64(length),
	}, nil
}

func (s *segment) open() error {
	if s.file != nil {
		return ErrFileAlreadyOpen
	}

	file, err := os.OpenFile(s.path, os.O_RDWR, 0755)
	if err != nil {
		return err
	}

	s.file = file
	return nil
}

func (s *segment) write(message *Entry) error {
	err := writeEntry(s.file, message)
	if err != nil {
		return err
	}

	// Update segment metadata
	s.lastIndex = message.Index
	s.lastTimestamp = message.Timestamp
	if s.fileLength == 0 {
		s.firstTimestamp = s.lastTimestamp
	}
	s.fileLength += message.size()
	return nil
}

func (s *segment) flush() error {
	return s.file.Sync()
}

func (s *segment) close() error {
	err := s.file.Close()
	s.file = nil
	return err
}
