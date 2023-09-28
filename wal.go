package wal

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"time"
)

var ErrNotADirectory = errors.New("not a directory")
var ErrConfigNotFound = errors.New("config file not found")
var ErrParseConfig = errors.New("error parsing config file")

// Wal is a write-ahead log
type Wal struct {
	path     string
	current  *segment
	segments []*segment
	config   config
	index    uint64
}

type config struct {
	MaxSegmentSize     uint64
	MaxSegmentCount    uint64
	ExpirationTime     time.Duration
	ExpirationInterval time.Duration
}

// New creates a new Wal instance and initializes the directory/file structure
func New(path string, options ...Option) (*Wal, error) {
	var err error
	wal := &Wal{}

	wal.path = path
	wal.path, err = filepath.Abs(wal.path)
	if err != nil {
		return nil, err
	}
	wal.path = filepath.ToSlash(wal.path)

	for _, option := range options {
		option(wal)
	}

	// Check if path exists, if so, error
	_, err = os.Lstat(path)
	if err == nil {
		return nil, os.ErrExist
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// Create wal directory
	err = os.MkdirAll(wal.path, 0755)
	if err != nil {
		return nil, err
	}

	// Create first segment
	wal.current, err = createSegment(wal.path, 0)
	if err != nil {
		return nil, err
	}
	wal.segments = append(wal.segments, wal.current)

	// Write config file to wal directory
	err = wal.writeConfigToDisk()
	if err != nil {
		return nil, err
	}

	return wal, nil
}

// Load loads an existing Wal instance from disk
func Load(dir string) (*Wal, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	// Check if dir exists and is a directory
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, ErrNotADirectory
	}

	// Create wal instance
	var wal Wal
	wal.path = dir
	wal.path = filepath.ToSlash(wal.path)

	// Read config file
	cpath := path.Join(dir, "config.json")
	config, err := os.Open(cpath)
	if err != nil {
		return nil, errors.Join(err, ErrConfigNotFound)
	}
	defer config.Close()
	dec := json.NewDecoder(config)
	err = dec.Decode(&wal.config)
	if err != nil {
		return nil, errors.Join(err, ErrParseConfig)
	}

	// Load segments
	paths, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		return nil, err
	}
	slices.Sort(paths)
	for _, p := range paths {
		// Make sure it's a file
		info, err := os.Lstat(p)
		if err != nil {
			return nil, err
		}

		name := info.Name()
		if !info.IsDir() && filepath.Ext(name) == ".wal" {
			s, err := loadSegment(p)
			if err != nil {
				return nil, err
			}

			wal.segments = append(wal.segments, s)
		}
	}

	// Set current segment info
	wal.current = wal.segments[len(wal.segments)-1]
	err = wal.current.open()
	if err != nil {
		return nil, err
	}

	// Seek to end of file
	_, err = wal.current.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	if wal.current.fileLength == 0 {
		wal.index = wal.current.lastIndex
	} else {
		wal.index = wal.current.lastIndex + 1
	}

	return &wal, err
}

// Write writes a message to the Wal in binary encoded Entry format
func (wal *Wal) Write(message []byte) (err error) {
	// check if current segment exists
	if wal.current == nil {
		return ErrNoSegmentsFound
	}

	// check if current segment is full
	if wal.config.MaxSegmentSize != 0 && wal.current.fileLength >= wal.config.MaxSegmentSize {
		err := wal.cycle()
		if err != nil {
			return err
		}
	}

	// Write message data length to segment
	err = wal.current.write(newMessage(wal.index, message))
	wal.index++

	return err
}

// writeConfigToDisk writes the current config to disk
func (wal *Wal) writeConfigToDisk() error {
	cpath := path.Join(wal.path, "config.json")
	config, err := os.Create(cpath)
	if err != nil {
		return err
	}
	defer config.Close()

	enc := json.NewEncoder(config)
	enc.SetIndent("", "\t")
	return enc.Encode(wal.config)
}

// Flush flushes the current segment to disk
func (wal *Wal) Flush() error {
	return wal.current.flush()
}

// cycle cycles the segments
func (wal *Wal) cycle() error {
	err := wal.Close()
	if err != nil {
		return err
	}

	// create new segment
	wal.current, err = createSegment(wal.path, wal.index)
	if err != nil {
		return err
	}
	wal.segments = append(wal.segments, wal.current)

	return nil
}

// Close closes the current segment
func (wal *Wal) Close() error {
	err := wal.current.flush()
	if err != nil {
		return err
	}
	return wal.current.close()
}

func (wal *Wal) Reader(options ...ReaderOption) (*Reader, error) {
	var err error
	reader := &Reader{
		wal: wal,
	}

	for _, option := range options {
		err = option(reader)
		if err != nil {
			return nil, err
		}
	}

	// Find the relevant segment based on timestamp or index
	if reader.timestamp.IsZero() {
		// Find segment based on index
		sindex := -1
		for i, s := range wal.segments {
			if reader.index > s.firstIndex && reader.index < s.lastIndex {
				sindex = i
				break
			}

			if reader.index == s.firstIndex || reader.index == s.lastIndex {
				sindex = i
				break
			}
		}

		if sindex != -1 {
			reader.current, err = loadSegment(wal.segments[sindex].path)
			if err != nil {
				return nil, err
			}
		}

		if reader.current == nil {
			if reader.index < wal.segments[0].firstIndex {
				reader.current, err = loadSegment(wal.segments[0].path)
			} else {
				reader.current, err = loadSegment(wal.segments[len(wal.segments)-1].path)
			}
			if err != nil {
				return nil, err
			}
		}
		err = reader.current.open()
		if err != nil {
			return nil, err
		}

		// Seek to index
		i := reader.current.firstIndex
		for reader.index > i {
			entry, err := readEntry(reader.current.file)
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			i = entry.Index
		}
		// Set it back to the index in question
		err = gotoPreviousEntry(reader.current.file)
		if errors.Is(err, ErrNoPreviousEntry) {
		} else if err != nil {
			return nil, err
		}

		// Set index
		reader.index = i
	} else {
		// Find segment based on timestamp
		sindex := -1
		for i, s := range wal.segments {
			if reader.timestamp.After(s.firstTimestamp) && reader.timestamp.Before(s.lastTimestamp) {
				sindex = i
				break
			}

			if reader.timestamp.Equal(s.firstTimestamp) || reader.timestamp.Equal(s.lastTimestamp) {
				sindex = i
				break
			}

			if len(wal.segments) > i+1 {
				if reader.timestamp.After(s.lastTimestamp) && reader.timestamp.Before(wal.segments[i+1].firstTimestamp) {
					sindex = i
					break
				}
			}
		}

		if sindex != -1 {
			reader.current, err = loadSegment(wal.segments[sindex].path)
			if err != nil {
				return nil, err
			}
		}

		if reader.current == nil {
			if reader.timestamp.Before(wal.segments[0].firstTimestamp) {
				reader.current, err = loadSegment(wal.segments[0].path)
			} else {
				reader.current, err = loadSegment(wal.segments[len(wal.segments)-1].path)
			}
			if err != nil {
				return nil, err
			}
		}
		err = reader.current.open()
		if err != nil {
			return nil, err
		}

		// Seek to timestamp
		t := reader.current.firstTimestamp
		i := reader.current.firstIndex
		for reader.timestamp.After(t) {
			entry, err := readEntry(reader.current.file)
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			t = entry.Timestamp
			i = entry.Index
		}
		// Set it back to the index in question
		err = gotoPreviousEntry(reader.current.file)
		if errors.Is(err, ErrNoPreviousEntry) {
		} else if err != nil {
			return nil, err
		}

		// Set index
		reader.index = i
	}

	return reader, nil
}
