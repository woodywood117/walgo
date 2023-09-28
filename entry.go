package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"time"
)

var table = crc32.MakeTable(crc32.Castagnoli)
var ErrCrc32Mismatch = errors.New("crc32 mismatch")
var ErrNoPreviousEntry = errors.New("no previous entry")

// Entry is a single message in a Wal
type Entry struct {
	Index     uint64
	Length    uint32
	Data      []byte
	Timestamp time.Time
	Crc32     uint32
}

func newMessage(index uint64, data []byte) *Entry {
	return &Entry{
		Index:     index,
		Length:    uint32(len(data)),
		Data:      data,
		Timestamp: time.Now(),
		Crc32:     crc32.Checksum(data, table),
	}
}

// ConstEntrySize is the size of the constant portion of an Entry
// 8 + 4 + 15 + 4 + 4 = 35
const ConstEntrySize = 35

func (m *Entry) size() uint64 {
	return ConstEntrySize + uint64(len(m.Data))
}

// Writes entry in the order of:
// - Index (8 bytes)
// - Length (4 bytes)
// - Data (Length bytes)
// - Timestamp (binary encoded) (15 bytes)
// - Crc32 (4 bytes)
// - Length (4 bytes)
func writeEntry(writer io.Writer, m *Entry) error {
	if m == nil {
		return nil
	}

	buffer := bytes.NewBuffer(make([]byte, 0, m.size()))

	if err := binary.Write(buffer, binary.LittleEndian, m.Index); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.LittleEndian, m.Length); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.LittleEndian, m.Data); err != nil {
		return err
	}

	t := m.Timestamp.In(time.UTC)
	tbin, err := t.MarshalBinary()
	if err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, tbin); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.LittleEndian, m.Crc32); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.LittleEndian, m.Length); err != nil {
		return err
	}

	_, err = writer.Write(buffer.Bytes())
	return err
}

func readEntry(reader io.Reader) (*Entry, error) {
	var m Entry

	if err := binary.Read(reader, binary.LittleEndian, &m.Index); err != nil {
		return nil, err
	}

	if err := binary.Read(reader, binary.LittleEndian, &m.Length); err != nil {
		return nil, err
	}

	m.Data = make([]byte, m.Length)
	if err := binary.Read(reader, binary.LittleEndian, &m.Data); err != nil {
		return nil, err
	}

	tbin := make([]byte, 15)
	if err := binary.Read(reader, binary.LittleEndian, &tbin); err != nil {
		return nil, err
	}
	err := m.Timestamp.UnmarshalBinary(tbin)
	if err != nil {
		return nil, err
	}

	if err := binary.Read(reader, binary.LittleEndian, &m.Crc32); err != nil {
		return nil, err
	}

	if err := binary.Read(reader, binary.LittleEndian, &m.Length); err != nil {
		return nil, err
	}

	if crc32.Checksum(m.Data, table) != m.Crc32 {
		return nil, ErrCrc32Mismatch
	}

	return &m, nil
}

func readPreviousEntry(reader io.ReadSeeker) (*Entry, error) {
	if err := gotoPreviousEntry(reader); err != nil {
		return nil, err
	}

	return readEntry(reader)
}

func gotoPreviousEntry(reader io.ReadSeeker) error {
	var length uint32

	current, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	} else if current == 0 {
		return ErrNoPreviousEntry
	}

	// Seek back 4 bytes to read the length of the previous message
	_, err = reader.Seek(-4, io.SeekCurrent)
	if err != nil {
		return err
	}

	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}

	// Seek back the length of the previous message
	_, err = reader.Seek(-int64(length+ConstEntrySize), io.SeekCurrent)
	if err != nil {
		return err
	}

	return nil
}
