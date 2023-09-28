package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024*1024), // 1MB
		WithMaxSegmentCount(10),
		WithExpiration(1000, 2000),
	)
	if err != nil {
		t.Fatal(err)
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	expected := filepath.Join(wd, "datastore")
	expected = filepath.ToSlash(expected)
	if w.path != expected {
		t.Errorf("expected rootdir to be '%s', got '%s'", expected, w.path)
	}

	if w.current == nil {
		t.Errorf("expected current segment to not be nil")
	}

	if w.current.firstIndex != 0 {
		t.Errorf("expected current segment name to be 0, got '%d'", w.current.firstIndex)
	}

	if len(w.segments) != 1 {
		t.Errorf("expected segments to have length 1, got '%d'", len(w.segments))
	}

	if w.index != 0 {
		t.Errorf("expected index to be 0, got '%d'", w.index)
	}

	if w.config.MaxSegmentSize != 1024*1024 {
		t.Errorf("expected max segment size to be 1MB, got '%d'", w.config.MaxSegmentSize)
	}

	if w.config.MaxSegmentCount != 10 {
		t.Errorf("expected max segment count to be 10, got '%d'", w.config.MaxSegmentCount)
	}

	if w.config.ExpirationTime != 1000 {
		t.Errorf("expected expiration time to be 1000, got '%d'", w.config.ExpirationTime)
	}

	if w.config.ExpirationInterval != 2000 {
		t.Errorf("expected expiration interval to be 2000, got '%d'", w.config.ExpirationInterval)
	}

	// cleanup
	w.Close()
	err = os.RemoveAll("datastore")
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewLoad(t *testing.T) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024*1024), // 1MB
	)
	if err != nil {
		t.Fatal(err)
	}

	wload, err := Load("datastore")
	if err != nil {
		t.Fatal(err)
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	expected := filepath.Join(wd, "datastore")
	expected = filepath.ToSlash(expected)
	if wload.path != expected {
		t.Errorf("expected rootdir to be '%s', got '%s'", expected, w.path)
	}

	if wload.path != w.path {
		t.Errorf("expected subwal path to be '%s', got '%s'", w.path, wload.path)
	}

	if wload.current.firstIndex != w.current.firstIndex {
		t.Errorf("expected subwal current segment name to be '%d', got '%d'", w.current.firstIndex, wload.current.firstIndex)
	}

	if wload.current.fileLength != w.current.fileLength {
		t.Errorf("expected subwal current segment name to be '%d', got '%d'", w.current.fileLength, wload.current.fileLength)
	}

	if wload.config != w.config {
		t.Errorf("expected subwal config to be '%v', got '%v'", w.config, wload.config)
	}

	// cleanup
	w.Close()
	err = wload.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = os.RemoveAll("datastore")
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteToSubWal(t *testing.T) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024*1024), // 1MB
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100000; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		err = w.Write(data)
		if err != nil {
			t.Fatal(err)
		}
	}

	// cleanup
	w.Close()
	err = os.RemoveAll("datastore")
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkWrite(b *testing.B) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024*1024), // 1MB
	)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		err = w.Write(data)
		if err != nil {
			b.Fatal(err)
		}
	}

	// cleanup
	w.Close()
	err = os.RemoveAll("datastore")
	if err != nil {
		b.Fatal(err)
	}
}

func TestReadFromBeginning(t *testing.T) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024*1024), // 1MB
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		err = w.Write(data)
		if err != nil {
			t.Fatal(err)
		}
	}

	r, err := w.Reader()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		entry, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}

		if string(entry.Data) != fmt.Sprintf("test-%d", i) {
			t.Errorf("expected data to be '%s', got '%s'", fmt.Sprintf("test-%d", i), string(entry.Data))
		}
	}

	r.Close()
	w.Close()
	err = os.RemoveAll("datastore")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadWithIndex(t *testing.T) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024*1024), // 1MB
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		err = w.Write(data)
		if err != nil {
			t.Fatal(err)
		}
	}

	r, err := w.Reader(WithIndex(50))
	if err != nil {
		t.Fatal(err)
	}
	for i := 50; i < 100; i++ {
		entry, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}

		if string(entry.Data) != fmt.Sprintf("test-%d", i) {
			t.Errorf("expected data to be '%s', got '%s'", fmt.Sprintf("test-%d", i), string(entry.Data))
		}
	}

	r.Close()
	w.Close()
	err = os.RemoveAll("datastore")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadWithIndexAfter(t *testing.T) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024*1024), // 1MB
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		err = w.Write(data)
		if err != nil {
			t.Fatal(err)
		}
	}

	r, err := w.Reader(WithIndex(101))
	if err != nil {
		t.Fatal(err)
	}
	for i := 99; i < 100; i++ {
		entry, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}

		if string(entry.Data) != fmt.Sprintf("test-%d", i) {
			t.Errorf("expected data to be '%s', got '%s'", fmt.Sprintf("test-%d", i), string(entry.Data))
		}
	}

	r.Close()
	w.Close()
	err = os.RemoveAll("datastore")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadWithTimestamp(t *testing.T) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024*1024), // 1MB
	)
	if err != nil {
		t.Fatal(err)
	}

	var ts time.Time
	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		err = w.Write(data)
		if err != nil {
			t.Fatal(err)
		}

		if i == 49 {
			ts = time.Now()
			time.Sleep(10 * time.Millisecond)
		}
	}

	r, err := w.Reader(WithTimestamp(ts))
	if err != nil {
		t.Fatal(err)
	}
	for i := 50; i < 100; i++ {
		entry, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}

		if string(entry.Data) != fmt.Sprintf("test-%d", i) {
			t.Errorf("expected data to be '%s', got '%s'", fmt.Sprintf("test-%d", i), string(entry.Data))
		}
	}

	r.Close()
	w.Close()
	err = os.RemoveAll("datastore")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadWithTimestampBefore(t *testing.T) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024*1024), // 1MB
	)
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Now()
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		err = w.Write(data)
		if err != nil {
			t.Fatal(err)
		}
	}

	r, err := w.Reader(WithTimestamp(ts))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		entry, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}

		if string(entry.Data) != fmt.Sprintf("test-%d", i) {
			t.Errorf("expected data to be '%s', got '%s'", fmt.Sprintf("test-%d", i), string(entry.Data))
		}
	}

	r.Close()
	w.Close()
	err = os.RemoveAll("datastore")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadWithTimestampAfter(t *testing.T) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024*1024), // 1MB
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		err = w.Write(data)
		if err != nil {
			t.Fatal(err)
		}
	}

	ts := time.Now()
	time.Sleep(10 * time.Millisecond)

	r, err := w.Reader(WithTimestamp(ts))
	if err != nil {
		t.Fatal(err)
	}
	for i := 99; i < 100; i++ {
		entry, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}

		if string(entry.Data) != fmt.Sprintf("test-%d", i) {
			t.Errorf("expected data to be '%s', got '%s'", fmt.Sprintf("test-%d", i), string(entry.Data))
		}
	}

	r.Close()
	w.Close()
	err = os.RemoveAll("datastore")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadWithTimestampMulti(t *testing.T) {
	w, err := New("datastore",
		WithMaxSegmentSize(1024), // 1MB
	)
	if err != nil {
		t.Fatal(err)
	}

	var ts time.Time
	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		err = w.Write(data)
		if err != nil {
			t.Fatal(err)
		}

		if i == 49 {
			ts = time.Now()
			time.Sleep(10 * time.Millisecond)
		}
	}

	r, err := w.Reader(WithTimestamp(ts))
	if err != nil {
		t.Fatal(err)
	}
	for i := 49; i < 100; i++ {
		entry, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}

		if string(entry.Data) != fmt.Sprintf("test-%d", i) {
			t.Errorf("expected data to be '%s', got '%s'", fmt.Sprintf("test-%d", i), string(entry.Data))
		}
	}

	r.Close()
	w.Close()
	err = os.RemoveAll("datastore")
	if err != nil {
		t.Fatal(err)
	}
}

// TODO: Expiration of segments
// TODO: Removal of segments based on max segment count
// TODO: Implement locking (for writes, in the event that it is used asynchronously) (does this need to be done for reads too? RWLock?)
// TODO: Implement buffered writing to speed it up?
