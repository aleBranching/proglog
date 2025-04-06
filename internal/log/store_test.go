package log

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

var write = []byte("hello world")
var width = uint64(len(write)) + lenWidth

func setupStore(t *testing.T) (*store, *os.File) {
	t.Helper()
	f, err := os.CreateTemp("", "store")
	if err != nil {
		t.Errorf("Errored")
	}

	s, err := NewStore(f)
	if err != nil {
		t.Errorf("Errored")
	}
	return s, f

}

func TestAppendBasic(t *testing.T) {
	s, f := setupStore(t)
	defer os.Remove(f.Name())

	n, pos, err := s.Append(write)
	if err != nil {
		t.Errorf("Errored")
	}
	if n != width {
		t.Errorf("got: %v want: %v", n, width)
	}
	if pos != 0 {
		t.Errorf("got: %v want: %v", n, width)
	}

}
func TestAppendReadBasic(t *testing.T) {
	s, f := setupStore(t)
	defer os.Remove(f.Name())

	n, pos, err := s.Append(write)
	if err != nil {
		t.Errorf("Errored")
	}
	if n != width {
		t.Errorf("got: %v want: %v", n, width)
	}
	if pos != 0 {
		t.Errorf("got: %v want: %v", n, width)
	}

	data, err := s.Read(uint64(0))
	if err != nil {
		t.Errorf("Errored")
	}
	if !bytes.Equal(data, write) {
		t.Errorf("Erorred: got: %v \n want: %v", data, write)
		t.Errorf("Erorred: got: %v \n want: %v", string(data), string(write))
	}

}

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store")
	if err != nil {
		t.Errorf("Errored")
	}
	defer os.Remove(f.Name())

	s, err := NewStore(f)
	if err != nil {
		t.Errorf("Errored")
	}

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := 0; i < 3; i++ {
		n, pos, err := s.Append(write)
		if err != nil {
			t.Errorf("there was an error")
		}
		if width*uint64(i) == pos+n {
			t.Errorf("The amount written does not match the amount that should have veen written")
		}
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := 0; i < 3; i++ {
		data, err := s.Read(uint64(pos))
		if err != nil {
			t.Errorf("there was an error")
		}
		if !bytes.Equal(write, data) {
			t.Errorf("don't match")
		}
		pos += width
	}
}
func testReadAt(t *testing.T, s *store) {
	t.Helper()

	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		if err != nil {
			t.Errorf("oh no")
		}
		if n != lenWidth {
			t.Errorf("oh no")
		}

		size := enc.Uint64(b)

		c := make([]byte, size)

		n2, err := s.ReadAt(c, off+lenWidth)

		if n2 != len(write) {
			t.Errorf("oh no")
		}
		if err != nil {
			t.Errorf("oh no")
		}

		if !bytes.Equal(c, write) {
			t.Errorf("oh no")
		}

		off += int64(width)

	}

}

func TestStoreClose(t *testing.T) {
	s, f := setupStore(t)
	defer os.Remove(f.Name())

	var err error

	_, _, err = s.Append(write)
	if err != nil {
		t.Errorf("oh oh")
	}

	f, beforeSize, err := openFile(f.Name())
	fmt.Println("beforeSize, ", beforeSize)
	if err != nil {
		t.Errorf("oh no")
	}

	err = s.Close()
	if err != nil {
		t.Errorf("oh oh ")
	}

	_, afterSize, err := openFile(f.Name())

	if afterSize <= beforeSize {
		t.Errorf("oh no")
	}
	fmt.Println("afterSize, ", afterSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
