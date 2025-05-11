package log

import (
	"os"
	"testing"
)

//var (
//	offWidth uint64 = 4
//	posWidth uint64 = 8
//	entWidth        = offWidth + posWidth
//)

func TestIndex(t *testing.T) {

	f, err := os.CreateTemp("", "index")
	if err != nil {
		t.Errorf("Errored")
	}

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	if err != nil {
		t.Fatal("couldn't create an index: \n", err)
	}
	_, _, err = idx.Read(-1)
	if err == nil {
		t.Errorf("Should have failed")
	}
	_, _, err = idx.Read(0)
	if err == nil {
		t.Errorf("Should have failed")
	}

	entries := []struct {
		pos    uint32
		offset uint64
	}{
		{
			pos:    uint32(0),
			offset: uint64(0),
		},
		{
			pos:    uint32(1),
			offset: uint64(10),
		},
	}

	for _, entry := range entries {
		err = idx.Write(entry.pos, entry.offset)
		if err != nil {
			t.Errorf("Error %s", err)
		}
		pos, off, err := idx.Read(int64(entry.pos))
		if err != nil {
			t.Errorf("errored")
		}
		if entry.pos != pos || entry.offset != off {
			t.Errorf("mismatch")
		}

	}
	_, _, err = idx.Read(int64(len(entries)))
	if err == nil {
		t.Fatalf("Should have errored")
	}
	err = idx.Close()
	if err != nil {
		t.Fatalf("Can;t close")
	}
	//restoring an index
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	if err != nil {
		t.Fatal("couldn't create an index: \n", err)
	}
	pos, off, err := idx.Read(-1)
	if err != nil {
		t.Errorf("failed to read")
	}
	if pos != entries[1].pos || off != entries[1].offset {
		t.Errorf("Should have matched %v : %v	%v : %v", pos, entries[1].pos, off, entries[1].offset)
	}
	for _, entry := range entries {
		pos, off, err := idx.Read(int64(entry.pos))
		if err != nil {
			t.Errorf("errored")
		}
		if entry.pos != pos || entry.offset != off {
			t.Errorf("mismatch")
		}

	}
	if idx.Name() != f.Name() {
		t.Errorf("names don't match")
	}

}
func TestIndexWriteAboveLimit(t *testing.T) {

	f, err := os.CreateTemp("", "index")
	if err != nil {
		t.Errorf("Errored")
	}

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	if err != nil {
		t.Fatal("couldn't create an index: \n", err)
	}

	maxEntries := c.Segment.MaxIndexBytes / entWidth

	for i := 0; i < int(maxEntries); i++ {
		err := idx.Write(uint32(i), uint64(3))
		if err != nil {
			t.Fatalf("should not have failed")
		}
	}
	err = idx.Write(uint32(maxEntries+1), uint64(3))
	if err == nil {
		t.Errorf("should have failed")
	}

}
