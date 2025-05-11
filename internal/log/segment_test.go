package log

import (
	"bytes"
	"fmt"
	api "github.com/aleBranching/proglog/api/v1"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "testSegment-another")
	if err != nil {
		t.Errorf("failed to create tempdir")
	}
	defer os.RemoveAll(tempDir)
	want := &api.Record{
		Value: []byte("hello world"),
	}
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3
	s, err := NewSegment(tempDir, uint64(16), c)
	if s.nextOffset != uint64(16) {
		t.Errorf("something gone wong")
	}
	if err != nil {
		t.Errorf("something gone wong")
	}
	if s.isMaxed() != false {
		t.Errorf("something gone wong")
	}
	for i := 0; i < 3; i++ {
		off, err := s.Append(want)
		if err != nil {
			t.Errorf("something gone wong")
		}
		if off != uint64(16+i) {
			t.Errorf("failure")
		}
		record, err := s.Read(uint64(off))
		if err != nil {
			t.Errorf("something gone wong")
		}

		if !bytes.Equal(record.Value, want.Value) {
			t.Errorf("something gone wong")
		}
	}
	_, err = s.Append(want)
	if err == nil {
		t.Errorf("Should have errored")
	}
	maxed := s.isMaxed()
	if !maxed {
		t.Errorf("should have maxed out")
	}
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = NewSegment(tempDir, 16, c)
	if err != nil {
		t.Errorf("can't create segment")
	}
	maxed = s.isMaxed()
	if !maxed {
		t.Errorf("should have maxed out")
	}
	err = s.Remove()
	if err != nil {
		t.Errorf("can't remove")
	}
	s, err = NewSegment(tempDir, 16, c)
	if err != nil {
		t.Errorf("can't create segment")
	}
	maxed = s.isMaxed()
	if maxed {
		t.Errorf("should not be maxed")
	}

}
func TestSomething(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "example")
	if err != nil {
		fmt.Errorf("aaa")
		return
	}
	defer os.RemoveAll(tempDir)
	fmt.Println(tempDir)
	dirLS, err := os.ReadDir(tempDir)
	if err != nil {
		fmt.Errorf("aaa")
		return
	}

	for _, file := range dirLS {
		fmt.Println("file", file)
	}
}
