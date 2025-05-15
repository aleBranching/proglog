package log

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	api "github.com/aleBranching/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	scenarios := map[string]func(t *testing.T, log *Log){
		"append and read":                     testAppendRead,
		"out of range read":                   testOutOfRangeErr,
		"init with existing segment":          testInitExisting,
		"reader":                              testReader,
		"truncating":                          testTruncate,
		"append and read twice":               testAppendReadTwice,
		"append and read twice Large segment": testAppendReadTwice,
	}

	for scenario, fn := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			c := Config{}
			if scenario == "append and read twice Large segment" {
				c.Segment.MaxStoreBytes = 1024
			} else {
				c.Segment.MaxStoreBytes = 32
			}
			dir, err := os.MkdirTemp("", "exampleLog")
			defer os.RemoveAll(dir)
			if err != nil {
				t.Fatal("Can't create dir")
			}
			log, err := NewLog(dir, c)
			if err != nil {
				t.Fatal("Can't create log")
			}
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {

	recordToAppend := &api.Record{
		Value: []byte("Hello world"),
	}
	offA, err := log.Append(recordToAppend)
	if err != nil {
		t.Fatal("Can't append")
	}
	if offA != uint64(offA) {
		t.Fatal("Can't stop")
	}
	readRecord, err := log.Read(offA)
	if err != nil {
		t.Fatalf("Can't read")
	}

	if readRecord.Offset != offA {
		t.Fatal("offset ain't right")
	}
	if !bytes.Equal(readRecord.Value, recordToAppend.Value) {
		t.Fatal("Should be equal")
	}
}
func testAppendReadTwice(t *testing.T, log *Log) {

	{

		recordToAppend := &api.Record{
			Value: []byte("Hello world"),
		}
		offA, err := log.Append(recordToAppend)
		if err != nil {
			t.Fatal("Can't append")
		}
		if offA != uint64(0) {
			t.Fatal("Can't stop")
		}
		readRecord, err := log.Read(offA)
		if err != nil {
			t.Fatalf("Can't read")
		}

		if readRecord.Offset != offA {
			t.Fatal("offset ain't right")
		}
		if !bytes.Equal(readRecord.Value, recordToAppend.Value) {
			t.Fatal("Should be equal")
		}
	}
	{

		recordToAppend := &api.Record{
			Value: []byte("Hello world 2"),
		}
		offA, err := log.Append(recordToAppend)
		if err != nil {
			t.Fatal("Can't append")
		}
		if offA != uint64(1) {
			t.Fatal("Can't stop")
		}
		readRecord, err := log.Read(offA)
		if err != nil {
			t.Fatalf("Can't read")
		}

		if readRecord.Offset != offA {
			t.Fatal("offset ain't right")
		}
		if !bytes.Equal(readRecord.Value, recordToAppend.Value) {
			t.Fatal("Should be equal")
		}
	}
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	if err == nil {
		t.Errorf("Didn;t dail")
	}
	if read != nil {
		t.Errorf("there is a record")
	}
	var apiErr api.ErrOffsetOutOfRange
	worked := errors.As(err, &apiErr)
	if !worked {
		t.Errorf("couldn't assert")
	}
	if uint64(1) != apiErr.Offset {
		t.Errorf("did not equal")
	}

}

func testInitExisting(t *testing.T, log *Log) {
	recordToAppend := &api.Record{Value: []byte("hello world")}

	for i := 0; i < 3; i++ {
		offA, err := log.Append(recordToAppend)
		if offA != uint64(i) {
			t.Fatal("offset ain't right")
		}
		if err != nil {
			t.Fatal("Can't append")
		}
	}
	err := log.Close()
	if err != nil {
		t.Errorf("should not have failed")
	}
	off, err := log.LowestOffset()
	if off != uint64(0) {
		t.Errorf("Should have been 0")
	}
	off, err = log.HighestOffset()
	if off != uint64(2) {
		t.Errorf("Should have been 2")
	}
	log2, err := NewLog(log.Dir, log.Config)
	if err != nil {
		t.Errorf("should not have failed")
	}
	off, err = log2.LowestOffset()
	if off != uint64(0) {
		t.Errorf("Should have been 0")
	}
	off, err = log2.HighestOffset()
	if off != uint64(2) {
		t.Errorf("Should have been 2")
	}

}

func testReader(t *testing.T, log *Log) {

	recordToAppend := &api.Record{
		Value: []byte("Hello world"),
	}
	offA, err := log.Append(recordToAppend)
	if err != nil {
		t.Fatal("Can't append")
	}
	if offA != uint64(0) {
		t.Fatal("Can't stop")
	}

	reader := log.Reader()
	if err != nil {
		t.Fatal("Can't read")
	}
	allRead, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal("Can't read")
	}
	read := &api.Record{}

	err = proto.Unmarshal(allRead[lenWidth:], read)
	if err != nil {
		t.Fatal("Can't read")
	}
	if !bytes.Equal(read.Value, recordToAppend.Value) {
		t.Fatal("they aren;y same")
	}

}

func testTruncate(t *testing.T, log *Log) {
	recordToAppend := &api.Record{Value: []byte("hello world")}

	for i := 0; i < 3; i++ {
		offA, err := log.Append(recordToAppend)
		if offA != uint64(i) {
			t.Fatal("offset ain't right")
		}
		if err != nil {
			t.Fatal("Can't append")
		}
	}

	err := log.Truncate(1)
	if err != nil {
		t.Fatal("Failed to truncate")
	}
	_, err = log.Read(0)
	if err == nil {
		t.Fatal("should have failed")
	}

}
