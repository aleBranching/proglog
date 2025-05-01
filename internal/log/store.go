package log

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
	"sync"
)

var enc = binary.BigEndian

// how many bytes
// this would be uint64
const lenWidth = 8

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func NewStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		mu:   sync.Mutex{},
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil

}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size

	// len
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// data
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// 8 bytes of len
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	length := make([]byte, lenWidth)

	_, err := s.File.ReadAt(length, int64(pos))
	if err != nil {
		return nil, err
	}

	data := make([]byte, enc.Uint64(length))
	// changed to lenwidth
	_, err = s.File.ReadAt(data, int64(pos)+lenWidth)
	if err != nil {
		log.Fatal("can't read")
	}
	return data, err
}

// so this will work when you do s.ReadAt
func (s *store) ReadAt(p []byte, off int64) (int, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	//length := make([]byte, lenWidth)
	n, err := s.File.ReadAt(p, off)
	return n, err

}

func (s *store) Close() error {

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
