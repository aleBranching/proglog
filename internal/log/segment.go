package log

import (
	"fmt"
	api "github.com/aleBranching/proglog/api/v1"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
)

type segmnent struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segmnent, error) {
	s := &segmnent{
		config:     c,
		baseOffset: baseOffset,
	}
	storeFilePath := path.Join(dir, fmt.Sprintf("%v.store", baseOffset))
	storef, err := os.OpenFile(storeFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	segmentStore, err := NewStore(storef)
	if err != nil {
		return nil, err
	}

	indexFilePath := path.Join(dir, fmt.Sprintf("%v.index", baseOffset))
	indexf, err := os.OpenFile(indexFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	segmentIndex, err := newIndex(indexf, c)
	if err != nil {
		return nil, err
	}
	s.index = segmentIndex
	s.store = segmentStore

	if off, _, err := segmentIndex.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segmnent) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	marshalled, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(marshalled)
	if err != nil {
		return 0, err
	}
	err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos)
	if err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

func (s *segmnent) Read(off uint64) (*api.Record, error) {
	_, storeOffset, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	data, err := s.store.Read(storeOffset)
	if err != nil {
		return nil, err
	}
	var record = api.Record{}
	err = proto.Unmarshal(data, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *segmnent) isMaxed() bool {
	return s.config.Segment.MaxIndexBytes <= s.index.size || s.config.Segment.MaxStoreBytes <= s.store.size
}

func (s *segmnent) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segmnent) Close() error {
	if err := s.store.Close(); err != nil {
		return err
	}
	if err := s.index.Close(); err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
