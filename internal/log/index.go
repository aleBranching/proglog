package log

import (
	"github.com/tysonmote/gommap"
	"os"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = posWidth + offWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return &index{}, err
	}

	idx.size = uint64(fi.Size())
	err = os.Truncate(fi.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		return &index{}, err
	}

	idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return &index{}, err
	}
	return idx, nil
}

func (i *)
