package wikimultistreamindexparser

import (
	"compress/bzip2"
	"errors"
	"io"
)

var ErrEmptyReader = errors.New("error: empty r (io.Reader)")

// NewParser Create new Parser
//
// - r: file reader
func NewParser(r io.Reader, opts ...func(*Parser)) (*Parser, error) {
	p := &Parser{}
	if r == nil {
		return nil, ErrEmptyReader
	}
	for _, optFn := range opts {
		optFn(p)
	}
	if p.OptIsBz2 {
		p.Reader = bzip2.NewReader(r)
	} else {
		p.Reader = r
	}
	return p, nil
}

// Parser Wikipedia Index Dump Reader
type Parser struct {
	// Reader file reader
	Reader io.Reader
	// OptIsBz2 specify Reader is came from
	// bz2 format
	OptIsBz2 bool
}
