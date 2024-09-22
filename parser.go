package wikimultistreamindexparser

import "io"

// NewParser Create new Parser
//
// - r: file reader
func NewParser(r io.Reader) (*Parser, error) {
	return &Parser{
		Reader: r,
	}, nil
}

// Parser Wikipedia Index Dump Reader
type Parser struct {
	// Reader file reader
	Reader io.Reader
	// OptIsBz2 specify Reader is came from
	// bz2 format
	OptIsBz2 bool
}
