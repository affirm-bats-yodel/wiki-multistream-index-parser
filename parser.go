package wikimultistreamindexparser

import (
	"compress/bzip2"
	"errors"
	"fmt"
	"io"
	"strconv"
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

// Index Wikipedia Index dump information
//
// there are three information concatenated with ":"
//
//	540:10:Hello
//
// first section points to offset of the dumped bz2
// file.
//
// second section points to article's ID
//
// third secion points to the name of the article
type Index struct {
	// Offset Bzip2 Offset
	Offset uint64
	// PageID ID of the Page
	PageID uint64
	// Title Article Title
	Title string
}

// String implements fmt.Stringer.
func (i *Index) String() string {
	return strconv.FormatUint(i.Offset, 10) +
		":" +
		strconv.FormatUint(i.PageID, 10) +
		":" +
		i.Title +
		"\n"
}

var _ fmt.Stringer = (*Index)(nil)
