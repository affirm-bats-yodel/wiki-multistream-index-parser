package wikimultistreamindexparser

import (
	"bufio"
	"compress/bzip2"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var ErrEmptyReader = errors.New("error: empty r (io.Reader)")

// NewParser Create new Parser
//
// # Parameters
//
// - r: underlying stream (can be a file, reader, etc...)
//
// - opts: options
//
// # Options
//
// - if the reader (r) is bzip2 format, use WithBz2(true)
// to set the reader stream is bzip2.
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

// Parse index line by line
//
// it reads a file line by line, split by ":" and
// sends index information over the channel
//
// if error occurred, error will be propagated to
// ErrIndex's err field
//
// # Parameters
//
// - ctx: external context for handling termination
func (p *Parser) Parse(ctx context.Context) <-chan *ErrIndex {
	ic := make(chan *ErrIndex)
	go func() {
		defer close(ic)
		scanner := bufio.NewScanner(p.Reader)
		for scanner.Scan() {
			if err := ctx.Err(); err != nil {
				ic <- &ErrIndex{
					err: err,
				}
				return
			}
			v := strings.SplitN(scanner.Text(), ":", 3)
			if len(v) != 3 {
				ic <- &ErrIndex{
					err: fmt.Errorf("error: malformed index: %q", scanner.Text()),
				}
				return
			}
			offset, err := strconv.ParseUint(v[0], 10, 64)
			if err != nil {
				ic <- &ErrIndex{
					err: fmt.Errorf("error: parse offset %q: %v", v[0], err),
				}
				return
			}
			pageID, err := strconv.ParseUint(v[1], 10, 64)
			if err != nil {
				ic <- &ErrIndex{
					err: fmt.Errorf("error: parse pageID %q: %v", v[1], err),
				}
				return
			}
			ic <- &ErrIndex{
				Index: Index{
					Offset: offset,
					PageID: pageID,
					Title:  v[2],
				},
			}
		}
		if err := scanner.Err(); err != nil {
			ic <- &ErrIndex{
				err: err,
			}
		}
	}()
	return ic
}

type ErrIndex struct {
	Index
	err error
}

// IsErrored check is Errored
func (e *ErrIndex) IsErrored() bool {
	return e.err != nil
}

// Error implements error.
func (e *ErrIndex) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return ""
}

var _ error = (*ErrIndex)(nil)

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
