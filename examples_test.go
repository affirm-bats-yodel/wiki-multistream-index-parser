package wikimultistreamindexparser_test

import (
	"context"
	"fmt"
	"os"

	wikimultistreamindexparser "github.com/affirm-bats-yodel/wiki-multistream-index-parser"
)

func ExampleNewParser() {
	// first, grab an index file from
	// wikipedia.
	//
	// Documentation:
	// https://en.wikipedia.org/wiki/Wikipedia:Database_download
	//
	// Dump Index:
	// https://dumps.wikimedia.org/enwiki/
	//
	// however, you can use any reader that implements
	// io.Reader interface, just properly specify when
	// the stream is formatted as bzip2.

	// below is an example for parsing a bzip2 compressed
	// index file.

	// open a file
	f, err := os.Open("enwiki-YYYYMMDD-pages-articles-multistream-index.txt.bz2")
	if err != nil {
		// handle error gracefully
		fmt.Fprintf(os.Stderr, "error open file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// create a Parser
	//
	// you can specify stream is compressed with bzip2
	// it'll wrap provided reader to bzip2 Reader
	p, err := wikimultistreamindexparser.NewParser(
		f,
		wikimultistreamindexparser.WithBz2(true),
	)
	if err != nil {
		// handle error gracefully
		fmt.Fprintf(os.Stderr, "error create parser: %v\n", err)
		os.Exit(1)
	}

	// run the parser and get data
	//
	// it'll parse line by line and return Information
	// of the Index.
	//
	// when error occurred, it'll return an error and you
	// should check Error is occurred using IsErrored() method
	//
	// error is implemented on ErrIndex. so you can use as an Error
	//
	// you can add external context to terminate goroutine
	// when required. (like timeout, etc ...)

	for idxChan := range p.Parse(context.Background()) {
		if idxChan.IsErrored() {
			fmt.Fprintf(os.Stderr, "error parse: %v\n", idxChan)
			os.Exit(1)
		}
		_ = idxChan // use given data as you want...
	}

	// you can list offsets like this
	//
	// NOTE: please make sure that you've successfully read all
	// entries.
	//
	// if not, you'll get unfinished list of the offsets

	offsets := p.GetOffsets()
	_ = offsets
}
