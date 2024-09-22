package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	wikimultistreamindexparser "github.com/affirm-bats-yodel/wiki-multistream-index-parser"
)

func main() {
	var (
		f             *os.File
		indexFilePath string
		fromStdin     bool
		isBzip2       bool
		idxCount      int64
		readIdx       int64
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	flag.StringVar(&indexFilePath, "indexfile", "", "path of the index file")
	flag.BoolVar(&fromStdin, "stdin", false, "read from stdin")
	flag.BoolVar(&isBzip2, "bzip2", false, "specify stream is bzip2")
	flag.Int64Var(&idxCount, "count", 0, "lines to read")
	flag.Parse()

	if fromStdin {
		f = os.Stdin
	} else {
		log.Printf("open file: %s", indexFilePath)
		indexF, err := os.Open(indexFilePath)
		if err != nil {
			log.Fatalf("error open file %q: %v", indexFilePath, err)
		}
		f = indexF
	}
	defer f.Close()

	log.Println("create parser", "isBzip2", isBzip2)

	p, err := wikimultistreamindexparser.NewParser(
		f,
		wikimultistreamindexparser.WithBz2(isBzip2),
	)
	if err != nil {
		log.Fatalf("error create parser: %v", err)
	}

	log.Println("read index", "idxCount", idxCount)

	for s := range p.Parse(ctx) {
		if s.IsErrored() {
			log.Fatal(s)
		}
		log.Printf("index: %+v", s.Index)
		readIdx++
		if idxCount > 0 && readIdx >= idxCount {
			break
		}
	}
}
