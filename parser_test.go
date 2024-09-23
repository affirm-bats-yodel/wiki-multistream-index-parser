package wikimultistreamindexparser_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	wikimultistreamindexparser "github.com/affirm-bats-yodel/wiki-multistream-index-parser"
	"github.com/dsnet/compress/bzip2"
	"github.com/stretchr/testify/assert"
)

const (
	testData = `540:10:AccessibleComputing
540:12:Anarchism
540:13:AfghanistanHistory
540:14:AfghanistanGeography
540:15:AfghanistanPeople
540:18:AfghanistanCommunications
540:19:AfghanistanTransportations
540:20:AfghanistanMilitary
540:21:AfghanistanTransnationalIssues
540:23:AssistiveTechnology
540:24:AmoeboidTaxa
540:25:Autism spectrum
`
)

// TestNewParser Test NewParser method
func TestNewParser(t *testing.T) {
	t.Run("EmptyReader", func(t *testing.T) {
		_, err := wikimultistreamindexparser.NewParser(nil)
		if assert.Error(t, err) {
			assert.ErrorIs(t, err, wikimultistreamindexparser.ErrEmptyReader)
		}
	})
	t.Run("ReaderWithOpts", func(t *testing.T) {
		p, err := wikimultistreamindexparser.NewParser(
			strings.NewReader(testData),
			wikimultistreamindexparser.WithBz2(true),
		)
		if assert.NoError(t, err) {
			assert.Equal(t, true, p.OptIsBz2)
		}
	})
	t.Run("Reader", func(t *testing.T) {
		p, err := wikimultistreamindexparser.NewParser(
			strings.NewReader(testData),
		)
		if assert.NoError(t, err) {
			assert.Equal(t, false, p.OptIsBz2)
		}
	})
}

func TestParser_Parse(t *testing.T) {
	t.Run("ParseWithPlain", func(t *testing.T) {
		p, err := wikimultistreamindexparser.NewParser(
			strings.NewReader(testData),
		)
		if err != nil {
			t.Error(err)
			return
		}
		for s := range p.Parse(context.Background()) {
			if assert.Equal(t, false, s.IsErrored()) {
				assert.NotEmpty(t, s.Index.Offset)
				assert.NotEmpty(t, s.Index.PageID)
				assert.NotEmpty(t, s.Index.Title)
			} else {
				t.Error(s)
				return
			}
		}
	})
	t.Run("ParseWithBzip2Stream", func(t *testing.T) {
		var buf bytes.Buffer

		// set WriterConfig for DefaultCompression
		// Level
		//
		// See: https://github.com/dsnet/compress/blob/v0.0.1/bzip2/writer.go#L52
		w, err := bzip2.NewWriter(&buf, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer w.Close()

		_, err = io.Copy(w, strings.NewReader(testData))
		if err != nil {
			t.Error(err)
			return
		}

		err = w.Close()
		if err != nil {
			t.Error(err)
			return
		}

		// create parser with bzip2 enabled
		p, err := wikimultistreamindexparser.NewParser(
			bytes.NewReader(buf.Bytes()),
			wikimultistreamindexparser.WithBz2(true),
		)
		if err != nil {
			t.Error(err)
			return
		}

		for s := range p.Parse(context.Background()) {
			if assert.Equal(t, false, s.IsErrored()) {
				assert.NotEmpty(t, s.Index.Offset)
				assert.NotEmpty(t, s.Index.PageID)
				assert.NotEmpty(t, s.Index.Title)
			} else {
				t.Error(s)
				return
			}
		}
	})
	t.Run("ParseWithTestData", func(t *testing.T) {
		var articleIndexes []*wikimultistreamindexparser.Index
		f, err := os.Open("./testdata/index.txt.bz2")
		if err != nil {
			t.Error(err)
			return
		}
		defer f.Close()

		p, err := wikimultistreamindexparser.NewParser(
			f,
			wikimultistreamindexparser.WithBz2(true),
		)
		if err != nil {
			t.Error(err)
			return
		}

		for s := range p.Parse(context.Background()) {
			if assert.Equal(t, false, s.IsErrored()) {
				assert.NotEmpty(t, s.Index.Offset)
				assert.NotEmpty(t, s.Index.PageID)
				assert.NotEmpty(t, s.Index.Title)
				articleIndexes = append(articleIndexes, &s.Index)
			} else {
				t.Error(s)
				return
			}
		}

		if err := f.Close(); err != nil {
			t.Error(err)
			return
		}

		// $ bzip2 -dc testdata/index.txt.bz2 | wc -l
		assert.Len(t, articleIndexes, 200)

		// offset: 540, 706858
		offsets := p.GetOffsets()
		t.Logf("offsets: %+v", offsets)
		if assert.Len(t, offsets, 2) {
			assert.Contains(t, offsets, uint64(540))
			assert.Contains(t, offsets, uint64(706858))
		}
	})
}

// TestIndex_String assert Index stringer return
// a correct string
func TestIndex_String(t *testing.T) {
	expected := "1:2:Hello!\n"
	i := &wikimultistreamindexparser.Index{
		Offset: 1,
		PageID: 2,
		Title:  "Hello!",
	}
	assert.Equal(t, expected, i.String())
}
