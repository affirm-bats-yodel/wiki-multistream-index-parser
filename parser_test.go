package wikimultistreamindexparser_test

import (
	"context"
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
			t.Error(t, err)
			return
		}
		for s := range p.Parse(context.Background()) {
			assert.Equal(t, false, s.IsErrored())
			t.Logf("stream: %+v", s.Index)
		}
	})
	t.Run("ParseWithBzip2Stream", func(t *testing.T) {})
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
