package wikimultistreamindexparser

// WithBz2 set reader is bz2 format
func WithBz2(isBz2 bool) func(*Parser) {
	return func(p *Parser) {
		p.OptIsBz2 = isBz2
	}
}
