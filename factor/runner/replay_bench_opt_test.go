package runner

import (
	"io"

	"github.com/banbox/banbot/factor"
)

func setOwnedPages(o *StorageInputOptions) { o.OwnedPages = true }

type benchHasher struct{ h factor.RecordHasher }

func newBenchHasher() *benchHasher { return &benchHasher{} }
func (b *benchHasher) write(w io.Writer, r *factor.VersionRecord) {
	d, err := b.h.Digest(r)
	if err != nil {
		panic(err)
	}
	_, _ = w.Write(d[:])
}
