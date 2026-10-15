package execution

import (
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const CommittedArchiveVersion = 1

type CommittedArchiveChunk struct {
	SchemaVersion    int
	Account          AccountKey
	FromExclusive    int64
	ThroughInclusive int64
	Events           []CommittedEvent
}

type CommittedArchiveFile struct {
	Name             string `json:"name"`
	SHA256           string `json:"sha256"`
	FromExclusive    int64  `json:"from_exclusive"`
	ThroughInclusive int64  `json:"through_inclusive"`
	Events           int    `json:"events"`
	Postings         int    `json:"postings"`
}

type CommittedArchiveManifest struct {
	SchemaVersion    int                    `json:"schema_version"`
	Schema           string                 `json:"schema"`
	Account          AccountKey             `json:"account"`
	Status           string                 `json:"status"`
	Reason           string                 `json:"reason,omitempty"`
	FromExclusive    int64                  `json:"from_exclusive"`
	ThroughInclusive int64                  `json:"through_inclusive"`
	ExportedThrough  int64                  `json:"exported_through"`
	Files            []CommittedArchiveFile `json:"files"`
}

// ArchiveCommittedEvents streams a fixed committed highwater into bounded
// versioned Gob chunks. The new directory must have an existing parent. This
// is an audit export, not a resume snapshot or permission to prune the ledger.
// Cancellation/read/write failures retain an incomplete manifest and every
// previously published chunk. Each complete chunk and manifest is staged,
// synced and atomically renamed within this private output directory.
func (s *Store) ArchiveCommittedEvents(ctx context.Context, directory string, fromExclusive int64, chunkEvents int) (CommittedArchiveManifest, error) {
	if _, scoped := ctx.Value(storeTxContextKey{}).(scopedStoreTx); scoped {
		return CommittedArchiveManifest{}, errors.New("execution: archive must run outside an account transaction")
	}
	if err := s.beginOwnerOperation(); err != nil {
		return CommittedArchiveManifest{}, err
	}
	defer s.work.Done()
	var through int64
	// Capture the committed bound even for a canceled run, so its output can
	// retain an explicit incomplete status with the expected range.
	err := s.readRecord(context.WithoutCancel(ctx), func(tx *storeTxn) error { return tx.QueryRow(opReadAccountCheckpoint, s.accountID).Scan(&through) })
	if err != nil {
		return CommittedArchiveManifest{}, err
	}
	return writeCommittedArchive(ctx, directory, s.key, fromExclusive, through, chunkEvents, s.EventsAfter)
}

// Archive before releasing this borrow. File work joins the process's owner
// boundary and observes caller, borrower and process cancellation.
func (b *SharedAccountBorrow) ArchiveCommittedEvents(ctx context.Context, directory string, fromExclusive int64, chunkEvents int) (CommittedArchiveManifest, error) {
	var manifest CommittedArchiveManifest
	err := b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			operationCtx, cancel := context.WithCancel(ownerCtx)
			stopBorrow := context.AfterFunc(b.ctx, cancel)
			stopCaller := context.AfterFunc(ctx, cancel)
			defer func() { stopBorrow(); stopCaller(); cancel() }()
			if b.ctx.Err() != nil || ctx.Err() != nil {
				cancel()
			}
			var err error
			manifest, err = s.store.ArchiveCommittedEvents(operationCtx, directory, fromExclusive, chunkEvents)
			return err
		})
	})
	return manifest, err
}

func writeCommittedArchive(ctx context.Context, directory string, key AccountKey, from, through int64, chunkEvents int, read func(context.Context, int64, int) ([]CommittedEvent, error)) (manifest CommittedArchiveManifest, resultErr error) {
	if !filepath.IsAbs(directory) || from < 0 || through < from || chunkEvents < 1 || chunkEvents > 10000 || read == nil {
		return manifest, errors.New("execution: invalid committed archive range/path/page size")
	}
	if err := key.Validate(); err != nil {
		return manifest, err
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		return manifest, err
	}
	manifest = CommittedArchiveManifest{SchemaVersion: CommittedArchiveVersion, Schema: "banbot.execution.committed-events+ledger", Account: key, Status: "incomplete", Reason: "archive in progress", FromExclusive: from, ThroughInclusive: through, ExportedThrough: from, Files: []CommittedArchiveFile{}}
	defer func() {
		if resultErr != nil {
			manifest.Status = "incomplete"
			manifest.Reason = resultErr.Error()
		}
		if err := publishArchiveManifest(directory, manifest); err != nil {
			resultErr = errors.Join(resultErr, err)
			manifest.Status = "incomplete"
			manifest.Reason = resultErr.Error()
		}
	}()
	if err := publishArchiveManifest(directory, manifest); err != nil {
		return manifest, err
	}
	for manifest.ExportedThrough < through {
		if err := ctx.Err(); err != nil {
			return manifest, err
		}
		page, err := read(ctx, manifest.ExportedThrough, chunkEvents)
		if err != nil {
			return manifest, err
		}
		if len(page) > chunkEvents {
			return manifest, errors.New("execution: archive reader exceeded bounded page")
		}
		n := 0
		for n < len(page) && page[n].Checkpoint <= through {
			n++
		}
		page = page[:n]
		if len(page) == 0 {
			return manifest, errors.New("execution: committed archive missing expected checkpoint")
		}
		for i, event := range page {
			if event.Checkpoint != manifest.ExportedThrough+int64(i)+1 || !canonicalID(event.ID) || !canonicalID(event.Kind) || !json.Valid(event.Payload) {
				return manifest, errors.New("execution: invalid committed archive event order/body")
			}
			for _, posting := range event.Ledger {
				if posting.EventID != event.ID {
					return manifest, errors.New("execution: archive posting belongs to another event")
				}
			}
		}
		chunk := CommittedArchiveChunk{SchemaVersion: CommittedArchiveVersion, Account: key, FromExclusive: manifest.ExportedThrough, ThroughInclusive: page[len(page)-1].Checkpoint, Events: page}
		file, err := publishArchiveChunk(directory, len(manifest.Files)+1, chunk)
		if err != nil {
			return manifest, err
		}
		manifest.Files = append(manifest.Files, file)
		manifest.ExportedThrough = chunk.ThroughInclusive
		if err := publishArchiveManifest(directory, manifest); err != nil {
			return manifest, err
		}
	}
	if err := ctx.Err(); err != nil {
		return manifest, err
	}
	manifest.Status = "complete"
	manifest.Reason = ""
	return manifest, nil
}

func publishArchiveChunk(directory string, number int, chunk CommittedArchiveChunk) (CommittedArchiveFile, error) {
	metadata := CommittedArchiveFile{Name: fmt.Sprintf("chunk-%06d.gob", number), FromExclusive: chunk.FromExclusive, ThroughInclusive: chunk.ThroughInclusive, Events: len(chunk.Events)}
	for _, event := range chunk.Events {
		metadata.Postings += len(event.Ledger)
	}
	hash := sha256.New()
	err := publishArchiveFile(directory, metadata.Name, func(file io.Writer) error { return gob.NewEncoder(io.MultiWriter(file, hash)).Encode(chunk) })
	metadata.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return metadata, err
}

func publishArchiveManifest(directory string, manifest CommittedArchiveManifest) error {
	return publishArchiveFile(directory, "manifest.json", func(file io.Writer) error { return json.NewEncoder(file).Encode(manifest) })
}

func publishArchiveFile(directory, name string, write func(io.Writer) error) error {
	file, err := os.CreateTemp(directory, ".archive-*.tmp")
	if err != nil {
		return err
	}
	path := file.Name()
	defer os.Remove(path)
	err = write(file)
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	return os.Rename(path, filepath.Join(directory, name))
}
