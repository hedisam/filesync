package indexer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/client/ops"
	"github.com/hedisam/filesync/lib/chans"
	"github.com/hedisam/pipeline"
	"github.com/hedisam/pipeline/stage"
)

type FileMetadata struct {
	Path           string
	Size           int64
	SHA256Checksum string
	MTime          int64

	Operation ops.Op
	EventTime time.Time
}

type Indexer struct {
	logger        *logrus.Logger
	mu            sync.RWMutex
	keyToMetadata map[string]FileMetadata
	idxMailbox    chan *FileMetadata
	inflight      sync.WaitGroup
	workers       uint
}

func New(logger *logrus.Logger, workers uint) *Indexer {
	return &Indexer{
		logger:        logger,
		keyToMetadata: make(map[string]FileMetadata),
		idxMailbox:    make(chan *FileMetadata, workers),
		workers:       min(workers, 1),
	}
}

func (idx *Indexer) Run(ctx context.Context) error {
	metadataExtractorStage := stage.WorkerPoolRunner(idx.workers, idx.ExtractMetadata)
	src := idx
	sink := idx.UpdateIndex
	p := pipeline.NewPipeline(src, sink)
	err := p.Run(ctx, metadataExtractorStage)
	if err != nil {
		return fmt.Errorf("index pipeline error: %w", err)
	}

	return nil
}

func (idx *Indexer) WaitOnInFlights() {
	idx.inflight.Wait()
}

func (idx *Indexer) Snapshot() map[string]FileMetadata {
	idx.mu.RLock()
	snapshot := maps.Clone(idx.keyToMetadata)
	idx.mu.RUnlock()

	// Exclude any files that were removed locally.
	// When syncing, files present on the server but marked removed here
	// should be deleted on the server side.
	maps.DeleteFunc(snapshot, func(_ string, md FileMetadata) bool {
		return md.Operation == ops.OpRemoved
	})

	return snapshot
}

// Index queues the given file info to be processed by the index pipeline.
func (idx *Indexer) Index(ctx context.Context, md *FileMetadata) error {
	idx.inflight.Add(1)

	if !chans.SendOrDone(ctx, idx.idxMailbox, md) {
		idx.inflight.Done()
		return fmt.Errorf("could not queue file for indexing: %w", ctx.Err())
	}

	return nil
}

// Next implements pipeline.Source and is called by the pipeline. It returns the next filePath from the mailbox.
// It blocks until either the context is canceled or the channel is closed.
func (idx *Indexer) Next(ctx context.Context) (any, error) {
	md, ok := chans.ReceiveOrDone(ctx, idx.idxMailbox)
	if !ok {
		return nil, io.EOF
	}
	return md, nil
}

// ExtractMetadata is a stage processor that is run by the pipeline.
// It enriches the input payload with file metadata and its sha256 checksum and pass it on to the next pipeline stage
// which would be the pipeline sink.
func (idx *Indexer) ExtractMetadata(_ context.Context, payload any) (out any, dropped bool, err error) {
	defer func() {
		if dropped {
			idx.inflight.Done()
		}
	}()

	md, ok := payload.(*FileMetadata)
	if !ok {
		return nil, false, fmt.Errorf("invalid payload type, expected to get *FileMetadata, got %T", payload)
	}

	logger := idx.logger.WithField("path", md.Path)

	if md.Operation == ops.OpRemoved {
		return md, false, nil
	}

	f, err := os.Open(md.Path)
	if err != nil {
		logger.WithError(err).Warn("Could not open file to extract metadata, dropping")
		// drop this payload and return no error so the pipeline keeps running
		return nil, true, nil
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		logger.WithError(err).Warn("Could get file stats to extract metadata, dropping")
		return nil, true, nil
	}

	if st.IsDir() {
		logger.Warn("Directory has been queued for indexing, dropping")
		return nil, true, nil
	}

	md.Size = st.Size()
	md.MTime = st.ModTime().Unix()

	hasher := sha256.New()
	_, err = io.Copy(hasher, f)
	if err != nil {
		logger.WithError(err).Warn("Could calculate sha256 checksum, dropping")
		return nil, true, nil
	}

	md.SHA256Checksum = hex.EncodeToString(hasher.Sum(nil))

	return md, false, nil
}

// UpdateIndex implements pipeline.Sink and it's run by the pipeline. It is the last step of the index processor
// where we store and update the indexer.
func (idx *Indexer) UpdateIndex(_ context.Context, payload any) error {
	// update the indexer; check seq for out of order events.
	defer idx.inflight.Done()

	md, ok := payload.(*FileMetadata)
	if !ok {
		return fmt.Errorf("invalid payload type, expected to get *FileMetadata, got %T", payload)
	}

	logger := idx.logger.WithField("path", md.Path)
	logger.Info("Updating index")

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if existingMD, ok := idx.keyToMetadata[md.Path]; ok && existingMD.EventTime.After(md.EventTime) {
		logger.WithFields(logrus.Fields{
			"existing_seq": existingMD.EventTime.String(),
			"input_seq":    md.EventTime.String(),
		}).Warn("Index updater sink received FileMetadata input with an older event timestamp, dropping")
		return nil
	}

	idx.keyToMetadata[md.Path] = *md
	return nil
}
