package index

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/client/ops"
	"github.com/hedisam/pipeline"
	"github.com/hedisam/pipeline/stage"
)

const (
	DefaultIndexSize = 100
)

type FileMetadata struct {
	Path   string
	Size   int64
	SHA256 string
	MTime  int64

	Op        ops.Op
	Timestamp time.Time
}

type Index struct {
	logger *logrus.Logger
	size   uint
	idx    map[string]*FileMetadata
	mu     sync.RWMutex
}

func New(logger *logrus.Logger, size uint) *Index {
	return &Index{
		logger: logger,
		size:   size,
		idx:    make(map[string]*FileMetadata, size),
	}
}

func (i *Index) UnmarshalWALDataProcessor() stage.Processor {
	return func(ctx context.Context, payload any) (out any, drop bool, err error) {
		data, ok := payload.([]byte)
		if !ok {
			return nil, false, fmt.Errorf("unexpected payload type while unmarshalling WAL data: %T", payload)
		}

		var op ops.FileOp
		err = json.Unmarshal(data, &op)
		if err != nil {
			return nil, false, fmt.Errorf("could not unmarshal WAL data: %w", err)
		}

		return &op, false, nil
	}
}

// MetadataExtractorProcessor returns stage processor that is run by the pipeline.
// It enriches the input payload with file metadata and its sha256 checksum and pass it on to the next pipeline stage
// which would be the pipeline sink.
func (i *Index) MetadataExtractorProcessor() stage.Processor {
	return func(_ context.Context, payload any) (out any, drop bool, err error) {
		fileOp, ok := payload.(*ops.FileOp)
		if !ok {
			return nil, false, fmt.Errorf("invalid payload type: %T", payload)
		}

		logger := i.logger.WithField("path", fileOp.Path)

		if fileOp.Op == ops.OpRemoved {
			// no need for metadata for removals; pass it over to the next stage.
			return &FileMetadata{
				Path:      fileOp.Path,
				Op:        fileOp.Op,
				Timestamp: fileOp.Timestamp,
			}, false, nil
		}

		f, err := os.Open(fileOp.Path)
		if err != nil {
			logger.WithError(err).Warn("Could not open file to extract metadata, dropping")
			// drop this payload and return no errors so the pipeline keeps running; file could be deleted.
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

		hasher := sha256.New()
		_, err = io.Copy(hasher, f)
		if err != nil {
			logger.WithError(err).Warn("Could calculate sha256 checksum, dropping")
			return nil, true, nil
		}

		return &FileMetadata{
			Path:      fileOp.Path,
			Size:      st.Size(),
			SHA256:    hex.EncodeToString(hasher.Sum(nil)),
			MTime:     st.ModTime().UTC().Unix(),
			Op:        fileOp.Op,
			Timestamp: fileOp.Timestamp,
		}, false, nil
	}
}

// IndexerSink returns a pipeline.Sink that receives processed file info and stores them in the index.
func (i *Index) IndexerSink() pipeline.Sink {
	return func(_ context.Context, payload any) error {
		// update the indexer; check timestamp for out of order events.
		md, ok := payload.(*FileMetadata)
		if !ok {
			return fmt.Errorf("invalid payload type: %T", payload)
		}

		logger := i.logger.WithField("path", md.Path)
		logger.Debugf("Updating index")

		i.mu.Lock()
		defer i.mu.Unlock()

		if existingMD, ok := i.idx[md.Path]; ok && existingMD.Timestamp.After(md.Timestamp) {
			logger.WithFields(logrus.Fields{
				"existing_timestamp": existingMD.Timestamp.String(),
				"new_timestamp":      md.Timestamp.String(),
			}).Debug("Indexer process received file metadata with an older event timestamp, dropping")
			return nil
		}

		// add new metadata or replace any existing one from a more recent file change event
		i.idx[md.Path] = md
		return nil
	}
}

// SnapshotAndPurge returns a snapshot and purges the index's state.
func (i *Index) SnapshotAndPurge() map[string]*FileMetadata {
	i.mu.Lock()
	defer i.mu.Unlock()
	snapshot := maps.Clone(i.idx)
	i.idx = make(map[string]*FileMetadata, i.size)
	return snapshot
}
