package plan

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/sirupsen/logrus"

	restapi "github.com/hedisam/filesync/client/api/rest"
	"github.com/hedisam/filesync/client/indexer"
	"github.com/hedisam/filesync/lib/psurls"
	"github.com/hedisam/pipeline"
	"github.com/hedisam/pipeline/stage"
)

const ()

type Credentials struct {
	AccessKeyID string
	SecretKey   string
}

type Indexer interface {
	Snapshot() map[string]indexer.FileMetadata
}

type RestClient interface {
	Snapshot(ctx context.Context) (map[string]restapi.File, error)
	Delete(ctx context.Context, fileKey string) error
	Upload(ctx context.Context, r io.Reader, url string, size int64) error
}

type Plan struct {
	// uploads are PUT operations, they will replace existing files on the server.
	ToUpload []*indexer.FileMetadata
	ToDelete []string
}

func Generate(ctx context.Context, restClient RestClient, idx Indexer) (*Plan, error) {
	// we should retrieve the snapshot in a background job while we are building the index
	// ideally, the snapshot will be retrieved incrementally.
	serverSnapshot, err := restClient.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve server snapshot: %w", err)
	}

	localSnapshot := idx.Snapshot()

	var toUpload []*indexer.FileMetadata
	for fileName, localFile := range localSnapshot {
		remoteFile, ok := serverSnapshot[fileName]
		if !ok || localFile.SHA256Checksum != remoteFile.SHA256Checksum {
			fmt.Printf("to upload: %+v\n", localFile)
			toUpload = append(toUpload, &localFile)
		}
	}
	var toDelete []string
	for fileName := range serverSnapshot {
		_, ok := localSnapshot[fileName]
		if !ok {
			toDelete = append(toDelete, fileName)
		}
	}

	return &Plan{
		ToUpload: toUpload,
		ToDelete: toDelete,
	}, nil
}

func Apply(ctx context.Context, logger *logrus.Logger, uploadBaseURL string, restClient RestClient, plan *Plan, workersNum uint, creds Credentials) error {
	var data []any
	uploadKeys := make([]string, 0, len(plan.ToUpload))
	for upload := range slices.Values(plan.ToUpload) {
		data = append(data, upload)
		uploadKeys = append(uploadKeys, upload.Path)
	}
	for deletion := range slices.Values(plan.ToDelete) {
		data = append(data, deletion)
	}
	logger.WithFields(logrus.Fields{
		"to_upload": uploadKeys,
		"to_delete": plan.ToDelete,
	}).Debug("Applying plan")

	ws := &workerStage{
		logger:        logger,
		client:        restClient,
		creds:         creds,
		uploadBaseURL: uploadBaseURL,
	}

	workersNum = min(workersNum, uint(len(data)))
	source := pipeline.SeqSource(slices.Values(data))
	p := pipeline.NewPipeline(source, ws.sink)
	err := p.Run(ctx, stage.WorkerPoolRunner(workersNum, ws.worker))
	if err != nil {
		return fmt.Errorf("run pipeline: %w", err)
	}

	logger.Info("Successfully applied plan")

	return nil
}

type workerStage struct {
	logger        *logrus.Logger
	client        RestClient
	creds         Credentials
	uploadBaseURL string
}

func (s *workerStage) worker(ctx context.Context, payload any) (out any, drop bool, err error) {
	switch val := payload.(type) {
	case *indexer.FileMetadata:
		err = s.upload(ctx, val)
		if err != nil {
			return nil, false, fmt.Errorf("upload file %q: %w", val.Path, err)
		}
		return nil, false, nil
	case string:
		err = s.delete(ctx, val)
		if err != nil {
			return nil, false, fmt.Errorf("delete file %q: %w", val, err)
		}
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("unknown payload type: %T", payload)
	}
}

func (s *workerStage) upload(ctx context.Context, md *indexer.FileMetadata) error {
	f, err := os.Open(md.Path)
	if err != nil {
		// must've been deleted; ignore
		s.logger.WithError(err).WithField("path", md.Path).Warn("Failed to open file for upload, ignoring")
		return nil
	}
	defer f.Close()

	// todo: add an idempotency key
	urlData := psurls.URLData{
		ObjectKey:      md.Path,
		SHA256Checksum: md.SHA256Checksum,
		Size:           md.Size,
		MTime:          md.MTime,
		Expiry:         time.Now().UTC().Add(10 * time.Minute).Unix(),
		AccessKeyID:    s.creds.AccessKeyID,
	}

	url, err := psurls.Generate(urlData, s.uploadBaseURL, s.creds.SecretKey)
	if err != nil {
		return fmt.Errorf("generate presigned url for %q: %w", md.Path, err)
	}

	return s.client.Upload(ctx, f, url, md.Size)
}

func (s *workerStage) delete(ctx context.Context, key string) error {
	// todo: would be better to have a bulk deletion method
	return s.client.Delete(ctx, key)
}

// sink is a no-op sink.
func (s *workerStage) sink(ctx context.Context, payload any) error {
	return nil
}
