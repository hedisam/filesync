package plan

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/client/index"
	"github.com/hedisam/filesync/lib/psurls"
)

type RestClient interface {
	UploadURL() string
	Upload(ctx context.Context, reader io.Reader, presignedURL string, size int64) error
	Delete(ctx context.Context, key string) error
}

type PlanRequest interface {
	Apply(ctx context.Context, client RestClient, opts ...Option) error
	String() string
}

type Plan struct {
	Requests []PlanRequest
}

type applyConfig struct {
	accessKeyID string
	secretKey   string
}

type Option func(*applyConfig)

func ApplyWithCreds(accessKeyID, secretKey string) Option {
	return func(cfg *applyConfig) {
		cfg.accessKeyID = accessKeyID
		cfg.secretKey = secretKey
	}
}

type uploadRequest struct {
	logger       *logrus.Logger
	fileMetadata *index.FileMetadata
}

func (pr *uploadRequest) Apply(ctx context.Context, client RestClient, opts ...Option) error {
	cfg := &applyConfig{}
	for opt := range slices.Values(opts) {
		opt(cfg)
	}

	md := pr.fileMetadata
	f, err := os.Open(md.Path)
	if err != nil {
		// must've been deleted; ignore
		pr.logger.WithError(err).WithField("path", md.Path).Warn("Failed to open file for upload, ignoring")
		return nil
	}
	defer f.Close()

	urlData := psurls.URLData{
		ObjectKey:      md.Path,
		SHA256Checksum: md.SHA256,
		Size:           md.Size,
		MTime:          md.MTime,
		Expiry:         time.Now().UTC().Add(10 * time.Minute).Unix(),
		AccessKeyID:    cfg.accessKeyID,
	}

	url, err := psurls.Generate(urlData, client.UploadURL(), cfg.secretKey)
	if err != nil {
		return fmt.Errorf("generate presigned url for %q: %w", md.Path, err)
	}

	err = client.Upload(ctx, f, url, md.Size)
	if err != nil {
		return fmt.Errorf("upload via presigned url for %q: %w", md.Path, err)
	}

	return nil
}

func (pr *uploadRequest) String() string {
	return fmt.Sprintf("Planned request to upload %q", pr.fileMetadata.Path)
}

type deleteRequest struct {
	filePath string
}

func (pr *deleteRequest) Apply(ctx context.Context, client RestClient, _ ...Option) error {
	err := client.Delete(ctx, pr.filePath)
	if err != nil {
		return fmt.Errorf("delete via rest client for file %q: %w", pr.filePath, err)
	}

	return nil
}

func (pr *deleteRequest) String() string {
	return fmt.Sprintf("Planned request to delete %q", pr.filePath)
}
