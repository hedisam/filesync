package rest

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/server/internal/store"
)

type FileMetadataStore interface {
	Delete(ctx context.Context, key string) error
	Snapshot(context.Context) (map[string]store.ObjectMetadata, error)
}

// FileServer is an implementation of our Restful server.
type FileServer struct {
	logger            *logrus.Logger
	fileMetadataStore FileMetadataStore
}

func NewFilesServer(logger *logrus.Logger, fileMetadataStore FileMetadataStore) *FileServer {
	return &FileServer{
		logger:            logger,
		fileMetadataStore: fileMetadataStore,
	}
}

func (s *FileServer) Snapshot(ctx context.Context, _ *GetSnapshotRequest) (*GetSnapshotResponse, error) {
	logger := s.logger.WithContext(ctx)
	snapshot, err := s.fileMetadataStore.Snapshot(ctx)
	if err != nil {
		logger.WithError(err).Error("Failed to get metadata snapshot")
		return nil, NewErrf(http.StatusInternalServerError, "get snapshot from store: %v", err)
	}

	keyToObject := make(map[string]*Metadata, len(snapshot))
	for k, md := range snapshot {
		keyToObject[k] = &Metadata{
			Key:            k,
			Size:           md.Size,
			SHA256Checksum: md.SHA256Checksum,
		}
	}

	return &GetSnapshotResponse{
		KeyToMetadata: keyToObject,
	}, nil
}

func (s *FileServer) DeleteFile(ctx context.Context, req *DeleteFileRequest) (*DeleteFileResponse, error) {
	logger := s.logger.WithContext(ctx).WithField("key", req.Key)

	key := strings.TrimSpace(req.Key)
	if key == "" {
		logger.Warn("Empty file key provided in file deletion request")
		return nil, NewErrf(http.StatusBadRequest, "invalid request body: 'key' is required")
	}

	err := s.fileMetadataStore.Delete(ctx, key)
	if err != nil {
		logger.WithError(err).Error("Failed to delete file metadata in store")
		return nil, fmt.Errorf("could not delete file metadata: %w", err)
	}

	logger.Debug("Object marked as deleted")

	return &DeleteFileResponse{}, nil
}

type Metadata struct {
	Key            string `json:"key"`
	Size           int64  `json:"size"`
	SHA256Checksum string `json:"sha256_checksum"`
}

type GetSnapshotRequest struct{}

type GetSnapshotResponse struct {
	KeyToMetadata map[string]*Metadata `json:"key_to_metadata"`
}

type DeleteFileRequest struct {
	Key string `json:"key"`
}

type DeleteFileResponse struct{}
