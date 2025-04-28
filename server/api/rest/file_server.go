package rest

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"
)

type FileMetadataStore interface {
	Delete(ctx context.Context, key string) error
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

func (s *FileServer) Snapshot(context.Context, any) (resp any, err error) {
	return nil, NewErrf(http.StatusNotImplemented, "Not Implemented")
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

type DeleteFileRequest struct {
	Key string `json:"key"`
}

type DeleteFileResponse struct{}
