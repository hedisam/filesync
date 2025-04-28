package rest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/lib/psurls"
	"github.com/hedisam/filesync/server/internal/store"
)

type Auth interface {
	GetSecretKeyByID(keyID string) (string, bool)
}

type FileStorage interface {
	PutObject(ctx context.Context, r io.Reader, objectID string) (checksum string, written int64, err error)
}

type UploadMetadataStore interface {
	Create(ctx context.Context, md *store.ObjectMetadata) error
	PutObjectCompleted(ctx context.Context, key, objectID string) error
}

type UploadServer struct {
	logger      *logrus.Logger
	fileStorage FileStorage
	mdStore     UploadMetadataStore
	auth        Auth
}

func NewUploadServer(logger *logrus.Logger, fileStorage FileStorage, mdStore UploadMetadataStore, auth Auth) *UploadServer {
	return &UploadServer{
		logger:      logger,
		fileStorage: fileStorage,
		mdStore:     mdStore,
		auth:        auth,
	}
}

func (s *UploadServer) UploadFile(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.WithContext(r.Context())

	rawURL := r.URL.String()
	u, err := url.Parse(rawURL)
	if err != nil {
		logger.WithField("url", rawURL).Warn("Failed to parse url when uploading file")
		http.Error(w, "could not parse url", http.StatusBadRequest)
		return
	}

	accessKeyID := u.Query().Get(psurls.AccessKeyID)
	secretKey, ok := s.auth.GetSecretKeyByID(accessKeyID)
	if !ok {
		logger.WithField("access_key_id", accessKeyID).Warn("Could not authorise request when uploading file")
		http.Error(w, "invalid access key id", http.StatusUnauthorized)
		return
	}

	urlData, err := psurls.Validate(u.Query(), secretKey)
	if err != nil {
		logger.WithError(err).Warn("Failed to validate presigned URL while uploading file")
		if errors.Is(err, psurls.ErrURLExpired) || errors.Is(err, psurls.ErrSignatureMismatch) {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		http.Error(w, fmt.Sprintf("invalid presigned URL: %q", err.Error()), http.StatusBadRequest)
		return
	}

	logger = logger.WithField("key", urlData.ObjectKey)

	if r.ContentLength != urlData.Size {
		// fail early if Content-Length doesn't match the size value in the
		// presigned url; no point in wasting resources on an invalid request
		logger.WithFields(logrus.Fields{
			"content_length": r.ContentLength,
			"size":           urlData.Size,
		}).Warn("Mismatched Content-Length with presigned url size while uploading file")
		http.Error(w, "mismatched Content-Length and size", http.StatusBadRequest)
	}

	objectID := mustUUIDV7()
	err = s.mdStore.Create(r.Context(), &store.ObjectMetadata{
		Key:            urlData.ObjectKey,
		ObjectID:       objectID,
		SHA256Checksum: urlData.SHA256Checksum,
		Size:           urlData.Size,
		MTime:          urlData.MTime,
		CreatedAt:      time.Now().UTC(),
	})
	if err != nil {
		logger.WithError(err).Error("Failed to create object metadata in store")
		http.Error(w, fmt.Sprintf("could not create object metadata in store: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	checksum, written, err := s.fileStorage.PutObject(r.Context(), r.Body, objectID)
	if err != nil {
		logger.WithError(err).Warn("Failed to save file to storage")
		http.Error(w, fmt.Sprintf("failed to save file to storage: %q", err.Error()), http.StatusInternalServerError)
	}

	if urlData.SHA256Checksum != checksum {
		logger.Warn("Provided checksum did not match what was uploaded")
		http.Error(w, "provided checksum did not match what was uploaded", http.StatusBadRequest)
		return
	}
	if urlData.Size != written {
		logger.WithFields(logrus.Fields{
			"size":    urlData.Size,
			"written": written,
		}).Warn("Provided file size did not match what was uploaded")
		http.Error(w, "provided file size did not match what was uploaded", http.StatusBadRequest)
		return
	}

	err = s.mdStore.PutObjectCompleted(r.Context(), urlData.ObjectKey, objectID)
	if err != nil {
		logger.WithError(err).Error("Failed to mark object metadata as completed when uploading file")
		http.Error(w, fmt.Sprintf("failed to mark object metadata as completed when uploading file: %q", err.Error()), http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusCreated)
	logger.Debug("Successfully uploaded file to storage")
}

func mustUUIDV7() string {
	u, err := uuid.NewV7()
	if err != nil {
		panic(fmt.Errorf("failed to generate uuid: %v", err))
	}
	return u.String()
}
