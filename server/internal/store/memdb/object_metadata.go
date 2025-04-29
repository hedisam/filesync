package memdb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/hedisam/filesync/server/internal/store"
)

var (
	ErrNotFound = errors.New("not found")
)

type Emitter interface {
	Emit(ctx context.Context, obj *store.ObjectMetadata) error
}

// MetadataStore stores objects metadata. The underlying store is a simple map of key to a list file metadata.
// The map value is a list of metadata instead of a single one to count for existing objects with the same key
// that are going to be replaced soon by an in progress upload. While the new object is being uploaded, we still need
// to make sure the existing object is visible to the client.
type MetadataStore struct {
	mu                   sync.RWMutex
	keyToObjectMetadata  map[string]*store.ObjectMetadata
	keyToInflightUploads map[string][]*store.ObjectMetadata
	emitter              Emitter
}

func NewMetadataStore(e Emitter) *MetadataStore {
	return &MetadataStore{
		keyToObjectMetadata:  make(map[string]*store.ObjectMetadata),
		keyToInflightUploads: make(map[string][]*store.ObjectMetadata),
		emitter:              e,
	}
}

func (s *MetadataStore) Snapshot(context.Context) (map[string]store.ObjectMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := make(map[string]store.ObjectMetadata, len(s.keyToObjectMetadata))
	for k, v := range s.keyToObjectMetadata {
		snapshot[k] = *v
	}
	return snapshot, nil
}

// Create creates a file metadata record in the database. It will be marked as completed when the upload is done.
func (s *MetadataStore) Create(_ context.Context, md *store.ObjectMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if md.Key == "" {
		return errors.New("key is required for storing metadata")
	}
	if md.ObjectID == "" {
		return errors.New("object ID is required for storing metadata")
	}

	s.keyToInflightUploads[md.Key] = append(s.keyToInflightUploads[md.Key], &store.ObjectMetadata{
		Key:            md.Key,
		ObjectID:       md.ObjectID,
		SHA256Checksum: md.SHA256Checksum,
		Size:           md.Size,
		MTime:          md.MTime,
		CreatedAt:      md.CreatedAt,
	})

	return nil
}

// Delete marks the object with the provided key as deleted.
// Since we can have multiple object metadata associated with the same key, we should make sure we only mark the one
// that is marked as completed and not already deleted.
func (s *MetadataStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	object, ok := s.keyToObjectMetadata[key]
	if !ok {
		return nil
	}

	err := s.emitter.Emit(ctx, object)
	if err != nil {
		return fmt.Errorf("could not emit object deletion event: %w", err)
	}

	delete(s.keyToObjectMetadata, key)

	return nil
}

// PutObjectCompleted is called to update the file metadata when an object file has been stored on our storage
// system successfully. It queues any existing object under the same key for deletion.
func (s *MetadataStore) PutObjectCompleted(ctx context.Context, key, objectID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	inflightObjects, ok := s.keyToInflightUploads[key]
	if !ok {
		return ErrNotFound
	}

	var object *store.ObjectMetadata
	for obj := range slices.Values(inflightObjects) {
		if obj.ObjectID == objectID {
			object = obj
			break
		}
	}
	if object == nil {
		return fmt.Errorf("object not found in inflight uploads: %w", ErrNotFound)
	}

	existingObject, ok := s.keyToObjectMetadata[key]
	if ok {
		err := s.emitter.Emit(ctx, existingObject)
		if err != nil {
			return fmt.Errorf("could not emit deletion event for the existing object: %w", err)
		}
	}

	now := time.Now().UTC()
	object.CompletedAt = &now
	s.keyToObjectMetadata[key] = object

	inflightObjects = slices.DeleteFunc(inflightObjects, func(obj *store.ObjectMetadata) bool {
		return obj.ObjectID == objectID
	})
	if len(inflightObjects) == 0 {
		delete(s.keyToInflightUploads, key)
		return nil
	}
	s.keyToInflightUploads[key] = inflightObjects

	return nil
}
