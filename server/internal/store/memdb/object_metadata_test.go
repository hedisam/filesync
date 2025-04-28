package memdb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hedisam/filesync/server/internal/store"
	"github.com/hedisam/filesync/server/internal/store/memdb"
	"github.com/hedisam/filesync/server/internal/store/memdb/mocks"
)

//go:generate moq -out mocks/emitter.go -pkg mocks -skip-ensure . Emitter

func TestCreate(t *testing.T) {
	tests := map[string]struct {
		md          *store.ObjectMetadata
		errContains string
	}{
		"missing key": {
			md:          &store.ObjectMetadata{Key: "", ObjectID: "id"},
			errContains: "key is required",
		},
		"missing objectID": {
			md:          &store.ObjectMetadata{Key: "k", ObjectID: ""},
			errContains: "object ID is required",
		},
		"success": {
			md: &store.ObjectMetadata{
				Key:            "k",
				ObjectID:       "id",
				SHA256Checksum: "c",
				Size:           1,
				MTime:          time.Now().UTC().Unix(),
				CreatedAt:      time.Now().UTC(),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ms := memdb.NewMetadataStore(nil)

			err := ms.Create(context.Background(), tc.md)
			if tc.errContains != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	tests := map[string]struct {
		key                  string
		initial              *store.ObjectMetadata
		emitterError         error
		errContains          string
		expectedEmitterCalls int
	}{
		"no object": {
			key:     "k",
			initial: nil,
		},
		"with object": {
			key:                  "k",
			initial:              &store.ObjectMetadata{Key: "k", ObjectID: "id"},
			expectedEmitterCalls: 1,
		},
		"emitter error": {
			key:                  "k",
			initial:              &store.ObjectMetadata{Key: "k", ObjectID: "id"},
			emitterError:         errors.New("boom"),
			errContains:          "boom",
			expectedEmitterCalls: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mock := &mocks.EmitterMock{
				EmitFunc: func(ctx context.Context, obj *store.ObjectMetadata) error {
					assert.NotNil(t, obj.CompletedAt)
					obj.CompletedAt = nil
					assert.EqualValues(t, tc.initial, obj)
					return tc.emitterError
				},
			}
			ms := memdb.NewMetadataStore(mock)

			if tc.initial != nil {
				err := ms.Create(ctx, tc.initial)
				require.NoError(t, err)
				err = ms.PutObjectCompleted(ctx, tc.initial.Key, tc.initial.ObjectID)
				require.NoError(t, err)
			}

			err := ms.Delete(ctx, tc.key)
			if tc.errContains != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.errContains)
				return
			}

			require.NoError(t, err)

			assert.Equal(t, tc.expectedEmitterCalls, len(mock.EmitCalls()))
		})
	}
}

func TestPutObjectCompleted(t *testing.T) {
	ctx := context.Background()
	tests := map[string]struct {
		completedKey      string
		completedObjectID string
		initialOld        *store.ObjectMetadata
		initialNew        *store.ObjectMetadata
		emitterErr        error

		expectedEvents int
		errContains    string
	}{
		"missing inflight": {
			completedKey:      "key",
			completedObjectID: "not-found",
			initialNew: &store.ObjectMetadata{
				Key:      "key",
				ObjectID: "object-new",
			},
			errContains: "not found",
		},
		"new only": {
			completedKey:      "key",
			completedObjectID: "object-new",
			initialNew: &store.ObjectMetadata{
				Key:      "key",
				ObjectID: "object-new",
			},
		},
		"old and new": {
			completedKey:      "key",
			completedObjectID: "object-new",
			initialOld: &store.ObjectMetadata{
				Key:      "key",
				ObjectID: "object-old",
			},
			initialNew: &store.ObjectMetadata{
				Key:      "key",
				ObjectID: "object-new",
			},
			expectedEvents: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mock := &mocks.EmitterMock{
				EmitFunc: func(ctx context.Context, obj *store.ObjectMetadata) error {
					assert.Equal(t, tc.initialOld.ObjectID, obj.ObjectID)
					return tc.emitterErr
				},
			}
			ms := memdb.NewMetadataStore(mock)

			if tc.initialOld != nil {
				err := ms.Create(ctx, tc.initialOld)
				require.NoError(t, err)
				err = ms.PutObjectCompleted(ctx, tc.initialOld.Key, tc.initialOld.ObjectID)
				require.NoError(t, err)
			}
			if tc.initialNew != nil {
				err := ms.Create(ctx, tc.initialNew)
				require.NoError(t, err)
			}

			err := ms.PutObjectCompleted(ctx, tc.completedKey, tc.completedObjectID)
			if tc.errContains != "" {
				require.ErrorContains(t, err, tc.errContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedEvents, len(mock.EmitCalls()))
		})
	}
}
