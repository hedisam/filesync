package emitter_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hedisam/filesync/server/internal/emitter"
	"github.com/hedisam/filesync/server/internal/store"
)

func TestEmitter(t *testing.T) {
	t.Run("emit and receive", func(t *testing.T) {
		e := emitter.New()
		obj := &store.ObjectMetadata{ObjectID: uuid.NewString()}
		err := e.Emit(context.Background(), obj)
		require.NoError(t, err)

		select {
		case got := <-e.Chan():
			assert.Equal(t, obj, got)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timed out waiting for object on channel")
		}
	})

	t.Run("emit after close", func(t *testing.T) {
		e := emitter.New()
		e.Close()
		err := e.Emit(context.Background(), &store.ObjectMetadata{})
		require.ErrorIs(t, err, emitter.ErrClosed)
	})

	t.Run("emit context canceled", func(t *testing.T) {
		e := emitter.New()
		ctx, cancel := context.WithCancel(context.Background())
		err := e.Emit(ctx, &store.ObjectMetadata{})
		cancel()
		err = e.Emit(ctx, &store.ObjectMetadata{})
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("close closes channel", func(t *testing.T) {
		e := emitter.New()
		e.Close()
		_, ok := <-e.Chan()
		assert.False(t, ok)
	})

	t.Run("close idempotent", func(t *testing.T) {
		e := emitter.New()
		e.Close()
		// second close should do nothing (no panic, channel remains closed)
		e.Close()
	})
}
