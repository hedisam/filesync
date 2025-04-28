package emitter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/hedisam/filesync/server/internal/store"
)

var (
	ErrClosed = errors.New("emitter closed")
)

type Emitter struct {
	ch     chan *store.ObjectMetadata
	closed atomic.Bool
	done   chan struct{}
	wg     sync.WaitGroup
}

func New() *Emitter {
	return &Emitter{
		ch:   make(chan *store.ObjectMetadata, 1),
		done: make(chan struct{}),
	}
}

func (e *Emitter) Emit(ctx context.Context, obj *store.ObjectMetadata) error {
	e.wg.Add(1)
	defer e.wg.Done()

	if e.closed.Load() {
		return ErrClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return ErrClosed
	case e.ch <- obj:
		return nil
	}
}

func (e *Emitter) Chan() <-chan *store.ObjectMetadata {
	return e.ch
}

func (e *Emitter) Close() {
	if !e.closed.CompareAndSwap(false, true) {
		return // already closed
	}

	// signal blocked emit calls
	close(e.done)
	// wait for inflight emit calls to finish
	e.wg.Wait()
	// now we're safe to close the multi-writer queue channel
	close(e.ch)
}
