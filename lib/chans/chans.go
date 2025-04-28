package chans

import (
	"context"
	"iter"
)

// ReceiveOrDone attempts to receive a message of type T from the given channel.
// It blocks until one of the following occurs:
// 1. A message is received from the channel (returns the message and true)
// 2. The channel is closed (returns the zero value of T and false)
// 3. The provided context is canceled (returns the zero value of T and false)
// The boolean return value indicates whether a message was successfully received.
func ReceiveOrDone[T any](ctx context.Context, ch <-chan T) (T, bool) {
	select {
	case <-ctx.Done():
		var zero T
		return zero, false
	case data, ok := <-ch:
		return data, ok
	}
}

// ReceiveOrDoneSeq same as ReceiveOrDone but it returns an iter.Seq that can be used with for-range loops.
func ReceiveOrDoneSeq[T any](ctx context.Context, ch <-chan T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			data, ok := ReceiveOrDone(ctx, ch)
			if !ok || !yield(data) {
				return
			}
		}
	}
}
