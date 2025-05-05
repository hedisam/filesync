package wal

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/pipeline/chans"
)

const (
	openFlag = iota
	writerClosedFlag
	readerWriterClosedFlag
)

var (
	ErrClosed = errors.New("wal closed")
)

// WAL provides append-only logging and tail-style consumption of JSON messages.
type WAL struct {
	logger *logrus.Entry

	closed    atomic.Int32
	writeFile *os.File
	writeBuf  *bytes.Buffer

	readFile *os.File
	reader   *bufio.Reader
}

// New opens (or creates) a WAL file at the given path.
// It returns a WAL instance for producing and consuming messages.
func New(logger *logrus.Logger, path string) (*WAL, error) {
	wf, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}

	rf, err := os.Open(path)
	if err != nil {
		_ = wf.Close()
		return nil, err
	}

	return &WAL{
		logger:    logger.WithField("name", filepath.Base(path)),
		writeFile: wf,
		writeBuf:  new(bytes.Buffer),
		readFile:  rf,
		reader:    bufio.NewReader(rf),
	}, nil
}

// Append a message to the append-only log. It returns an error if either the underlying writer is closed or the entry
// cannot be encoded by the json encoder.
func (w *WAL) Append(msg []byte) error {
	w.writeBuf.Write(msg)
	w.writeBuf.WriteString("\n")
	n, err := w.writeBuf.WriteTo(w.writeFile)
	if err != nil {
		return fmt.Errorf("write wal file: %w", err)
	}
	if int(n)-1 != len(msg) {
		return fmt.Errorf("write wal file: write want '%d', got '%d'", len(msg), n)
	}

	return nil
}

// Next implements pipeline.Source.
func (w *WAL) Next(ctx context.Context) (any, error) {
	v, err := w.next(ctx)
	if err != nil {
		if errors.Is(err, ErrClosed) {
			return nil, io.EOF // pipeline.Source expects io.EOF for such cases
		}
		return nil, err
	}

	return v, nil
}

// Consume returns a channel that emits every WAL entry in order as they are written.
// It tails the file and will block waiting for new messages until the context is canceled or the WAL is closed.
func (w *WAL) Consume(ctx context.Context) (<-chan []byte, <-chan error) {
	out := make(chan []byte)
	errc := make(chan error)

	go func() {
		defer close(out)
		defer close(errc)

		for {
			entry, err := w.next(ctx)
			if err != nil {
				switch {
				case errors.Is(err, ErrClosed):
					return
				case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
					errCtx, cancel := context.WithTimeout(context.Background(), time.Second)
					chans.SendOrDone(errCtx, errc, err)
					cancel()
					return
				default:
					chans.SendOrDone(ctx, errc, err)
					return
				}
			}

			ok := chans.SendOrDone(ctx, out, entry)
			if !ok {
				return
			}
		}
	}()

	return out, errc
}

// Close cleans up file descriptors used by the WAL.
func (w *WAL) Close() {
	if w.closed.CompareAndSwap(openFlag, writerClosedFlag) {
		_ = w.writeFile.Close()
	}
}

func (w *WAL) next(ctx context.Context) ([]byte, error) {
	if w.closed.Load() == readerWriterClosedFlag {
		return nil, ErrClosed
	}

	var partialRead []byte
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		line, err := w.reader.ReadBytes('\n')
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				if w.closed.Load() == writerClosedFlag {
					w.closed.Store(readerWriterClosedFlag)
					_ = w.readFile.Close()
					return nil, ErrClosed
				}
				time.Sleep(time.Millisecond * 100)
				// we could read partial data if the producer's data writes are not atomic with the writing the delimiter
				// this will cause a partial data read along with an io.EOF, we shouldn't lose the partial data.
				// note: partial reads shouldn't happen if the writer uses json.Encoder but we shouldn't couple
				// the producer and consumer together.
				if len(line) > 0 {
					partialRead = append(partialRead, line...)
				}
				continue
			case errors.Is(err, os.ErrClosed):
				return nil, ErrClosed
			default:
				w.logger.WithError(err).Error("Failed to read from the WAL")
				return nil, fmt.Errorf("failed to read from WAL: %w", err)
			}
		}
		if len(partialRead) > 0 {
			line = append(partialRead, line...)
			partialRead = partialRead[:0]
		}

		// don't return the \n appended to the wal input
		return line[:len(line)-1], nil
	}
}
