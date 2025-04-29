package wal

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/filesync/lib/chans"
)

// Entry represents a record in the WAL.
type Entry struct {
	Timestamp    time.Time `json:"timestamp"`
	Message      []byte    `json:"message"`
	StageNum     int64     `json:"stage_num"`
	Error        string    `json:"-"`
	ErroredBytes []byte    `json:"-"`
}

// WAL provides append-only logging and tail-style consumption of JSON messages.
type WAL struct {
	logger *logrus.Logger

	closed    atomic.Bool
	writeFile *os.File
	encoder   *json.Encoder

	readFile  *os.File
	reader    *bufio.Reader
	bytesRead int64
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
		logger:    logger,
		writeFile: wf,
		encoder:   json.NewEncoder(wf),
		readFile:  rf,
		reader:    bufio.NewReader(rf),
	}, nil
}

// Append a message to the append-only log. It returns an error if either the underlying writer is closed or the entry
// cannot be encoded by the json encoder.
func (w *WAL) Append(stageNum int64, msg []byte) error {
	err := w.encoder.Encode(&Entry{
		Timestamp: time.Now().UTC(),
		Message:   msg,
		StageNum:  stageNum,
	})
	if err != nil {
		return fmt.Errorf("encode entry: %w", err)
	}

	return nil
}

type StopFunc func(entry *Entry) (stop bool)

type consumerConfig struct {
	stopFunc StopFunc
}

type Option func(cfg *consumerConfig)

func WithStopFunc(stopFunc StopFunc) Option {
	return func(cfg *consumerConfig) {
		cfg.stopFunc = stopFunc
	}
}

// Consume returns a channel that emits every WAL entry in order as they are written.
// It tails the file and will block waiting for new messages until the context is canceled.
func (w *WAL) Consume(ctx context.Context, opts ...Option) <-chan *Entry {
	out := make(chan *Entry)

	cfg := &consumerConfig{}
	for opt := range slices.Values(opts) {
		opt(cfg)
	}

	go func() {
		defer close(out)

		var partialData []byte
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			line, err := w.reader.ReadBytes('\n')
			if err != nil {
				if errors.Is(err, io.EOF) {
					time.Sleep(time.Millisecond * 100)
					// we could read partial data if the producer's data writes are not atomic with the writing the delimiter
					// this will cause a partial data read along with an io.EOF, we shouldn't lose the partial data.
					// note: partial reads shouldn't happen if the writer uses json.Encoder but we shouldn't couple
					// the producer and consumer together.
					if len(line) > 0 {
						partialData = append(partialData, line...)
					}
					continue
				}
				if !errors.Is(err, os.ErrClosed) {
					w.logger.WithError(err).Error("Failed to read from the WAL")
				}
				return
			}
			if len(partialData) > 0 {
				line = append(partialData, line...)
				partialData = partialData[:0]
			}

			var entry Entry
			err = json.Unmarshal(line, &entry)
			if err != nil {
				w.logger.WithError(err).Error("Failed to unmarshal WAL entry")
				entry = Entry{
					Timestamp:    time.Now().UTC(),
					Error:        err.Error(),
					ErroredBytes: line,
				}
			}

			if cfg.stopFunc != nil && cfg.stopFunc(&entry) {
				// stop the consumer and rewind the offset to make sure the already read data is not lost restarting consumer.
				_, seekErr := w.readFile.Seek(w.bytesRead, io.SeekStart)
				if seekErr != nil {
					w.logger.WithError(seekErr).Error("Failed to rewind wal consumer offset")
				}
				w.reader.Reset(w.readFile)
				return
			}

			ok := chans.SendOrDone(ctx, out, &entry)
			if !ok || err != nil {
				return
			}

			w.bytesRead += int64(len(line))
		}
	}()

	return out
}

// Close cleans up file descriptors used by the WAL.
func (w *WAL) Close() {
	if w.closed.CompareAndSwap(false, true) {
		_ = w.writeFile.Close()
		_ = w.readFile.Close()
	}
}
