package wal_test

import (
	"context"
	"io"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hedisam/pipeline/chans"

	"github.com/hedisam/filesync/lib/wal"
)

func TestConsume(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		messages            [][]byte
		expected            [][]byte
		closedBeforeReading bool
	}{
		"single": {
			messages: [][]byte{
				[]byte("foo"),
			},
			expected: [][]byte{
				[]byte("foo"),
			},
		},
		"multiple": {
			messages: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
			expected: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
		},
		"no messages": {
			messages: nil,
			expected: nil,
		},
		"closed before reading": {
			messages: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
			closedBeforeReading: true,
			expected: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Create a temp WAL file
			tmpf, err := os.CreateTemp("", "wal_test")
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = os.Remove(tmpf.Name())
			})
			err = tmpf.Close()
			require.NoError(t, err)

			logger := logrus.New()
			w, err := wal.New(logger, tmpf.Name())
			require.NoError(t, err)
			defer w.Close()

			for msg := range slices.Values(tc.messages) {
				err = w.Append(msg)
				require.NoError(t, err)
			}

			if tc.closedBeforeReading {
				w.Close()
			}

			// consume produced messages
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			outCh, errCh := w.Consume(ctx)

			var got [][]byte
			for msg := range chans.ReceiveOrDoneSeq(ctx, outCh) {
				got = append(got, msg)
			}

			err = <-errCh
			if !tc.closedBeforeReading {
				require.Error(t, err)
				assert.ErrorIs(t, err, context.DeadlineExceeded)
			}

			require.Equal(t, len(tc.expected), len(got))
			for i, msg := range tc.expected {
				assert.Equal(t, string(msg), string(got[i]))
			}
		})
	}
}

func TestNext(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		messages            [][]byte
		expected            [][]byte
		closedBeforeReading bool
	}{
		"single": {
			messages: [][]byte{
				[]byte("foo"),
			},
			expected: [][]byte{
				[]byte("foo"),
			},
		},
		"multiple": {
			messages: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
			expected: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
		},
		"no messages": {
			messages: nil,
			expected: nil,
		},
		"closed before reading": {
			messages: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
			closedBeforeReading: true,
			expected: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Create a temp WAL file
			tmpf, err := os.CreateTemp("", "wal_test")
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = os.Remove(tmpf.Name())
			})
			err = tmpf.Close()
			require.NoError(t, err)

			logger := logrus.New()
			w, err := wal.New(logger, tmpf.Name())
			require.NoError(t, err)
			defer w.Close()

			for msg := range slices.Values(tc.messages) {
				err = w.Append(msg)
				require.NoError(t, err)
			}

			if tc.closedBeforeReading {
				w.Close()
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			var got [][]byte
			for {
				data, err := w.Next(ctx)
				if err != nil {
					if tc.closedBeforeReading {
						assert.ErrorIs(t, err, io.EOF)
						break
					}
					assert.ErrorIs(t, err, context.DeadlineExceeded)
					break
				}

				dataBytes, _ := data.([]byte)
				got = append(got, dataBytes)
			}

			require.Equal(t, len(tc.expected), len(got))
			for i, msg := range tc.expected {
				assert.Equal(t, string(msg), string(got[i]))
			}
		})
	}
}
