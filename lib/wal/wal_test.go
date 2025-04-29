package wal_test

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hedisam/filesync/lib/wal"
)

func TestProduceConsume(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Millisecond)

	cases := map[string]struct {
		messages            [][]byte
		expected            []*wal.Entry
		closedBeforeReading bool
	}{
		"single": {
			messages: [][]byte{
				[]byte("foo"),
			},
			expected: []*wal.Entry{
				{Timestamp: now, Message: []byte("foo")},
			},
		},
		"multiple": {
			messages: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
			expected: []*wal.Entry{
				{Timestamp: now, Message: []byte("one")},
				{Timestamp: now, Message: []byte("two")},
				{Timestamp: now, Message: []byte("three")},
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
			expected:            nil,
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
			stageNum := int64(100)
			w, err := wal.New(logger, tmpf.Name())
			require.NoError(t, err)
			defer w.Close()

			// start producing messages
			producingDone := make(chan struct{})
			go func() {
				defer close(producingDone)
				for msg := range slices.Values(tc.messages) {
					err := w.Append(stageNum, msg)
					require.NoError(t, err)
				}
			}()

			if tc.closedBeforeReading {
				<-producingDone
				w.Close()
			}

			// consume produced messages
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			outCh := w.Consume(ctx)

			var got []*wal.Entry
			for entry := range outCh {
				got = append(got, entry)
			}

			for i, msg := range tc.expected {
				assert.Equal(t, msg.Message, got[i].Message)
				assert.WithinDuration(t, msg.Timestamp, got[i].Timestamp, 3*time.Second)
				assert.Equal(t, msg.Error, got[i].Error)
				assert.Equal(t, stageNum, got[i].StageNum)
			}
		})
	}
}
