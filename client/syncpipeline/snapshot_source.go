package syncpipeline

import (
	"context"
	"fmt"
	"io"
	"time"

	restapi "github.com/hedisam/filesync/client/api/rest"
	"github.com/hedisam/filesync/client/index"
	"github.com/hedisam/pipeline/chans"
)

type Snapshot struct {
	Server map[string]*restapi.File
	Local  map[string]*index.FileMetadata
}

type SnapshotSource struct {
	restClient                *restapi.Client
	initialServerSnapshotDone bool
	localSnapshotChan         <-chan map[string]*index.FileMetadata
}

func NewSnapshotSource(ctx context.Context, restClient *restapi.Client, idx *index.Index, interval time.Duration) *SnapshotSource {
	localSnapshotChan := startLocalSnapshotting(ctx, idx, interval)

	return &SnapshotSource{
		restClient:        restClient,
		localSnapshotChan: localSnapshotChan,
	}
}

func (s *SnapshotSource) Next(ctx context.Context) (any, error) {
	var err error
	var serverSnapshot map[string]*restapi.File

	if !s.initialServerSnapshotDone {
		// we want the server snapshot only once on startups
		serverSnapshot, err = s.restClient.Snapshot(ctx)
		if err != nil {
			return nil, fmt.Errorf("get initial server snapshot: %w", err)
		}
		s.initialServerSnapshotDone = true
	}

	localSnapshot, ok := chans.ReceiveOrDone(ctx, s.localSnapshotChan)
	if !ok {
		return nil, io.EOF
	}

	return &Snapshot{
		Server: serverSnapshot,
		Local:  localSnapshot,
	}, nil
}

func startLocalSnapshotting(ctx context.Context, idx *index.Index, interval time.Duration) <-chan map[string]*index.FileMetadata {
	out := make(chan map[string]*index.FileMetadata)

	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		defer close(out)

		for range chans.ReceiveOrDoneSeq(ctx, t.C) {
			snapshot := idx.SnapshotAndPurge()
			if !chans.SendOrDone(ctx, out, snapshot) {
				return
			}
		}
	}()

	return out
}
