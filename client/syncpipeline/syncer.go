package syncpipeline

import (
	"context"
	"fmt"
	"iter"
	"slices"

	"github.com/sirupsen/logrus"

	restapi "github.com/hedisam/filesync/client/api/rest"
	"github.com/hedisam/filesync/client/index"
	"github.com/hedisam/filesync/client/plan"
	"github.com/hedisam/pipeline"
	"github.com/hedisam/pipeline/stage"
)

type Planner interface {
	Generate(localSnapshot map[string]*index.FileMetadata, serverSnapshot map[string]*restapi.File) *plan.Plan
}

type RestClient = plan.RestClient

type Syncer struct {
	logger      *logrus.Logger
	client      RestClient
	planner     Planner
	accessKeyID string
	secretKey   string
}

func New(logger *logrus.Logger, client RestClient, planner Planner, accessKeyID, secretKey string) *Syncer {
	return &Syncer{
		logger:      logger,
		client:      client,
		planner:     planner,
		accessKeyID: accessKeyID,
		secretKey:   secretKey,
	}
}

func (s *Syncer) PlanGenerator() stage.Processor {
	return func(ctx context.Context, payload any) (out any, drop bool, err error) {
		snapshot, ok := payload.(*Snapshot)
		if !ok {
			return nil, false, fmt.Errorf("invalid payload type received by plan generator: %T", payload)
		}

		plan := s.planner.Generate(snapshot.Local, snapshot.Server)
		return plan, false, nil
	}
}

func (s *Syncer) PlanSplitter() stage.SplitterProcessor {
	return func(ctx context.Context, payload any) (iterator iter.Seq[any], drop bool, err error) {
		pln, ok := payload.(*plan.Plan)
		if !ok {
			return nil, false, fmt.Errorf("invalid payload type received by plan splitter: %T", payload)
		}

		var requests []any
		for req := range slices.Values(pln.Requests) {
			requests = append(requests, req)
		}

		return slices.Values(requests), false, nil
	}
}

func (s *Syncer) PlanRequestApplier() stage.Processor {
	return func(ctx context.Context, payload any) (out any, drop bool, err error) {
		req, ok := payload.(plan.PlanRequest)
		if !ok {
			return nil, false, fmt.Errorf("unknown payload type when applying plan request: %T", payload)
		}

		err = req.Apply(ctx, s.client, plan.ApplyWithCreds(s.accessKeyID, s.secretKey))
		if err != nil {
			return nil, false, fmt.Errorf("could not apply planned request: %w", err)
		}

		return payload, false, nil
	}
}

func (s *Syncer) OutputSink() pipeline.Sink {
	return func(ctx context.Context, out any) error {
		req, ok := out.(plan.PlanRequest)
		if !ok {
			return fmt.Errorf("invalid plan request received by output sink: %T", out)
		}

		fmt.Printf("[!] %s was done successfully\n", req)
		return nil
	}
}
