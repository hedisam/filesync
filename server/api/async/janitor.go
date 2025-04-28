package async

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"

	"github.com/hedisam/filesync/lib/chans"
	"github.com/hedisam/filesync/server/internal/store"
)

type FileStorage interface {
	DeleteObject(ctx context.Context, objectID string) error
}

type Janitor struct {
	logger  *logrus.Logger
	storage FileStorage
}

func NewJanitor(logger *logrus.Logger, storage FileStorage) *Janitor {
	return &Janitor{
		logger:  logger,
		storage: storage,
	}
}

func (j *Janitor) Run(ctx context.Context, in <-chan *store.ObjectMetadata) {
	j.logger.WithContext(ctx).Info("Running Janitor")

	for md := range chans.ReceiveOrDoneSeq(ctx, in) {
		j.cleanup(ctx, md)
	}
}

func (j *Janitor) cleanup(ctx context.Context, md *store.ObjectMetadata) {
	ctx, span := otel.Tracer("").Start(ctx, "janitor")
	defer span.End()

	logger := j.logger.WithContext(ctx).WithFields(logrus.Fields{
		"object_id": md.ObjectID,
		"key":       md.Key,
	})
	logger.Debug("Cleaning up object")

	bk := backoff.NewExponentialBackOff(
		backoff.WithMaxElapsedTime(time.Second*3),
		backoff.WithMaxInterval(time.Second),
		backoff.WithInitialInterval(time.Millisecond*100),
		backoff.WithMultiplier(2),
		backoff.WithRandomizationFactor(0.2),
	)
	err := backoff.Retry(func() error {
		err := j.storage.DeleteObject(ctx, md.Key)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				logger.WithError(err).Error("Failed to cleanup object due to context cancellation; will be retried later")
				// the event shouldn't be acknowledged so that it can be retried later
				return backoff.Permanent(err)
			}
			logger.WithError(err).Error("Failed to delete object, retrying")
			return err
		}

		return nil
	}, backoff.WithContext(bk, ctx))
	if err != nil {
		logger.WithError(err).Error("Failed to clean up object in janitor")
		return
	}
}
