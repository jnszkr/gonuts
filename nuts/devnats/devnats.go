// Package devnats creates spec infrastructure for local development and tests.
// Production code must not import it: applications never change infrastructure.
package devnats

import (
	"context"
	"fmt"

	"github.com/jnszkr/gonuts/nuts"
	"github.com/nats-io/nats.go/jetstream"
)

// Provision creates or updates everything in the Infra.
func Provision(ctx context.Context, js jetstream.JetStream, in nuts.Infra) error {
	for _, s := range in.Streams {
		if _, err := js.CreateOrUpdateStream(ctx, s); err != nil {
			return fmt.Errorf("devnats: stream %s: %w", s.Name, err)
		}
	}
	for _, c := range in.Consumers {
		if _, err := js.CreateOrUpdateConsumer(ctx, c.Stream, c.Config); err != nil {
			return fmt.Errorf("devnats: consumer %s: %w", c.Config.Durable, err)
		}
	}
	for _, b := range in.Buckets {
		if _, err := js.CreateOrUpdateKeyValue(ctx, b); err != nil {
			return fmt.Errorf("devnats: bucket %s: %w", b.Bucket, err)
		}
	}
	return nil
}
