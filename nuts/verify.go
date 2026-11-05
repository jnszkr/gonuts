package nuts

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Drift is one difference between the server and the spec.
type Drift struct {
	Resource string `json:"resource"`
	Field    string `json:"field"`
	Want     string `json:"want"`
	Got      string `json:"got"`
}

func (d Drift) String() string {
	return fmt.Sprintf("%s %s: want %s, got %s", d.Resource, d.Field, d.Want, d.Got)
}

type drifts []Drift

func (ds *drifts) add(resource, field string, want, got any) {
	*ds = append(*ds, Drift{resource, field, fmt.Sprint(want), fmt.Sprint(got)})
}

// compare records a difference, skipping fields the spec leaves at zero.
func compare[V comparable](ds *drifts, resource, field string, want, got V) {
	var zero V
	if want != zero && want != got {
		ds.add(resource, field, want, got)
	}
}

// Verify compares the server with the spec's main settings. It only reads.
func Verify(ctx context.Context, js jetstream.JetStream, in Infra) ([]Drift, error) {
	var ds drifts
	for _, want := range in.Streams {
		if err := verifyStream(ctx, js, want, &ds); err != nil {
			return nil, err
		}
	}
	for _, want := range in.Consumers {
		if err := verifyConsumer(ctx, js, want, &ds); err != nil {
			return nil, err
		}
	}
	for _, want := range in.Buckets {
		if err := verifyBucket(ctx, js, want, &ds); err != nil {
			return nil, err
		}
	}
	return ds, nil
}

func verifyStream(ctx context.Context, js jetstream.JetStream, want jetstream.StreamConfig, ds *drifts) error {
	res := "stream " + want.Name
	st, err := js.Stream(ctx, want.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		ds.add(res, "exists", true, false)
		return nil
	}
	if err != nil {
		return fmt.Errorf("nuts: verify %s: %w", res, err)
	}
	info, err := st.Info(ctx)
	if err != nil {
		return fmt.Errorf("nuts: verify %s: %w", res, err)
	}
	got := info.Config
	if !sameSet(want.Subjects, got.Subjects) {
		ds.add(res, "subjects", want.Subjects, got.Subjects)
	}
	if want.Retention != got.Retention {
		ds.add(res, "retention", want.Retention, got.Retention)
	}
	if want.Storage != got.Storage {
		ds.add(res, "storage", want.Storage, got.Storage)
	}
	if want.AllowMsgSchedules != got.AllowMsgSchedules {
		ds.add(res, "allow_msg_schedules", want.AllowMsgSchedules, got.AllowMsgSchedules)
	}
	compare(ds, res, "replicas", want.Replicas, got.Replicas)
	compare(ds, res, "max_age", want.MaxAge, got.MaxAge)
	compare(ds, res, "duplicate_window", want.Duplicates, got.Duplicates)
	return nil
}

func verifyConsumer(ctx context.Context, js jetstream.JetStream, want ConsumerInfra, ds *drifts) error {
	res := "consumer " + want.Stream + "/" + want.Config.Durable
	c, err := js.Consumer(ctx, want.Stream, want.Config.Durable)
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
		ds.add(res, "exists", true, false)
		return nil
	}
	if err != nil {
		return fmt.Errorf("nuts: verify %s: %w", res, err)
	}
	info, err := c.Info(ctx)
	if err != nil {
		return fmt.Errorf("nuts: verify %s: %w", res, err)
	}
	got := info.Config
	if !sameSet(filters(want.Config), filters(got)) {
		ds.add(res, "filter_subjects", filters(want.Config), filters(got))
	}
	if want.Config.AckPolicy != got.AckPolicy {
		ds.add(res, "ack_policy", want.Config.AckPolicy, got.AckPolicy)
	}
	if len(want.Config.BackOff) > 0 && !slices.Equal[[]time.Duration](want.Config.BackOff, got.BackOff) {
		ds.add(res, "backoff", want.Config.BackOff, got.BackOff)
	}
	compare(ds, res, "ack_wait", want.Config.AckWait, got.AckWait)
	compare(ds, res, "max_deliver", want.Config.MaxDeliver, got.MaxDeliver)
	compare(ds, res, "max_ack_pending", want.Config.MaxAckPending, got.MaxAckPending)
	compare(ds, res, "replicas", want.Config.Replicas, got.Replicas)
	return nil
}

func filters(c jetstream.ConsumerConfig) []string {
	if c.FilterSubject != "" {
		return []string{c.FilterSubject}
	}
	return c.FilterSubjects
}

func verifyBucket(ctx context.Context, js jetstream.JetStream, want jetstream.KeyValueConfig, ds *drifts) error {
	res := "bucket " + want.Bucket
	kv, err := js.KeyValue(ctx, want.Bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		ds.add(res, "exists", true, false)
		return nil
	}
	if err != nil {
		return fmt.Errorf("nuts: verify %s: %w", res, err)
	}
	st, err := kv.Status(ctx)
	if err != nil {
		return fmt.Errorf("nuts: verify %s: %w", res, err)
	}
	if want.Storage != st.Config().Storage {
		ds.add(res, "storage", want.Storage, st.Config().Storage)
	}
	compare(ds, res, "history", want.History, uint8(st.History()))
	compare(ds, res, "ttl", want.TTL, st.TTL())
	return nil
}

func sameSet(a, b []string) bool {
	a, b = slices.Sorted(slices.Values(a)), slices.Sorted(slices.Values(b))
	return slices.Equal(a, b)
}
