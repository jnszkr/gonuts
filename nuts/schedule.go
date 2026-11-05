package nuts

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// ScheduleAt delivers one message to target at the given time. Each call uses
// its own schedule subject, so delayed messages never replace each other.
func ScheduleAt[T any](ctx context.Context, r *Runtime, name, subject, target string, at time.Time, v T) error {
	data, err := encode(v)
	if err != nil {
		return err
	}
	full := subject + "." + nuid.Next()
	return publishSchedule(ctx, r, name, full, data, nats.Header{}, jetstream.WithScheduleAt(at), jetstream.WithScheduleTarget(target))
}

// ScheduleSpec is a recurring schedule from the YAML.
type ScheduleSpec struct {
	Name     string
	Subject  string
	Target   string
	Cron     string
	Every    time.Duration
	TimeZone string
	Payload  []byte
}

// SyncSchedule stores a recurring schedule. A recurring schedule is a stored
// message rather than stream configuration, so Terraform cannot create it.
// Running this again replaces the schedule instead of adding another.
func SyncSchedule(ctx context.Context, r *Runtime, s ScheduleSpec) error {
	opts := []jetstream.PublishOpt{jetstream.WithScheduleTarget(s.Target)}
	if s.Every > 0 {
		opts = append(opts, jetstream.WithScheduleEvery(s.Every))
	} else {
		opts = append(opts, jetstream.WithScheduleCron(s.Cron))
	}
	if s.TimeZone != "" {
		opts = append(opts, jetstream.WithScheduleTimeZone(s.TimeZone))
	}
	return publishSchedule(ctx, r, s.Name, s.Subject, s.Payload, nats.Header{jetstream.MsgRollup: []string{jetstream.MsgRollupSubject}}, opts...)
}

func publishSchedule(ctx context.Context, r *Runtime, name, subject string, data []byte, headers nats.Header, opts ...jetstream.PublishOpt) error {
	c := &Call{Kind: KindSchedule, Name: name, Subject: subject, Headers: headers}
	return r.do(ctx, c, func(ctx context.Context) error {
		if _, err := r.js.PublishMsg(ctx, &nats.Msg{Subject: subject, Data: data, Header: c.Headers}, opts...); err != nil {
			return fmt.Errorf("nuts: schedule %s: %w", name, err)
		}
		return nil
	})
}
