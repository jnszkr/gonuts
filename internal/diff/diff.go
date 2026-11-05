// Package diff reports what a change to the spec means for services that are
// still running the old version, as they do during a blue-green deployment.
package diff

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/jnszkr/gonuts/internal/spec"
)

// Severity says how risky a change is.
type Severity string

// The severities, from least to most risky.
const (
	Safe        Severity = "safe"
	NeedsBoth   Severity = "needs-both-versions"
	Destructive Severity = "destructive"
)

// Change is one difference between two specs.
type Change struct {
	Severity Severity `json:"severity"`
	Path     string   `json:"path"`
	Message  string   `json:"message"`
}

func (c Change) String() string {
	return fmt.Sprintf("%-20s %s: %s", c.Severity, c.Path, c.Message)
}

var severityOrder = map[Severity]int{Destructive: 0, NeedsBoth: 1, Safe: 2}

type comparer struct {
	changes []Change
}

func (c *comparer) add(sev Severity, path, format string, args ...any) {
	c.changes = append(c.changes, Change{sev, path, fmt.Sprintf(format, args...)})
}

// Compare lists the differences between two specs, most severe first.
func Compare(oldSpec, newSpec *spec.Spec) []Change {
	c := &comparer{}
	c.models(oldSpec, newSpec)
	c.events(oldSpec, newSpec)
	c.streams(oldSpec, newSpec)
	c.consumers(oldSpec, newSpec)
	c.buckets(oldSpec, newSpec)
	c.schedules(oldSpec, newSpec)
	c.requests(oldSpec, newSpec)
	slices.SortStableFunc(c.changes, func(a, b Change) int {
		if d := severityOrder[a.Severity] - severityOrder[b.Severity]; d != 0 {
			return d
		}
		return strings.Compare(a.Path, b.Path)
	})
	return c.changes
}

// HasDestructive reports whether any change is destructive.
func HasDestructive(changes []Change) bool {
	return slices.ContainsFunc(changes, func(c Change) bool { return c.Severity == Destructive })
}

// membership reports added and removed names, and calls both for the rest.
func membership[V any](c *comparer, prefix string, before, after map[string]V, removed Severity, removedMsg string, both func(path string, a, b V)) {
	for _, name := range spec.SortedKeys(before) {
		path := prefix + "." + name
		next, ok := after[name]
		if !ok {
			c.add(removed, path, "removed; %s", removedMsg)
			continue
		}
		both(path, before[name], next)
	}
	for _, name := range spec.SortedKeys(after) {
		if _, ok := before[name]; !ok {
			c.add(Safe, prefix+"."+name, "added")
		}
	}
}

func (c *comparer) models(before, after *spec.Spec) {
	membership(c, "models", before.Models, after.Models, NeedsBoth, "old services may still use it", func(path string, a, b *spec.Model) {
		for _, f := range a.Fields {
			n := field(b, f.Name)
			if n == nil {
				c.add(NeedsBoth, path+"."+f.Name, "field removed; old services may still read it")
				continue
			}
			switch {
			case n.Type != f.Type:
				c.add(Destructive, path+"."+f.Name, "type changed from %s to %s; old and new services cannot read each other", f.Type, n.Type)
			case f.Optional && !n.Optional:
				c.add(NeedsBoth, path+"."+f.Name, "now required; old publishers may omit it")
			case !f.Optional && n.Optional:
				c.add(Safe, path+"."+f.Name, "now optional; check old readers handle it missing")
			}
		}
		for _, f := range b.Fields {
			if field(a, f.Name) != nil {
				continue
			}
			if f.Optional {
				c.add(Safe, path+"."+f.Name, "optional field added")
			} else {
				c.add(NeedsBoth, path+"."+f.Name, "required field added; old publishers will not send it")
			}
		}
	})
}

func field(m *spec.Model, name string) *spec.Field {
	for _, f := range m.Fields {
		if f.Name == name {
			return f
		}
	}
	return nil
}

func (c *comparer) events(before, after *spec.Spec) {
	membership(c, "events", before.Events, after.Events, NeedsBoth, "old services may still publish it", func(path string, a, b spec.Event) {
		if a.Subject != b.Subject {
			c.add(Destructive, path+".subject", "subject changed from %s to %s; old and new services stop reaching each other", a.Subject, b.Subject)
		}
		if a.Model != b.Model {
			c.add(Destructive, path+".model", "model changed from %s to %s; stored messages keep the old shape", a.Model, b.Model)
		}
	})
}

func (c *comparer) streams(before, after *spec.Spec) {
	membership(c, "streams", before.Streams, after.Streams, Destructive, "its messages are deleted", func(path string, a, b spec.Stream) {
		name := strings.TrimPrefix(path, "streams.")
		if a.Storage != b.Storage {
			c.add(Destructive, path+".storage", "storage cannot change in place, so the stream would be recreated")
		}
		if a.Retention != b.Retention {
			c.add(Destructive, path+".retention", "retention change affects existing consumers and may be refused")
		}
		for _, subject := range before.StreamSubjects(name) {
			if !slices.Contains(after.StreamSubjects(name), subject) {
				c.add(NeedsBoth, path+".subjects", "%s removed; old publishers would fail", subject)
			}
		}
		if !a.AllowMsgSchedules && b.AllowMsgSchedules {
			c.add(NeedsBoth, path+".allow_msg_schedules", "enabled, and it cannot be turned off again")
		}
		if a.AllowMsgSchedules && !b.AllowMsgSchedules {
			c.add(Destructive, path+".allow_msg_schedules", "cannot be turned off once enabled")
		}
		a.Storage, a.Retention, a.Events, a.Subjects, a.AllowMsgSchedules = b.Storage, b.Retention, b.Events, b.Subjects, b.AllowMsgSchedules
		if !reflect.DeepEqual(a, b) {
			c.add(Safe, path, "settings changed")
		}
	})
}

func (c *comparer) consumers(before, after *spec.Spec) {
	const lost = "the consumer would be recreated and lose its position"
	membership(c, "consumers", before.Consumers, after.Consumers, Destructive, "its position is lost", func(path string, a, b spec.Consumer) {
		if a.Stream != b.Stream {
			c.add(Destructive, path+".stream", "%s", lost)
		}
		if a.AckPolicy != b.AckPolicy {
			c.add(Destructive, path+".ack_policy", "%s", lost)
		}
		if a.DeliverPolicy != b.DeliverPolicy {
			c.add(Destructive, path+".deliver_policy", "%s", lost)
		}
		if !slices.Equal(a.Events, b.Events) {
			c.add(NeedsBoth, path+".events", "the handler interface changes; old services would get events they cannot handle")
		}
		a.Stream, a.AckPolicy, a.DeliverPolicy, a.Events = b.Stream, b.AckPolicy, b.DeliverPolicy, b.Events
		if !reflect.DeepEqual(a, b) {
			c.add(Safe, path, "settings changed")
		}
	})
}

func (c *comparer) buckets(before, after *spec.Spec) {
	membership(c, "kv", before.KV, after.KV, Destructive, "its values are deleted", func(path string, a, b spec.KVBucket) {
		if a.Model != b.Model {
			c.add(Destructive, path+".model", "stored values keep the old shape")
		}
		if a.Key != b.Key {
			c.add(Destructive, path+".key", "stored keys keep the old form")
		}
		if a.Storage != b.Storage {
			c.add(Destructive, path+".storage", "storage cannot change in place, so the bucket would be recreated")
		}
		a.Model, a.Key, a.Storage = b.Model, b.Key, b.Storage
		if !reflect.DeepEqual(a, b) {
			c.add(Safe, path, "settings changed")
		}
	})
}

func (c *comparer) schedules(before, after *spec.Spec) {
	membership(c, "schedules", before.Schedules, after.Schedules, NeedsBoth, "the stored schedule keeps firing until you purge its subject", func(path string, a, b spec.Schedule) {
		if !reflect.DeepEqual(a, b) {
			c.add(Safe, path, "changed; run gonuts schedules sync to apply it")
		}
	})
}

func (c *comparer) requests(before, after *spec.Spec) {
	membership(c, "requests", before.Requests, after.Requests, NeedsBoth, "old clients may still call it", func(path string, a, b spec.RequestGroup) {
		membership(c, path, a.Endpoints, b.Endpoints, NeedsBoth, "old clients may still call it", func(path string, x, y spec.Endpoint) {
			if x.Subject != y.Subject {
				c.add(Destructive, path+".subject", "subject changed; old clients get no responder")
			}
			if x.Request != y.Request {
				c.add(Destructive, path+".request", "request model changed; old clients send the old shape")
			}
			if x.Reply != y.Reply {
				c.add(Destructive, path+".reply", "reply model changed; old clients expect the old shape")
			}
		})
	})
}
