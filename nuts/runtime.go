// Package nuts is the small runtime that generated gonuts code builds on.
package nuts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Kind says what sort of operation a Call is.
type Kind string

// The kinds of Call a plugin can see.
const (
	KindPublish  Kind = "publish"
	KindConsume  Kind = "consume"
	KindRequest  Kind = "request"
	KindServe    Kind = "serve"
	KindKV       Kind = "kv"
	KindSchedule Kind = "schedule"
)

// Call describes one operation passing through the plugin chain.
type Call struct {
	Kind    Kind
	Name    string // the name from the YAML, such as "order.placed"
	Subject string
	Service string
	Headers nats.Header // trace context travels here
}

// Handler runs a Call.
type Handler func(ctx context.Context, c *Call) error

// Plugin wraps every generated operation, for tracing, metrics and the like.
type Plugin interface {
	Wrap(next Handler) Handler
}

// VerifyMode says what Check does when the server differs from the spec.
type VerifyMode int

// The ways to treat differences between the server and the spec.
const (
	VerifyOff VerifyMode = iota
	VerifyWarn
	VerifyFail
)

// Runtime holds the connection and plugin chain shared by generated code.
type Runtime struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	owned   bool
	plugins []Plugin
	service string
	verify  VerifyMode
	natsOps []nats.Option
}

// Option configures a Runtime.
type Option func(*Runtime)

// WithPlugins adds plugins. The first one is the outermost.
func WithPlugins(p ...Plugin) Option {
	return func(r *Runtime) { r.plugins = append(r.plugins, p...) }
}

// WithService names the application in each Call.
func WithService(name string) Option {
	return func(r *Runtime) { r.service = name }
}

// WithNATSOptions passes options to nats.Connect, such as credentials.
func WithNATSOptions(o ...nats.Option) Option {
	return func(r *Runtime) { r.natsOps = append(r.natsOps, o...) }
}

// WithVerify sets how Check treats differences from the spec.
func WithVerify(m VerifyMode) Option {
	return func(r *Runtime) { r.verify = m }
}

// Dial connects to NATS. Close closes the connection.
func Dial(url string, opts ...Option) (*Runtime, error) {
	r := &Runtime{owned: true}
	for _, o := range opts {
		o(r)
	}
	nc, err := nats.Connect(url, r.natsOps...)
	if err != nil {
		return nil, fmt.Errorf("nuts: connect: %w", err)
	}
	if err := r.bind(nc); err != nil {
		nc.Close()
		return nil, err
	}
	return r, nil
}

// New wraps an existing connection. Close leaves that connection open.
func New(nc *nats.Conn, opts ...Option) (*Runtime, error) {
	r := &Runtime{}
	for _, o := range opts {
		o(r)
	}
	if err := r.bind(nc); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Runtime) bind(nc *nats.Conn) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("nuts: jetstream: %w", err)
	}
	r.nc, r.js = nc, js
	return nil
}

// Conn returns the plain NATS connection, for anything the spec does not cover.
func (r *Runtime) Conn() *nats.Conn { return r.nc }

// JetStream returns the plain JetStream client.
func (r *Runtime) JetStream() jetstream.JetStream { return r.js }

// Close drains and closes a connection opened by Dial.
func (r *Runtime) Close() {
	if r.owned {
		_ = r.nc.Drain()
		r.nc.Close()
	}
}

// Check compares the server with the spec according to the verify mode. It only reads.
func (r *Runtime) Check(ctx context.Context, in Infra) error {
	if r.verify == VerifyOff {
		return nil
	}
	drift, err := Verify(ctx, r.js, in)
	if err != nil {
		return err
	}
	for _, d := range drift {
		slog.WarnContext(ctx, "gonuts: server differs from spec", "resource", d.Resource, "field", d.Field, "want", d.Want, "got", d.Got)
	}
	if len(drift) > 0 && r.verify == VerifyFail {
		return fmt.Errorf("nuts: server differs from spec in %d places, first: %s", len(drift), drift[0])
	}
	return nil
}

func (r *Runtime) do(ctx context.Context, c *Call, fn func(context.Context) error) error {
	c.Service = r.service
	if c.Headers == nil {
		c.Headers = nats.Header{}
	}
	h := Handler(func(ctx context.Context, _ *Call) error { return fn(ctx) })
	for i := len(r.plugins) - 1; i >= 0; i-- {
		h = r.plugins[i].Wrap(h)
	}
	return h(ctx, c)
}

func encode(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("nuts: encode: %w", err)
	}
	return data, nil
}

// PublishCore sends an event over core NATS, with no delivery guarantee.
func PublishCore[T any](ctx context.Context, r *Runtime, name, subject string, v T) error {
	data, err := encode(v)
	if err != nil {
		return err
	}
	c := &Call{Kind: KindPublish, Name: name, Subject: subject}
	return r.do(ctx, c, func(context.Context) error {
		return r.nc.PublishMsg(&nats.Msg{Subject: subject, Data: data, Header: c.Headers})
	})
}

// PublishStream sends an event through JetStream and waits for it to be stored.
func PublishStream[T any](ctx context.Context, r *Runtime, name, subject string, v T, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	data, err := encode(v)
	if err != nil {
		return nil, err
	}
	var ack *jetstream.PubAck
	c := &Call{Kind: KindPublish, Name: name, Subject: subject}
	err = r.do(ctx, c, func(ctx context.Context) error {
		var err error
		ack, err = r.js.PublishMsg(ctx, &nats.Msg{Subject: subject, Data: data, Header: c.Headers}, opts...)
		return err
	})
	return ack, err
}

// FormatSubject fills a subject's %s placeholders. It rejects values that would
// change the subject's shape, such as an empty value, a dot or a wildcard.
func FormatSubject(format string, params ...string) (string, error) {
	args := make([]any, len(params))
	for i, p := range params {
		if p == "" || strings.ContainsAny(p, ". \t\r\n*>") {
			return "", fmt.Errorf("nuts: invalid subject token %q", p)
		}
		args[i] = p
	}
	return fmt.Sprintf(format, args...), nil
}

// Msg is a decoded message handed to a handler.
type Msg[T, P any] struct {
	Data    T
	Params  P
	Subject string
	Headers nats.Header
	Raw     jetstream.Msg // nil for core NATS events
}

type terminalError struct{ err error }

func (e *terminalError) Error() string { return e.err.Error() }
func (e *terminalError) Unwrap() error { return e.err }

// Terminate marks a handler error as final, so the message is never redelivered.
func Terminate(err error) error {
	if err == nil {
		return nil
	}
	return &terminalError{err}
}

// Route connects one event's subject to its handler.
type Route struct {
	event   string
	pattern string
	handle  func(ctx context.Context, subject string, headers nats.Header, data []byte, raw jetstream.Msg) error
}

// NewRoute builds a Route. params turns subject tokens into typed parameters.
func NewRoute[T, P any](event, pattern string, params func(tokens []string) P, h func(context.Context, Msg[T, P]) error) Route {
	return Route{
		event:   event,
		pattern: pattern,
		handle: func(ctx context.Context, subject string, headers nats.Header, data []byte, raw jetstream.Msg) error {
			m := Msg[T, P]{
				Params:  params(strings.Split(subject, ".")),
				Subject: subject,
				Headers: headers,
				Raw:     raw,
			}
			if err := json.Unmarshal(data, &m.Data); err != nil {
				return Terminate(fmt.Errorf("nuts: decode %s: %w", event, err))
			}
			return h(ctx, m)
		},
	}
}

// SubjectMatches reports whether a concrete subject matches a NATS pattern.
func SubjectMatches(pattern, subject string) bool {
	st := strings.Split(subject, ".")
	for i, tok := range strings.Split(pattern, ".") {
		if tok == ">" {
			return i < len(st)
		}
		if i >= len(st) || tok != "*" && tok != st[i] {
			return false
		}
	}
	return len(strings.Split(pattern, ".")) == len(st)
}

func recovered(fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("nuts: handler panicked: %v", p)
		}
	}()
	return fn()
}

// ConsumeConfig names the existing consumer to read from.
type ConsumeConfig struct {
	Stream   string
	Consumer string
	BackOff  []time.Duration
}

// Consume runs handlers on an existing durable consumer. It never creates one.
// The returned function drains the consumer and waits for it to stop.
func Consume(ctx context.Context, r *Runtime, cfg ConsumeConfig, routes []Route) (stop func(), err error) {
	consumer, err := r.js.Consumer(ctx, cfg.Stream, cfg.Consumer)
	if err != nil {
		return nil, fmt.Errorf("nuts: bind consumer %s/%s: %w", cfg.Stream, cfg.Consumer, err)
	}
	cc, err := consumer.Consume(func(m jetstream.Msg) {
		settle(m, cfg.BackOff, dispatch(ctx, r, m, routes))
	})
	if err != nil {
		return nil, fmt.Errorf("nuts: consume %s/%s: %w", cfg.Stream, cfg.Consumer, err)
	}
	return func() {
		cc.Drain()
		<-cc.Closed()
	}, nil
}

func dispatch(ctx context.Context, r *Runtime, m jetstream.Msg, routes []Route) error {
	for _, route := range routes {
		if !SubjectMatches(route.pattern, m.Subject()) {
			continue
		}
		c := &Call{Kind: KindConsume, Name: route.event, Subject: m.Subject(), Headers: m.Headers()}
		return r.do(ctx, c, func(ctx context.Context) error {
			return recovered(func() error {
				return route.handle(ctx, m.Subject(), m.Headers(), m.Data(), m)
			})
		})
	}
	return Terminate(fmt.Errorf("nuts: no handler for subject %s", m.Subject()))
}

func settle(m jetstream.Msg, backoff []time.Duration, err error) {
	var terminal *terminalError
	switch {
	case err == nil:
		_ = m.Ack()
	case errors.As(err, &terminal):
		_ = m.Term()
	default:
		if delay := retryDelay(m, backoff); delay > 0 {
			_ = m.NakWithDelay(delay)
			return
		}
		_ = m.Nak()
	}
}

func retryDelay(m jetstream.Msg, backoff []time.Duration) time.Duration {
	if len(backoff) == 0 {
		return 0
	}
	meta, err := m.Metadata()
	if err != nil {
		return backoff[0]
	}
	return backoff[min(int(meta.NumDelivered)-1, len(backoff)-1)]
}

// SubOption configures a core subscription.
type SubOption func(*subConfig)

type subConfig struct{ queue string }

// Queue makes the subscription part of a queue group.
func Queue(name string) SubOption {
	return func(c *subConfig) { c.queue = name }
}

// Subscribe runs a handler for a core NATS event. Errors reach plugins only,
// because core NATS has no redelivery.
func Subscribe(ctx context.Context, r *Runtime, route Route, opts ...SubOption) (stop func(), err error) {
	var cfg subConfig
	for _, o := range opts {
		o(&cfg)
	}
	sub, err := r.nc.QueueSubscribe(route.pattern, cfg.queue, func(m *nats.Msg) {
		c := &Call{Kind: KindConsume, Name: route.event, Subject: m.Subject, Headers: m.Header}
		_ = r.do(ctx, c, func(ctx context.Context) error {
			return recovered(func() error {
				return route.handle(ctx, m.Subject, m.Header, m.Data, nil)
			})
		})
	})
	if err != nil {
		return nil, fmt.Errorf("nuts: subscribe %s: %w", route.pattern, err)
	}
	return func() { _ = sub.Drain() }, nil
}
