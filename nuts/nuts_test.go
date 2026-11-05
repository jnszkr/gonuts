package nuts_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jnszkr/gonuts/internal/natstest"
	"github.com/jnszkr/gonuts/nuts"
	"github.com/jnszkr/gonuts/nuts/devnats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type payload struct {
	Value string `json:"value"`
}

type noParams struct{}

func params([]string) noParams { return noParams{} }

type regionParams struct{ Region string }

func runtimeFor(t *testing.T, opts ...nuts.Option) *nuts.Runtime {
	t.Helper()
	rt, err := nuts.Dial(natstest.Start(t), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	return rt
}

func ctxFor(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func provision(t *testing.T, rt *nuts.Runtime, consumer jetstream.ConsumerConfig) {
	t.Helper()
	err := devnats.Provision(ctxFor(t), rt.JetStream(), nuts.Infra{
		Streams:   []jetstream.StreamConfig{{Name: "S", Subjects: []string{"s.>"}, Storage: jetstream.MemoryStorage}},
		Consumers: []nuts.ConsumerInfra{{Stream: "S", Config: consumer}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type recorder struct {
	name string
	mu   sync.Mutex
	logs *[]string
	last []nuts.Call
	err  []error
}

func (p *recorder) Wrap(next nuts.Handler) nuts.Handler {
	return func(ctx context.Context, c *nuts.Call) error {
		p.mu.Lock()
		*p.logs = append(*p.logs, p.name+":before")
		p.last = append(p.last, *c)
		p.mu.Unlock()
		err := next(ctx, c)
		p.mu.Lock()
		*p.logs = append(*p.logs, p.name+":after")
		p.err = append(p.err, err)
		p.mu.Unlock()
		return err
	}
}

func (p *recorder) lastErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.err) == 0 {
		return nil
	}
	return p.err[len(p.err)-1]
}

func newRecorder(name string) *recorder { return &recorder{name: name, logs: &[]string{}} }

func TestPluginsRunInOrderWithCallDetails(t *testing.T) {
	logs := []string{}
	a, b := &recorder{name: "a", logs: &logs}, &recorder{name: "b", logs: &logs}
	rt := runtimeFor(t, nuts.WithPlugins(a, b), nuts.WithService("billing"))
	if err := nuts.PublishCore(ctxFor(t), rt, "price.changed", "prices.x", payload{"v"}); err != nil {
		t.Fatal(err)
	}
	want := "a:before b:before b:after a:after"
	if got := strings.Join(logs, " "); got != want {
		t.Fatalf("order = %q, want %q", got, want)
	}
	c := a.last[0]
	if c.Kind != nuts.KindPublish || c.Name != "price.changed" || c.Subject != "prices.x" || c.Service != "billing" {
		t.Fatalf("call = %+v", c)
	}
}

func TestPluginCanStopACall(t *testing.T) {
	boom := errors.New("denied")
	rt := runtimeFor(t, nuts.WithPlugins(pluginFunc(func(nuts.Handler) nuts.Handler {
		return func(context.Context, *nuts.Call) error { return boom }
	})))
	if err := nuts.PublishCore(ctxFor(t), rt, "e", "x", payload{}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

type pluginFunc func(nuts.Handler) nuts.Handler

func (f pluginFunc) Wrap(next nuts.Handler) nuts.Handler { return f(next) }

func TestPublishStreamStoresAndCarriesPluginHeaders(t *testing.T) {
	rt := runtimeFor(t, nuts.WithPlugins(pluginFunc(func(next nuts.Handler) nuts.Handler {
		return func(ctx context.Context, c *nuts.Call) error {
			c.Headers.Set("Traceparent", "t1")
			return next(ctx, c)
		}
	})))
	ctx := ctxFor(t)
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c"})
	ack, err := nuts.PublishStream(ctx, rt, "e", "s.a", payload{"hello"})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Stream != "S" || ack.Sequence != 1 {
		t.Fatalf("ack = %+v", ack)
	}
	st, _ := rt.JetStream().Stream(ctx, "S")
	msg, err := st.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Data) != `{"value":"hello"}` || msg.Header.Get("Traceparent") != "t1" || msg.Subject != "s.a" {
		t.Fatalf("stored = %+v", msg)
	}
}

func TestPublishStreamPassesOptions(t *testing.T) {
	rt := runtimeFor(t)
	ctx := ctxFor(t)
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c"})
	for range 2 {
		if _, err := nuts.PublishStream(ctx, rt, "e", "s.a", payload{}, jetstream.WithMsgID("same")); err != nil {
			t.Fatal(err)
		}
	}
	st, _ := rt.JetStream().Stream(ctx, "S")
	info, _ := st.Info(ctx)
	if info.State.Msgs != 1 {
		t.Fatalf("msgs = %d, want deduplication to 1", info.State.Msgs)
	}
}

func startConsumer(t *testing.T, rt *nuts.Runtime, backoff []time.Duration, h func(context.Context, nuts.Msg[payload, regionParams]) error) func() {
	t.Helper()
	stop, err := nuts.Consume(ctxFor(t), rt, nuts.ConsumeConfig{Stream: "S", Consumer: "c", BackOff: backoff}, []nuts.Route{
		nuts.NewRoute("e", "s.*", func(tokens []string) regionParams { return regionParams{Region: tokens[1]} }, h),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	return stop
}

func publish(t *testing.T, rt *nuts.Runtime, subject string, data string) {
	t.Helper()
	if _, err := rt.JetStream().Publish(ctxFor(t), subject, []byte(data)); err != nil {
		t.Fatal(err)
	}
}

func consumerState(t *testing.T, rt *nuts.Runtime) *jetstream.ConsumerInfo {
	t.Helper()
	ctx := ctxFor(t)
	c, err := rt.JetStream().Consumer(ctx, "S", "c")
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestConsumeAcksOnNilAndDecodesParams(t *testing.T) {
	rec := newRecorder("r")
	rt := runtimeFor(t, nuts.WithPlugins(rec))
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c", AckWait: 30 * time.Second})
	got := make(chan nuts.Msg[payload, regionParams], 1)
	startConsumer(t, rt, nil, func(_ context.Context, m nuts.Msg[payload, regionParams]) error {
		got <- m
		return nil
	})
	publish(t, rt, "s.eu", `{"value":"v"}`)
	m := <-got
	if m.Data.Value != "v" || m.Params.Region != "eu" || m.Subject != "s.eu" || m.Raw == nil {
		t.Fatalf("msg = %+v", m)
	}
	waitFor(t, "ack", func() bool { return consumerState(t, rt).AckFloor.Consumer == 1 })
	if c := rec.last[0]; c.Kind != nuts.KindConsume || c.Name != "e" || c.Subject != "s.eu" {
		t.Fatalf("call = %+v", c)
	}
}

func TestConsumeNaksOnErrorAndDelaysByBackoff(t *testing.T) {
	rt := runtimeFor(t)
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c", AckWait: time.Minute})
	var mu sync.Mutex
	var times []time.Time
	startConsumer(t, rt, []time.Duration{300 * time.Millisecond, 10 * time.Millisecond}, func(context.Context, nuts.Msg[payload, regionParams]) error {
		mu.Lock()
		defer mu.Unlock()
		times = append(times, time.Now())
		if len(times) < 3 {
			return errors.New("retry")
		}
		return nil
	})
	publish(t, rt, "s.eu", `{}`)
	waitFor(t, "three deliveries", func() bool { mu.Lock(); defer mu.Unlock(); return len(times) == 3 })
	if d := times[1].Sub(times[0]); d < 250*time.Millisecond {
		t.Errorf("first retry after %v, want the first backoff of 300ms", d)
	}
	if d := times[2].Sub(times[1]); d > 250*time.Millisecond {
		t.Errorf("second retry after %v, want the second backoff of 10ms", d)
	}
}

func TestConsumeBackoffStaysOnLastEntry(t *testing.T) {
	rt := runtimeFor(t)
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c", AckWait: time.Minute})
	var n atomic.Int32
	startConsumer(t, rt, []time.Duration{time.Millisecond}, func(context.Context, nuts.Msg[payload, regionParams]) error {
		if n.Add(1) < 3 {
			return errors.New("retry")
		}
		return nil
	})
	publish(t, rt, "s.eu", `{}`)
	waitFor(t, "third delivery", func() bool { return n.Load() == 3 })
}

func TestConsumeWithoutBackoffNaksImmediately(t *testing.T) {
	rt := runtimeFor(t)
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c", AckWait: time.Minute})
	var n atomic.Int32
	startConsumer(t, rt, nil, func(context.Context, nuts.Msg[payload, regionParams]) error {
		if n.Add(1) == 1 {
			return errors.New("retry")
		}
		return nil
	})
	publish(t, rt, "s.eu", `{}`)
	waitFor(t, "redelivery", func() bool { return n.Load() == 2 })
}

func assertNoRedelivery(t *testing.T, rt *nuts.Runtime, n *atomic.Int32) {
	t.Helper()
	waitFor(t, "term", func() bool { return consumerState(t, rt).NumAckPending == 0 && n.Load() >= 0 })
	time.Sleep(100 * time.Millisecond)
}

func TestConsumeTerminateNeverRedelivers(t *testing.T) {
	rec := newRecorder("r")
	rt := runtimeFor(t, nuts.WithPlugins(rec))
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c", AckWait: 100 * time.Millisecond})
	var n atomic.Int32
	cause := errors.New("poison")
	startConsumer(t, rt, nil, func(context.Context, nuts.Msg[payload, regionParams]) error {
		n.Add(1)
		return nuts.Terminate(cause)
	})
	publish(t, rt, "s.eu", `{}`)
	waitFor(t, "delivery", func() bool { return n.Load() == 1 })
	assertNoRedelivery(t, rt, &n)
	time.Sleep(300 * time.Millisecond)
	if n.Load() != 1 {
		t.Fatalf("delivered %d times", n.Load())
	}
	if err := rec.lastErr(); !errors.Is(err, cause) {
		t.Fatalf("plugin saw %v", err)
	}
}

func TestConsumeUndecodablePayloadIsTerminatedAndReported(t *testing.T) {
	rec := newRecorder("r")
	rt := runtimeFor(t, nuts.WithPlugins(rec))
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c", AckWait: 100 * time.Millisecond})
	var n atomic.Int32
	startConsumer(t, rt, nil, func(context.Context, nuts.Msg[payload, regionParams]) error {
		n.Add(1)
		return nil
	})
	publish(t, rt, "s.eu", `not json`)
	waitFor(t, "plugin report", func() bool { return rec.lastErr() != nil })
	time.Sleep(300 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatal("handler ran on an undecodable payload")
	}
	if c := consumerState(t, rt); c.NumAckPending != 0 || c.NumRedelivered != 0 {
		t.Fatalf("message was not terminated: %+v", c)
	}
}

func TestConsumeRecoversPanicsAndRetries(t *testing.T) {
	rec := newRecorder("r")
	rt := runtimeFor(t, nuts.WithPlugins(rec))
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c", AckWait: time.Minute})
	var n atomic.Int32
	startConsumer(t, rt, nil, func(context.Context, nuts.Msg[payload, regionParams]) error {
		if n.Add(1) == 1 {
			panic("kaboom")
		}
		return nil
	})
	publish(t, rt, "s.eu", `{}`)
	waitFor(t, "retry after panic", func() bool { return n.Load() == 2 })
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.err) == 0 || !strings.Contains(rec.err[0].Error(), "kaboom") {
		t.Fatalf("plugin errors = %v", rec.err)
	}
}

func TestConsumeRoutesByEvent(t *testing.T) {
	rt := runtimeFor(t)
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c", AckWait: time.Minute})
	a, b := make(chan string, 1), make(chan string, 1)
	stop, err := nuts.Consume(ctxFor(t), rt, nuts.ConsumeConfig{Stream: "S", Consumer: "c"}, []nuts.Route{
		nuts.NewRoute("a", "s.a.*", params, func(_ context.Context, m nuts.Msg[payload, noParams]) error { a <- m.Subject; return nil }),
		nuts.NewRoute("b", "s.b.*", params, func(_ context.Context, m nuts.Msg[payload, noParams]) error { b <- m.Subject; return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	publish(t, rt, "s.b.1", `{}`)
	publish(t, rt, "s.a.1", `{}`)
	if got := <-a; got != "s.a.1" {
		t.Errorf("a got %q", got)
	}
	if got := <-b; got != "s.b.1" {
		t.Errorf("b got %q", got)
	}
}

func TestConsumeTerminatesSubjectsWithNoRoute(t *testing.T) {
	rt := runtimeFor(t)
	provision(t, rt, jetstream.ConsumerConfig{Durable: "c", AckWait: 100 * time.Millisecond})
	startConsumer(t, rt, nil, func(context.Context, nuts.Msg[payload, regionParams]) error { return nil })
	publish(t, rt, "s.other.thing", `{}`)
	waitFor(t, "termination", func() bool {
		c := consumerState(t, rt)
		return c.Delivered.Consumer == 1 && c.NumAckPending == 0
	})
	if c := consumerState(t, rt); c.NumRedelivered != 0 {
		t.Fatalf("redelivered: %+v", c)
	}
}

func TestConsumeNeverCreatesAMissingConsumer(t *testing.T) {
	rt := runtimeFor(t)
	ctx := ctxFor(t)
	provision(t, rt, jetstream.ConsumerConfig{Durable: "other"})
	_, err := nuts.Consume(ctx, rt, nuts.ConsumeConfig{Stream: "S", Consumer: "c"}, nil)
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("err = %v", err)
	}
	if _, err := rt.JetStream().Consumer(ctx, "S", "c"); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("consumer was created: %v", err)
	}
}

func TestSubscribeDeliversCoreEvents(t *testing.T) {
	rec := newRecorder("r")
	rt := runtimeFor(t, nuts.WithPlugins(rec))
	got := make(chan nuts.Msg[payload, regionParams], 1)
	stop, err := nuts.Subscribe(ctxFor(t), rt, nuts.NewRoute("p", "p.*", func(tokens []string) regionParams { return regionParams{tokens[1]} },
		func(_ context.Context, m nuts.Msg[payload, regionParams]) error { got <- m; return nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if err := rt.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	if err := nuts.PublishCore(ctxFor(t), rt, "p", "p.eu", payload{"v"}); err != nil {
		t.Fatal(err)
	}
	m := <-got
	if m.Data.Value != "v" || m.Params.Region != "eu" || m.Raw != nil {
		t.Fatalf("msg = %+v", m)
	}
	if c := rec.last[len(rec.last)-1]; c.Kind != nuts.KindConsume && c.Kind != nuts.KindPublish {
		t.Fatalf("call kind = %v", c.Kind)
	}
}

func TestSubscribeQueueGroupDeliversOnce(t *testing.T) {
	rt := runtimeFor(t)
	var n atomic.Int32
	for range 2 {
		stop, err := nuts.Subscribe(ctxFor(t), rt, nuts.NewRoute("p", "p.*", params,
			func(context.Context, nuts.Msg[payload, noParams]) error { n.Add(1); return nil }), nuts.Queue("q"))
		if err != nil {
			t.Fatal(err)
		}
		defer stop()
	}
	_ = rt.Conn().Flush()
	_ = nuts.PublishCore(ctxFor(t), rt, "p", "p.x", payload{})
	waitFor(t, "delivery", func() bool { return n.Load() >= 1 })
	time.Sleep(50 * time.Millisecond)
	if n.Load() != 1 {
		t.Fatalf("delivered %d times", n.Load())
	}
}

func TestSubscribeSurvivesPanicAndBadPayload(t *testing.T) {
	rec := newRecorder("r")
	rt := runtimeFor(t, nuts.WithPlugins(rec))
	var n atomic.Int32
	stop, err := nuts.Subscribe(ctxFor(t), rt, nuts.NewRoute("p", "p.*", params,
		func(context.Context, nuts.Msg[payload, noParams]) error {
			if n.Add(1) == 1 {
				panic("kaboom")
			}
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	_ = rt.Conn().Flush()
	for _, data := range []string{`x`, `{}`, `{}`} {
		if err := rt.Conn().Publish("p.a", []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "second good message", func() bool { return n.Load() == 2 })
}

type reqT struct {
	ID string `json:"id"`
}
type respT struct {
	Name string `json:"name"`
}
type notFound struct{ Message string }

func (e *notFound) Error() string        { return "NotFound: " + e.Message }
func (e *notFound) ReplyCode() string    { return "NotFound" }
func (e *notFound) ReplyMessage() string { return e.Message }

func serve(t *testing.T, rt *nuts.Runtime, h func(context.Context, reqT) (*respT, error)) {
	t.Helper()
	srv, err := nuts.NewServer(ctxFor(t), rt, nuts.ServerConfig{Name: "orders", Version: "1.2.3", QueueGroup: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	if err := nuts.AddEndpoint(srv, "Get", "api.get", h); err != nil {
		t.Fatal(err)
	}
	if err := rt.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestRequestReply(t *testing.T) {
	rec := newRecorder("r")
	rt := runtimeFor(t, nuts.WithPlugins(rec))
	serve(t, rt, func(_ context.Context, r reqT) (*respT, error) { return &respT{Name: "n-" + r.ID}, nil })
	got, err := nuts.Request[reqT, respT](ctxFor(t), rt, "orders.Get", "api.get", time.Second, reqT{"1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "n-1" {
		t.Fatalf("reply = %+v", got)
	}
	kinds := map[nuts.Kind]string{}
	rec.mu.Lock()
	for _, c := range rec.last {
		kinds[c.Kind] = c.Name
	}
	rec.mu.Unlock()
	if kinds[nuts.KindRequest] != "orders.Get" || kinds[nuts.KindServe] != "orders.Get" {
		t.Fatalf("kinds = %v", kinds)
	}
}

func TestServerRegistersWithMicroDiscovery(t *testing.T) {
	rt := runtimeFor(t)
	serve(t, rt, func(context.Context, reqT) (*respT, error) { return &respT{}, nil })
	msg, err := rt.Conn().Request("$SRV.INFO.orders", nil, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"name":"orders"`, `"version":"1.2.3"`, `"subject":"api.get"`, `"queue_group":"workers"`} {
		if !strings.Contains(string(msg.Data), want) {
			t.Errorf("info %s lacks %s", msg.Data, want)
		}
	}
}

func TestDeclaredErrorsReachTheClientWithCode(t *testing.T) {
	rt := runtimeFor(t)
	serve(t, rt, func(context.Context, reqT) (*respT, error) { return nil, &notFound{"no such order"} })
	_, err := nuts.Request[reqT, respT](ctxFor(t), rt, "x", "api.get", time.Second, reqT{})
	var re *nuts.ReplyError
	if !errors.As(err, &re) || re.Code != "NotFound" || re.Message != "no such order" {
		t.Fatalf("err = %#v", err)
	}
}

func TestUndeclaredErrorsDoNotLeakDetails(t *testing.T) {
	rt := runtimeFor(t)
	serve(t, rt, func(context.Context, reqT) (*respT, error) { return nil, errors.New("password=hunter2") })
	_, err := nuts.Request[reqT, respT](ctxFor(t), rt, "x", "api.get", time.Second, reqT{})
	var re *nuts.ReplyError
	if !errors.As(err, &re) || re.Code != nuts.CodeInternal || strings.Contains(re.Message, "hunter2") {
		t.Fatalf("err = %v", err)
	}
}

func TestServerRecoversPanics(t *testing.T) {
	rt := runtimeFor(t)
	serve(t, rt, func(context.Context, reqT) (*respT, error) { panic("kaboom") })
	_, err := nuts.Request[reqT, respT](ctxFor(t), rt, "x", "api.get", time.Second, reqT{})
	var re *nuts.ReplyError
	if !errors.As(err, &re) || re.Code != nuts.CodeInternal {
		t.Fatalf("err = %v", err)
	}
}

func TestServerRejectsBadPayload(t *testing.T) {
	rt := runtimeFor(t)
	serve(t, rt, func(context.Context, reqT) (*respT, error) { return &respT{}, nil })
	msg, err := rt.Conn().Request("api.get", []byte("nope"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.Header.Get("Nats-Service-Error-Code"); got != nuts.CodeBadRequest {
		t.Fatalf("code = %q", got)
	}
}

func TestRequestUsesTimeoutFromSpec(t *testing.T) {
	rt := runtimeFor(t)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	serve(t, rt, func(context.Context, reqT) (*respT, error) { <-block; return &respT{}, nil })
	start := time.Now()
	_, err := nuts.Request[reqT, respT](context.Background(), rt, "x", "api.get", 50*time.Millisecond, reqT{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %v", d)
	}
}

func TestRequestShorterContextWins(t *testing.T) {
	rt := runtimeFor(t)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	serve(t, rt, func(context.Context, reqT) (*respT, error) { <-block; return &respT{}, nil })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := nuts.Request[reqT, respT](ctx, rt, "x", "api.get", time.Minute, reqT{})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatalf("err = %v after %v", err, time.Since(start))
	}
}

func TestRequestWithNoServer(t *testing.T) {
	rt := runtimeFor(t)
	_, err := nuts.Request[reqT, respT](ctxFor(t), rt, "x", "api.none", time.Second, reqT{})
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("err = %v", err)
	}
}

func bucket(t *testing.T, rt *nuts.Runtime) *nuts.Bucket[payload] {
	t.Helper()
	err := devnats.Provision(ctxFor(t), rt.JetStream(), nuts.Infra{
		Buckets: []jetstream.KeyValueConfig{{Bucket: "b", History: 3, Storage: jetstream.MemoryStorage}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return nuts.NewBucket[payload](rt, "b")
}

func TestBucketPutGetUpdateDelete(t *testing.T) {
	rec := newRecorder("r")
	rt := runtimeFor(t, nuts.WithPlugins(rec))
	ctx := ctxFor(t)
	b := bucket(t, rt)
	rev, err := b.Put(ctx, "k", payload{"1"})
	if err != nil {
		t.Fatal(err)
	}
	v, gotRev, err := b.Get(ctx, "k")
	if err != nil || v.Value != "1" || gotRev != rev {
		t.Fatalf("get = %+v %d %v", v, gotRev, err)
	}
	rev2, err := b.Update(ctx, "k", payload{"2"}, rev)
	if err != nil || rev2 <= rev {
		t.Fatalf("update = %d %v", rev2, err)
	}
	if _, err := b.Update(ctx, "k", payload{"3"}, rev); err == nil {
		t.Fatal("stale update accepted")
	}
	if _, err := b.Create(ctx, "k", payload{"4"}); err == nil {
		t.Fatal("create over existing key accepted")
	}
	if _, err := b.Create(ctx, "new", payload{"4"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Get(ctx, "k"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("err = %v", err)
	}
	c := rec.last[0]
	if c.Kind != nuts.KindKV || c.Name != "b" || c.Subject != "k" {
		t.Fatalf("call = %+v", c)
	}
}

func TestBucketGetReportsBadStoredValue(t *testing.T) {
	rt := runtimeFor(t)
	ctx := ctxFor(t)
	b := bucket(t, rt)
	kv, _ := rt.JetStream().KeyValue(ctx, "b")
	_, _ = kv.Put(ctx, "k", []byte("not json"))
	if _, _, err := b.Get(ctx, "k"); err == nil {
		t.Fatal("expected a decode error")
	}
}

func TestBucketMissingIsAnError(t *testing.T) {
	rt := runtimeFor(t)
	b := nuts.NewBucket[payload](rt, "missing")
	if _, _, err := b.Get(ctxFor(t), "k"); !errors.Is(err, jetstream.ErrBucketNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestBucketWatch(t *testing.T) {
	rt := runtimeFor(t)
	ctx := ctxFor(t)
	b := bucket(t, rt)
	_, _ = b.Put(ctx, "k", payload{"1"})
	got := make(chan nuts.Entry[payload], 4)
	stop, err := b.Watch(ctx, func(e nuts.Entry[payload]) { got <- e })
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if e := <-got; e.Key != "k" || e.Value.Value != "1" || e.Deleted {
		t.Fatalf("entry = %+v", e)
	}
	_ = b.Delete(ctx, "k")
	if e := <-got; e.Key != "k" || !e.Deleted || e.Revision == 0 {
		t.Fatalf("entry = %+v", e)
	}
}

func TestBucketWatchPassesThroughPlugins(t *testing.T) {
	rec := newRecorder("p")
	rt := runtimeFor(t, nuts.WithPlugins(rec), nuts.WithService("svc"))
	stop, err := bucket(t, rt).Watch(ctxFor(t), func(nuts.Entry[payload]) {})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.last) != 1 || rec.last[0].Kind != nuts.KindKV || rec.last[0].Service != "svc" || rec.last[0].Subject != ">" {
		t.Fatalf("calls = %+v", rec.last)
	}
}

func TestBucketWatchPluginCanStopIt(t *testing.T) {
	boom := errors.New("boom")
	rt := runtimeFor(t, nuts.WithPlugins(stopper{boom}))
	if _, err := nuts.NewBucket[payload](rt, "any").Watch(ctxFor(t), func(nuts.Entry[payload]) {}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestBucketWatchOutlivesAPluginContext(t *testing.T) {
	rt := runtimeFor(t, nuts.WithPlugins(shortContext{}))
	ctx := ctxFor(t)
	b := bucket(t, rt)
	got := make(chan nuts.Entry[payload], 4)
	stop, err := b.Watch(ctx, func(e nuts.Entry[payload]) { got <- e })
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	// A plugin may cancel the context it hands down as soon as the call returns.
	raw, err := rt.JetStream().KeyValue(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := raw.Put(ctx, "k", []byte(`{"value":"later"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-got:
		if e.Value == nil || e.Value.Value != "later" {
			t.Fatalf("entry = %+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch ended when the plugin's context did")
	}
}

type stopper struct{ err error }

func (s stopper) Wrap(nuts.Handler) nuts.Handler {
	return func(context.Context, *nuts.Call) error { return s.err }
}

type shortContext struct{}

func (shortContext) Wrap(next nuts.Handler) nuts.Handler {
	return func(ctx context.Context, c *nuts.Call) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		return next(ctx, c)
	}
}

func scheduleStream(t *testing.T, rt *nuts.Runtime) jetstream.Stream {
	t.Helper()
	ctx := ctxFor(t)
	err := devnats.Provision(ctx, rt.JetStream(), nuts.Infra{Streams: []jetstream.StreamConfig{{
		Name: "S", Subjects: []string{"target", "sched.>"}, AllowMsgSchedules: true, AllowRollup: true, Storage: jetstream.MemoryStorage,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	st, err := rt.JetStream().Stream(ctx, "S")
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func countOn(t *testing.T, st jetstream.Stream, subject string) uint64 {
	t.Helper()
	info, err := st.Info(ctxFor(t), jetstream.WithSubjectFilter(subject))
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Subjects[subject]
}

func TestScheduleAtDeliversOnceToTheTarget(t *testing.T) {
	rec := newRecorder("r")
	rt := runtimeFor(t, nuts.WithPlugins(rec))
	st := scheduleStream(t, rt)
	ctx := ctxFor(t)
	for range 2 { // two delayed messages must not replace each other
		if err := nuts.ScheduleAt(ctx, rt, "sweep", "sched.x", "target", time.Now().Add(time.Second), payload{"later"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := countOn(t, st, "target"); got != 0 {
		t.Fatalf("delivered %d messages before the time", got)
	}
	waitFor(t, "delivery", func() bool { return countOn(t, st, "target") == 2 })
	if c := rec.last[0]; c.Kind != nuts.KindSchedule || c.Name != "sweep" || !strings.HasPrefix(c.Subject, "sched.x.") {
		t.Fatalf("call = %+v", c)
	}
}

func TestSyncScheduleRepeatsAndReplaces(t *testing.T) {
	rt := runtimeFor(t)
	st := scheduleStream(t, rt)
	ctx := ctxFor(t)
	sch := nuts.ScheduleSpec{Name: "s", Subject: "sched.a", Target: "target", Every: time.Second, Payload: []byte(`{"value":"tick"}`)}
	for range 2 { // deploying twice must not double the schedule
		if err := nuts.SyncSchedule(ctx, rt, sch); err != nil {
			t.Fatal(err)
		}
	}
	if got := countOn(t, st, "sched.a"); got != 1 {
		t.Fatalf("schedule stored %d times", got)
	}
	waitFor(t, "two ticks", func() bool { return countOn(t, st, "target") >= 2 })
}

func TestSyncScheduleWithCronAndTimeZone(t *testing.T) {
	rt := runtimeFor(t)
	st := scheduleStream(t, rt)
	ctx := ctxFor(t)
	err := nuts.SyncSchedule(ctx, rt, nuts.ScheduleSpec{Name: "s", Subject: "sched.c", Target: "target", Cron: "* * * * * *", TimeZone: "Europe/Budapest", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := st.GetLastMsgForSubject(ctx, "sched.c")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Header.Get("Nats-Schedule") != "* * * * * *" || msg.Header.Get("Nats-Schedule-Time-Zone") != "Europe/Budapest" || msg.Header.Get("Nats-Schedule-Target") != "target" {
		t.Fatalf("headers = %v", msg.Header)
	}
	waitFor(t, "a tick", func() bool { return countOn(t, st, "target") >= 1 })
}

func verifyInfra() nuts.Infra {
	return nuts.Infra{
		Streams: []jetstream.StreamConfig{{Name: "S", Subjects: []string{"s.>"}, Storage: jetstream.MemoryStorage, MaxAge: time.Hour, Replicas: 1, Retention: jetstream.LimitsPolicy, Duplicates: time.Minute}},
		Consumers: []nuts.ConsumerInfra{{Stream: "S", Config: jetstream.ConsumerConfig{
			Durable: "c", FilterSubject: "s.a", AckWait: time.Second, MaxDeliver: 3, MaxAckPending: 5, BackOff: []time.Duration{time.Second},
		}}},
		Buckets: []jetstream.KeyValueConfig{{Bucket: "b", History: 2, TTL: time.Hour, Storage: jetstream.MemoryStorage}},
	}
}

func TestVerifyPassesWhenServerMatches(t *testing.T) {
	rt := runtimeFor(t)
	ctx := ctxFor(t)
	in := verifyInfra()
	if err := devnats.Provision(ctx, rt.JetStream(), in); err != nil {
		t.Fatal(err)
	}
	drift, err := nuts.Verify(ctx, rt.JetStream(), in)
	if err != nil || len(drift) != 0 {
		t.Fatalf("drift = %v, err = %v", drift, err)
	}
}

func TestVerifyReportsMissingResources(t *testing.T) {
	rt := runtimeFor(t)
	ctx := ctxFor(t)
	drift, err := nuts.Verify(ctx, rt.JetStream(), verifyInfra())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, d := range drift {
		got = append(got, d.Resource+"/"+d.Field)
	}
	want := "stream S/exists consumer S/c/exists bucket b/exists"
	if strings.Join(got, " ") != want {
		t.Fatalf("drift = %v, want %s", got, want)
	}
}

func TestVerifyReportsEachDifferentSetting(t *testing.T) {
	cases := []struct {
		field  string
		mutate func(*nuts.Infra)
	}{
		{"subjects", func(in *nuts.Infra) { in.Streams[0].Subjects = []string{"s.x"} }},
		{"max_age", func(in *nuts.Infra) { in.Streams[0].MaxAge = 2 * time.Hour }},
		{"duplicate_window", func(in *nuts.Infra) { in.Streams[0].Duplicates = 2 * time.Minute }},
		{"retention", func(in *nuts.Infra) { in.Streams[0].Retention = jetstream.InterestPolicy }},
		{"storage", func(in *nuts.Infra) { in.Streams[0].Storage = jetstream.FileStorage }},
		{"replicas", func(in *nuts.Infra) { in.Streams[0].Replicas = 3 }},
		{"allow_msg_schedules", func(in *nuts.Infra) { in.Streams[0].AllowMsgSchedules = true }},
		{"filter_subjects", func(in *nuts.Infra) { in.Consumers[0].Config.FilterSubject = "s.b" }},
		{"ack_wait", func(in *nuts.Infra) { in.Consumers[0].Config.AckWait = time.Hour }},
		{"max_deliver", func(in *nuts.Infra) { in.Consumers[0].Config.MaxDeliver = 9 }},
		{"max_ack_pending", func(in *nuts.Infra) { in.Consumers[0].Config.MaxAckPending = 9 }},
		{"backoff", func(in *nuts.Infra) { in.Consumers[0].Config.BackOff = []time.Duration{time.Minute} }},
		{"ack_policy", func(in *nuts.Infra) { in.Consumers[0].Config.AckPolicy = jetstream.AckAllPolicy }},
		{"history", func(in *nuts.Infra) { in.Buckets[0].History = 9 }},
		{"ttl", func(in *nuts.Infra) { in.Buckets[0].TTL = 2 * time.Hour }},
		{"storage", func(in *nuts.Infra) { in.Buckets[0].Storage = jetstream.FileStorage }},
	}
	rt := runtimeFor(t)
	ctx := ctxFor(t)
	if err := devnats.Provision(ctx, rt.JetStream(), verifyInfra()); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.field, func(t *testing.T) {
			in := verifyInfra()
			c.mutate(&in)
			drift, err := nuts.Verify(ctx, rt.JetStream(), in)
			if err != nil {
				t.Fatal(err)
			}
			if len(drift) != 1 || drift[0].Field != c.field {
				t.Fatalf("drift = %v, want one on %s", drift, c.field)
			}
		})
	}
}

func TestVerifyOnlyComparesWhatTheSpecSets(t *testing.T) {
	rt := runtimeFor(t)
	ctx := ctxFor(t)
	if err := devnats.Provision(ctx, rt.JetStream(), verifyInfra()); err != nil {
		t.Fatal(err)
	}
	in := verifyInfra()
	in.Streams[0].MaxAge, in.Streams[0].Duplicates, in.Streams[0].Replicas = 0, 0, 0
	in.Consumers[0].Config.AckWait, in.Consumers[0].Config.MaxDeliver, in.Consumers[0].Config.MaxAckPending = 0, 0, 0
	in.Consumers[0].Config.BackOff = nil
	in.Buckets[0].TTL, in.Buckets[0].History = 0, 0
	drift, err := nuts.Verify(ctx, rt.JetStream(), in)
	if err != nil || len(drift) != 0 {
		t.Fatalf("drift = %v, err = %v", drift, err)
	}
}

func TestCheckModes(t *testing.T) {
	ctx := ctxFor(t)
	in := verifyInfra()
	url := natstest.Start(t)
	for _, c := range []struct {
		mode    nuts.VerifyMode
		wantErr bool
	}{{nuts.VerifyOff, false}, {nuts.VerifyWarn, false}, {nuts.VerifyFail, true}} {
		rt, err := nuts.Dial(url, nuts.WithVerify(c.mode))
		if err != nil {
			t.Fatal(err)
		}
		defer rt.Close()
		if err := rt.Check(ctx, in); (err != nil) != c.wantErr {
			t.Errorf("mode %v: err = %v", c.mode, err)
		}
	}
}

func TestDialFailsOnBadURL(t *testing.T) {
	if _, err := nuts.Dial("nats://127.0.0.1:1", nuts.WithNATSOptions(nats.Timeout(100*time.Millisecond))); err == nil {
		t.Fatal("expected a connection error")
	}
}

func TestNewWrapsAnExistingConnection(t *testing.T) {
	nc, err := nats.Connect(natstest.Start(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	rt, err := nuts.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if rt.Conn() != nc || rt.JetStream() == nil {
		t.Fatal("New did not keep the connection")
	}
}

func TestProvisionIsIdempotent(t *testing.T) {
	rt := runtimeFor(t)
	ctx := ctxFor(t)
	for range 2 {
		if err := devnats.Provision(ctx, rt.JetStream(), verifyInfra()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTerminateOfNilIsNil(t *testing.T) {
	if nuts.Terminate(nil) != nil {
		t.Fatal("Terminate(nil) must be nil")
	}
}

func TestSubjectMatches(t *testing.T) {
	for _, c := range []struct {
		pattern, subject string
		want             bool
	}{
		{"a.*", "a.b", true}, {"a.*", "a.b.c", false}, {"a.b", "a.b", true}, {"a.b", "a.c", false},
		{"a.>", "a.b.c", true}, {"a.>", "a", false}, {"a.b.c", "a.b", false},
	} {
		if got := nuts.SubjectMatches(c.pattern, c.subject); got != c.want {
			t.Errorf("SubjectMatches(%q, %q) = %v", c.pattern, c.subject, got)
		}
	}
}

func TestFormatSubject(t *testing.T) {
	got, err := nuts.FormatSubject("orders.%s.placed.%s", "eu", "1")
	if err != nil || got != "orders.eu.placed.1" {
		t.Fatalf("got %q, %v", got, err)
	}
	for _, bad := range []string{"", "a.b", "a b", "*", ">", "a*", "a\tb", "a\nb", "a\rb"} {
		if _, err := nuts.FormatSubject("x.%s", bad); err == nil {
			t.Errorf("FormatSubject accepted param %q", bad)
		}
	}
}
