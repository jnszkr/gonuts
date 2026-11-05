// Package shop_test runs the generated example code against a real NATS server.
package shop_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	shopnats "github.com/jnszkr/gonuts/examples/shop/gen"
	"github.com/jnszkr/gonuts/examples/shop/gen/client/orders"
	"github.com/jnszkr/gonuts/examples/shop/gen/consumers"
	"github.com/jnszkr/gonuts/examples/shop/gen/events"
	"github.com/jnszkr/gonuts/examples/shop/gen/models"
	ordersserver "github.com/jnszkr/gonuts/examples/shop/gen/server/orders"
	"github.com/jnszkr/gonuts/internal/cli"
	"github.com/jnszkr/gonuts/internal/natstest"
	"github.com/jnszkr/gonuts/nuts"
	"github.com/jnszkr/gonuts/nuts/devnats"
	"github.com/nats-io/nats.go/jetstream"
)

func connect(t *testing.T, url string, opts ...nuts.Option) *shopnats.App {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	app, err := shopnats.Connect(ctx, url, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Close)
	return app
}

func ready(t *testing.T, opts ...nuts.Option) (*shopnats.App, context.Context) {
	t.Helper()
	app := connect(t, natstest.Start(t), opts...)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	if err := devnats.Provision(ctx, app.Runtime().JetStream(), shopnats.Infra()); err != nil {
		t.Fatal(err)
	}
	return app, ctx
}

type billing struct{ got chan consumers.OrderPlacedMsg }

func (b billing) OnOrderPlaced(_ context.Context, m consumers.OrderPlacedMsg) error {
	b.got <- m
	return nil
}

type fulfillment struct {
	placed  chan models.OrderPlaced
	shipped chan models.OrderShipped
}

func (f fulfillment) OnOrderPlaced(_ context.Context, m consumers.OrderPlacedMsg) error {
	f.placed <- m.Data
	return nil
}

func (f fulfillment) OnOrderShipped(_ context.Context, m consumers.OrderShippedMsg) error {
	f.shipped <- m.Data
	return nil
}

func TestPublishReachesTheConsumerWithTypedData(t *testing.T) {
	app, ctx := ready(t)
	b := billing{got: make(chan consumers.OrderPlacedMsg, 1)}
	stop, err := app.Consumers.BillingOrders.Run(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	note := "gift"
	ack, err := app.Events.PublishOrderPlaced(ctx, "eu", models.OrderPlaced{ID: "1", TotalCents: 4200, Note: &note, Items: []models.LineItem{{SKU: "a", Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Stream != "ORDERS" {
		t.Fatalf("stored in %s", ack.Stream)
	}
	m := <-b.got
	if m.Params.Region != "eu" || m.Data.ID != "1" || m.Data.TotalCents != 4200 || *m.Data.Note != "gift" || m.Data.Items[0].SKU != "a" {
		t.Fatalf("msg = %+v", m)
	}
}

func TestConsumerWithSeveralEventsRoutesEach(t *testing.T) {
	app, ctx := ready(t)
	f := fulfillment{placed: make(chan models.OrderPlaced, 1), shipped: make(chan models.OrderShipped, 1)}
	stop, err := app.Consumers.FulfillmentOrders.Run(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if _, err := app.Events.PublishOrderShipped(ctx, "us", models.OrderShipped{ID: "9", Carrier: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Events.PublishOrderPlaced(ctx, "us", models.OrderPlaced{ID: "8"}); err != nil {
		t.Fatal(err)
	}
	if got := <-f.shipped; got.Carrier != "x" {
		t.Fatalf("shipped = %+v", got)
	}
	if got := <-f.placed; got.ID != "8" {
		t.Fatalf("placed = %+v", got)
	}
}

func TestPublishRejectsParamsThatChangeTheSubject(t *testing.T) {
	app, ctx := ready(t)
	if _, err := app.Events.PublishOrderPlaced(ctx, "eu.evil", models.OrderPlaced{}); err == nil {
		t.Fatal("a dot in a subject param was accepted")
	}
}

func TestCoreEventsDoNotNeedAStream(t *testing.T) {
	app, ctx := ready(t)
	got := make(chan events.PriceChangedMsg, 1)
	stop, err := app.Events.SubscribePriceChanged(ctx, func(_ context.Context, m events.PriceChangedMsg) error {
		got <- m
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if err := app.Runtime().Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	if err := app.Events.PublishPriceChanged(ctx, "abc", models.LineItem{SKU: "abc", Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	if m := <-got; m.Params.SKU != "abc" || m.Data.Quantity != 1 {
		t.Fatalf("msg = %+v", m)
	}
}

type orderAPI struct{}

func (orderAPI) Get(_ context.Context, req models.GetOrder) (*models.Order, error) {
	if req.ID == "missing" {
		return nil, &ordersserver.NotFoundError{Message: "no order " + req.ID}
	}
	return &models.Order{ID: req.ID, Status: "paid"}, nil
}

func TestRequestReplyAcrossGeneratedClientAndServer(t *testing.T) {
	app, ctx := ready(t)
	srv, err := ordersserver.Serve(ctx, app.Runtime(), orderAPI{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()
	if err := app.Runtime().Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := app.Orders.Get(ctx, models.GetOrder{ID: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "paid" {
		t.Fatalf("reply = %+v", got)
	}
	_, err = app.Orders.Get(ctx, models.GetOrder{ID: "missing"})
	var notFound *orders.NotFoundError
	if !errors.As(err, &notFound) || notFound.Message != "no order missing" {
		t.Fatalf("err = %#v", err)
	}
}

func TestKeyValue(t *testing.T) {
	app, ctx := ready(t)
	rev, err := app.KV.Carts.Put(ctx, "u1", models.Cart{Items: []models.LineItem{{SKU: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	cart, got, err := app.KV.Carts.Get(ctx, "u1")
	if err != nil || got != rev || cart.Items[0].SKU != "a" {
		t.Fatalf("cart = %+v, rev = %d, err = %v", cart, got, err)
	}
	if _, err := app.KV.Carts.Update(ctx, "u1", models.Cart{}, rev+5); err == nil {
		t.Fatal("stale update accepted")
	}
	if err := app.KV.Carts.Delete(ctx, "u1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := app.KV.Carts.Get(ctx, "u1"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestSchedulesDeliverTheEvent(t *testing.T) {
	app, ctx := ready(t)
	consumer, err := app.Runtime().JetStream().CreateConsumer(ctx, "CARTS", jetstream.ConsumerConfig{FilterSubject: "carts.sweep"})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Schedules.NightlyCartSweepAt(ctx, time.Now().Add(500*time.Millisecond), models.CartSweep{OlderThan: "1h"}); err != nil {
		t.Fatal(err)
	}
	msg, err := consumer.Next(jetstream.FetchMaxWait(10 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Data()) != `{"older_than":"1h"}` {
		t.Fatalf("data = %s", msg.Data())
	}
	if err := app.Schedules.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	st, err := app.Runtime().JetStream().Stream(ctx, "CARTS")
	if err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetLastMsgForSubject(ctx, "carts.schedules.sweep")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Header.Get("Nats-Schedule") != "0 0 3 * * *" {
		t.Fatalf("headers = %v", stored.Header)
	}
}

func TestConnectNeverCreatesInfrastructure(t *testing.T) {
	url := natstest.Start(t)
	app := connect(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := app.Events.PublishOrderPlaced(ctx, "eu", models.OrderPlaced{}); err == nil {
		t.Error("publish succeeded with no stream")
	}
	if _, err := app.Consumers.BillingOrders.Run(ctx, billing{}); err == nil {
		t.Error("consumer bound with no stream")
	}
	if _, err := app.KV.Carts.Put(ctx, "u", models.Cart{}); err == nil {
		t.Error("bucket used with none created")
	}
	names := app.Runtime().JetStream().StreamNames(ctx)
	for name := range names.Name() {
		t.Errorf("stream %s was created", name)
	}
}

func TestVerifyFailStopsConnectUntilInfrastructureExists(t *testing.T) {
	url := natstest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := shopnats.Connect(ctx, url, nuts.WithVerify(nuts.VerifyFail)); err == nil {
		t.Fatal("connect succeeded with missing infrastructure")
	}
	app := connect(t, url)
	if err := devnats.Provision(ctx, app.Runtime().JetStream(), shopnats.Infra()); err != nil {
		t.Fatal(err)
	}
	checked, err := shopnats.Connect(ctx, url, nuts.WithVerify(nuts.VerifyFail))
	if err != nil {
		t.Fatalf("connect failed on a matching server: %v", err)
	}
	checked.Close()
}

type tracer struct {
	mu    sync.Mutex
	calls []string
}

func (p *tracer) Wrap(next nuts.Handler) nuts.Handler {
	return func(ctx context.Context, c *nuts.Call) error {
		if c.Kind == nuts.KindPublish {
			c.Headers.Set("Traceparent", "trace-1")
		}
		p.mu.Lock()
		p.calls = append(p.calls, string(c.Kind)+":"+c.Name+":"+c.Headers.Get("Traceparent"))
		p.mu.Unlock()
		return next(ctx, c)
	}
}

func TestPluginTraceCrossesFromPublisherToConsumer(t *testing.T) {
	p := &tracer{}
	app, ctx := ready(t, nuts.WithPlugins(p))
	b := billing{got: make(chan consumers.OrderPlacedMsg, 1)}
	stop, err := app.Consumers.BillingOrders.Run(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if _, err := app.Events.PublishOrderPlaced(ctx, "eu", models.OrderPlaced{}); err != nil {
		t.Fatal(err)
	}
	<-b.got
	p.mu.Lock()
	defer p.mu.Unlock()
	want := []string{"publish:order.placed:trace-1", "consume:order.placed:trace-1"}
	if len(p.calls) != 2 || p.calls[0] != want[0] || p.calls[1] != want[1] {
		t.Fatalf("calls = %v, want %v", p.calls, want)
	}
}

func TestCommittedOutputMatchesTheSpec(t *testing.T) {
	for _, args := range [][]string{
		{"generate", "--check", "--spec", "nuts.yaml", "--out", "gen"},
		{"generate", "--check", "--spec", "nuts.yaml", "--terraform", "--terraform-out", "terraform"},
	} {
		if code := cli.Run(args, os.Stdout, os.Stderr); code != 0 {
			t.Fatalf("%v exited %d; run make generate", args, code)
		}
	}
}
