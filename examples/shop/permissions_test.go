package shop_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	shopnats "github.com/jnszkr/gonuts/examples/shop/gen"
	"github.com/jnszkr/gonuts/examples/shop/gen/consumers"
	"github.com/jnszkr/gonuts/examples/shop/gen/events"
	"github.com/jnszkr/gonuts/examples/shop/gen/models"
	ordersserver "github.com/jnszkr/gonuts/examples/shop/gen/server/orders"
	"github.com/jnszkr/gonuts/internal/natstest"
	"github.com/jnszkr/gonuts/nuts"
	"github.com/jnszkr/gonuts/nuts/devnats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// secureServer runs a real server whose users have the generated permissions.
func secureServer(t *testing.T) string {
	t.Helper()
	generated, err := os.ReadFile(filepath.Join("permissions", "permissions.conf"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "permissions.conf"), generated, 0o600); err != nil {
		t.Fatal(err)
	}
	conf := `include "permissions.conf"
authorization {
  users: [
    { user: admin, password: test }
    { user: checkout, password: test, permissions: $CHECKOUT_PERMISSIONS }
    { user: billing, password: test, permissions: $BILLING_PERMISSIONS }
    { user: orders-api, password: test, permissions: $ORDERS_API_PERMISSIONS }
    { user: cart-sweeper, password: test, permissions: $CART_SWEEPER_PERMISSIONS }
  ]
}
`
	path := filepath.Join(dir, "server.conf")
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	url := natstest.StartConfig(t, path)

	admin := serviceApp(t, url, "admin")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := devnats.Provision(ctx, admin.Runtime().JetStream(), shopnats.Infra()); err != nil {
		t.Fatal(err)
	}
	return url
}

type user struct {
	*shopnats.App
	name string
	mu   sync.Mutex
	errs []string
}

func serviceApp(t *testing.T, url, name string) *user {
	t.Helper()
	u := &user{name: name}
	u.App = connect(t, url, nuts.WithNATSOptions(
		nats.UserInfo(name, "test"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			u.mu.Lock()
			defer u.mu.Unlock()
			u.errs = append(u.errs, err.Error())
		}),
	))
	return u
}

// violations flushes the connection, so the server has answered everything so far.
func (u *user) violations(t *testing.T) string {
	t.Helper()
	if err := u.Runtime().Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	return strings.Join(u.errs, "\n")
}

func (u *user) mustBeClean(t *testing.T) {
	t.Helper()
	if v := u.violations(t); v != "" {
		t.Errorf("%s was refused something it declares:\n%s", u.name, v)
	}
}

// verify reads the given resources the way nuts.WithVerify does at startup.
func (u *user) verify(ctx context.Context, t *testing.T, keep func(name string) bool) {
	t.Helper()
	var in nuts.Infra
	all := shopnats.Infra()
	for _, st := range all.Streams {
		if keep(st.Name) {
			in.Streams = append(in.Streams, st)
		}
	}
	for _, c := range all.Consumers {
		if keep(c.Config.Durable) {
			in.Consumers = append(in.Consumers, c)
		}
	}
	for _, b := range all.Buckets {
		if keep(b.Bucket) {
			in.Buckets = append(in.Buckets, b)
		}
	}
	drift, err := nuts.Verify(ctx, u.Runtime().JetStream(), in)
	if err != nil || len(drift) != 0 {
		t.Fatalf("%s verify: %v, drift %v", u.name, err, drift)
	}
}

func oneOf(names ...string) func(string) bool {
	return func(name string) bool {
		for _, n := range names {
			if n == name {
				return true
			}
		}
		return false
	}
}

func receive[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func TestEveryServiceCanDoWhatItDeclares(t *testing.T) {
	url := secureServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := serviceApp(t, url, "admin")

	// orders-api serves its group, and answers discovery.
	api := serviceApp(t, url, "orders-api")
	srv, err := ordersserver.Serve(ctx, api.Runtime(), orderAPI{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()
	if err := api.Runtime().Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	reply, err := admin.Runtime().Conn().RequestWithContext(ctx, "$SRV.PING.orders", nil)
	if err != nil || !strings.Contains(string(reply.Data), `"orders"`) {
		t.Fatalf("discovery: %v, %v", reply, err)
	}
	for _, subject := range []string{"$SRV.INFO", "$SRV.STATS.orders"} {
		if _, err := admin.Runtime().Conn().RequestWithContext(ctx, subject, nil); err != nil {
			t.Fatalf("%s: %v", subject, err)
		}
	}

	// billing reads its consumer and publishes.
	bill := serviceApp(t, url, "billing")
	bill.verify(ctx, t, oneOf("ORDERS", "billing-orders"))
	placed := make(chan consumers.OrderPlacedMsg, 1)
	stopBilling, err := bill.Consumers.BillingOrders.Run(ctx, billing{got: placed})
	if err != nil {
		t.Fatal(err)
	}
	defer stopBilling()

	// checkout publishes, subscribes, calls and owns the bucket.
	shop := serviceApp(t, url, "checkout")
	shop.verify(ctx, t, oneOf("ORDERS", "carts"))
	prices := make(chan events.PriceChangedMsg, 1)
	stopPrices, err := shop.Events.SubscribePriceChanged(ctx, func(_ context.Context, m events.PriceChangedMsg) error {
		prices <- m
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stopPrices()
	if err := shop.Runtime().Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := shop.Events.PublishOrderPlaced(ctx, "eu", models.OrderPlaced{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if m := receive(t, "order.placed", placed); m.Data.ID != "1" {
		t.Fatalf("msg = %+v", m)
	}
	if _, err := bill.Events.PublishOrderShipped(ctx, "eu", models.OrderShipped{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := admin.Events.PublishPriceChanged(ctx, "abc", models.LineItem{SKU: "abc"}); err != nil {
		t.Fatal(err)
	}
	receive(t, "price.changed", prices)
	if got, err := shop.Orders.Get(ctx, models.GetOrder{ID: "1"}); err != nil || got.Status != "paid" {
		t.Fatalf("reply = %+v, %v", got, err)
	}

	writes := make(chan nuts.Entry[models.Cart], 8)
	stopWriterWatch, err := shop.KV.Carts.Watch(ctx, func(e nuts.Entry[models.Cart]) { writes <- e })
	if err != nil {
		t.Fatal(err)
	}
	rev, err := shop.KV.Carts.Create(ctx, "u1", models.Cart{})
	if err != nil {
		t.Fatal(err)
	}
	if rev, err = shop.KV.Carts.Update(ctx, "u1", models.Cart{}, rev); err != nil {
		t.Fatal(err)
	}
	if _, err = shop.KV.Carts.Put(ctx, "u2", models.Cart{}); err != nil {
		t.Fatal(err)
	}
	if _, got, err := shop.KV.Carts.Get(ctx, "u1"); err != nil || got != rev {
		t.Fatalf("get: %d, %v", got, err)
	}
	if err := shop.KV.Carts.Delete(ctx, "u2"); err != nil {
		t.Fatal(err)
	}
	receive(t, "the writer's watch", writes)

	// cart-sweeper only reads the bucket, and owns its schedules.
	sweeper := serviceApp(t, url, "cart-sweeper")
	sweeper.verify(ctx, t, oneOf("CARTS", "carts"))
	if _, _, err := sweeper.KV.Carts.Get(ctx, "u1"); err != nil {
		t.Fatal(err)
	}
	seen := make(chan nuts.Entry[models.Cart], 8)
	stopWatch, err := sweeper.KV.Carts.Watch(ctx, func(e nuts.Entry[models.Cart]) { seen <- e })
	if err != nil {
		t.Fatal(err)
	}
	if e := receive(t, "the reader's watch", seen); e.Key == "" {
		t.Fatalf("entry = %+v", e)
	}
	if err := sweeper.Schedules.NightlyCartSweepAt(ctx, time.Now().Add(time.Hour), models.CartSweep{}); err != nil {
		t.Fatal(err)
	}
	if err := sweeper.Schedules.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Stopping a watch deletes its consumer.
	stopWriterWatch()
	stopWatch()

	api.mustBeClean(t)
	bill.mustBeClean(t)
	shop.mustBeClean(t)
	sweeper.mustBeClean(t)
}

// The server asks for flow control once a watcher has 16 MB outstanding, and the
// watch stalls if the reply is refused.
func TestWatchesKeepUpUnderFlowControl(t *testing.T) {
	url := secureServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	writer := serviceApp(t, url, "checkout")
	reader := serviceApp(t, url, "cart-sweeper")

	const values = 48
	watch := func(u *user) (<-chan nuts.Entry[models.Cart], func()) {
		got := make(chan nuts.Entry[models.Cart], values)
		stop, err := u.KV.Carts.Watch(ctx, func(e nuts.Entry[models.Cart]) { got <- e })
		if err != nil {
			t.Fatal(err)
		}
		return got, stop
	}
	writes, stopWriter := watch(writer)
	defer stopWriter()
	reads, stopReader := watch(reader)
	defer stopReader()

	big := models.Cart{Items: []models.LineItem{{SKU: strings.Repeat("x", 512*1024)}}}
	for i := range values {
		if _, err := writer.KV.Carts.Put(ctx, "user"+strconv.Itoa(i), big); err != nil {
			t.Fatal(err)
		}
	}
	for range values {
		receive(t, "the writer's watch", writes)
		receive(t, "the reader's watch", reads)
	}
	writer.mustBeClean(t)
	reader.mustBeClean(t)
}

// A bucket with direct gets turned off is read through the stream API instead.
func TestBucketsReadWithoutDirectGet(t *testing.T) {
	url := secureServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := serviceApp(t, url, "admin")
	st, err := admin.Runtime().JetStream().Stream(ctx, "KV_carts")
	if err != nil {
		t.Fatal(err)
	}
	cfg := st.CachedInfo().Config
	cfg.AllowDirect = false
	if _, err := admin.Runtime().JetStream().UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.KV.Carts.Put(ctx, "u1", models.Cart{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"checkout", "cart-sweeper"} {
		u := serviceApp(t, url, name)
		if _, _, err := u.KV.Carts.Get(ctx, "u1"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		u.mustBeClean(t)
	}
}

func TestServicesAreRefusedWhatTheyDoNotDeclare(t *testing.T) {
	url := secureServer(t)
	admin := serviceApp(t, url, "admin")
	checkout := serviceApp(t, url, "checkout")
	billing := serviceApp(t, url, "billing")
	api := serviceApp(t, url, "orders-api")
	sweeper := serviceApp(t, url, "cart-sweeper")

	cases := []struct {
		name string
		who  *user
		try  func(ctx context.Context, u *user) error
	}{
		{"create a stream", checkout, func(ctx context.Context, u *user) error {
			_, err := u.Runtime().JetStream().CreateStream(ctx, jetstream.StreamConfig{Name: "ROGUE", Subjects: []string{"rogue"}})
			return err
		}},
		{"delete a stream", billing, func(ctx context.Context, u *user) error {
			return u.Runtime().JetStream().DeleteStream(ctx, "ORDERS")
		}},
		{"purge through the API", sweeper, func(_ context.Context, u *user) error {
			return u.Runtime().Conn().Publish("$JS.API.STREAM.PURGE.CARTS", nil)
		}},
		{"create a consumer on a stream", billing, func(ctx context.Context, u *user) error {
			_, err := u.Runtime().JetStream().CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{Durable: "rogue"})
			return err
		}},
		{"delete its own consumer", billing, func(ctx context.Context, u *user) error {
			return u.Runtime().JetStream().DeleteConsumer(ctx, "ORDERS", "billing-orders")
		}},
		{"read another consumer", billing, func(ctx context.Context, u *user) error {
			_, err := u.Runtime().JetStream().Consumer(ctx, "ORDERS", "fulfillment-orders")
			return err
		}},
		{"delete a bucket", checkout, func(ctx context.Context, u *user) error {
			return u.Runtime().JetStream().DeleteKeyValue(ctx, "carts")
		}},
		{"publish an event it does not own", billing, func(ctx context.Context, u *user) error {
			_, err := u.Events.PublishOrderPlaced(ctx, "eu", models.OrderPlaced{ID: "1"})
			return err
		}},
		{"publish a core event it only subscribes to", checkout, func(ctx context.Context, u *user) error {
			return u.Events.PublishPriceChanged(ctx, "abc", models.LineItem{SKU: "abc"})
		}},
		{"subscribe to a stored event", checkout, func(ctx context.Context, u *user) error {
			_, err := u.Runtime().Conn().SubscribeSync("orders.>")
			_ = ctx
			return err
		}},
		{"write with read access", sweeper, func(ctx context.Context, u *user) error {
			_, err := u.KV.Carts.Put(ctx, "u1", models.Cart{})
			return err
		}},
		{"read a bucket it does not use", billing, func(ctx context.Context, u *user) error {
			_, _, err := u.KV.Carts.Get(ctx, "u1")
			return err
		}},
		{"call an endpoint it does not use", billing, func(ctx context.Context, u *user) error {
			_, err := u.Orders.Get(ctx, models.GetOrder{ID: "1"})
			return err
		}},
		{"publish anything from a server", api, func(_ context.Context, u *user) error {
			return u.Runtime().Conn().Publish("orders.placed.eu", nil)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.who.mu.Lock()
			c.who.errs = nil
			c.who.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			_ = c.try(ctx, c.who)
			waitForViolation(t, c.who)
		})
	}

	// None of those attempts changed anything.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	drift, err := nuts.Verify(ctx, admin.Runtime().JetStream(), shopnats.Infra())
	if err != nil || len(drift) != 0 {
		t.Fatalf("infrastructure changed: %v, %v", drift, err)
	}
	if _, err := admin.Runtime().JetStream().Stream(ctx, "ROGUE"); err == nil {
		t.Fatal("a service created a stream")
	}
}

func waitForViolation(t *testing.T, u *user) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(strings.ToLower(u.violations(t)), "permissions violation") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s was not refused; errors: %q", u.name, u.violations(t))
}
