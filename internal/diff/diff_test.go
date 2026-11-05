package diff

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jnszkr/gonuts/internal/spec"
)

func shop(t *testing.T) *spec.Spec {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "examples", "shop", "nuts.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := spec.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestNoChanges(t *testing.T) {
	if got := Compare(shop(t), shop(t)); len(got) != 0 {
		t.Fatalf("changes = %v", got)
	}
}

func TestCompareClassifiesChanges(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*spec.Spec)
		path     string
		severity Severity
		msg      string
	}{
		{"model added", func(s *spec.Spec) { s.Models["New"] = &spec.Model{} }, "models.New", Safe, "added"},
		{"model removed", func(s *spec.Spec) { delete(s.Models, "Cart") }, "models.Cart", NeedsBoth, "removed"},
		{"optional field added", func(s *spec.Spec) {
			m := s.Models["Cart"]
			m.Fields = append(m.Fields, &spec.Field{Name: "x", Type: "string", Optional: true})
		}, "models.Cart.x", Safe, "optional"},
		{"required field added", func(s *spec.Spec) {
			m := s.Models["Cart"]
			m.Fields = append(m.Fields, &spec.Field{Name: "x", Type: "string"})
		}, "models.Cart.x", NeedsBoth, "required"},
		{"field removed", func(s *spec.Spec) { s.Models["Cart"].Fields = nil }, "models.Cart.items", NeedsBoth, "removed"},
		{"field type", func(s *spec.Spec) { s.Models["Order"].Fields[1].Type = "int" }, "models.Order.status", Destructive, "type"},
		{"field made required", func(s *spec.Spec) { s.Models["OrderPlaced"].Fields[3].Optional = false }, "models.OrderPlaced.note", NeedsBoth, "required"},
		{"field made optional", func(s *spec.Spec) { s.Models["Order"].Fields[1].Optional = true }, "models.Order.status", Safe, "optional"},
		{"event added", func(s *spec.Spec) { s.Events["n"] = spec.Event{Subject: "n", Model: "Order"} }, "events.n", Safe, "added"},
		{"event removed", func(s *spec.Spec) { delete(s.Events, "price.changed") }, "events.price.changed", NeedsBoth, "removed"},
		{"event subject", func(s *spec.Spec) { e := s.Events["price.changed"]; e.Subject = "x"; s.Events["price.changed"] = e }, "events.price.changed.subject", Destructive, "subject"},
		{"event model", func(s *spec.Spec) { e := s.Events["price.changed"]; e.Model = "Order"; s.Events["price.changed"] = e }, "events.price.changed.model", Destructive, "model"},
		{"stream added", func(s *spec.Spec) { s.Streams["N"] = spec.Stream{} }, "streams.N", Safe, "added"},
		{"stream removed", func(s *spec.Spec) { delete(s.Streams, "CARTS") }, "streams.CARTS", Destructive, "removed"},
		{"stream storage", func(s *spec.Spec) { x := s.Streams["ORDERS"]; x.Storage = "memory"; s.Streams["ORDERS"] = x }, "streams.ORDERS.storage", Destructive, "recreated"},
		{"stream retention", func(s *spec.Spec) { x := s.Streams["ORDERS"]; x.Retention = "workqueue"; s.Streams["ORDERS"] = x }, "streams.ORDERS.retention", Destructive, "retention"},
		{"stream subject dropped", func(s *spec.Spec) { x := s.Streams["ORDERS"]; x.Events = x.Events[:1]; s.Streams["ORDERS"] = x }, "streams.ORDERS.subjects", NeedsBoth, "orders.shipped"},
		{"schedules enabled", func(s *spec.Spec) { x := s.Streams["ORDERS"]; x.AllowMsgSchedules = true; s.Streams["ORDERS"] = x }, "streams.ORDERS.allow_msg_schedules", NeedsBoth, "cannot be turned off"},
		{"schedules disabled", func(s *spec.Spec) { x := s.Streams["CARTS"]; x.AllowMsgSchedules = false; s.Streams["CARTS"] = x }, "streams.CARTS.allow_msg_schedules", Destructive, "cannot be turned off"},
		{"stream setting", func(s *spec.Spec) {
			x := s.Streams["ORDERS"]
			x.MaxAge = spec.Duration{Duration: time.Hour}
			s.Streams["ORDERS"] = x
		}, "streams.ORDERS", Safe, "settings"},
		{"consumer added", func(s *spec.Spec) { s.Consumers["n"] = spec.Consumer{} }, "consumers.n", Safe, "added"},
		{"consumer removed", func(s *spec.Spec) { delete(s.Consumers, "billing-orders") }, "consumers.billing-orders", Destructive, "position"},
		{"consumer stream", func(s *spec.Spec) {
			c := s.Consumers["billing-orders"]
			c.Stream = "CARTS"
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.stream", Destructive, "position"},
		{"consumer ack", func(s *spec.Spec) {
			c := s.Consumers["billing-orders"]
			c.AckPolicy = "all"
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.ack_policy", Destructive, "position"},
		{"consumer deliver", func(s *spec.Spec) {
			c := s.Consumers["billing-orders"]
			c.DeliverPolicy = "new"
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.deliver_policy", Destructive, "position"},
		{"consumer events", func(s *spec.Spec) {
			c := s.Consumers["billing-orders"]
			c.Events = []string{"order.shipped"}
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.events", NeedsBoth, "handler"},
		{"consumer setting", func(s *spec.Spec) {
			c := s.Consumers["billing-orders"]
			c.MaxAckPending = 1
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders", Safe, "settings"},
		{"bucket added", func(s *spec.Spec) { s.KV["n"] = spec.KVBucket{} }, "kv.n", Safe, "added"},
		{"bucket removed", func(s *spec.Spec) { delete(s.KV, "carts") }, "kv.carts", Destructive, "removed"},
		{"bucket model", func(s *spec.Spec) { k := s.KV["carts"]; k.Model = "Order"; s.KV["carts"] = k }, "kv.carts.model", Destructive, "stored"},
		{"bucket key", func(s *spec.Spec) { k := s.KV["carts"]; k.Key = "{x}.{y}"; s.KV["carts"] = k }, "kv.carts.key", Destructive, "stored"},
		{"bucket storage", func(s *spec.Spec) { k := s.KV["carts"]; k.Storage = "memory"; s.KV["carts"] = k }, "kv.carts.storage", Destructive, "recreated"},
		{"bucket setting", func(s *spec.Spec) { k := s.KV["carts"]; k.History = 9; s.KV["carts"] = k }, "kv.carts", Safe, "settings"},
		{"schedule removed", func(s *spec.Spec) { delete(s.Schedules, "nightly-cart-sweep") }, "schedules.nightly-cart-sweep", NeedsBoth, "keeps firing"},
		{"schedule added", func(s *spec.Spec) { s.Schedules["n"] = spec.Schedule{} }, "schedules.n", Safe, "added"},
		{"schedule changed", func(s *spec.Spec) {
			x := s.Schedules["nightly-cart-sweep"]
			x.Cron = "@daily"
			s.Schedules["nightly-cart-sweep"] = x
		}, "schedules.nightly-cart-sweep", Safe, "sync"},
		{"group removed", func(s *spec.Spec) { delete(s.Requests, "orders") }, "requests.orders", NeedsBoth, "removed"},
		{"endpoint added", func(s *spec.Spec) { s.Requests["orders"].Endpoints["New"] = spec.Endpoint{} }, "requests.orders.New", Safe, "added"},
		{"endpoint removed", func(s *spec.Spec) { delete(s.Requests["orders"].Endpoints, "Get") }, "requests.orders.Get", NeedsBoth, "removed"},
		{"endpoint subject", func(s *spec.Spec) {
			e := s.Requests["orders"].Endpoints["Get"]
			e.Subject = "x"
			s.Requests["orders"].Endpoints["Get"] = e
		}, "requests.orders.Get.subject", Destructive, "subject"},
		{"endpoint request", func(s *spec.Spec) {
			e := s.Requests["orders"].Endpoints["Get"]
			e.Request = "Order"
			s.Requests["orders"].Endpoints["Get"] = e
		}, "requests.orders.Get.request", Destructive, "request"},
		{"endpoint reply", func(s *spec.Spec) {
			e := s.Requests["orders"].Endpoints["Get"]
			e.Reply = "Cart"
			s.Requests["orders"].Endpoints["Get"] = e
		}, "requests.orders.Get.reply", Destructive, "reply"},
		{"group added", func(s *spec.Spec) { s.Requests["n"] = spec.RequestGroup{} }, "requests.n", Safe, "added"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			next := shop(t)
			c.mutate(next)
			changes := Compare(shop(t), next)
			for _, ch := range changes {
				if ch.Path == c.path && ch.Severity == c.severity && strings.Contains(ch.Message, c.msg) {
					return
				}
			}
			t.Fatalf("want %s %s containing %q; got %v", c.severity, c.path, c.msg, changes)
		})
	}
}

func TestChangesAreSortedMostSevereFirst(t *testing.T) {
	next := shop(t)
	delete(next.Streams, "CARTS")
	next.Models["New"] = &spec.Model{}
	delete(next.Events, "price.changed")
	got := Compare(shop(t), next)
	if len(got) != 3 || got[0].Severity != Destructive || got[1].Severity != NeedsBoth || got[2].Severity != Safe {
		t.Fatalf("order = %v", got)
	}
}

func TestHasDestructive(t *testing.T) {
	if HasDestructive([]Change{{Severity: Safe}, {Severity: NeedsBoth}}) {
		t.Error("no destructive change present")
	}
	if !HasDestructive([]Change{{Severity: Safe}, {Severity: Destructive}}) {
		t.Error("destructive change missed")
	}
}
