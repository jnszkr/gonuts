package spec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func loadShop(t *testing.T) *Spec {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "examples", "shop", "nuts.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestParseReadsEveryBlockOfTheExample(t *testing.T) {
	s := loadShop(t)
	if s.Name != "shop" || s.Go.Package != "shopnats" || s.Encoding != "json" {
		t.Fatalf("header not parsed: %+v", s)
	}
	if got := s.Streams["ORDERS"].MaxAge; got.Duration != 720*time.Hour {
		t.Errorf("max_age = %v", got)
	}
	if got := s.Consumers["billing-orders"].BackOff; len(got) != 4 || got[3].Duration != 5*time.Minute {
		t.Errorf("backoff = %v", got)
	}
	if got := s.KV["carts"].History; got != 5 {
		t.Errorf("history = %d", got)
	}
	if got := s.Schedules["nightly-cart-sweep"].Cron; got != "0 0 3 * * *" {
		t.Errorf("cron = %q", got)
	}
	if got := s.Requests["orders"].Endpoints["Get"].Errors; !reflect.DeepEqual(got, []string{"NotFound"}) {
		t.Errorf("errors = %v", got)
	}
	if got := s.Services["checkout"].KV["carts"]; got != "readwrite" {
		t.Errorf("service kv = %q", got)
	}
}

func TestParseKeepsModelFieldOrderAndExpandsShorthand(t *testing.T) {
	m := loadShop(t).Models["OrderPlaced"]
	var names []string
	for _, f := range m.Fields {
		names = append(names, f.Name)
	}
	want := []string{"id", "total_cents", "items", "note"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("order = %v, want %v", names, want)
	}
	if m.Fields[0].Type != "uuid" || m.Fields[0].Optional {
		t.Errorf("shorthand field = %+v", m.Fields[0])
	}
	if m.Fields[3].Type != "string" || !m.Fields[3].Optional {
		t.Errorf("long field = %+v", m.Fields[3])
	}
}

func TestParseRejectsUnknownKeys(t *testing.T) {
	_, err := Parse([]byte("version: 1\nname: x\nstreams:\n  A:\n    max_agee: 1h\n"))
	if err == nil || !strings.Contains(err.Error(), "max_agee") {
		t.Fatalf("err = %v, want it to name the bad key", err)
	}
}

func TestParseRejectsBadDuration(t *testing.T) {
	_, err := Parse([]byte("version: 1\nname: x\nstreams:\n  A:\n    max_age: soon\n"))
	if err == nil || !strings.Contains(err.Error(), "soon") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseRejectsInvalidModelShapes(t *testing.T) {
	for name, y := range map[string]string{
		"fields not a map": "version: 1\nmodels:\n  A:\n    fields: [a]\n",
		"unknown key":      "version: 1\nmodels:\n  A:\n    fieldz: {}\n",
		"model not a map":  "version: 1\nmodels:\n  A: string\n",
		"bad field":        "version: 1\nmodels:\n  A:\n    fields:\n      a: [1]\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(y)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestShopExampleIsValid(t *testing.T) {
	if errs := Validate(loadShop(t)); len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
}

func TestValidateReportsMistakes(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Spec)
		path   string
		msg    string
	}{
		{"version", func(s *Spec) { s.Version = 2 }, "version", "must be 1"},
		{"name", func(s *Spec) { s.Name = "" }, "name", "required"},
		{"package", func(s *Spec) { s.Go.Package = "not valid" }, "go.package", "Go identifier"},
		{"encoding", func(s *Spec) { s.Encoding = "xml" }, "encoding", "json"},
		{"field type", func(s *Spec) { s.Models["Order"].Fields[0].Type = "Nope" }, "models.Order.fields.id", "unknown type"},
		{"slice of unknown", func(s *Spec) { s.Models["Order"].Fields[0].Type = "[]Nope" }, "models.Order.fields.id", "unknown type"},
		{"event model", func(s *Spec) { e := s.Events["cart.sweep"]; e.Model = "Nope"; s.Events["cart.sweep"] = e }, "events.cart.sweep.model", "unknown model"},
		{"event subject", func(s *Spec) { e := s.Events["cart.sweep"]; e.Subject = "a..b"; s.Events["cart.sweep"] = e }, "events.cart.sweep.subject", "invalid subject"},
		{"event overlap", func(s *Spec) { e := s.Events["cart.sweep"]; e.Subject = "orders.placed.eu"; s.Events["cart.sweep"] = e }, "events", "overlap"},
		{"stream event", func(s *Spec) { x := s.Streams["CARTS"]; x.Events = []string{"nope"}; s.Streams["CARTS"] = x }, "streams.CARTS.events", "unknown event"},
		{"stream without subjects", func(s *Spec) { x := s.Streams["CARTS"]; x.Events, x.Subjects = nil, nil; s.Streams["CARTS"] = x }, "streams.CARTS", "no subjects"},
		{"stream overlap", func(s *Spec) { x := s.Streams["CARTS"]; x.Subjects = []string{"orders.>"}; s.Streams["CARTS"] = x }, "streams", "overlap"},
		{"retention", func(s *Spec) { x := s.Streams["ORDERS"]; x.Retention = "forever"; s.Streams["ORDERS"] = x }, "streams.ORDERS.retention", "limits"},
		{"storage", func(s *Spec) { x := s.Streams["ORDERS"]; x.Storage = "tape"; s.Streams["ORDERS"] = x }, "streams.ORDERS.storage", "file"},
		{"discard", func(s *Spec) { x := s.Streams["ORDERS"]; x.Discard = "all"; s.Streams["ORDERS"] = x }, "streams.ORDERS.discard", "old"},
		{"compression", func(s *Spec) { x := s.Streams["ORDERS"]; x.Compression = "zip"; s.Streams["ORDERS"] = x }, "streams.ORDERS.compression", "s2"},
		{"persist", func(s *Spec) { x := s.Streams["ORDERS"]; x.PersistMode = "x"; s.Streams["ORDERS"] = x }, "streams.ORDERS.persist_mode", "async"},
		{"replicas high", func(s *Spec) { x := s.Streams["ORDERS"]; x.Replicas = 6; s.Streams["ORDERS"] = x }, "streams.ORDERS.replicas", "1 and 5"},
		{"replicas negative", func(s *Spec) { x := s.Streams["ORDERS"]; x.Replicas = -1; s.Streams["ORDERS"] = x }, "streams.ORDERS.replicas", "1 and 5"},
		{"schedules with mirror", func(s *Spec) {
			x := s.Streams["CARTS"]
			x.Mirror = &StreamSource{Name: "ORDERS"}
			s.Streams["CARTS"] = x
		}, "streams.CARTS.allow_msg_schedules", "mirror"},
		{"schedules with source", func(s *Spec) {
			x := s.Streams["CARTS"]
			x.Sources = []StreamSource{{Name: "ORDERS"}}
			s.Streams["CARTS"] = x
		}, "streams.CARTS.allow_msg_schedules", "mirror"},
		{"consumer stream", func(s *Spec) {
			c := s.Consumers["billing-orders"]
			c.Stream = "NOPE"
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.stream", "unknown stream"},
		{"consumer event", func(s *Spec) {
			c := s.Consumers["billing-orders"]
			c.Events = []string{"nope"}
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.events", "unknown event"},
		{"consumer foreign event", func(s *Spec) {
			c := s.Consumers["billing-orders"]
			c.Events = []string{"cart.sweep"}
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.events", "not in stream"},
		{"ephemeral", func(s *Spec) {
			c := s.Consumers["billing-orders"]
			f := false
			c.Durable = &f
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.durable", "durable"},
		{"backoff too long", func(s *Spec) { c := s.Consumers["billing-orders"]; c.MaxDeliver = 4; s.Consumers["billing-orders"] = c }, "consumers.billing-orders.backoff", "max_deliver"},
		{"ack_wait with backoff", func(s *Spec) {
			c := s.Consumers["billing-orders"]
			c.AckWait = Duration{Duration: time.Minute}
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.ack_wait", "backoff"},
		{"ack policy", func(s *Spec) {
			c := s.Consumers["billing-orders"]
			c.AckPolicy = "maybe"
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.ack_policy", "explicit"},
		{"deliver policy", func(s *Spec) {
			c := s.Consumers["billing-orders"]
			c.DeliverPolicy = "x"
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.deliver_policy", "all"},
		{"replay policy", func(s *Spec) {
			c := s.Consumers["billing-orders"]
			c.ReplayPolicy = "x"
			s.Consumers["billing-orders"] = c
		}, "consumers.billing-orders.replay_policy", "instant"},
		{"consumer replicas", func(s *Spec) { c := s.Consumers["billing-orders"]; c.Replicas = 9; s.Consumers["billing-orders"] = c }, "consumers.billing-orders.replicas", "1 and 5"},
		{"kv model", func(s *Spec) { k := s.KV["carts"]; k.Model = "Nope"; s.KV["carts"] = k }, "kv.carts.model", "unknown model"},
		{"kv key", func(s *Spec) { k := s.KV["carts"]; k.Key = ""; s.KV["carts"] = k }, "kv.carts.key", "required"},
		{"kv key bad", func(s *Spec) { k := s.KV["carts"]; k.Key = "a b"; s.KV["carts"] = k }, "kv.carts.key", "invalid"},
		{"kv replicas", func(s *Spec) { k := s.KV["carts"]; k.Replicas = 6; s.KV["carts"] = k }, "kv.carts.replicas", "1 and 5"},
		{"schedule stream", func(s *Spec) {
			x := s.Schedules["nightly-cart-sweep"]
			x.Stream = "NOPE"
			s.Schedules["nightly-cart-sweep"] = x
		}, "schedules.nightly-cart-sweep.stream", "unknown stream"},
		{"schedule not enabled", func(s *Spec) { x := s.Streams["CARTS"]; x.AllowMsgSchedules = false; s.Streams["CARTS"] = x }, "schedules.nightly-cart-sweep.stream", "allow_msg_schedules"},
		{"schedule event", func(s *Spec) {
			x := s.Schedules["nightly-cart-sweep"]
			x.Event = "nope"
			s.Schedules["nightly-cart-sweep"] = x
		}, "schedules.nightly-cart-sweep.event", "unknown event"},
		{"schedule event elsewhere", func(s *Spec) {
			x := s.Schedules["nightly-cart-sweep"]
			x.Event = "order.placed"
			s.Schedules["nightly-cart-sweep"] = x
		}, "schedules.nightly-cart-sweep.event", "not in stream"},
		{"schedule target with params", func(s *Spec) {
			e := s.Events["cart.sweep"]
			e.Subject = "carts.sweep.{id}"
			s.Events["cart.sweep"] = e
		}, "schedules.nightly-cart-sweep.event", "params"},
		{"schedule target same", func(s *Spec) {
			x := s.Schedules["nightly-cart-sweep"]
			x.Subject = "carts.sweep"
			s.Schedules["nightly-cart-sweep"] = x
		}, "schedules.nightly-cart-sweep.subject", "differ"},
		{"schedule uncovered", func(s *Spec) {
			x := s.Schedules["nightly-cart-sweep"]
			x.Subject = "other.thing"
			s.Schedules["nightly-cart-sweep"] = x
		}, "schedules.nightly-cart-sweep.subject", "not covered"},
		{"schedule both", func(s *Spec) {
			x := s.Schedules["nightly-cart-sweep"]
			x.Every = Duration{Duration: time.Minute}
			s.Schedules["nightly-cart-sweep"] = x
		}, "schedules.nightly-cart-sweep", "only one"},
		{"schedule every short", func(s *Spec) {
			x := s.Schedules["nightly-cart-sweep"]
			x.Cron, x.TimeZone, x.Every = "", "", Duration{Duration: time.Millisecond}
			s.Schedules["nightly-cart-sweep"] = x
		}, "schedules.nightly-cart-sweep.every", "1s"},
		{"schedule tz without cron", func(s *Spec) {
			x := s.Schedules["nightly-cart-sweep"]
			x.Cron, x.Every = "", Duration{Duration: time.Minute}
			s.Schedules["nightly-cart-sweep"] = x
		}, "schedules.nightly-cart-sweep.time_zone", "cron"},
		{"schedule payload", func(s *Spec) {
			x := s.Schedules["nightly-cart-sweep"]
			x.Payload = map[string]any{"nope": 1}
			s.Schedules["nightly-cart-sweep"] = x
		}, "schedules.nightly-cart-sweep.payload.nope", "not a field"},
		{"endpoint subject", func(s *Spec) {
			s.Requests["orders"].Endpoints["Get"] = Endpoint{Subject: "a b", Request: "GetOrder", Reply: "Order"}
		}, "requests.orders.endpoints.Get.subject", "invalid subject"},
		{"endpoint models", func(s *Spec) {
			s.Requests["orders"].Endpoints["Get"] = Endpoint{Subject: "a.b", Request: "X", Reply: "Y"}
		}, "requests.orders.endpoints.Get.request", "unknown model"},
		{"endpoint reply", func(s *Spec) {
			s.Requests["orders"].Endpoints["Get"] = Endpoint{Subject: "a.b", Request: "GetOrder", Reply: "Y"}
		}, "requests.orders.endpoints.Get.reply", "unknown model"},
		{"endpoint name", func(s *Spec) {
			s.Requests["orders"].Endpoints["get-it"] = Endpoint{Subject: "a.b", Request: "GetOrder", Reply: "Order"}
		}, "requests.orders.endpoints.get-it", "Go identifier"},
		{"endpoint error name", func(s *Spec) {
			s.Requests["orders"].Endpoints["Get"] = Endpoint{Subject: "a.b", Request: "GetOrder", Reply: "Order", Errors: []string{"not found"}}
		}, "requests.orders.endpoints.Get.errors", "Go identifier"},
		{"endpoint overlaps event", func(s *Spec) {
			s.Requests["orders"].Endpoints["Get"] = Endpoint{Subject: "carts.sweep", Request: "GetOrder", Reply: "Order"}
		}, "requests.orders.endpoints.Get.subject", "overlap"},
		{"service publishes", func(s *Spec) { x := s.Services["checkout"]; x.Publishes = []string{"nope"}; s.Services["checkout"] = x }, "services.checkout.publishes", "unknown event"},
		{"service subscribes", func(s *Spec) {
			x := s.Services["checkout"]
			x.Subscribes = []string{"nope"}
			s.Services["checkout"] = x
		}, "services.checkout.subscribes", "unknown event"},
		{"service subscribes stream event", func(s *Spec) {
			x := s.Services["checkout"]
			x.Subscribes = []string{"order.placed"}
			s.Services["checkout"] = x
		}, "services.checkout.subscribes", "use a consumer"},
		{"service consumes", func(s *Spec) { x := s.Services["billing"]; x.Consumes = []string{"nope"}; s.Services["billing"] = x }, "services.billing.consumes", "unknown consumer"},
		{"service kv", func(s *Spec) {
			x := s.Services["checkout"]
			x.KV = map[string]string{"nope": "read"}
			s.Services["checkout"] = x
		}, "services.checkout.kv", "unknown bucket"},
		{"service kv access", func(s *Spec) {
			x := s.Services["checkout"]
			x.KV = map[string]string{"carts": "all"}
			s.Services["checkout"] = x
		}, "services.checkout.kv.carts", "read"},
		{"service serves", func(s *Spec) {
			x := s.Services["orders-api"]
			x.Serves = []string{"nope"}
			s.Services["orders-api"] = x
		}, "services.orders-api.serves", "unknown request group"},
		{"service calls", func(s *Spec) {
			x := s.Services["checkout"]
			x.Calls = []string{"orders.Nope"}
			s.Services["checkout"] = x
		}, "services.checkout.calls", "unknown endpoint"},
		{"service schedules", func(s *Spec) { x := s.Services["checkout"]; x.Schedules = []string{"nope"}; s.Services["checkout"] = x }, "services.checkout.schedules", "unknown schedule"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := loadShop(t)
			c.mutate(s)
			for _, e := range Validate(s) {
				if e.Path == c.path && strings.Contains(e.Message, c.msg) {
					return
				}
			}
			t.Fatalf("no error at %q containing %q; got %v", c.path, c.msg, Validate(s))
		})
	}
}

func TestValidateAcceptsLimitValues(t *testing.T) {
	s := loadShop(t)
	x := s.Streams["ORDERS"]
	x.Replicas = 5
	s.Streams["ORDERS"] = x
	k := s.KV["carts"]
	k.Replicas = 0
	s.KV["carts"] = k
	sc := s.Schedules["nightly-cart-sweep"]
	sc.Cron, sc.TimeZone, sc.Every = "", "", Duration{Duration: time.Second}
	s.Schedules["nightly-cart-sweep"] = sc
	if errs := Validate(s); len(errs) != 0 {
		t.Fatalf("errors = %v", errs)
	}
}

func TestSubjectParamNames(t *testing.T) {
	for _, good := range []string{"a.{b0}", "a.{b9}", "a.{_b}", "a.{bz}", "a.{Az}", "a.{Zz}", "a.{az}"} {
		if _, err := ParseSubject(good); err != nil {
			t.Errorf("ParseSubject(%q): %v", good, err)
		}
	}
	for _, bad := range []string{"a.{0b}", "a.{9b}", "a.{b/}", "a.{b:}", "a.{b@}", "a.{b[}", "a.{b`}", "a.{b{}"} {
		if _, err := ParseSubject(bad); err == nil {
			t.Errorf("ParseSubject(%q) accepted", bad)
		}
	}
}

func TestValidationErrorsAreSortedAndPrintable(t *testing.T) {
	s := loadShop(t)
	s.Name, s.Encoding = "", "xml"
	errs := Validate(s)
	if len(errs) != 2 || errs[0].Path != "encoding" || errs[1].Path != "name" {
		t.Fatalf("errs = %v", errs)
	}
	if got := errs[1].Error(); got != "name: is required" {
		t.Errorf("Error() = %q", got)
	}
}

func TestParseSubject(t *testing.T) {
	sub, err := ParseSubject("orders.{region}.placed.{id}")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sub.Params(), []string{"region", "id"}) {
		t.Errorf("params = %v", sub.Params())
	}
	if got := sub.Wildcard(); got != "orders.*.placed.*" {
		t.Errorf("wildcard = %q", got)
	}
	if !reflect.DeepEqual(sub.ParamTokens(), []int{1, 3}) {
		t.Errorf("tokens = %v", sub.ParamTokens())
	}
	for _, bad := range []string{"", "a..b", ".a", "a.", "a b", "a.{b", "a.{}", "a.{b}{c}", "a.x{b}", "a.{1b}", "a.{b}.{b}", "a.{b-c}"} {
		if _, err := ParseSubject(bad); err == nil {
			t.Errorf("ParseSubject(%q) accepted", bad)
		}
	}
	if _, err := ParseSubject("a.*.>"); err != nil {
		t.Errorf("wildcards should parse: %v", err)
	}
}

func TestSubjectsOverlap(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"a.b", "a.b", true},
		{"a.b", "a.c", false},
		{"a.*", "a.b", true},
		{"a.>", "a.b.c", true},
		{"a.b.c", "a.>", true},
		{"a.>", "a", false},
		{"a", "a.b", false},
		{"a.*", "a.b.c", false},
		{"*.b", "a.*", true},
		{"a.b", "c.b", false},
		{">", "x", true},
	}
	for _, c := range cases {
		if got := SubjectsOverlap(c.a, c.b); got != c.want {
			t.Errorf("SubjectsOverlap(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestSubjectCovers(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"a.b", "a.b", true},
		{"a.*", "a.b", true},
		{"a.*", "a.b.c", false},
		{"a.>", "a.b.c", true},
		{"a.>", "a", false},
		{"a.b", "a.c", false},
		{"a.b.c", "a.b", false},
	}
	for _, c := range cases {
		if got := SubjectCovers(c.pattern, c.subject); got != c.want {
			t.Errorf("SubjectCovers(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
}

func TestStreamOfEventAndSubjects(t *testing.T) {
	s := loadShop(t)
	if got := s.StreamOf("order.placed"); got != "ORDERS" {
		t.Errorf("StreamOf = %q", got)
	}
	if got := s.StreamOf("price.changed"); got != "" {
		t.Errorf("core event has stream %q", got)
	}
	want := []string{"carts.sweep", "carts.schedules.>"}
	if got := s.StreamSubjects("CARTS"); !reflect.DeepEqual(got, want) {
		t.Errorf("StreamSubjects = %v, want %v", got, want)
	}
}

// jsonFields lists a struct's JSON field names, which are the NATS API names.
func jsonFields(typ reflect.Type) []string {
	var out []string
	for i := range typ.NumField() {
		tag, _, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
		if tag != "" && tag != "-" {
			out = append(out, tag)
		}
	}
	return out
}

func yamlFields(typ reflect.Type) map[string]bool {
	out := map[string]bool{}
	for i := range typ.NumField() {
		tag, _, _ := strings.Cut(typ.Field(i).Tag.Get("yaml"), ",")
		out[tag] = true
	}
	return out
}

// These tests fail when a nats.go upgrade adds a setting the spec lacks.
func TestSpecCoversEveryNATSSetting(t *testing.T) {
	renamed := map[string]string{"num_replicas": "replicas", "durable_name": "durable", "mem_storage": "memory_storage"}
	cases := []struct {
		name    string
		nats    reflect.Type
		ours    reflect.Type
		skipped map[string]bool
	}{
		{"stream", reflect.TypeFor[jetstream.StreamConfig](), reflect.TypeFor[Stream](),
			map[string]bool{"name": true, "template_owner": true}},
		{"consumer", reflect.TypeFor[jetstream.ConsumerConfig](), reflect.TypeFor[Consumer](),
			map[string]bool{"name": true}},
		{"kv", reflect.TypeFor[jetstream.KeyValueConfig](), reflect.TypeFor[KVBucket](),
			map[string]bool{"bucket": true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ours := yamlFields(c.ours)
			for _, f := range jsonFields(c.nats) {
				if c.skipped[f] {
					continue
				}
				if r, ok := renamed[f]; ok {
					f = r
				}
				if !ours[f] {
					t.Errorf("spec has no field for NATS setting %q", f)
				}
			}
		})
	}
}

func TestInfraMapsStreamSettings(t *testing.T) {
	s := loadShop(t)
	x := s.Streams["ORDERS"]
	x.Description = "d"
	x.Retention = "workqueue"
	x.Storage = "memory"
	x.Discard = "new"
	x.Compression = "s2"
	x.PersistMode = "async"
	x.MaxMsgs, x.MaxBytes, x.MaxConsumers, x.MaxMsgsPerSubject, x.MaxMsgSize = 1, 2, 3, 4, 5
	x.DiscardNewPerSubject, x.NoAck, x.Sealed, x.DenyPurge, x.AllowRollup = true, true, true, true, true
	x.AllowDirect, x.MirrorDirect, x.AllowMsgTTL, x.AllowMsgCounter, x.AllowAtomic, x.AllowBatched = true, true, true, true, true, true
	x.FirstSeq = 9
	x.SubjectDeleteMarkerTTL = Duration{Duration: time.Hour}
	x.Placement = &Placement{Cluster: "c", Tags: []string{"t"}, Preferred: "p"}
	x.SubjectTransform = &SubjectTransform{Source: "a", Destination: "b"}
	x.RePublish = &RePublish{Source: "a", Destination: "b", HeadersOnly: true}
	x.ConsumerLimits = &ConsumerLimits{InactiveThreshold: Duration{Duration: time.Minute}, MaxAckPending: 7}
	x.Metadata = map[string]string{"k": "v"}
	x.Mirror = &StreamSource{Name: "M", FilterSubject: "f", StartSeq: 2}
	x.Sources = []StreamSource{{Name: "S"}}
	s.Streams["ORDERS"] = x

	got := s.Infra().Streams[1]
	if got.Name != "ORDERS" {
		t.Fatalf("streams are not sorted by name: %s", got.Name)
	}
	if got.Retention != jetstream.WorkQueuePolicy || got.Storage != jetstream.MemoryStorage ||
		got.Discard != jetstream.DiscardNew || got.Compression != jetstream.S2Compression {
		t.Errorf("enums wrong: %+v", got)
	}
	if got.PersistMode != jetstream.AsyncPersistMode {
		t.Errorf("persist = %v", got.PersistMode)
	}
	if got.MaxMsgs != 1 || got.MaxBytes != 2 || got.MaxConsumers != 3 || got.MaxMsgsPerSubject != 4 || got.MaxMsgSize != 5 {
		t.Errorf("limits wrong: %+v", got)
	}
	if !got.DiscardNewPerSubject || !got.NoAck || !got.Sealed || !got.DenyDelete || !got.DenyPurge || !got.AllowRollup {
		t.Errorf("bools wrong: %+v", got)
	}
	if !got.AllowDirect || !got.MirrorDirect || !got.AllowMsgTTL || !got.AllowMsgCounter || !got.AllowAtomicPublish || !got.AllowBatchPublish {
		t.Errorf("allow flags wrong: %+v", got)
	}
	if got.Description != "d" || got.FirstSeq != 9 || got.MaxAge != 720*time.Hour || got.Duplicates != 2*time.Minute || got.SubjectDeleteMarkerTTL != time.Hour {
		t.Errorf("values wrong: %+v", got)
	}
	if got.Placement == nil || got.Placement.Cluster != "c" || got.Placement.Tags[0] != "t" {
		t.Errorf("placement = %+v", got.Placement)
	}
	if got.SubjectTransform == nil || got.SubjectTransform.Destination != "b" || got.SubjectTransform.Source != "a" {
		t.Errorf("transform = %+v", got.SubjectTransform)
	}
	if got.RePublish == nil || !got.RePublish.HeadersOnly || got.RePublish.Destination != "b" || got.RePublish.Source != "a" {
		t.Errorf("republish = %+v", got.RePublish)
	}
	if got.ConsumerLimits.InactiveThreshold != time.Minute || got.ConsumerLimits.MaxAckPending != 7 {
		t.Errorf("limits = %+v", got.ConsumerLimits)
	}
	if got.Metadata["k"] != "v" {
		t.Errorf("metadata = %v", got.Metadata)
	}
	if got.Mirror == nil || got.Mirror.Name != "M" || got.Mirror.FilterSubject != "f" || got.Mirror.OptStartSeq != 2 {
		t.Errorf("mirror = %+v", got.Mirror)
	}
	if len(got.Sources) != 1 || got.Sources[0].Name != "S" {
		t.Errorf("sources = %+v", got.Sources)
	}
	if !reflect.DeepEqual(got.Subjects, []string{"orders.placed.*", "orders.shipped.*"}) {
		t.Errorf("subjects = %v", got.Subjects)
	}
}

func TestInfraEnablesRollupForScheduleStreams(t *testing.T) {
	got := loadShop(t).Infra().Streams[0]
	if got.Name != "CARTS" || !got.AllowMsgSchedules || !got.AllowRollup {
		t.Fatalf("schedule stream = %+v", got)
	}
}

func TestInfraMapsConsumerSettings(t *testing.T) {
	s := loadShop(t)
	c := s.Consumers["billing-orders"]
	c.Description = "d"
	c.AckWait = Duration{Duration: 30 * time.Second}
	c.AckPolicy = "all"
	c.DeliverPolicy = "new"
	c.ReplayPolicy = "original"
	c.PriorityPolicy = "pinned_client"
	c.OptStartSeq = 3
	c.RateLimit, c.SampleFreq, c.MaxWaiting = 4, "10%", 5
	c.HeadersOnly, c.MemoryStorage, c.FlowControl = true, true, true
	c.MaxBatch, c.MaxBytes = 6, 7
	c.MaxExpires = Duration{Duration: time.Second}
	c.InactiveThreshold = Duration{Duration: 2 * time.Second}
	c.IdleHeartbeat = Duration{Duration: 3 * time.Second}
	c.PriorityTimeout = Duration{Duration: 4 * time.Second}
	c.PriorityGroups = []string{"g"}
	c.DeliverSubject, c.DeliverGroup = "ds", "dg"
	c.Metadata = map[string]string{"k": "v"}
	c.Replicas = 3
	s.Consumers["billing-orders"] = c

	got := s.Infra().Consumers[0]
	if got.Stream != "ORDERS" || got.Config.Durable != "billing-orders" {
		t.Fatalf("binding wrong: %+v", got)
	}
	cfg := got.Config
	if cfg.AckPolicy != jetstream.AckAllPolicy || cfg.DeliverPolicy != jetstream.DeliverNewPolicy ||
		cfg.ReplayPolicy != jetstream.ReplayOriginalPolicy || cfg.PriorityPolicy != jetstream.PriorityPolicyPinned {
		t.Errorf("enums: %+v", cfg)
	}
	if cfg.FilterSubject != "orders.placed.*" || cfg.FilterSubjects != nil {
		t.Errorf("one event should use filter_subject: %q %v", cfg.FilterSubject, cfg.FilterSubjects)
	}
	if cfg.AckWait != 30*time.Second || cfg.MaxDeliver != 5 || cfg.MaxAckPending != 500 || cfg.Replicas != 3 {
		t.Errorf("values: %+v", cfg)
	}
	if len(cfg.BackOff) != 4 || cfg.BackOff[0] != 30*time.Second {
		t.Errorf("backoff: %v", cfg.BackOff)
	}
	if cfg.Description != "d" || cfg.OptStartSeq != 3 || cfg.RateLimit != 4 || cfg.SampleFrequency != "10%" || cfg.MaxWaiting != 5 {
		t.Errorf("values: %+v", cfg)
	}
	if !cfg.HeadersOnly || !cfg.MemoryStorage || !cfg.FlowControl || cfg.MaxRequestBatch != 6 || cfg.MaxRequestMaxBytes != 7 {
		t.Errorf("flags: %+v", cfg)
	}
	if cfg.MaxRequestExpires != time.Second || cfg.InactiveThreshold != 2*time.Second || cfg.IdleHeartbeat != 3*time.Second || cfg.PinnedTTL != 4*time.Second {
		t.Errorf("durations: %+v", cfg)
	}
	if cfg.PriorityGroups[0] != "g" || cfg.DeliverSubject != "ds" || cfg.DeliverGroup != "dg" || cfg.Metadata["k"] != "v" {
		t.Errorf("rest: %+v", cfg)
	}
}

func TestInfraUsesFilterSubjectsForSeveralEvents(t *testing.T) {
	got := loadShop(t).Infra().Consumers[1].Config
	want := []string{"orders.placed.*", "orders.shipped.*"}
	if got.FilterSubject != "" || !reflect.DeepEqual(got.FilterSubjects, want) {
		t.Fatalf("filters = %q %v", got.FilterSubject, got.FilterSubjects)
	}
}

func TestInfraMapsBuckets(t *testing.T) {
	s := loadShop(t)
	k := s.KV["carts"]
	k.Description, k.Storage, k.Compression = "d", "memory", true
	k.MaxValueSize, k.MaxBytes = 1, 2
	k.LimitMarkerTTL = Duration{Duration: time.Hour}
	k.Metadata = map[string]string{"k": "v"}
	s.KV["carts"] = k
	got := s.Infra().Buckets[0]
	if got.Bucket != "carts" || got.History != 5 || got.TTL != 24*time.Hour || got.Replicas != 1 {
		t.Errorf("bucket: %+v", got)
	}
	if got.Description != "d" || got.Storage != jetstream.MemoryStorage || !got.Compression || got.MaxValueSize != 1 || got.MaxBytes != 2 || got.LimitMarkerTTL != time.Hour || got.Metadata["k"] != "v" {
		t.Errorf("bucket settings: %+v", got)
	}
}

func TestInfraRoundTripsThroughJSON(t *testing.T) {
	want := loadShop(t).Infra()
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got = want
	got.Streams = nil
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Streams[1].MaxAge != want.Streams[1].MaxAge {
		t.Fatalf("got %+v", got.Streams[1])
	}
}
