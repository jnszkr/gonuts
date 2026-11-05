# gonuts

One YAML file describes everything your system does on NATS. A CLI turns it into typed Go code and Terraform.

Streams, subjects, consumers, key-value buckets, schedules and request-reply all live in `nuts.yaml`, so you can see who does what without reading code.

gonuts is early and its API may change.

## Install

```bash
go install github.com/jnszkr/gonuts/cmd/gonuts@latest
```

Requires Go 1.26. The JetStream features need NATS Server 2.11 for the Terraform provider, 2.12 for delayed messages and 2.14 for cron and `@every` schedules.

## A taste

```yaml
version: 1
name: shop
go:
  package: shopnats

models:
  OrderPlaced:
    fields:
      id: uuid
      total_cents: int64

events:
  order.placed:
    subject: orders.placed.{region}
    model: OrderPlaced

streams:
  ORDERS:
    events: [order.placed]
    max_age: 720h
    replicas: 3

consumers:
  billing-orders:
    stream: ORDERS
    events: [order.placed]
    max_deliver: 5
    backoff: [30s, 1m, 5m]

services:
  checkout:
    publishes: [order.placed]
  billing:
    consumes: [billing-orders]
```

```bash
gonuts generate
```

```go
app, err := shopnats.Connect(ctx, natsURL)

// The region argument comes from orders.placed.{region}
ack, err := app.Events.PublishOrderPlaced(ctx, "eu", models.OrderPlaced{ID: id, TotalCents: 4200})

// Implement one method per event the consumer reads
type billing struct{}

func (billing) OnOrderPlaced(ctx context.Context, m consumers.OrderPlacedMsg) error {
	return charge(ctx, m.Data, m.Params.Region)
}

stop, err := app.Consumers.BillingOrders.Run(ctx, billing{})
defer stop()
```

[`examples/shop`](examples/shop) has a complete spec, its generated output and tests that run it against a real server.

## Generate what you need

With no component flag, `gonuts generate` writes all the Go code and no Terraform. Any component flag narrows it to what you asked for, plus the models that output needs.

| Flag | Generates |
| --- | --- |
| `--models` | Go structs |
| `--pubsub` | Publishers and core NATS subscribers |
| `--consumers` | JetStream consumer handlers |
| `--kv` | Typed key-value buckets |
| `--schedules` | Delayed publishing and recurring schedule setup |
| `--rr-client` | Request functions |
| `--rr-server` | Handler interfaces and server wiring |
| `--terraform` | Streams, consumers and buckets for the NATS JetStream provider |
| `--permissions` | NATS server permissions for each service |

```bash
gonuts generate --rr-client --models
gonuts generate --terraform --terraform-out ./infra/nats
gonuts generate --permissions --permissions-out ./infra/nats
gonuts generate --service billing
```

Other flags are `--spec`, `--out`, `--import-path` and `--check`. Run `gonuts help` for everything.

## How it behaves

**Handlers decide what happens to a message.**

| Handler does | NATS action |
| --- | --- |
| Returns `nil` | Ack |
| Returns an error | Nak, delayed by the `backoff` list |
| Returns `nuts.Terminate(err)` | Term, never redelivered |
| Panics | Recovers, reports to plugins, then naks |
| Payload cannot be decoded | Term, and reports to plugins |

NATS replaces `ack_wait` with the first `backoff` entry, so the validator rejects setting both.

**Request-reply is one entry.** A request, its reply and its errors sit together, and the client and server are separate packages.

```yaml
requests:
  orders:
    timeout: 2s
    queue_group: orders
    endpoints:
      Get:
        subject: orders.api.get
        request: GetOrder
        reply: Order
        errors: [NotFound]
```

```go
// Client
order, err := app.Orders.Get(ctx, models.GetOrder{ID: id})

var notFound *orders.NotFoundError
if errors.As(err, &notFound) { /* ... */ }

// Server
srv, err := orders.Serve(ctx, app.Runtime(), myHandler{})
```

Servers use the `nats.go` micro framework, so they get discovery, stats and queue groups. Undeclared errors reach the client as a generic internal error, so details do not leak.

**Applications never change infrastructure.** Generated code only binds to what exists, which keeps blue-green deployments safe. The flow is:

1. Change `nuts.yaml`.
2. Run `gonuts diff old.yaml new.yaml` to see the effect on services still running the old version.
3. Run `gonuts generate --terraform` and apply it.
4. Run `gonuts schedules sync --server URL` if schedules changed. A recurring schedule is a stored message, so Terraform cannot create it.
5. Deploy the new version.

Pass `nuts.WithVerify(nuts.VerifyFail)` to stop at startup if the server differs from the spec, or `nuts.VerifyWarn` to log the differences. Both only read. For tests and local development, `nuts/devnats` creates everything.

**The server can enforce the spec.** `gonuts generate --permissions` turns the `services` block into a server config file with one variable per service. Each one allows what that service declares and nothing else, so it cannot publish an event it does not own, write to a bucket it only reads, or create or delete a stream. Users and credentials stay in your own server config:

```
include "permissions.conf"

authorization {
  users: [
    { user: billing, password: $BILLING_PASSWORD, permissions: $BILLING_PERMISSIONS }
  ]
}
```

Generate the code with `--service` as well, so a service only holds the calls it is allowed to make. Terraform and `gonuts schedules sync` need their own user with wider access. Three things to know:

- Watching a bucket is an ephemeral consumer, so read access can create and delete consumers on that bucket's own stream.
- Replies use the shared `_INBOX.>` prefix. To keep services from reading each other's replies, give each one a prefix with `nats.CustomInboxPrefix` and tighten its permissions to match.
- While an old and a new version run side by side, grant what both need. `gonuts diff` flags a changed service for this reason.

**Plugins wrap every generated call**, so tracing and metrics need no helper code.

```go
type Plugin interface {
	Wrap(next nuts.Handler) nuts.Handler
}

app, err := shopnats.Connect(ctx, natsURL, nuts.WithPlugins(myTracer{}))
```

Each call exposes its kind, YAML name, subject, service and headers. Trace context travels in the headers from publisher to consumer.

## Commands

| Command | Purpose |
| --- | --- |
| `gonuts init` | Write a starter `nuts.yaml` |
| `gonuts validate [--json]` | Check the spec |
| `gonuts generate` | Generate code |
| `gonuts graph` | Print a Mermaid map of who talks to whom |
| `gonuts diff [--json] OLD NEW` | Classify a spec change; exits 1 if destructive |
| `gonuts schedules sync --server URL` | Store recurring schedules |

## Not here yet

- Published OpenTelemetry and Datadog plugins
- MQTT and other server-level protocol settings
- Generated Go for object stores
- Encodings other than JSON

## Contributing

`make check` runs what CI requires, and `make mutate` runs Gremlins mutation testing. See [AGENTS.md](AGENTS.md) for project rules, which apply to people and AI agents alike.

## License

Apache-2.0
