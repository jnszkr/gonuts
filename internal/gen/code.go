package gen

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/jnszkr/gonuts/internal/spec"
)

const (
	importContext   = `"context"`
	importTime      = `"time"`
	importJSON      = `"encoding/json"`
	importNuts      = `"github.com/jnszkr/gonuts/nuts"`
	importJetStream = `"github.com/nats-io/nats.go/jetstream"`
)

func (g *generator) models() error {
	names := g.usedModels()
	if len(names) == 0 {
		return nil
	}
	var b strings.Builder
	var imports []string
	for _, name := range names {
		m := g.s.Models[name]
		if m.Description != "" {
			fmt.Fprintf(&b, "// %s: %s\n", name, comment(m.Description))
		} else {
			fmt.Fprintf(&b, "// %s is generated from the %s model.\n", name, name)
		}
		fmt.Fprintf(&b, "type %s struct {\n", name)
		for _, f := range m.Fields {
			writeField(&b, f, &imports)
		}
		b.WriteString("}\n\n")
	}
	return g.write("models/models.go", "models", imports, "// Package models holds the message types.\n"+b.String())
}

func writeField(b *strings.Builder, f *spec.Field, imports *[]string) {
	t := goType(f.Type)
	if strings.Contains(t, "time.") && !slices.Contains(*imports, importTime) {
		*imports = append(*imports, importTime)
	}
	tag := f.Name
	if f.Optional {
		tag += ",omitempty"
		if !strings.HasPrefix(t, "[]") && !strings.HasPrefix(t, "map[") && t != "any" {
			t = "*" + t
		}
	}
	if f.Description != "" {
		fmt.Fprintf(b, "// %s\n", comment(f.Description))
	}
	fmt.Fprintf(b, "%s %s `json:%q`\n", GoName(f.Name), t, tag)
}

func (g *generator) events() error {
	if !g.c.PubSub {
		return nil
	}
	var b strings.Builder
	b.WriteString("// Events publishes and subscribes to events.\ntype Events struct {\nrt *nuts.Runtime\n}\n\n")
	b.WriteString("// New returns an Events.\nfunc New(rt *nuts.Runtime) *Events {\nreturn &Events{rt: rt}\n}\n\n")
	imports := []string{importContext, importNuts, g.modelsImport()}
	for _, e := range g.sc.publishes {
		usesJS, err := g.writePublish(&b, e)
		if err != nil {
			return err
		}
		if usesJS && !slices.Contains(imports, importJetStream) {
			imports = append(imports, importJetStream)
		}
	}
	for _, e := range g.sc.subscribes {
		if err := g.writeSubscribe(&b, e); err != nil {
			return err
		}
	}
	return g.write("events/events.go", "events", imports, "// Package events publishes and subscribes to events.\n"+b.String())
}

func (g *generator) writePublish(b *strings.Builder, event string) (usesJetStream bool, err error) {
	e := g.s.Events[event]
	si, err := parseSubject(e.Subject)
	if err != nil {
		return false, err
	}
	name := GoName(event)
	if st := g.s.StreamOf(event); st != "" {
		setup, expr := si.build("return nil, err")
		fmt.Fprintf(b, "// Publish%s stores a %s event in stream %s and waits for the ack.\n", name, event, st)
		fmt.Fprintf(b, "func (e *Events) Publish%s(ctx context.Context, %sv models.%s, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {\n", name, si.argList(), e.Model)
		fmt.Fprintf(b, "%sreturn nuts.PublishStream(ctx, e.rt, %q, %s, v, opts...)\n}\n\n", setup, event, expr)
		return true, nil
	}
	setup, expr := si.build("return err")
	fmt.Fprintf(b, "// Publish%s sends a %s event over core NATS.\n", name, event)
	fmt.Fprintf(b, "func (e *Events) Publish%s(ctx context.Context, %sv models.%s) error {\n", name, si.argList(), e.Model)
	fmt.Fprintf(b, "%sreturn nuts.PublishCore(ctx, e.rt, %q, %s, v)\n}\n\n", setup, event, expr)
	return false, nil
}

func (g *generator) writeSubscribe(b *strings.Builder, event string) error {
	si, err := parseSubject(g.s.Events[event].Subject)
	if err != nil {
		return err
	}
	if err := writeParams(b, g, event); err != nil {
		return err
	}
	name := GoName(event)
	fmt.Fprintf(b, "// Subscribe%s runs h for each %s event.\n", name, event)
	fmt.Fprintf(b, "func (e *Events) Subscribe%s(ctx context.Context, h func(context.Context, %sMsg) error, opts ...nuts.SubOption) (stop func(), err error) {\n", name, name)
	fmt.Fprintf(b, "return nuts.Subscribe(ctx, e.rt, nuts.NewRoute(%q, %q, %sParamsFrom, h), opts...)\n}\n\n", event, si.filter, name)
	return nil
}

func (g *generator) consumers() error {
	if !g.c.Consumers {
		return nil
	}
	var pull []string
	for _, c := range g.sc.consumers {
		if x := g.s.Consumers[c]; x.DeliverSubject == "" && len(x.Events) > 0 {
			pull = append(pull, c)
		}
	}
	var b strings.Builder
	written := map[string]bool{}
	for _, c := range pull {
		for _, e := range g.s.Consumers[c].Events {
			if !written[e] {
				written[e] = true
				if err := writeParams(&b, g, e); err != nil {
					return err
				}
			}
		}
	}
	b.WriteString("// Consumers holds each durable consumer.\ntype Consumers struct {\n")
	for _, c := range pull {
		fmt.Fprintf(&b, "%s *%s\n", GoName(c), GoName(c))
	}
	b.WriteString("}\n\n// New returns the consumers.\nfunc New(rt *nuts.Runtime) *Consumers {\nreturn &Consumers{\n")
	for _, c := range pull {
		fmt.Fprintf(&b, "%s: &%s{rt: rt},\n", GoName(c), GoName(c))
	}
	b.WriteString("}\n}\n\n")
	usesTime := false
	for _, c := range pull {
		if err := g.writeConsumer(&b, c); err != nil {
			return err
		}
		usesTime = usesTime || len(g.s.Consumers[c].BackOff) > 0
	}
	imports := []string{importNuts}
	if len(pull) > 0 {
		imports = append(imports, importContext, g.modelsImport())
	}
	if usesTime {
		imports = append(imports, importTime)
	}
	return g.write("consumers/consumers.go", "consumers", imports, "// Package consumers runs handlers on durable JetStream consumers.\n"+b.String())
}

func (g *generator) writeConsumer(b *strings.Builder, name string) error {
	c := g.s.Consumers[name]
	typeName := GoName(name)
	fmt.Fprintf(b, "// %sHandler handles the events consumer %s reads. Return nil to ack, an\n// error to retry, or nuts.Terminate(err) to give up on the message.\ntype %sHandler interface {\n", typeName, name, typeName)
	for _, e := range c.Events {
		fmt.Fprintf(b, "On%s(ctx context.Context, m %sMsg) error\n", GoName(e), GoName(e))
	}
	b.WriteString("}\n\n")
	fmt.Fprintf(b, "// %s is the %s durable consumer on stream %s.\ntype %s struct {\nrt *nuts.Runtime\n}\n\n", typeName, name, c.Stream, typeName)
	fmt.Fprintf(b, "// Run binds to the existing consumer and runs h until stop is called.\nfunc (c *%s) Run(ctx context.Context, h %sHandler) (stop func(), err error) {\n", typeName, typeName)
	backoff := "nil"
	if len(c.BackOff) > 0 {
		parts := make([]string, len(c.BackOff))
		for i, d := range c.BackOff {
			parts[i] = goDuration(d.Duration)
		}
		backoff = "[]time.Duration{" + strings.Join(parts, ", ") + "}"
	}
	fmt.Fprintf(b, "return nuts.Consume(ctx, c.rt, nuts.ConsumeConfig{Stream: %q, Consumer: %q, BackOff: %s}, []nuts.Route{\n", c.Stream, name, backoff)
	for _, e := range c.Events {
		si, err := parseSubject(g.s.Events[e].Subject)
		if err != nil {
			return err
		}
		fmt.Fprintf(b, "nuts.NewRoute(%q, %q, %sParamsFrom, h.On%s),\n", e, si.filter, GoName(e), GoName(e))
	}
	b.WriteString("})\n}\n\n")
	return nil
}

func (g *generator) kv() error {
	if !g.c.KV {
		return nil
	}
	names := spec.SortedKeys(g.sc.buckets)
	var b strings.Builder
	b.WriteString("// KV holds each key-value bucket.\ntype KV struct {\n")
	for _, n := range names {
		fmt.Fprintf(&b, "%s *%s\n", GoName(n), GoName(n))
	}
	b.WriteString("}\n\n// New returns the buckets. They bind on first use.\nfunc New(rt *nuts.Runtime) *KV {\nreturn &KV{\n")
	for _, n := range names {
		fmt.Fprintf(&b, "%s: &%s{b: nuts.NewBucket[models.%s](rt, %q)},\n", GoName(n), GoName(n), g.s.KV[n].Model, n)
	}
	b.WriteString("}\n}\n\n")
	for _, n := range names {
		if err := g.writeBucket(&b, n, g.sc.buckets[n] != "read"); err != nil {
			return err
		}
	}
	imports := []string{importContext, importNuts, g.modelsImport()}
	return g.write("kv/kv.go", "kv", imports, "// Package kv holds typed key-value buckets.\n"+b.String())
}

func (g *generator) writeBucket(b *strings.Builder, name string, writable bool) error {
	k := g.s.KV[name]
	si, err := parseSubject(k.Key)
	if err != nil {
		return err
	}
	t := GoName(name)
	model := "models." + k.Model
	args := si.argList()
	fmt.Fprintf(b, "// %s is the %s bucket, holding %s values.\ntype %s struct {\nb *nuts.Bucket[%s]\n}\n\n", t, name, k.Model, t, model)
	method := func(sig, zero, call string) {
		setup, expr := si.build("return " + zero + "err")
		fmt.Fprintf(b, "func (b *%s) %s {\n%sreturn b.b.%s\n}\n\n", t, sig, setup, strings.ReplaceAll(call, "KEY", expr))
	}
	fmt.Fprintf(b, "// Get returns a value and its revision.\n")
	method(fmt.Sprintf("Get(ctx context.Context, %s) (*%s, uint64, error)", strings.TrimSuffix(args, ", "), model), "nil, 0, ", "Get(ctx, KEY)")
	if writable {
		fmt.Fprintf(b, "// Put writes a value and returns its revision.\n")
		method(fmt.Sprintf("Put(ctx context.Context, %sv %s) (uint64, error)", args, model), "0, ", "Put(ctx, KEY, v)")
		fmt.Fprintf(b, "// Create writes a value only if the key does not exist.\n")
		method(fmt.Sprintf("Create(ctx context.Context, %sv %s) (uint64, error)", args, model), "0, ", "Create(ctx, KEY, v)")
		fmt.Fprintf(b, "// Update writes a value only if the key is still at revision.\n")
		method(fmt.Sprintf("Update(ctx context.Context, %sv %s, revision uint64) (uint64, error)", args, model), "0, ", "Update(ctx, KEY, v, revision)")
		fmt.Fprintf(b, "// Delete removes a key.\n")
		method(fmt.Sprintf("Delete(ctx context.Context, %s) error", strings.TrimSuffix(args, ", ")), "", "Delete(ctx, KEY)")
	}
	fmt.Fprintf(b, "// Watch calls fn with each key's latest value and then with every change.\n")
	fmt.Fprintf(b, "func (b *%s) Watch(ctx context.Context, fn func(nuts.Entry[%s])) (stop func(), err error) {\nreturn b.b.Watch(ctx, fn)\n}\n\n", t, model)
	return nil
}

func (g *generator) schedules() error {
	if !g.c.Schedules {
		return nil
	}
	var b strings.Builder
	b.WriteString("// Schedules publishes delayed and recurring messages.\ntype Schedules struct {\nrt *nuts.Runtime\n}\n\n")
	b.WriteString("// New returns a Schedules.\nfunc New(rt *nuts.Runtime) *Schedules {\nreturn &Schedules{rt: rt}\n}\n\n")
	var sync strings.Builder
	for _, name := range g.sc.schedules {
		x := g.s.Schedules[name]
		ev := g.s.Events[x.Event]
		t := GoName(name)
		fmt.Fprintf(&b, "// %sAt delivers one %s event at the given time.\n", t, x.Event)
		fmt.Fprintf(&b, "func (s *Schedules) %sAt(ctx context.Context, at time.Time, v models.%s) error {\n", t, ev.Model)
		fmt.Fprintf(&b, "return nuts.ScheduleAt(ctx, s.rt, %q, %q, %q, at, v)\n}\n\n", name, x.Subject, ev.Subject)
		if x.Cron == "" && x.Every.Duration == 0 {
			continue
		}
		spec, err := scheduleLiteral(name, x, ev)
		if err != nil {
			return err
		}
		sync.WriteString(spec)
	}
	b.WriteString("// Sync stores every recurring schedule. Run it as a deploy step: a recurring\n// schedule is a stored message, so Terraform cannot create it.\nfunc (s *Schedules) Sync(ctx context.Context) error {\n")
	b.WriteString("for _, spec := range []nuts.ScheduleSpec{\n" + sync.String() + "} {\nif err := nuts.SyncSchedule(ctx, s.rt, spec); err != nil {\nreturn err\n}\n}\nreturn nil\n}\n\n")
	imports := []string{importContext, importTime, importNuts, g.modelsImport()}
	return g.write("schedules/schedules.go", "schedules", imports, "// Package schedules publishes delayed and recurring messages.\n"+b.String())
}

func scheduleLiteral(name string, x spec.Schedule, ev spec.Event) (string, error) {
	payload, err := SchedulePayload(x)
	if err != nil {
		return "", fmt.Errorf("schedule %s: %w", name, err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "{Name: %q, Subject: %q, Target: %q, ", name, x.Subject, ev.Subject)
	if x.Cron != "" {
		fmt.Fprintf(&b, "Cron: %q, ", x.Cron)
	}
	if x.Every.Duration != 0 {
		fmt.Fprintf(&b, "Every: %s, ", goDuration(x.Every.Duration))
	}
	if x.TimeZone != "" {
		fmt.Fprintf(&b, "TimeZone: %q, ", x.TimeZone)
	}
	fmt.Fprintf(&b, "Payload: []byte(%s)},\n", goString(string(payload)))
	return b.String(), nil
}

// SchedulePayload encodes a schedule's YAML payload as the message body.
func SchedulePayload(x spec.Schedule) ([]byte, error) {
	if x.Payload == nil {
		return []byte("{}"), nil
	}
	data, err := json.Marshal(x.Payload)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	return data, nil
}

func writeErrorTypes(b *strings.Builder, codes []string) {
	for _, code := range codes {
		fmt.Fprintf(b, "// %sError is the declared %s error.\ntype %sError struct {\nMessage string\n}\n\n", code, code, code)
		fmt.Fprintf(b, "func (e *%sError) Error() string { return %q + \": \" + e.Message }\n\n", code, code)
		fmt.Fprintf(b, "// ReplyCode returns the code sent on the wire.\nfunc (e *%sError) ReplyCode() string { return %q }\n\n", code, code)
		fmt.Fprintf(b, "// ReplyMessage returns the message sent on the wire.\nfunc (e *%sError) ReplyMessage() string { return e.Message }\n\n", code)
	}
}

func groupErrors(group spec.RequestGroup, endpoints []string) []string {
	seen := map[string]bool{}
	for _, ep := range endpoints {
		for _, code := range group.Endpoints[ep].Errors {
			seen[code] = true
		}
	}
	return spec.SortedKeys(seen)
}

func requestTimeout(group spec.RequestGroup) string {
	if group.Timeout.Duration > 0 {
		return goDuration(group.Timeout.Duration)
	}
	return "5 * time.Second"
}

func (g *generator) clients() error {
	if !g.c.RRClient {
		return nil
	}
	for _, name := range spec.SortedKeys(g.sc.clients) {
		if err := g.writeClient(name); err != nil {
			return err
		}
	}
	return nil
}

func (g *generator) writeClient(name string) error {
	group := g.s.Requests[name]
	endpoints := g.sc.clients[name]
	codes := groupErrors(group, endpoints)
	var b strings.Builder
	b.WriteString("// Client calls the " + name + " endpoints.\ntype Client struct {\nrt *nuts.Runtime\n}\n\n")
	b.WriteString("// New returns a Client.\nfunc New(rt *nuts.Runtime) *Client {\nreturn &Client{rt: rt}\n}\n\n")
	imports := []string{importContext, importTime, importNuts, g.modelsImport()}
	for _, ep := range endpoints {
		e := group.Endpoints[ep]
		fmt.Fprintf(&b, "// %s sends a %s request and returns the %s reply.\n", ep, e.Request, e.Reply)
		fmt.Fprintf(&b, "func (c *Client) %s(ctx context.Context, req models.%s) (*models.%s, error) {\n", ep, e.Request, e.Reply)
		fmt.Fprintf(&b, "resp, err := nuts.Request[models.%s, models.%s](ctx, c.rt, %q, %q, %s, req)\n", e.Request, e.Reply, name+"."+ep, e.Subject, requestTimeout(group))
		if len(e.Errors) == 0 {
			b.WriteString("return resp, err\n}\n\n")
			continue
		}
		b.WriteString("if err != nil {\nreturn nil, declaredError(err)\n}\nreturn resp, nil\n}\n\n")
	}
	writeErrorTypes(&b, codes)
	if len(codes) > 0 {
		imports = append(imports, `"errors"`)
		b.WriteString("func declaredError(err error) error {\nvar reply *nuts.ReplyError\nif !errors.As(err, &reply) {\nreturn err\n}\nswitch reply.Code {\n")
		for _, code := range codes {
			fmt.Fprintf(&b, "case %q:\nreturn &%sError{Message: reply.Message}\n", code, code)
		}
		b.WriteString("}\nreturn err\n}\n")
	}
	pkg := packageName(name)
	return g.write("client/"+pkg+"/client.go", pkg, imports, "// Package "+pkg+" calls the "+name+" endpoints.\n"+b.String())
}

func (g *generator) servers() error {
	if !g.c.RRServer {
		return nil
	}
	for _, name := range g.sc.servers {
		if err := g.writeServer(name); err != nil {
			return err
		}
	}
	return nil
}

func (g *generator) writeServer(name string) error {
	group := g.s.Requests[name]
	endpoints := spec.SortedKeys(group.Endpoints)
	var b strings.Builder
	b.WriteString("// Handler implements the " + name + " endpoints. Return a declared error type to\n// send its code to the client; any other error becomes an internal error.\ntype Handler interface {\n")
	for _, ep := range endpoints {
		e := group.Endpoints[ep]
		fmt.Fprintf(&b, "%s(ctx context.Context, req models.%s) (*models.%s, error)\n", ep, e.Request, e.Reply)
	}
	b.WriteString("}\n\n")
	b.WriteString("// Serve registers h on NATS until the returned server is stopped.\nfunc Serve(ctx context.Context, rt *nuts.Runtime, h Handler) (*nuts.Server, error) {\n")
	fmt.Fprintf(&b, "srv, err := nuts.NewServer(ctx, rt, nuts.ServerConfig{Name: %q, Version: %q, QueueGroup: %q})\nif err != nil {\nreturn nil, err\n}\n", name, group.Version, group.QueueGroup)
	for _, ep := range endpoints {
		fmt.Fprintf(&b, "if err := nuts.AddEndpoint(srv, %q, %q, h.%s); err != nil {\n_ = srv.Stop()\nreturn nil, err\n}\n", ep, group.Endpoints[ep].Subject, ep)
	}
	b.WriteString("return srv, nil\n}\n\n")
	writeErrorTypes(&b, groupErrors(group, endpoints))
	pkg := packageName(name)
	imports := []string{importContext, importNuts, g.modelsImport()}
	return g.write("server/"+pkg+"/server.go", pkg, imports, "// Package "+pkg+" serves the "+name+" endpoints.\n"+b.String())
}

func (g *generator) app() error {
	c := g.c
	if !c.PubSub && !c.Consumers && !c.KV && !c.Schedules && !c.RRClient && !c.RRServer {
		return nil
	}
	imports := []string{importContext, importJSON, importNuts}
	var fields, inits strings.Builder
	add := func(field, pkg, typ, ctor string) {
		imports = append(imports, strconv.Quote(g.opts.ImportPath+"/"+pkg))
		fmt.Fprintf(&fields, "%s *%s\n", field, typ)
		fmt.Fprintf(&inits, "%s: %s(rt),\n", field, ctor)
	}
	if c.PubSub {
		add("Events", "events", "events.Events", "events.New")
	}
	if c.Consumers {
		add("Consumers", "consumers", "consumers.Consumers", "consumers.New")
	}
	if c.KV {
		add("KV", "kv", "kv.KV", "kv.New")
	}
	if c.Schedules {
		add("Schedules", "schedules", "schedules.Schedules", "schedules.New")
	}
	if c.RRClient {
		for _, name := range spec.SortedKeys(g.sc.clients) {
			pkg := packageName(name)
			add(GoName(name), "client/"+pkg, pkg+".Client", pkg+".New")
		}
	}
	infra, err := json.MarshalIndent(g.sc.infra(g.s), "", "  ")
	if err != nil {
		return fmt.Errorf("encode infrastructure: %w", err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "// Package %s is the entry point to the generated %s code.\n", g.s.Go.Package, g.s.Name)
	fmt.Fprintf(&b, "// App holds a connection and everything generated from the spec.\ntype App struct {\nrt *nuts.Runtime\n%s}\n\n", fields.String())
	serviceOpt := ""
	if g.opts.Service != "" {
		serviceOpt = fmt.Sprintf("opts = append([]nuts.Option{nuts.WithService(%q)}, opts...)\n", g.opts.Service)
	}
	fmt.Fprintf(&b, "// Connect dials NATS. It binds to infrastructure and never creates any.\nfunc Connect(ctx context.Context, url string, opts ...nuts.Option) (*App, error) {\n%srt, err := nuts.Dial(url, opts...)\nif err != nil {\nreturn nil, err\n}\nif err := rt.Check(ctx, Infra()); err != nil {\nrt.Close()\nreturn nil, err\n}\nreturn New(rt), nil\n}\n\n", serviceOpt)
	fmt.Fprintf(&b, "// New builds an App on an existing runtime.\nfunc New(rt *nuts.Runtime) *App {\nreturn &App{\nrt: rt,\n%s}\n}\n\n", inits.String())
	b.WriteString("// Runtime returns the underlying runtime, for starting servers.\nfunc (a *App) Runtime() *nuts.Runtime { return a.rt }\n\n")
	b.WriteString("// Close closes the connection.\nfunc (a *App) Close() { a.rt.Close() }\n\n")
	fmt.Fprintf(&b, "const infraJSON = %s\n\n", goString(string(infra)))
	b.WriteString("// Infra returns the infrastructure the spec expects to exist.\nfunc Infra() nuts.Infra {\nvar in nuts.Infra\nif err := json.Unmarshal([]byte(infraJSON), &in); err != nil {\npanic(err)\n}\nreturn in\n}\n")
	return g.write("app.go", g.s.Go.Package, imports, b.String())
}
