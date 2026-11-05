package spec

import (
	"fmt"
	"go/token"
	"slices"
	"strings"
	"time"
)

var builtinTypes = map[string]bool{
	"string": true, "bool": true, "int": true, "int32": true, "int64": true, "uint": true,
	"uint32": true, "uint64": true, "float32": true, "float64": true, "uuid": true,
	"time": true, "duration": true, "bytes": true, "any": true,
}

type validator struct {
	s    *Spec
	errs []Error
}

func (v *validator) addf(path, format string, args ...any) {
	v.errs = append(v.errs, Error{Path: path, Message: fmt.Sprintf(format, args...)})
}

// Validate returns every problem in the spec, sorted by path.
func Validate(s *Spec) []Error {
	v := &validator{s: s}
	v.header()
	v.models()
	v.events()
	v.streams()
	v.consumers()
	v.buckets()
	v.schedules()
	v.requests()
	v.services()
	v.goNames()
	slices.SortStableFunc(v.errs, func(a, b Error) int { return strings.Compare(a.Path, b.Path) })
	return v.errs
}

func (v *validator) header() {
	if v.s.Version != 1 {
		v.addf("version", "must be 1")
	}
	if v.s.Name == "" {
		v.addf("name", "is required")
	}
	if !token.IsIdentifier(v.s.Go.Package) {
		v.addf("go.package", "must be a Go identifier")
	}
	if v.s.Encoding != "" && v.s.Encoding != "json" {
		v.addf("encoding", "only json is supported")
	}
}

func (v *validator) models() {
	for _, name := range SortedKeys(v.s.Models) {
		path := "models." + name
		if !exportedIdent(name) {
			v.addf(path, "name must be an exported Go identifier")
		}
		if v.containsItself(name) {
			v.addf(path, "contains itself, so it has no Go type; make the field optional or a list")
		}
		seen := map[string]string{}
		for _, f := range v.s.Models[name].Fields {
			fieldPath := path + ".fields." + f.Name
			if !v.knownType(f.Type) {
				v.addf(fieldPath, "unknown type %q", f.Type)
			}
			goName := GoName(f.Name)
			if !exportedIdent(goName) {
				v.addf(fieldPath, "name does not make a Go identifier")
			}
			if other, ok := seen[goName]; ok {
				v.addf(fieldPath, "has the same Go name %s as %s", goName, other)
			}
			seen[goName] = f.Name
		}
	}
}

// containsItself reports whether a model holds itself by value. Optional fields
// are pointers and lists and maps are references, so those are fine.
func (v *validator) containsItself(name string) bool {
	seen := map[string]bool{}
	var visit func(string) bool
	visit = func(current string) bool {
		m := v.s.Models[current]
		if m == nil {
			return false
		}
		for _, f := range m.Fields {
			if f.Optional {
				continue
			}
			if f.Type == name {
				return true
			}
			if !seen[f.Type] {
				seen[f.Type] = true
				if visit(f.Type) {
					return true
				}
			}
		}
		return false
	}
	return visit(name)
}

func exportedIdent(s string) bool {
	return token.IsIdentifier(s) && token.IsExported(s)
}

// concrete checks a subject that generated code publishes to or reads a key
// from, which a wildcard would never match.
func (v *validator) concrete(path string, sub Subject) {
	if sub.HasWildcard() {
		v.addf(path, "must not hold a wildcard; use a {param} for the variable part")
	}
	seen := map[string]string{}
	for _, p := range sub.Params() {
		goName := GoName(p)
		if !exportedIdent(goName) {
			v.addf(path, "param %q has no Go name", p)
		}
		if other, ok := seen[goName]; ok {
			v.addf(path, "params %s and %s have the same Go name %s", other, p, goName)
		}
		seen[goName] = p
	}
}

// fixed checks a subject that generated code uses exactly as written.
func (v *validator) fixed(path, raw string) {
	sub, err := ParseSubject(raw)
	switch {
	case err != nil:
		v.addf(path, "%v", err)
	case len(sub.Params()) > 0:
		v.addf(path, "%s cannot have parameters", raw)
	default:
		v.concrete(path, sub)
	}
}

func (v *validator) knownType(t string) bool {
	if rest, ok := strings.CutPrefix(t, "[]"); ok {
		return v.knownType(rest)
	}
	if rest, ok := strings.CutPrefix(t, "map[string]"); ok {
		return v.knownType(rest)
	}
	return builtinTypes[t] || v.s.Models[t] != nil
}

func (v *validator) model(path, name string) {
	if v.s.Models[name] == nil {
		v.addf(path, "unknown model %q", name)
	}
}

func (v *validator) replicas(path string, n int) {
	if n < 0 || n > 5 {
		v.addf(path, "must be between 1 and 5")
	}
}

func oneOf[V any](v *validator, path, val string, allowed map[string]V) {
	if val == "" {
		return
	}
	if _, ok := allowed[val]; !ok {
		v.addf(path, "must be one of %s", strings.Join(SortedKeys(allowed), ", "))
	}
}

func (v *validator) events() {
	names := SortedKeys(v.s.Events)
	for i, name := range names {
		e := v.s.Events[name]
		path := "events." + name
		v.model(path+".model", e.Model)
		sub, err := ParseSubject(e.Subject)
		if err != nil {
			v.addf(path+".subject", "%v", err)
			continue
		}
		v.concrete(path+".subject", sub)
		for _, other := range names[i+1:] {
			if SubjectsOverlap(v.s.EventFilter(name), v.s.EventFilter(other)) {
				v.addf("events", "%s and %s overlap", name, other)
			}
		}
	}
}

func (v *validator) streams() {
	names := SortedKeys(v.s.Streams)
	for i, name := range names {
		st := v.s.Streams[name]
		path := "streams." + name
		v.natsName(path, name)
		for _, e := range st.Events {
			if _, ok := v.s.Events[e]; !ok {
				v.addf(path+".events", "unknown event %q", e)
			}
		}
		if len(st.Events)+len(st.Subjects) == 0 && st.Mirror == nil && len(st.Sources) == 0 {
			v.addf(path, "has no subjects, mirror or sources")
		}
		oneOf(v, path+".retention", st.Retention, retentionPolicies)
		oneOf(v, path+".storage", st.Storage, storageTypes)
		oneOf(v, path+".discard", st.Discard, discardPolicies)
		oneOf(v, path+".compression", st.Compression, compressions)
		oneOf(v, path+".persist_mode", st.PersistMode, persistModes)
		v.replicas(path+".replicas", st.Replicas)
		if st.AllowMsgSchedules && (st.Mirror != nil || len(st.Sources) > 0) {
			v.addf(path+".allow_msg_schedules", "cannot be enabled on a mirror or source stream")
		}
		for _, other := range names[i+1:] {
			v.streamOverlap(name, other)
		}
	}
}

func (v *validator) streamOverlap(a, b string) {
	for _, x := range v.s.StreamSubjects(a) {
		for _, y := range v.s.StreamSubjects(b) {
			if SubjectsOverlap(x, y) {
				v.addf("streams", "%s and %s overlap on %s and %s", a, b, x, y)
			}
		}
	}
}

func (v *validator) consumers() {
	for _, name := range SortedKeys(v.s.Consumers) {
		c := v.s.Consumers[name]
		path := "consumers." + name
		v.natsName(path, name)
		if _, ok := v.s.Streams[c.Stream]; !ok {
			v.addf(path+".stream", "unknown stream %q", c.Stream)
		}
		v.consumerEvents(path, c)
		if c.Durable != nil && !*c.Durable {
			v.addf(path+".durable", "ephemeral consumers are not supported; applications never create consumers, so every consumer is durable")
		}
		if c.MaxDeliver > 0 && len(c.BackOff) >= c.MaxDeliver {
			v.addf(path+".backoff", "must have fewer entries than max_deliver")
		}
		if c.AckWait.Duration != 0 && len(c.BackOff) > 0 {
			v.addf(path+".ack_wait", "cannot be combined with backoff, because NATS replaces ack_wait with the first backoff entry")
		}
		oneOf(v, path+".ack_policy", c.AckPolicy, ackPolicies)
		oneOf(v, path+".deliver_policy", c.DeliverPolicy, deliverPolicies)
		oneOf(v, path+".replay_policy", c.ReplayPolicy, replayPolicies)
		oneOf(v, path+".priority_policy", c.PriorityPolicy, priorityPolicies)
		v.replicas(path+".replicas", c.Replicas)
	}
}

func (v *validator) consumerEvents(path string, c Consumer) {
	for _, e := range c.Events {
		if _, ok := v.s.Events[e]; !ok {
			v.addf(path+".events", "unknown event %q", e)
		} else if v.s.StreamOf(e) != c.Stream {
			v.addf(path+".events", "event %q is not in stream %s", e, c.Stream)
		}
	}
}

func (v *validator) buckets() {
	for _, name := range SortedKeys(v.s.KV) {
		k := v.s.KV[name]
		path := "kv." + name
		if !validBucketName(name) {
			v.addf(path, "name may only hold letters, digits, - and _")
		}
		v.model(path+".model", k.Model)
		if k.Key == "" {
			v.addf(path+".key", "is required")
		} else if sub, err := ParseSubject(k.Key); err != nil {
			v.addf(path+".key", "%v", err)
		} else {
			v.concrete(path+".key", sub)
		}
		oneOf(v, path+".storage", k.Storage, storageTypes)
		v.replicas(path+".replicas", k.Replicas)
	}
}

// natsName checks the name of a stream or consumer, which NATS puts in subjects.
func (v *validator) natsName(path, name string) {
	if strings.ContainsAny(name, ". \t\r\n*>/\\") {
		v.addf(path, "name must not contain dots, wildcards, spaces or slashes")
	}
}

func validBucketName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r != '-' && r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

func (v *validator) schedules() {
	for _, name := range SortedKeys(v.s.Schedules) {
		x := v.s.Schedules[name]
		path := "schedules." + name
		st, ok := v.s.Streams[x.Stream]
		switch {
		case !ok:
			v.addf(path+".stream", "unknown stream %q", x.Stream)
		case !st.AllowMsgSchedules:
			v.addf(path+".stream", "stream %s must set allow_msg_schedules", x.Stream)
		}
		ev, ok := v.s.Events[x.Event]
		switch {
		case !ok:
			v.addf(path+".event", "unknown event %q", x.Event)
		case v.s.StreamOf(x.Event) != x.Stream:
			v.addf(path+".event", "event %q is not in stream %s", x.Event, x.Stream)
		}
		if sub, err := ParseSubject(ev.Subject); err == nil && len(sub.Params()) > 0 {
			v.addf(path+".event", "event %q has subject params, so a schedule cannot target it", x.Event)
		}
		v.scheduleSubject(path, x, ev)
		v.scheduleTiming(path, x)
		if m := v.s.Models[ev.Model]; m != nil {
			for _, key := range SortedKeys(x.Payload) {
				if !slices.ContainsFunc(m.Fields, func(f *Field) bool { return f.Name == key }) {
					v.addf(path+".payload."+key, "is not a field of %s", ev.Model)
				}
			}
		}
	}
}

func (v *validator) scheduleSubject(path string, x Schedule, ev Event) {
	v.fixed(path+".subject", x.Subject)
	if x.Subject == ev.Subject {
		v.addf(path+".subject", "must differ from the target event's subject")
	}
	// Recurring schedules use the subject itself. Delayed messages add a token to
	// it, so that they never replace each other.
	for _, subject := range []string{x.Subject, x.Subject + ".id"} {
		covered := slices.ContainsFunc(v.s.StreamSubjects(x.Stream), func(p string) bool {
			return SubjectCovers(p, subject)
		})
		if !covered {
			v.addf(path+".subject", "%s is not covered by stream %s", subject, x.Stream)
		}
	}
}

func (v *validator) scheduleTiming(path string, x Schedule) {
	if x.Cron != "" && x.Every.Duration != 0 {
		v.addf(path, "set only one of cron and every")
	}
	if x.Every.Duration != 0 && x.Every.Duration < time.Second {
		v.addf(path+".every", "must be at least 1s")
	}
	if x.TimeZone != "" && x.Cron == "" {
		v.addf(path+".time_zone", "only applies to cron")
	}
}

func (v *validator) requests() {
	for _, group := range SortedKeys(v.s.Requests) {
		for _, name := range SortedKeys(v.s.Requests[group].Endpoints) {
			v.endpoint("requests."+group+".endpoints."+name, name, v.s.Requests[group].Endpoints[name])
		}
	}
}

func (v *validator) endpoint(path, name string, e Endpoint) {
	if !exportedIdent(name) {
		v.addf(path, "name must be an exported Go identifier")
	}
	v.model(path+".request", e.Request)
	v.model(path+".reply", e.Reply)
	for _, code := range e.Errors {
		if !exportedIdent(code) {
			v.addf(path+".errors", "%q must be an exported Go identifier", code)
		}
	}
	v.fixed(path+".subject", e.Subject)
	sub, err := ParseSubject(e.Subject)
	if err != nil {
		return
	}
	for _, ev := range SortedKeys(v.s.Events) {
		if SubjectsOverlap(sub.Wildcard(), v.s.EventFilter(ev)) {
			v.addf(path+".subject", "%s and event %s overlap", e.Subject, ev)
		}
	}
}

func (v *validator) services() {
	for _, name := range SortedKeys(v.s.Services) {
		x := v.s.Services[name]
		path := "services." + name
		for _, e := range x.Publishes {
			if _, ok := v.s.Events[e]; !ok {
				v.addf(path+".publishes", "unknown event %q", e)
			}
		}
		v.subscriptions(path, x)
		for _, c := range x.Consumes {
			if _, ok := v.s.Consumers[c]; !ok {
				v.addf(path+".consumes", "unknown consumer %q", c)
			}
		}
		v.serviceRequests(path, x)
		for _, b := range SortedKeys(x.KV) {
			if _, ok := v.s.KV[b]; !ok {
				v.addf(path+".kv", "unknown bucket %q", b)
			}
			if a := x.KV[b]; a != "read" && a != "readwrite" {
				v.addf(path+".kv."+b, "must be read or readwrite")
			}
		}
		for _, sc := range x.Schedules {
			if _, ok := v.s.Schedules[sc]; !ok {
				v.addf(path+".schedules", "unknown schedule %q", sc)
			}
		}
	}
}

func (v *validator) subscriptions(path string, x Service) {
	for _, e := range x.Subscribes {
		if _, ok := v.s.Events[e]; !ok {
			v.addf(path+".subscribes", "unknown event %q", e)
		} else if v.s.StreamOf(e) != "" {
			v.addf(path+".subscribes", "event %q is stored in a stream; use a consumer", e)
		}
	}
}

func (v *validator) serviceRequests(path string, x Service) {
	for _, g := range x.Serves {
		if _, ok := v.s.Requests[g]; !ok {
			v.addf(path+".serves", "unknown request group %q", g)
		}
	}
	for _, call := range x.Calls {
		group, ep, _ := strings.Cut(call, ".")
		if _, ok := v.s.Requests[group].Endpoints[ep]; !ok {
			v.addf(path+".calls", "unknown endpoint %q", call)
		}
	}
}

// Names that generated code already declares, in each place a spec name can land.
var (
	reservedBuckets   = map[string]bool{"KV": true, "New": true}
	reservedConsumers = map[string]bool{"Consumers": true, "New": true}
	// Fields and methods of the generated App.
	reservedGroups = map[string]bool{"Events": true, "Consumers": true, "KV": true, "Schedules": true, "Runtime": true, "Close": true}
	// Packages that generated files for a request group sit next to or import.
	reservedPackages = map[string]bool{"context": true, "json": true, "nuts": true, "events": true, "consumers": true, "kv": true, "schedules": true, "models": true}
)

// goNames rejects spec names that would produce code that does not compile, or
// that would silently overwrite another name's generated file.
func (v *validator) goNames() {
	v.unique("events", SortedKeys(v.s.Events))
	v.declared("kv", SortedKeys(v.s.KV), reservedBuckets)
	v.declared("consumers", SortedKeys(v.s.Consumers), reservedConsumers)
	v.consumerDeclarations()
	v.groupNames()
}

// unique reports names without a Go identifier, or sharing one.
func (v *validator) unique(section string, names []string) {
	seen := map[string]string{}
	for _, name := range names {
		goName := GoName(name)
		if !exportedIdent(goName) {
			v.addf(section+"."+name, "name does not make a Go identifier")
		}
		if other, ok := seen[goName]; ok {
			v.addf(section+"."+name, "has the same Go name %s as %s", goName, other)
		}
		seen[goName] = name
	}
}

func (v *validator) declared(section string, names []string, reserved map[string]bool) {
	v.unique(section, names)
	for _, name := range names {
		if reserved[GoName(name)] {
			v.addf(section+"."+name, "Go name %s is reserved", GoName(name))
		}
	}
}

// consumerDeclarations checks the one generated package where consumer and event
// names share a scope: each consumer declares a type and a handler, and each
// event it reads declares its parameters and message.
func (v *validator) consumerDeclarations() {
	owner := map[string]string{}
	for _, name := range SortedKeys(v.s.Consumers) {
		for _, e := range v.s.Consumers[name].Events {
			for _, suffix := range []string{"Params", "ParamsFrom", "Msg"} {
				owner[GoName(e)+suffix] = "event " + e
			}
		}
	}
	for _, name := range SortedKeys(v.s.Consumers) {
		for _, decl := range []string{GoName(name), GoName(name) + "Handler"} {
			if other, ok := owner[decl]; ok {
				v.addf("consumers."+name, "has the same Go name %s as %s", decl, other)
			}
			owner[decl] = "consumer " + name
		}
	}
}

func (v *validator) groupNames() {
	groups := SortedKeys(v.s.Requests)
	v.declared("requests", groups, reservedGroups)
	packages := map[string]string{}
	for _, group := range groups {
		path := "requests." + group
		pkg := PackageName(group)
		switch {
		case !token.IsIdentifier(pkg):
			v.addf(path, "name does not make a Go package name")
		case reservedPackages[pkg]:
			v.addf(path, "package name %s is reserved", pkg)
		}
		if other, ok := packages[pkg]; ok {
			v.addf(path, "has the same package %s as %s, so one would overwrite the other", pkg, other)
		}
		packages[pkg] = group
	}
}
