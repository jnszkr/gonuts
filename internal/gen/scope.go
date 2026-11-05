package gen

import (
	"fmt"
	"slices"
	"strings"

	"github.com/jnszkr/gonuts/internal/spec"
	"github.com/jnszkr/gonuts/nuts"
)

// scope is the part of a spec to generate: all of it, or one service's share.
type scope struct {
	publishes  []string
	subscribes []string
	consumers  []string
	buckets    map[string]string // name to access
	schedules  []string
	clients    map[string][]string // group to endpoints
	servers    []string
}

func newScope(s *spec.Spec, service string) (*scope, error) {
	if service == "" {
		return wholeSpec(s), nil
	}
	x, ok := s.Services[service]
	if !ok {
		return nil, fmt.Errorf("unknown service %q; known services: %s", service, strings.Join(spec.SortedKeys(s.Services), ", "))
	}
	sc := &scope{
		publishes:  sorted(x.Publishes),
		subscribes: sorted(x.Subscribes),
		consumers:  sorted(x.Consumes),
		buckets:    x.KV,
		schedules:  sorted(x.Schedules),
		clients:    map[string][]string{},
		servers:    sorted(x.Serves),
	}
	for _, call := range x.Calls {
		group, ep, _ := strings.Cut(call, ".")
		sc.clients[group] = append(sc.clients[group], ep)
	}
	for group := range sc.clients {
		slices.Sort(sc.clients[group])
	}
	return sc, nil
}

func wholeSpec(s *spec.Spec) *scope {
	sc := &scope{
		publishes: spec.SortedKeys(s.Events),
		consumers: spec.SortedKeys(s.Consumers),
		buckets:   map[string]string{},
		schedules: spec.SortedKeys(s.Schedules),
		clients:   map[string][]string{},
		servers:   spec.SortedKeys(s.Requests),
	}
	for _, e := range sc.publishes {
		if s.StreamOf(e) == "" {
			sc.subscribes = append(sc.subscribes, e)
		}
	}
	for name := range s.KV {
		sc.buckets[name] = "readwrite"
	}
	for _, group := range sc.servers {
		sc.clients[group] = spec.SortedKeys(s.Requests[group].Endpoints)
	}
	return sc
}

func sorted(in []string) []string {
	return slices.Sorted(slices.Values(in))
}

// streams lists the streams a scope touches.
func (sc *scope) streams(s *spec.Spec) map[string]bool {
	out := map[string]bool{}
	for _, e := range sc.publishes {
		if st := s.StreamOf(e); st != "" {
			out[st] = true
		}
	}
	for _, c := range sc.consumers {
		out[s.Consumers[c].Stream] = true
	}
	for _, x := range sc.schedules {
		out[s.Schedules[x].Stream] = true
	}
	return out
}

// infra limits the spec's infrastructure to what the scope touches.
func (sc *scope) infra(s *spec.Spec) nuts.Infra {
	all := s.Infra()
	streams := sc.streams(s)
	var out nuts.Infra
	for _, st := range all.Streams {
		if streams[st.Name] {
			out.Streams = append(out.Streams, st)
		}
	}
	for _, c := range all.Consumers {
		if slices.Contains(sc.consumers, c.Config.Durable) {
			out.Consumers = append(out.Consumers, c)
		}
	}
	for _, b := range all.Buckets {
		if _, ok := sc.buckets[b.Bucket]; ok {
			out.Buckets = append(out.Buckets, b)
		}
	}
	return out
}

// usedModels lists models the selected components need, with their dependencies.
func (g *generator) usedModels() []string {
	used := map[string]bool{}
	add := func(m string) { addModel(g.s, used, m) }
	if g.c.Models {
		if g.opts.Service == "" {
			return spec.SortedKeys(g.s.Models)
		}
		c := g.c
		g.c = DefaultComponents()
		defer func() { g.c = c }()
	}
	if g.c.PubSub {
		for _, e := range append(slices.Clone(g.sc.publishes), g.sc.subscribes...) {
			add(g.s.Events[e].Model)
		}
	}
	if g.c.Consumers {
		for _, c := range g.sc.consumers {
			for _, e := range g.s.Consumers[c].Events {
				add(g.s.Events[e].Model)
			}
		}
	}
	if g.c.KV {
		for b := range g.sc.buckets {
			add(g.s.KV[b].Model)
		}
	}
	if g.c.Schedules {
		for _, x := range g.sc.schedules {
			add(g.s.Events[g.s.Schedules[x].Event].Model)
		}
	}
	if g.c.RRClient {
		for group, eps := range g.sc.clients {
			for _, ep := range eps {
				e := g.s.Requests[group].Endpoints[ep]
				add(e.Request)
				add(e.Reply)
			}
		}
	}
	if g.c.RRServer {
		for _, group := range g.sc.servers {
			for _, e := range g.s.Requests[group].Endpoints {
				add(e.Request)
				add(e.Reply)
			}
		}
	}
	return spec.SortedKeys(used)
}

func addModel(s *spec.Spec, used map[string]bool, name string) {
	m := s.Models[name]
	if m == nil || used[name] {
		return
	}
	used[name] = true
	for _, f := range m.Fields {
		addModel(s, used, baseType(f.Type))
	}
}

func baseType(t string) string {
	for {
		rest, ok := strings.CutPrefix(t, "[]")
		if !ok {
			rest, ok = strings.CutPrefix(t, "map[string]")
		}
		if !ok {
			return t
		}
		t = rest
	}
}
