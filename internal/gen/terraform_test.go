package gen

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jnszkr/gonuts/internal/natstest"
	"github.com/jnszkr/gonuts/internal/spec"
	"github.com/jnszkr/gonuts/nuts"
)

func terraform(t *testing.T, s *spec.Spec) map[string]string {
	t.Helper()
	files, err := Terraform(s)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for k, v := range files {
		out[k] = string(v)
	}
	return out
}

func mustMatch(t *testing.T, text string, patterns ...string) {
	t.Helper()
	for _, p := range patterns {
		if !regexp.MustCompile(p).MatchString(text) {
			t.Errorf("no match for %q in:\n%s", p, text)
		}
	}
}

func TestTerraformFiles(t *testing.T) {
	got := names(terraform(t, shop(t)))
	want := []string{"consumers.tf", "kv.tf", "streams.tf", "variables.tf", "versions.tf"}
	if !slices.Equal(got, want) {
		t.Fatalf("files = %v", got)
	}
}

func TestTerraformSkipsEmptyFiles(t *testing.T) {
	s := shop(t)
	s.KV, s.Consumers = nil, nil
	got := names(terraform(t, s))
	if slices.Contains(got, "kv.tf") || slices.Contains(got, "consumers.tf") {
		t.Fatalf("files = %v", got)
	}
}

func TestTerraformProviderKeepsCredentialsInVariables(t *testing.T) {
	f := terraform(t, shop(t))
	mustMatch(t, f["versions.tf"], `source\s*=\s*"nats-io/jetstream"`, `provider "jetstream"`, `servers\s*=\s*var.nats_servers`, `credentials\s*=\s*var.nats_credentials`)
	mustMatch(t, f["variables.tf"], `variable "nats_servers"`, `variable "nats_credentials"`, `sensitive\s*=\s*true`)
}

func TestTerraformStream(t *testing.T) {
	st := terraform(t, shop(t))["streams.tf"]
	mustMatch(t, st,
		`resource "jetstream_stream" "orders"`,
		`name\s*=\s*"ORDERS"`,
		`subjects\s*=\s*\["orders.placed.\*", "orders.shipped.\*"\]`,
		`retention\s*=\s*"limits"`,
		`storage\s*=\s*"file"`,
		`max_age\s*=\s*2592000`,
		`duplicate_window\s*=\s*120`,
		`deny_delete\s*=\s*true`,
		`allow_msg_schedules\s*=\s*true`,
		`allow_rollup_hdrs\s*=\s*true`,
	)
}

func TestTerraformMapsAllStreamSettings(t *testing.T) {
	s := shop(t)
	x := s.Streams["ORDERS"]
	x.Description, x.Discard, x.Compression, x.PersistMode = "d", "new", "s2", "async"
	x.MaxConsumers, x.MaxMsgs, x.MaxBytes, x.MaxMsgsPerSubject, x.MaxMsgSize = 1, 2, 3, 4, 5
	x.DiscardNewPerSubject, x.NoAck, x.DenyPurge, x.AllowDirect, x.MirrorDirect = true, true, true, true, true
	x.AllowMsgTTL, x.AllowMsgCounter, x.AllowAtomic, x.AllowBatched = true, true, true, true
	x.FirstSeq = 6
	x.SubjectDeleteMarkerTTL = spec.Duration{Duration: time.Minute}
	x.Placement = &spec.Placement{Cluster: "c", Tags: []string{"t"}, Preferred: "p"}
	x.RePublish = &spec.RePublish{Source: "a", Destination: "b", HeadersOnly: true}
	x.ConsumerLimits = &spec.ConsumerLimits{InactiveThreshold: spec.Duration{Duration: time.Minute}, MaxAckPending: 7}
	x.Metadata = map[string]string{"k": "v"}
	x.SubjectTransform = &spec.SubjectTransform{Source: "s", Destination: "d"}
	x.Mirror = &spec.StreamSource{Name: "M", FilterSubject: "f", StartSeq: 2}
	x.Sources = []spec.StreamSource{{Name: "S"}, {Name: "T"}}
	s.Streams["ORDERS"] = x
	st := terraform(t, s)["streams.tf"]
	mustMatch(t, st,
		`description\s*=\s*"d"`, `discard\s*=\s*"new"`, `compression\s*=\s*"s2"`, `persist_mode\s*=\s*"async"`,
		`max_consumers\s*=\s*1\b`, `max_msgs\s*=\s*2\b`, `max_bytes\s*=\s*3\b`, `max_msgs_per_subject\s*=\s*4\b`, `max_msg_size\s*=\s*5\b`,
		`discard_new_per_subject\s*=\s*true`, `\back\s*=\s*false`, `deny_purge\s*=\s*true`, `allow_direct\s*=\s*true`, `mirror_direct\s*=\s*true`,
		`allow_msg_ttl\s*=\s*true`, `allow_msg_counter\s*=\s*true`, `allow_atomic\s*=\s*true`, `allow_batched\s*=\s*true`, `first_seq\s*=\s*6`,
		`subject_delete_marker_ttl\s*=\s*60`, `placement_cluster\s*=\s*"c"`, `placement_tags\s*=\s*\["t"\]`, `placement_preferred\s*=\s*"p"`,
		`republish_source\s*=\s*"a"`, `republish_destination\s*=\s*"b"`, `republish_headers_only\s*=\s*true`,
		`inactive_threshold\s*=\s*60`, `max_ack_pending\s*=\s*7`, `metadata\s*=\s*\{ k = "v" \}`,
		`subject_transform \{`, `destination\s*=\s*"d"`,
		`mirror \{`, `start_seq\s*=\s*2`, `filter_subject\s*=\s*"f"`,
		`source \{\n\s*name\s*=\s*"S"`, `source \{\n\s*name\s*=\s*"T"`,
	)
}

func TestTerraformConsumer(t *testing.T) {
	c := terraform(t, shop(t))["consumers.tf"]
	mustMatch(t, c,
		`resource "jetstream_consumer" "billing_orders"`,
		`stream_id\s*=\s*jetstream_stream.orders.id`,
		`durable_name\s*=\s*"billing-orders"`,
		`filter_subject\s*=\s*"orders.placed.\*"`,
		`filter_subjects\s*=\s*\["orders.placed.\*", "orders.shipped.\*"\]`,
		`max_delivery\s*=\s*5`,
		`backoff\s*=\s*\[30, 60, 120, 300\]`,
		`max_ack_pending\s*=\s*500`,
		`deliver_all\s*=\s*true`,
		`ack_wait\s*=\s*30`,
	)
}

func TestTerraformMapsAllConsumerSettings(t *testing.T) {
	s := shop(t)
	c := s.Consumers["fulfillment-orders"]
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c.Description, c.AckPolicy, c.ReplayPolicy, c.PriorityPolicy = "d", "all", "original", "pinned_client"
	c.DeliverPolicy, c.OptStartSeq, c.OptStartTime = "by_start_time", 2, &start
	c.RateLimit, c.SampleFreq, c.MaxWaiting, c.MaxBatch, c.MaxBytes, c.Replicas = 3, "10%", 4, 5, 6, 3
	c.HeadersOnly, c.MemoryStorage, c.FlowControl = true, true, true
	c.MaxExpires = spec.Duration{Duration: time.Minute}
	c.InactiveThreshold = spec.Duration{Duration: 2 * time.Minute}
	c.IdleHeartbeat = spec.Duration{Duration: 3 * time.Minute}
	c.PriorityTimeout = spec.Duration{Duration: 4 * time.Minute}
	c.PriorityGroups = []string{"g"}
	c.DeliverSubject, c.DeliverGroup = "ds", "dg"
	c.Metadata = map[string]string{"k": "v"}
	s.Consumers["fulfillment-orders"] = c
	got := terraform(t, s)["consumers.tf"]
	mustMatch(t, got,
		`description\s*=\s*"d"`, `ack_policy\s*=\s*"all"`, `replay_policy\s*=\s*"original"`, `priority_policy\s*=\s*"pinned_client"`,
		`start_time\s*=\s*"2026-01-02T03:04:05Z"`, `stream_sequence\s*=\s*2`, `ratelimit\s*=\s*3`, `sample_freq\s*=\s*10\b`, `max_waiting\s*=\s*4`,
		`max_batch\s*=\s*5`, `max_bytes\s*=\s*6`, `replicas\s*=\s*3`, `headers_only\s*=\s*true`, `memory\s*=\s*true`, `flow_control\s*=\s*true`,
		`max_expires\s*=\s*60`, `inactive_threshold\s*=\s*120`, `heartbeat\s*=\s*180`, `priority_timeout\s*=\s*240`, `priority_groups\s*=\s*\["g"\]`,
		`delivery_subject\s*=\s*"ds"`, `delivery_group\s*=\s*"dg"`, `metadata\s*=\s*\{ k = "v" \}`,
	)
}

func TestTerraformDeliverPolicies(t *testing.T) {
	for policy, attr := range map[string]string{"": "deliver_all", "all": "deliver_all", "last": "deliver_last", "new": "deliver_new", "last_per_subject": "deliver_last_per_subject"} {
		s := shop(t)
		c := s.Consumers["fulfillment-orders"]
		c.DeliverPolicy = policy
		s.Consumers["fulfillment-orders"] = c
		mustMatch(t, terraform(t, s)["consumers.tf"], attr+`\s*=\s*true`)
	}
}

func TestTerraformBucket(t *testing.T) {
	s := shop(t)
	k := s.KV["carts"]
	k.Description, k.Storage = "d", "memory"
	k.MaxValueSize, k.MaxBytes = 1, 2
	k.LimitMarkerTTL = spec.Duration{Duration: time.Minute}
	k.Placement = &spec.Placement{Cluster: "c", Tags: []string{"t"}}
	s.KV["carts"] = k
	mustMatch(t, terraform(t, s)["kv.tf"],
		`resource "jetstream_kv_bucket" "carts"`, `name\s*=\s*"carts"`, `history\s*=\s*5`, `ttl\s*=\s*86400`, `replicas\s*=\s*1`,
		`description\s*=\s*"d"`, `storage\s*=\s*"memory"`, `max_value_size\s*=\s*1`, `max_bucket_size\s*=\s*2`, `limit_marker_ttl\s*=\s*60`,
		`placement_cluster\s*=\s*"c"`, `placement_tags\s*=\s*\["t"\]`,
	)
}

func TestTerraformStopsOnSettingsTheProviderLacks(t *testing.T) {
	when := time.Now()
	cases := map[string]func(*spec.Spec){
		"sealed": func(s *spec.Spec) { x := s.Streams["ORDERS"]; x.Sealed = true; s.Streams["ORDERS"] = x },
		"pause_until": func(s *spec.Spec) {
			c := s.Consumers["fulfillment-orders"]
			c.PauseUntil = &when
			s.Consumers["fulfillment-orders"] = c
		},
		"republish": func(s *spec.Spec) {
			k := s.KV["carts"]
			k.RePublish = &spec.RePublish{Destination: "x"}
			s.KV["carts"] = k
		},
		"mirror": func(s *spec.Spec) { k := s.KV["carts"]; k.Mirror = &spec.StreamSource{Name: "x"}; s.KV["carts"] = k },
		"sources": func(s *spec.Spec) {
			k := s.KV["carts"]
			k.Sources = []spec.StreamSource{{Name: "x"}}
			s.KV["carts"] = k
		},
		"compression": func(s *spec.Spec) {
			k := s.KV["carts"]
			k.Compression = true
			s.KV["carts"] = k
		},
		"metadata": func(s *spec.Spec) { k := s.KV["carts"]; k.Metadata = map[string]string{"a": "b"}; s.KV["carts"] = k },
	}
	for field, mutate := range cases {
		t.Run(field, func(t *testing.T) {
			s := shop(t)
			mutate(s)
			_, err := Terraform(s)
			if err == nil || !strings.Contains(err.Error(), field) {
				t.Fatalf("err = %v, want it to name %s", err, field)
			}
		})
	}
}

func TestTerraformLayoutMatchesFmt(t *testing.T) {
	got := terraform(t, shop(t))["versions.tf"]
	want := `# Code generated by gonuts. DO NOT EDIT.

terraform {
  required_providers {
    jetstream = {
      source = "nats-io/jetstream"
    }
  }
}

provider "jetstream" {
  servers     = var.nats_servers
  credentials = var.nats_credentials
}
`
	if got != want {
		t.Fatalf("got:\n%s", got)
	}
}

func TestTerraformNestedBlocksAreSeparated(t *testing.T) {
	s := shop(t)
	x := s.Streams["ORDERS"]
	x.Sources = []spec.StreamSource{{Name: "A"}, {Name: "B"}}
	s.Streams["ORDERS"] = x
	got := terraform(t, s)["streams.tf"]
	if !strings.Contains(got, "  source {\n    name = \"A\"\n  }\n\n  source {\n    name = \"B\"\n  }\n}\n") {
		t.Fatalf("got:\n%s", got)
	}
}

func TestTerraformOmitsEmptyBackoff(t *testing.T) {
	s := shop(t)
	c := s.Consumers["billing-orders"]
	c.BackOff = nil
	s.Consumers["billing-orders"] = c
	mustNotContain(t, terraform(t, s)["consumers.tf"], "backoff")
}

func TestTerraformOutputIsDeterministic(t *testing.T) {
	a := terraform(t, shop(t))
	for range 5 {
		b := terraform(t, shop(t))
		for k := range a {
			if a[k] != b[k] {
				t.Fatalf("%s changed", k)
			}
		}
	}
}

// TestTerraformAppliesAndMatchesTheSpec applies the output to a real server and
// checks the server against the spec, proving the attribute mapping. It needs
// terraform and network access for the provider, so it runs only on request.
func TestTerraformAppliesAndMatchesTheSpec(t *testing.T) {
	if os.Getenv("GONUTS_TERRAFORM_TEST") == "" {
		t.Skip("set GONUTS_TERRAFORM_TEST=1 to run")
	}
	s := shop(t)
	dir := t.TempDir()
	for name, data := range terraform(t, s) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	url := natstest.Start(t)
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("terraform", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "TF_VAR_nats_servers="+url, "TF_IN_AUTOMATION=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("terraform %v: %v\n%s", args, err, out)
		}
	}
	run("fmt", "-check")
	run("init", "-no-color")
	run("validate", "-no-color")
	run("apply", "-auto-approve", "-no-color")

	rt, err := nuts.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	drift, err := nuts.Verify(ctx, rt.JetStream(), s.Infra())
	if err != nil {
		t.Fatal(err)
	}
	if len(drift) > 0 {
		data, _ := json.MarshalIndent(drift, "", " ")
		t.Fatalf("server differs from spec after terraform apply:\n%s", data)
	}
	// A second plan with no changes proves the output does not fight the provider.
	cmd := exec.Command("terraform", "plan", "-detailed-exitcode", "-no-color")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "TF_VAR_nats_servers="+url)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("plan is not empty after apply: %v\n%s", err, out)
	}
}
