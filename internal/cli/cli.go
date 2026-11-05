// Package cli implements the gonuts command.
package cli

import (
	"bytes"
	"context"
	_ "embed" // for the starter spec
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/jnszkr/gonuts/internal/diff"
	"github.com/jnszkr/gonuts/internal/gen"
	"github.com/jnszkr/gonuts/internal/spec"
	"github.com/jnszkr/gonuts/nuts"
)

// Exit codes.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// Version is set at build time. Otherwise it comes from the module version that
// go install records.
var Version = "dev"

//go:embed starter.yaml
var starterSpec []byte

const usage = `gonuts generates Go code and Terraform from a nuts.yaml file.

Commands:
  gonuts init       Write a starter nuts.yaml
  gonuts validate   Check the spec (--json for machine-readable output)
  gonuts generate   Generate code
  gonuts graph      Print a Mermaid map of who talks to whom
  gonuts diff OLD NEW
                    Classify a spec change (--json); exits 1 if destructive
  gonuts schedules sync --server URL
                    Store recurring schedules on the server
  gonuts version    Print the version

Generate flags. With no component flag, all Go code and no Terraform:
  --models          Go structs
  --pubsub          Publishers and core NATS subscribers
  --consumers       JetStream consumer handlers
  --kv              Typed key-value buckets
  --schedules       Delayed publishing and recurring schedule setup
  --rr-client       Request functions
  --rr-server       Handler interfaces and server wiring
  --terraform       Streams, consumers and buckets for the NATS JetStream provider
  --permissions     NATS server permissions for each service

  --spec PATH            Spec file (default nuts.yaml)
  --out DIR              Go output directory (default gen/nuts)
  --terraform-out DIR    Terraform output directory (default terraform)
  --permissions-out DIR  Permissions output directory (default permissions)
  --service NAME         Generate only what this service uses
  --import-path PATH     Import path of --out (default: detected from go.mod)
  --check                Fail if the output on disk is out of date; writes nothing

Exit codes: 0 success, 1 error, 2 bad usage.
`

// Run executes the command and returns its exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = io.WriteString(stderr, usage)
		return exitUsage
	}
	c := &command{out: stdout, err: stderr}
	switch args[0] {
	case "generate":
		return c.generate(args[1:])
	case "validate":
		return c.validate(args[1:])
	case "init":
		return c.init(args[1:])
	case "graph":
		return c.graph(args[1:])
	case "diff":
		return c.diff(args[1:])
	case "schedules":
		return c.schedules(args[1:])
	case "version":
		info, _ := debug.ReadBuildInfo()
		fmt.Fprintln(stdout, resolveVersion(Version, info))
		return exitOK
	case "help", "-h", "--help":
		_, _ = io.WriteString(stdout, usage)
		return exitOK
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return exitUsage
	}
}

func resolveVersion(set string, info *debug.BuildInfo) string {
	if set != "dev" || info == nil || info.Main.Version == "" || info.Main.Version == "(devel)" {
		return set
	}
	return info.Main.Version
}

type command struct {
	out, err io.Writer
}

func (c *command) fail(err error) int {
	fmt.Fprintln(c.err, err)
	return exitError
}

func (c *command) flags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(c.err)
	return fs
}

func load(path string) (*spec.Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spec: %w", err)
	}
	return spec.Parse(data)
}

// loadValid reads a spec and rejects it if it has validation errors.
func loadValid(path string) (*spec.Spec, error) {
	s, err := load(path)
	if err != nil {
		return nil, err
	}
	if err := spec.Join(spec.Validate(s)); err != nil {
		return nil, err
	}
	return s, nil
}

func (c *command) generate(args []string) int {
	fs := c.flags("generate")
	var comps gen.Components
	fs.BoolVar(&comps.Models, "models", false, "Go structs")
	fs.BoolVar(&comps.PubSub, "pubsub", false, "publishers and core subscribers")
	fs.BoolVar(&comps.Consumers, "consumers", false, "JetStream consumer handlers")
	fs.BoolVar(&comps.KV, "kv", false, "typed key-value buckets")
	fs.BoolVar(&comps.Schedules, "schedules", false, "delayed and recurring messages")
	fs.BoolVar(&comps.RRClient, "rr-client", false, "request functions")
	fs.BoolVar(&comps.RRServer, "rr-server", false, "handler interfaces and server wiring")
	terraform := fs.Bool("terraform", false, "Terraform for the NATS JetStream provider")
	permissions := fs.Bool("permissions", false, "NATS server permissions for each service")
	specPath := fs.String("spec", "nuts.yaml", "spec file")
	outDir := fs.String("out", filepath.Join("gen", "nuts"), "Go output directory")
	tfDir := fs.String("terraform-out", "terraform", "Terraform output directory")
	permsDir := fs.String("permissions-out", "permissions", "permissions output directory")
	service := fs.String("service", "", "generate only what this service uses")
	importPath := fs.String("import-path", "", "import path of --out")
	check := fs.Bool("check", false, "fail if the output is out of date")
	if fs.Parse(args) != nil {
		return exitUsage
	}
	if *terraform && *service != "" {
		fmt.Fprintln(c.err, "--terraform cannot be combined with --service: infrastructure is shared by every service")
		return exitUsage
	}
	if *permissions && *service != "" {
		fmt.Fprintln(c.err, "--permissions cannot be combined with --service: the file covers every service")
		return exitUsage
	}
	s, err := loadValid(*specPath)
	if err != nil {
		return c.fail(err)
	}
	if comps == (gen.Components{}) && !*terraform && !*permissions {
		comps = gen.DefaultComponents()
	}
	outputs := map[string][]byte{}
	if comps != (gen.Components{}) {
		path := *importPath
		if path == "" {
			if path, err = detectImportPath(*outDir); err != nil {
				return c.fail(err)
			}
		}
		files, err := gen.Go(s, gen.Options{Components: comps, Service: *service, ImportPath: path})
		if err != nil {
			return c.fail(err)
		}
		addOutputs(outputs, *outDir, files)
	}
	if *terraform {
		files, err := gen.Terraform(s)
		if err != nil {
			return c.fail(err)
		}
		addOutputs(outputs, *tfDir, files)
	}
	if *permissions {
		files, err := gen.Permissions(s)
		if err != nil {
			return c.fail(err)
		}
		addOutputs(outputs, *permsDir, files)
	}
	if *check {
		return c.checkOutputs(outputs)
	}
	for path, data := range outputs {
		if err := writeFile(path, data); err != nil {
			return c.fail(err)
		}
	}
	fmt.Fprintf(c.out, "wrote %d files\n", len(outputs))
	return exitOK
}

func addOutputs(outputs map[string][]byte, dir string, files map[string][]byte) {
	for name, data := range files {
		outputs[filepath.Join(dir, filepath.FromSlash(name))] = data
	}
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create output folder: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func (c *command) checkOutputs(outputs map[string][]byte) int {
	stale := 0
	for _, path := range spec.SortedKeys(outputs) {
		current, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(current, outputs[path]) {
			fmt.Fprintf(c.err, "out of date: %s\n", path)
			stale++
		}
	}
	if stale > 0 {
		return exitError
	}
	fmt.Fprintln(c.out, "generated files are up to date")
	return exitOK
}

// detectImportPath finds the nearest go.mod above dir and derives dir's import path.
func detectImportPath(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", dir, err)
	}
	for root := abs; ; root = filepath.Dir(root) {
		data, err := os.ReadFile(filepath.Join(root, "go.mod"))
		if err == nil {
			module, ok := moduleName(data)
			if !ok {
				return "", fmt.Errorf("no module line in %s/go.mod; pass --import-path", root)
			}
			rel, err := filepath.Rel(root, abs)
			if err != nil {
				return "", fmt.Errorf("resolve %s: %w", dir, err)
			}
			return strings.TrimSuffix(module+"/"+filepath.ToSlash(rel), "/."), nil
		}
		if root == filepath.Dir(root) {
			return "", errors.New("no go.mod found above the output folder; pass --import-path")
		}
	}
}

func moduleName(gomod []byte) (string, bool) {
	for line := range strings.Lines(string(gomod)) {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.Trim(strings.TrimSpace(rest), `"`), true
		}
	}
	return "", false
}

func (c *command) validate(args []string) int {
	fs := c.flags("validate")
	specPath := fs.String("spec", "nuts.yaml", "spec file")
	asJSON := fs.Bool("json", false, "machine-readable output")
	if fs.Parse(args) != nil {
		return exitUsage
	}
	var errs []spec.Error
	s, err := load(*specPath)
	if err != nil {
		errs = []spec.Error{{Path: *specPath, Message: err.Error()}}
	} else {
		errs = spec.Validate(s)
	}
	if *asJSON {
		_ = json.NewEncoder(c.out).Encode(struct {
			Valid  bool         `json:"valid"`
			Errors []spec.Error `json:"errors"`
		}{len(errs) == 0, errs})
	} else if len(errs) == 0 {
		fmt.Fprintln(c.out, "spec is valid")
	}
	if len(errs) > 0 {
		if !*asJSON {
			for _, e := range errs {
				fmt.Fprintln(c.err, e)
			}
		}
		return exitError
	}
	return exitOK
}

func (c *command) init(args []string) int {
	fs := c.flags("init")
	specPath := fs.String("spec", "nuts.yaml", "spec file")
	if fs.Parse(args) != nil {
		return exitUsage
	}
	if _, err := os.Stat(*specPath); err == nil {
		return c.fail(fmt.Errorf("%s already exists", *specPath))
	}
	if err := writeFile(*specPath, starterSpec); err != nil {
		return c.fail(err)
	}
	fmt.Fprintf(c.out, "wrote %s\n", *specPath)
	return exitOK
}

func nodeID(prefix, name string) string {
	return prefix + "_" + strings.NewReplacer("-", "_", ".", "_").Replace(name)
}

func (c *command) graph(args []string) int {
	fs := c.flags("graph")
	specPath := fs.String("spec", "nuts.yaml", "spec file")
	if fs.Parse(args) != nil {
		return exitUsage
	}
	s, err := loadValid(*specPath)
	if err != nil {
		return c.fail(err)
	}
	var b strings.Builder
	b.WriteString("flowchart LR\n")
	for _, name := range spec.SortedKeys(s.Events) {
		fmt.Fprintf(&b, "  %s([%q])\n", nodeID("evt", name), name)
	}
	for _, name := range spec.SortedKeys(s.KV) {
		fmt.Fprintf(&b, "  %s[(%q)]\n", nodeID("kv", name), name)
	}
	for _, name := range spec.SortedKeys(s.Services) {
		writeServiceEdges(&b, s, name)
	}
	_, _ = io.WriteString(c.out, b.String())
	return exitOK
}

func writeServiceEdges(b *strings.Builder, s *spec.Spec, name string) {
	x := s.Services[name]
	svc := nodeID("svc", name)
	for _, e := range x.Publishes {
		fmt.Fprintf(b, "  %s[%q] -->|publishes| %s\n", svc, name, nodeID("evt", e))
	}
	for _, e := range x.Subscribes {
		fmt.Fprintf(b, "  %s -->|subscribes| %s[%q]\n", nodeID("evt", e), svc, name)
	}
	for _, consumer := range x.Consumes {
		for _, e := range s.Consumers[consumer].Events {
			fmt.Fprintf(b, "  %s -->|%s| %s[%q]\n", nodeID("evt", e), consumer, svc, name)
		}
	}
	for _, call := range x.Calls {
		group, _, _ := strings.Cut(call, ".")
		for _, server := range spec.SortedKeys(s.Services) {
			for _, served := range s.Services[server].Serves {
				if served == group {
					fmt.Fprintf(b, "  %s -->|%s| %s\n", svc, call, nodeID("svc", server))
				}
			}
		}
	}
	for _, bucket := range spec.SortedKeys(x.KV) {
		fmt.Fprintf(b, "  %s -->|%s| %s\n", svc, x.KV[bucket], nodeID("kv", bucket))
	}
}

func (c *command) diff(args []string) int {
	fs := c.flags("diff")
	asJSON := fs.Bool("json", false, "machine-readable output")
	if fs.Parse(args) != nil {
		return exitUsage
	}
	if fs.NArg() != 2 {
		fmt.Fprintln(c.err, "usage: gonuts diff [--json] OLD NEW")
		return exitUsage
	}
	before, err := load(fs.Arg(0))
	if err != nil {
		return c.fail(err)
	}
	after, err := load(fs.Arg(1))
	if err != nil {
		return c.fail(err)
	}
	changes := diff.Compare(before, after)
	switch {
	case *asJSON:
		if changes == nil {
			changes = []diff.Change{}
		}
		_ = json.NewEncoder(c.out).Encode(changes)
	case len(changes) == 0:
		fmt.Fprintln(c.out, "no changes")
	default:
		for _, ch := range changes {
			fmt.Fprintln(c.out, ch)
		}
	}
	if diff.HasDestructive(changes) {
		return exitError
	}
	return exitOK
}

func (c *command) schedules(args []string) int {
	if len(args) == 0 || args[0] != "sync" {
		fmt.Fprintln(c.err, "usage: gonuts schedules sync --server URL [--spec PATH]")
		return exitUsage
	}
	fs := c.flags("schedules sync")
	specPath := fs.String("spec", "nuts.yaml", "spec file")
	server := fs.String("server", "", "NATS server URL")
	if fs.Parse(args[1:]) != nil {
		return exitUsage
	}
	if *server == "" {
		fmt.Fprintln(c.err, "--server is required")
		return exitUsage
	}
	s, err := loadValid(*specPath)
	if err != nil {
		return c.fail(err)
	}
	rt, err := nuts.Dial(*server)
	if err != nil {
		return c.fail(err)
	}
	defer rt.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	synced := 0
	for _, name := range spec.SortedKeys(s.Schedules) {
		x := s.Schedules[name]
		if x.Cron == "" && x.Every.Duration == 0 {
			continue
		}
		payload, err := gen.SchedulePayload(x)
		if err != nil {
			return c.fail(fmt.Errorf("schedule %s: %w", name, err))
		}
		err = nuts.SyncSchedule(ctx, rt, nuts.ScheduleSpec{
			Name: name, Subject: x.Subject, Target: s.Events[x.Event].Subject,
			Cron: x.Cron, Every: x.Every.Duration, TimeZone: x.TimeZone, Payload: payload,
		})
		if err != nil {
			return c.fail(err)
		}
		synced++
	}
	fmt.Fprintf(c.out, "synced %d schedule(s)\n", synced)
	return exitOK
}
