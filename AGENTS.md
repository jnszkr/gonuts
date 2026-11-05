# Agent guide

Instructions for AI coding agents working on gonuts. Humans are welcome too.

## What this is

`nuts.yaml` describes everything a system does on NATS. The `gonuts` CLI generates typed Go code and Terraform from it. The generated code calls a small runtime, the `nuts` package.

## Map

| Path | Purpose |
| --- | --- |
| `cmd/gonuts` | Program entry point |
| `internal/cli` | Commands and flags |
| `internal/spec` | YAML parsing, validation, spec-to-NATS config mapping |
| `internal/gen` | Go generators (`code.go`), scoping (`scope.go`), Terraform (`terraform.go`) |
| `internal/diff` | Classifies spec changes for blue-green deployments |
| `internal/natstest` | Starts a real JetStream server for tests |
| `nuts` | Runtime imported by generated code; public API |
| `nuts/devnats` | Creates infrastructure for tests and local development only |
| `examples/shop` | Example spec, its generated output and end-to-end tests |

## Commands

```bash
make test             # all tests
make test-race        # all tests with the race detector
make lint             # golangci-lint
make generate         # regenerate examples/shop after changing a generator
make check            # everything CI requires
make mutate           # Gremlins mutation testing; slow
make test-terraform   # applies generated Terraform to a real server; needs terraform and network
```

Before calling work done, run `make check`.

## Rules

1. **Write the test first and watch it fail.** This covers features and bug fixes. Prefer a real server from `internal/natstest` over mocks.
2. **Applications never change infrastructure.** Generated code and `nuts` only bind to streams, consumers and buckets. Only `nuts/devnats` and Terraform create them.
3. **Never edit generated files.** Anything under `examples/shop/gen` or `examples/shop/terraform` comes from `make generate`. Change the generator and regenerate.
4. **Every generator change needs a compile check.** `TestEveryCombinationCompiles` builds each flag combination; add a case for a new flag.
5. **Secrets never go in the YAML or generated output.**
6. **Output must be deterministic.** Iterate maps through `spec.SortedKeys`.
7. **Never drop a setting silently.** If Terraform cannot express a field, generation stops with an error.

## Adding a NATS setting

`TestSpecCoversEveryNATSSetting` fails when a `nats.go` upgrade adds a field the spec lacks. To add one:

1. Add the field to the struct in `internal/spec/spec.go`, using the NATS JSON name as its YAML name.
2. Map it in `internal/spec/infra.go`.
3. Map it in `internal/gen/terraform.go`, or report it as unsupported.
4. Extend the `TestInfraMaps*` and `TestTerraformMaps*` tests.

## Tips for working with the CLI

- `gonuts validate --json` and `gonuts diff --json` give structured output.
- Exit codes: 0 success, 1 error, 2 bad usage. `gonuts diff` exits 1 on a destructive change.
- `gonuts generate --check` confirms output is current and writes nothing.

## Test quality

Gremlins mutates production code and checks that a test fails, with a minimum efficacy set in `.gremlins.yaml`. When it reports a surviving mutant, add a test for that behavior. Do not lower the threshold to get a pass.

## Commits

Keep commits small and focused, in the imperative mood. Do not push or publish a release unless asked.
