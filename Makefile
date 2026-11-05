GREMLINS := go run github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0

.PHONY: all build test test-race test-terraform cover lint mutate generate check

all: check

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

# Applies the example to a real server. Needs terraform and network access.
test-terraform:
	GONUTS_TERRAFORM_TEST=1 go test -count=1 -run TestTerraformAppliesAndMatchesTheSpec ./internal/gen

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

lint:
	golangci-lint run ./...

# Mutation testing checks that the tests catch real bugs. It is slow, so it is
# not part of `make check`; CI runs it on a schedule.
mutate:
	$(GREMLINS) unleash --config .gremlins.yaml ./internal/spec
	$(GREMLINS) unleash --config .gremlins.yaml ./internal/diff
	$(GREMLINS) unleash --config .gremlins.yaml ./internal/gen
	$(GREMLINS) unleash --config .gremlins.yaml ./nuts

generate:
	go run ./cmd/gonuts generate --spec examples/shop/nuts.yaml --out examples/shop/gen
	go run ./cmd/gonuts generate --spec examples/shop/nuts.yaml --terraform --terraform-out examples/shop/terraform

# Everything CI requires before merge.
check: build lint test-race
	go run ./cmd/gonuts generate --check --spec examples/shop/nuts.yaml --out examples/shop/gen
	go run ./cmd/gonuts generate --check --spec examples/shop/nuts.yaml --terraform --terraform-out examples/shop/terraform
