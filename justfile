# Task runner — https://just.systems
# Run `just` with no args to list recipes.

set shell := ["bash", "-cu"]
set dotenv-load

default:
    @just --list

# Format + lint (read-only)
check:
    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features -- -D warnings

# Apply auto-fixes (formatter + clippy)
fix:
    cargo fmt --all
    cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged

# Run tests (cargo-nextest, default profile)
test:
    cargo nextest run --all-features

# Run floci-gated integration tests (requires `just floci-up` + `just floci-seed`).
test-floci BUCKET="rustyant-dev":
    #!/usr/bin/env bash
    set -euo pipefail
    export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
    export AWS_ENDPOINT_URL=http://localhost:4566
    export RUSTYANT_FLOCI_URL=http://localhost:4566
    export RUSTYANT_FLOCI_BUCKET={{BUCKET}}
    cargo nextest run --all-features --test floci

# Run integration + doc tests (nextest does not execute doctests)
test-doc:
    cargo test --doc --all-features

# Audit dependencies for vulnerabilities + license/ban policy
audit:
    cargo audit
    cargo deny check

# Detect unused dependencies declared in Cargo.toml
unused:
    cargo machete

# Spellcheck
typos:
    typos

# Build release binary/library
build:
    cargo build --release --all-features

# Generate docs
doc:
    cargo doc --no-deps --all-features

# Update dependencies (respects Cargo.lock semver)
update:
    cargo update

# Everything the CI should run before merge
all: check test audit typos unused

# ---- Floci (local S3 emulator; https://github.com/floci-io/floci) ----
# Requires docker. Brings up S3 on http://localhost:4566. rustyant itself runs
# natively via `cargo lambda watch` — floci handles S3 only.

# Start floci and wait until healthy.
floci-up:
    docker compose up -d floci
    @echo "Waiting for floci..."
    @timeout 60 sh -c 'until curl -sf http://localhost:4566/ > /dev/null 2>&1; do sleep 1; done'
    @echo "Floci ready (http://localhost:4566)."

# Stop + remove the floci container (state is lost in `memory` mode — re-seed after restart).
floci-down:
    docker compose down

# Create the bucket. Idempotent — rustyant writes its own key objects on first SET.
floci-seed BUCKET="rustyant-dev":
    #!/usr/bin/env bash
    set -euo pipefail
    export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 AWS_ENDPOINT_URL=http://localhost:4566
    aws s3api create-bucket --bucket {{BUCKET}} >/dev/null 2>&1 || true
    echo "Bucket ready: s3://{{BUCKET}}/"

# Run rustyant against floci via cargo lambda watch (http://localhost:9000).
rustyant-dev BUCKET="rustyant-dev" KEY_PREFIX="rustyant/":
    #!/usr/bin/env bash
    set -euo pipefail
    export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
    export AWS_ENDPOINT_URL=http://localhost:4566
    export BUCKET={{BUCKET}} KEY_PREFIX={{KEY_PREFIX}}
    export LOG_FORMAT=pretty LOG_LEVEL=info ENVIRONMENT=development
    cargo lambda watch

# ---- Lambda (cargo-lambda) ----
# Install once:  cargo binstall cargo-lambda  (or cargo install cargo-lambda)

lambda-build:
    cargo lambda build --release --arm64

lambda-watch:
    cargo lambda watch

lambda-deploy FUNCTION="rant-rustyant":
    cargo lambda deploy {{FUNCTION}}

# ---- SAM / CloudFormation (WebSocket transport) ----
# Requires the `sam` CLI (https://docs.aws.amazon.com/serverless-application-model/).
# Uses the rust-cargolambda BuildMethod, which needs `--beta-features`.

# Validate the template — catches syntax + IAM/reference errors without deploying.
ws-template-validate:
    sam validate --template-file infra/template.yaml --lint

# Build the rustyant-ws Lambda artifact (runs cargo-lambda under the hood).
ws-template-build:
    sam build --template-file infra/template.yaml --beta-features

# Deploy the WebSocket stack. Pass BUCKET=<globally-unique-name> on first use.
# STACK defaults match the dev account convention (`rant-*`).
ws-template-deploy STACK="rant-rustyant-ws" BUCKET="":
    sam deploy --template-file .aws-sam/build/template.yaml \
        --stack-name {{STACK}} \
        --capabilities CAPABILITY_IAM \
        --resolve-s3 \
        --parameter-overrides BucketName={{BUCKET}}

# Remove the deployed stack (destructive — empties the bucket first).
ws-template-destroy STACK="rant-rustyant-ws":
    sam delete --stack-name {{STACK}} --no-prompts
