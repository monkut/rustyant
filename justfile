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

# Run tests (cargo-nextest, default profile). Auto-starts floci — every test
# harness now requires RUSTYANT_FLOCI_URL, so `just test` brings it up first.
test BUCKET="rustyant-dev": (floci-up) (floci-seed BUCKET)
    #!/usr/bin/env bash
    set -euo pipefail
    export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
    export AWS_ENDPOINT_URL=http://localhost:4566
    export RUSTYANT_FLOCI_URL=http://localhost:4566
    export RUSTYANT_FLOCI_BUCKET={{BUCKET}}
    cargo nextest run --all-features

# Run redis-py compatibility tests (requires python3 + the `redis` package).
test-redis-py:
    #!/usr/bin/env bash
    set -euo pipefail
    python3 -c 'import redis' 2>/dev/null || {
        echo "installing redis-py via pip --user"
        python3 -m pip install --user redis
    }
    cargo nextest run --all-features --test redis_py_compat

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

# ---- DynamoDB Local ----
# Used by tests/dynamodb.rs and `rustyant-dev-dynamodb` to exercise the
# DynamoDB backend without standing up real DynamoDB tables.

# Start DynamoDB Local and wait until healthy.
dynamodb-up:
    docker compose up -d dynamodb
    @echo "Waiting for DynamoDB Local..."
    @timeout 60 sh -c 'until curl -s -o /dev/null http://localhost:8000/ ; do sleep 1; done'
    @echo "DynamoDB Local ready (http://localhost:8000)."

# Stop the DynamoDB Local container (state is in-memory; recreate after restart).
dynamodb-down:
    docker compose stop dynamodb && docker compose rm -f dynamodb

# Create the seven backend tables (index + six per-kind). Idempotent —
# running twice is a no-op.
dynamodb-seed PREFIX="rustyant-":
    #!/usr/bin/env bash
    set -euo pipefail
    export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 AWS_ENDPOINT_URL=http://localhost:8000
    for suffix in index string hash list set zset stream; do
      table="{{PREFIX}}${suffix}"
      if aws dynamodb describe-table --table-name "$table" >/dev/null 2>&1; then
        echo "Table exists: $table"
      else
        aws dynamodb create-table \
          --table-name "$table" \
          --attribute-definitions AttributeName=pk,AttributeType=S \
          --key-schema AttributeName=pk,KeyType=HASH \
          --billing-mode PAY_PER_REQUEST >/dev/null
        echo "Created table: $table"
      fi
    done

# Run rustyant against DynamoDB Local via cargo lambda watch.
rustyant-dev-dynamodb PREFIX="rustyant-":
    #!/usr/bin/env bash
    set -euo pipefail
    export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
    export AWS_ENDPOINT_URL=http://localhost:8000
    export RUSTYANT_BACKEND=dynamodb RUSTYANT_DYNAMODB_TABLE_PREFIX={{PREFIX}}
    export LOG_FORMAT=pretty LOG_LEVEL=info ENVIRONMENT=development
    cargo lambda watch

# Run the DynamoDB-gated integration suite. Brings up DynamoDB Local + seeds
# the seven tables (index + six per-kind) before running tests/dynamodb.rs.
test-dynamodb PREFIX="rustyant-": (dynamodb-up) (dynamodb-seed PREFIX)
    #!/usr/bin/env bash
    set -euo pipefail
    export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
    export AWS_ENDPOINT_URL=http://localhost:8000
    export RUSTYANT_DYNAMODB_URL=http://localhost:8000
    export RUSTYANT_DYNAMODB_TABLE_PREFIX={{PREFIX}}
    cargo nextest run --all-features --test dynamodb

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
