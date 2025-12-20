default:
    @just --list

build:
    cargo build --all-features

build-release:
    cargo build --all-features --release

test:
    cargo test --all-features

lint:
    cargo clippy --all-features -- -D warnings

fmt-check:
    cargo fmt -- --check

check: fmt-check lint test

doc:
    cargo doc --all-features --no-deps --open

# Run benchmark
bench:
    cargo run --release --features bench,async --bin bench

# Sync server (port 5555)
server:
    cd example && RUST_LOG=info cargo run --bin server

# Sync client
client:
    cd example && RUST_LOG=info cargo run --bin client

# Async server (port 5556)
async-server:
    cd example && RUST_LOG=info cargo run --bin async_server

# Async client
async-client:
    cd example && RUST_LOG=info cargo run --bin async_client

# Run sync example
example:
    #!/usr/bin/env bash
    set -e
    cd example && cargo build
    echo "Starting sync server..."
    RUST_LOG=info cargo run --bin server &
    SERVER_PID=$!
    sleep 2
    echo "Running sync client..."
    RUST_LOG=info cargo run --bin client || true
    echo "Stopping server..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true

# Run async example
example-async:
    #!/usr/bin/env bash
    set -e
    cd example && cargo build
    echo "Starting async server..."
    RUST_LOG=info cargo run --bin async_server &
    SERVER_PID=$!
    sleep 2
    echo "Running async client..."
    RUST_LOG=info cargo run --bin async_client || true
    echo "Stopping server..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true

# Install nix
install-nix:
    #!/usr/bin/env bash
    if command -v nix &> /dev/null; then
        echo "Nix is already installed"
        nix --version
    else
        echo "Installing Nix..."
        curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install
    fi

# Enter nix shell
shell:
    nix develop

# Publish dry run
publish-dry:
    cargo publish --dry-run --all-features

# Publish to crates.io
publish:
    cargo publish --all-features
