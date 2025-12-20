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

# Run the example server (in foreground)
server:
    cd example && RUST_LOG=info cargo run --bin server

# Run the example client
client:
    cd example && RUST_LOG=info cargo run --bin client

# Run example: starts server in background, runs client, then stops server
example:
    #!/usr/bin/env bash
    set -e
    echo "Building examples..."
    cd example && cargo build
    echo ""
    echo "Starting server in background..."
    RUST_LOG=info cargo run --bin server &
    SERVER_PID=$!
    sleep 2
    echo ""
    echo "Running client..."
    RUST_LOG=info cargo run --bin client || true
    echo ""
    echo "Stopping server..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
    echo "Done."

# Install nix (for systems without it)
install-nix:
    #!/usr/bin/env bash
    if command -v nix &> /dev/null; then
        echo "Nix is already installed"
        nix --version
    else
        echo "Installing Nix..."
        curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install
        echo ""
        echo "Nix installed. Please restart your shell or run:"
        echo "  . /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh"
    fi

# Enter nix development shell
shell:
    nix develop

# Publish to crates.io (dry run)
publish-dry:
    cargo publish --dry-run --all-features

# Publish to crates.io
publish:
    cargo publish --all-features

