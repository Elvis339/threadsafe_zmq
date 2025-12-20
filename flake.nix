{
  description = "Thread-safe ZeroMQ wrapper for Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Use stable Rust with the rust-src component for IDE support
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" "clippy" "rustfmt" ];
        };

        # System dependencies required for ZeroMQ
        # zeromq requires libsodium for encryption and pkg-config for discovery
        buildInputs = with pkgs; [
          zeromq
          pkg-config
        ];

        # Additional development tools
        devTools = with pkgs; [
          just
          cargo-watch
          cargo-edit
          cargo-outdated
        ];

      in {
        devShells.default = pkgs.mkShell {
          buildInputs = buildInputs ++ devTools ++ [ rustToolchain ];

          # pkg-config needs to find zeromq
          PKG_CONFIG_PATH = "${pkgs.zeromq}/lib/pkgconfig";

          # Some systems need explicit library path for dynamic linking
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [ pkgs.zeromq ];

          # macOS-specific: set DYLD_LIBRARY_PATH
          DYLD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [ pkgs.zeromq ];

          shellHook = ''
            echo "threadsafe_zmq development environment"
            echo ""
            echo "Available commands:"
            echo "  just         - Show all available tasks"
            echo "  just build   - Build the library"
            echo "  just test    - Run tests"
            echo "  just example - Run the example (server + client)"
            echo ""
            echo "ZeroMQ version: $(pkg-config --modversion libzmq 2>/dev/null || echo 'not found')"
            echo "Rust version: $(rustc --version)"
          '';
        };

        # Allow running tests via `nix build`
        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = "threadsafe_zmq";
          version = "2.0.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          nativeBuildInputs = [ pkgs.pkg-config ];
          buildInputs = [ pkgs.zeromq ];
        };
      }
    );
}

