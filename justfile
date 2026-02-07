# Justfile for WrongoDB development tasks

# Build the project
build:
    cargo build

# Run tests
test:
    cargo test

# Check compilation
check:
    cargo check

# Run clippy linter
clippy:
    cargo clippy -- -D warnings

# Format code
fmt:
    cargo fmt

# Clean build artifacts
clean:
    cargo clean

# Run all checks (check, test, clippy, fmt)
all: check test clippy fmt

# Run development server with auto-reload
dev-server:
    cargo watch --ignore "test.db*" -x 'run --bin wrongodb-server'

# Run wire-protocol A/B benchmark (WrongoDB vs MongoDB)
bench-wire-ab:
    cargo run --release --bin bench_wire_ab --
