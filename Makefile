.PHONY: default check unit-test integration-test test all debug release

default: check debug

check:
	cargo check --all --all-targets --all-features
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D clippy::all

debug:
	cargo build

release:
	cargo build --release

unit-test:
	cargo test --all

test: unit-test

all: check test
