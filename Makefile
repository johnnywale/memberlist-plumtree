# Local mirror of .github/workflows/ci.yml — every job is reproducible with
# `make <target>`, and `make ci` runs the whole suite.
#
# Targets map 1:1 to CI jobs:
#   fmt       -> CI "Formatting"
#   clippy    -> CI "Clippy"
#   docs      -> CI "Documentation"
#   audit     -> CI "Security Audit"          (skipped if cargo-deny missing)
#   test      -> CI "Test (stable matrix)"
#   features  -> CI "Feature Combinations"
#   ci        -> all of the above, fail-fast
#
# Set FAIL_FAST=0 to keep going past the first failure.

CARGO ?= cargo
CARGO_TERM_COLOR := always
export CARGO_TERM_COLOR

FAIL_FAST ?= 1

.PHONY: all ci fmt fmt-fix clippy docs audit test features \
        features-no-default features-tokio features-tokio-quic \
        features-sync features-storage-sled examples fuzz-check clean help

help:
	@echo "Targets:"
	@echo "  all           Run everything: ci + examples + fuzz-check (nightly, if available)"
	@echo "  ci            Run every CI validation (fmt + clippy + docs + audit + test + features)"
	@echo "  fmt           cargo fmt -- --check"
	@echo "  fmt-fix       cargo fmt (autoformat in place)"
	@echo "  clippy        cargo clippy --all-features --all-targets -- -D warnings"
	@echo "  docs          RUSTDOCFLAGS=-D warnings cargo doc --no-deps --all-features"
	@echo "  audit         cargo deny check (skipped if cargo-deny not installed)"
	@echo "  test          cargo test --all-features"
	@echo "  features      All feature-combination checks"
	@echo "  examples      cargo build --examples --all-features"
	@echo "  fuzz-check    cargo +nightly fuzz check (skipped if nightly missing)"

# Single entry point — everything we can validate locally, fail-fast.
all: ci examples fuzz-check
	@echo ""
	@echo "==> ALL VALIDATIONS PASSED (ci + examples + fuzz-check)"

ci: fmt clippy docs audit test features
	@echo ""
	@echo "==> ALL CI VALIDATIONS PASSED"

fmt:
	@echo "==> [fmt] cargo fmt -- --check"
	$(CARGO) fmt -- --check

fmt-fix:
	$(CARGO) fmt

clippy:
	@echo "==> [clippy] cargo clippy --all-features --all-targets -- -D warnings"
	$(CARGO) clippy --all-features --all-targets -- -D warnings

docs:
	@echo "==> [docs] cargo doc --no-deps --all-features (RUSTDOCFLAGS=-D warnings)"
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --no-deps --all-features

audit:
	@echo "==> [audit] cargo deny check"
	@if command -v cargo-deny >/dev/null 2>&1; then \
	  $(CARGO) deny check; \
	else \
	  echo "    cargo-deny not installed; install with: cargo install cargo-deny --locked"; \
	  echo "    SKIPPING (CI installs it automatically)"; \
	fi

test:
	@echo "==> [test] cargo test --all-features"
	$(CARGO) test --all-features

# Mirrors the "Feature Combinations" job
features: features-no-default features-tokio features-tokio-quic features-sync features-storage-sled

features-no-default:
	@echo "==> [features] cargo check --no-default-features"
	$(CARGO) check --no-default-features

features-tokio:
	@echo "==> [features] cargo check --no-default-features --features tokio"
	$(CARGO) check --no-default-features --features "tokio"

features-tokio-quic:
	@echo "==> [features] cargo check --no-default-features --features tokio,quic"
	$(CARGO) check --no-default-features --features "tokio,quic"

features-sync:
	@echo "==> [features] cargo test --features tokio,sync,metrics"
	$(CARGO) test --features "tokio,sync,metrics"

features-storage-sled:
	@echo "==> [features] cargo check --features storage-sled"
	$(CARGO) check --features "storage-sled"

examples:
	@echo "==> [examples] cargo build --examples --all-features"
	$(CARGO) build --examples --all-features

# Requires nightly (libFuzzer needs -Zsanitizer=address). Soft-skip if not present
# so `make all` stays runnable on a stable-only host.
fuzz-check:
	@echo "==> [fuzz-check] cargo +nightly fuzz check"
	@if rustup toolchain list 2>/dev/null | grep -q '^nightly'; then \
	  if command -v cargo-fuzz >/dev/null 2>&1; then \
	    $(CARGO) +nightly fuzz check; \
	  else \
	    echo "    cargo-fuzz not installed; install with: cargo install cargo-fuzz"; \
	    echo "    SKIPPING"; \
	  fi; \
	else \
	  echo "    nightly toolchain not installed; install with: rustup toolchain install nightly"; \
	  echo "    SKIPPING"; \
	fi

clean:
	$(CARGO) clean
