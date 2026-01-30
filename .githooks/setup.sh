#!/bin/sh
#
# Setup script for Git hooks
# Run this once after cloning the repository
#

echo "Setting up Git hooks..."
git config core.hooksPath .githooks
echo "✓ Git hooks configured!"
echo ""
echo "The following hooks are now active:"
echo "  - pre-commit: runs cargo fmt --check and cargo clippy"
