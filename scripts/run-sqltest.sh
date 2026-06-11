#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== Building zighouse (release) ==="
zig build -Doptimize=ReleaseFast -Dstrip=true

echo ""
echo "=== Running sqltest against zighouse ==="
echo ""

cd sqltest
python3 generate_tests.py dbs.zighouse 2>&1
