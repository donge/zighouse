#!/usr/bin/env bash
# scripts/functional-test.sh
# Run ZigHouse functional tests: start server, execute SQL files, diff output vs .reference.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TESTS_DIR="$REPO_DIR/tests/functional"
BINARY="$REPO_DIR/zig-out/bin/zighouse"
PORT="${ZIGHOUSE_PORT:-9999}"
HTTP_PORT=$((PORT + 1))   # TCP=PORT, HTTP=PORT+1 (zighouse convention)
DATA_DIR="$(mktemp -d /tmp/zh-func-test-XXXXXX)"
SERVER_PID=""
CURL="curl -s --noproxy localhost"

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# Build if necessary.
if [[ ! -f "$BINARY" ]]; then
    echo "Building zighouse..."
    (cd "$REPO_DIR" && zig build -Doptimize=ReleaseFast)
fi

# ── Pre-import: populate data dir before server starts so schema is auto-loaded ──
FIXTURE_PARQUET="$REPO_DIR/data/fixture_hits.parquet"
if [[ -f "$FIXTURE_PARQUET" ]]; then
    "$BINARY" import-parquet --format=ch-compact "$FIXTURE_PARQUET" "$DATA_DIR/default" hits >/dev/null 2>&1 || true
fi

# Start server.
"$BINARY" serve "--data-dir=$DATA_DIR" "--port=$PORT" &>/tmp/zh-func-server.log &
SERVER_PID=$!
echo "Server PID=$SERVER_PID, data=$DATA_DIR"

# Wait for server to be ready.
for i in $(seq 1 30); do
    if $CURL "http://localhost:$HTTP_PORT/?query=SELECT+1" >/dev/null 2>&1; then
        break
    fi
    sleep 0.2
    if [[ $i -eq 30 ]]; then
        echo "ERROR: server did not start in time"
        cat /tmp/zh-func-server.log
        exit 1
    fi
done

PASS=0
FAIL=0
ERRORS=()

for sql_file in "$TESTS_DIR"/*.sql; do
    test_name="$(basename "$sql_file" .sql)"
    ref_file="$TESTS_DIR/$test_name.reference"

    if [[ ! -f "$ref_file" ]]; then
        echo "SKIP  $test_name  (no .reference file)"
        continue
    fi

    # Optional pre-test shell hook (e.g. run compactor, import extra data).
    pretest_file="$TESTS_DIR/$test_name.pretest"
    if [[ -f "$pretest_file" ]]; then
        BINARY="$BINARY" DATA_DIR="$DATA_DIR" PORT="$PORT" bash "$pretest_file" || true
    fi

    # Execute each statement in the file via HTTP.
    # Non-SELECT statements go via POST body; SELECT goes via ?query=.
    actual_output=""
    while IFS= read -r line || [[ -n "$line" ]]; do
        # Strip trailing semicolon and whitespace.
        stmt="$(echo "$line" | sed 's/;[[:space:]]*$//' | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')"
        [[ -z "$stmt" ]] && continue

        upper="$(echo "$stmt" | tr '[:lower:]' '[:upper:]' | sed 's/^[[:space:]]*//')"
        if [[ "$upper" == SELECT* ]] || [[ "$upper" == WITH* ]]; then
            # GET with ?query= → TabSeparated (no header) output, matching ClickHouse behaviour.
            encoded="$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$stmt")"
            response="$($CURL "http://localhost:$HTTP_PORT/?query=$encoded&default_format=TabSeparated")"
            if [[ -n "$response" ]]; then
                actual_output="${actual_output}${response}"$'\n'
            fi
        else
            # POST body for DDL/INSERT.
            $CURL -X POST "http://localhost:$HTTP_PORT/" --data-binary "$stmt" >/dev/null
        fi
    done < <(grep -v '^--' "$sql_file" | tr ';' '\n')

    # Trim trailing newline from actual output for comparison.
    actual_trimmed="$(printf '%s' "$actual_output" | sed 's/[[:space:]]*$//')"
    expected_trimmed="$(sed 's/[[:space:]]*$//' "$ref_file")"

    # Optional normalize hook: pipe both through DATA_DIR-aware sed substitution.
    normalize_file="$TESTS_DIR/$test_name.normalize"
    if [[ -f "$normalize_file" ]]; then
        actual_trimmed="$(printf '%s' "$actual_trimmed" | DATA_DIR="$DATA_DIR" bash "$normalize_file")"
        expected_trimmed="$(printf '%s' "$expected_trimmed" | DATA_DIR="$DATA_DIR" bash "$normalize_file")"
    fi

    if [[ "$actual_trimmed" == "$expected_trimmed" ]]; then
        echo "PASS  $test_name"
        PASS=$((PASS + 1))
    else
        echo "FAIL  $test_name"
        echo "  expected: $(echo "$expected_trimmed" | head -5)"
        echo "  actual:   $(echo "$actual_trimmed" | head -5)"
        ERRORS+=("$test_name")
        FAIL=$((FAIL + 1))
    fi
done

echo ""
echo "Results: $PASS passed, $FAIL failed"

if [[ ${#ERRORS[@]} -gt 0 ]]; then
    echo "Failed tests:"
    for t in "${ERRORS[@]}"; do echo "  - $t"; done
    exit 1
fi
exit 0
