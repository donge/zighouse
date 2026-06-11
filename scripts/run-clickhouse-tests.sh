#!/usr/bin/env bash
# scripts/run-clickhouse-tests.sh
# Run ClickHouse stateless SQL tests against zighouse.
#
# Fetches .sql and .reference files from ClickHouse/ClickHouse GitHub repo,
# executes them against a local zighouse server, and reports pass/fail.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$REPO_DIR/zig-out/bin/zighouse"
OUT_DIR="$REPO_DIR/clickhouse-test-results"
TEST_CACHE="$OUT_DIR/tests"
PORT="${ZIGHOUSE_PORT:-19997}"
HTTP_PORT=$((PORT + 1))
DATA_DIR="$(mktemp -d /tmp/zh-ch-test-XXXXXX)"
SERVER_PID=""
CURL="curl -sf --noproxy localhost --max-time 10"
GH_BASE="https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/tests/queries/0_stateless"

mkdir -p "$TEST_CACHE"

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

fetch_test() {
    local name="$1"
    local sql_file="$TEST_CACHE/${name}.sql"
    local ref_file="$TEST_CACHE/${name}.reference"
    local need_dl=0

    if [[ ! -f "$sql_file" ]]; then
        echo "  fetching ${name}.sql ..." >&2
        curl -sf "$GH_BASE/${name}.sql" -o "$sql_file" || return 1
        need_dl=1
    fi
    if [[ ! -f "$ref_file" ]]; then
        echo "  fetching ${name}.reference ..." >&2
        curl -sf "$GH_BASE/${name}.reference" -o "$ref_file" || return 1
        need_dl=1
    fi
    return 0
}

run_sql() {
    local stmt="$1"
    local encoded
    encoded="$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$stmt" 2>/dev/null)"
    $CURL "http://localhost:$HTTP_PORT/?query=$encoded&default_format=TabSeparated" 2>/dev/null || true
}

run_test() {
    local name="$1"
    local sql_file="$TEST_CACHE/${name}.sql"
    local ref_file="$TEST_CACHE/${name}.reference"

    if ! fetch_test "$name"; then
        return 2  # test not found
    fi

    # Skip tests that require features zighouse doesn't have
    if grep -qiE '(kafka|zookeeper|rabbitmq|mysql|postgresql|mongodb|odbc|jdbc|dictionary|redis|hdfs|s3|grpc|distributed|cluster|replicated|materialized.view|window.function|explain|system\.|grant|revoke|role|user\s+default)' "$sql_file"; then
        return 3  # skipped (unsupported feature)
    fi

    local actual_output=""
    while IFS= read -r line || [[ -n "$line" ]]; do
        stmt="$(echo "$line" | sed 's/;[[:space:]]*$//' | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')"
        [[ -z "$stmt" ]] && continue

        # Skip comment-only lines and SET statements
        if echo "$stmt" | grep -qE '^(--|SET\s+|CREATE\s+DATABASE|USE\s+|SYSTEM\s+)'; then
            continue
        fi

        upper="$(echo "$stmt" | tr '[:lower:]' '[:upper:]' | sed 's/^[[:space:]]*//')"
        if [[ "$upper" == SELECT* ]] || [[ "$upper" == WITH* ]] || [[ "$upper" == DESCRIBE* ]] || [[ "$upper" == EXISTS* ]]; then
            resp="$(run_sql "$stmt")"
            if [[ -n "$resp" ]]; then
                actual_output="${actual_output}${resp}"$'\n'
            fi
        else
            # DDL/INSERT/DROP — POST
            $CURL -X POST "http://localhost:$HTTP_PORT/" --data-binary "$stmt" >/dev/null 2>&1 || true
        fi
    done < <(grep -v '^--' "$sql_file" | tr ';' '\n')

    actual_trimmed="$(printf '%s' "$actual_output" | sed 's/[[:space:]]*$//')"
    expected_trimmed="$(sed 's/[[:space:]]*$//' "$ref_file")"

    if [[ "$actual_trimmed" == "$expected_trimmed" ]]; then
        return 0  # PASS
    else
        return 1  # FAIL
    fi
}

# ── Build ──
if [[ ! -f "$BINARY" ]]; then
    echo "=== Building zighouse ==="
    (cd "$REPO_DIR" && zig build -Doptimize=ReleaseFast)
fi

# ── Start server ──
echo "=== Starting server (TCP=$PORT, HTTP=$HTTP_PORT) ==="
"$BINARY" serve "--data-dir=$DATA_DIR" "--port=$PORT" &>/dev/null &
SERVER_PID=$!

for i in $(seq 1 30); do
    if curl -sf "http://localhost:$HTTP_PORT/ping" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
    if [[ $i -eq 30 ]]; then
        echo "ERROR: server did not start"
        exit 1
    fi
done

# ── Discover tests ──
echo "=== Fetching test list ==="
PYTHON_SCRIPT='
import json, urllib.request, sys, re
url = "https://api.github.com/repos/ClickHouse/ClickHouse/contents/tests/queries/0_stateless"
req = urllib.request.Request(url, headers={"Accept": "application/vnd.github.v3+json"})
try:
    data = json.loads(urllib.request.urlopen(req, timeout=30).read())
except Exception as e:
    print("ERROR: " + str(e), file=sys.stderr)
    sys.exit(1)
names = set()
for f in data:
    m = re.match(r"(\d+[a-z_]+\w+)\.sql$", f["name"])
    if m:
        names.add(m.group(1))
    m = re.match(r"(\d+[a-z_]+\w+)\.reference$", f["name"])
    if m:
        names.add(m.group(1))
for n in sorted(names):
    print(n)
'
TEST_NAMES=$(python3 -c "$PYTHON_SCRIPT" 2>/dev/null || true)
TEST_COUNT=$(echo "$TEST_NAMES" | wc -l)
echo "Found $TEST_COUNT test names"

# ── Run tests ──
echo ""
echo "=== Running tests ==="
PASS=0
FAIL=0
SKIP=0
NOT_FOUND=0
FAILED_NAMES=()
SKIP_NAMES=()

for name in $TEST_NAMES; do
    result=0
    run_test "$name" || result=$?

    case $result in
        0) echo "PASS  $name"; PASS=$((PASS + 1)) ;;
        1) echo "FAIL  $name"; FAIL=$((FAIL + 1)); FAILED_NAMES+=("$name") ;;
        2) echo "MISS  $name  (no .sql/.reference)"; NOT_FOUND=$((NOT_FOUND + 1)) ;;
        3) echo "SKIP  $name  (uses unsupported feature)"; SKIP=$((SKIP + 1)); SKIP_NAMES+=("$name") ;;
    esac
done

echo ""
echo "=== Results ==="
echo "PASS:       $PASS"
echo "FAIL:       $FAIL"
echo "SKIP:       $SKIP"
echo "NOT FOUND:  $NOT_FOUND"
echo "TOTAL:      $((PASS + FAIL + SKIP + NOT_FOUND))"

if [[ ${#FAILED_NAMES[@]} -gt 0 ]]; then
    echo ""
    echo "Failed tests:"
    for t in "${FAILED_NAMES[@]}"; do echo "  $t"; done
fi
