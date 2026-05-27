#!/usr/bin/env bash
# Run ClickHouse official stateless tests against zighouse.
# Usage: ./scripts/ch-official-test.sh [test_dir] [port]
set -euo pipefail

TEST_DIR="${1:-/tmp/ch-tests-dl}"
PORT="${2:-29099}"
BINARY="./zig-out/bin/zighouse"
DATA_DIR="$(mktemp -d)"

cleanup() { kill "$SPID" 2>/dev/null || true; rm -rf "$DATA_DIR"; }
trap cleanup EXIT

"$BINARY" serve "--data-dir=$DATA_DIR" "--port=$PORT" 2>/dev/null &
SPID=$!
sleep 0.6

pass=0; fail=0; skip=0

run_sql() {
  local sql="$1"
  curl -s --noproxy localhost "http://localhost:$PORT/?query=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$sql")&default_format=TabSeparated"
}

for sql_file in "$TEST_DIR"/*.sql; do
  name="$(basename "$sql_file" .sql)"
  ref_file="$TEST_DIR/${name}.reference"

  # Skip if reference file missing
  [[ ! -f "$ref_file" ]] && { ((skip++)); continue; }

  # Skip 404 placeholder files
  first_line="$(head -c 14 "$sql_file")"
  [[ "$first_line" == "404: Not Found" ]] && { ((skip++)); continue; }

  actual=""
  stmt_buf=""
  while IFS= read -r line; do
    # Skip blank and pure comment lines
    [[ -z "$line" ]] && continue
    trimmed="${line#"${line%%[![:space:]]*}"}"
    [[ "$trimmed" == --* ]] && continue
    # Skip SET statements
    [[ "$trimmed" == SET\ * || "$trimmed" == set\ * ]] && continue
    # Skip lines with serverError annotation (they are expected to error)
    [[ "$line" == *"-- { serverError"* ]] && continue

    # Accumulate into stmt_buf
    if [[ -n "$stmt_buf" ]]; then
      stmt_buf="${stmt_buf} ${trimmed}"
    else
      stmt_buf="$trimmed"
    fi

    # Check if stmt_buf ends with a semicolon (end of statement)
    if [[ "$stmt_buf" == *\; ]]; then
      sql="${stmt_buf%;}"
      stmt_buf=""
      [[ -z "$sql" ]] && continue
      res="$(run_sql "$sql" 2>/dev/null || true)"
      [[ -n "$res" ]] && actual="${actual}${res}"$'\n'
    fi
  done < "$sql_file"
  # Flush any remaining buffer (no trailing semicolon)
  if [[ -n "$stmt_buf" ]]; then
    sql="$stmt_buf"
    res="$(run_sql "$sql" 2>/dev/null || true)"
    [[ -n "$res" ]] && actual="${actual}${res}"$'\n'
  fi

  ref="$(cat "$ref_file")"
  # Normalize: strip trailing newline from both
  actual_norm="${actual%$'\n'}"
  ref_norm="${ref%$'\n'}"

  if [[ "$actual_norm" == "$ref_norm" ]]; then
    echo "PASS $name"
    ((pass++))
  else
    echo "FAIL $name"
    if [[ "${VERBOSE:-0}" == "1" ]]; then
      diff <(echo "$ref_norm") <(echo "$actual_norm") | head -10
    fi
    ((fail++))
  fi
done

echo ""
echo "Results: $pass passed, $fail failed, $skip skipped"
