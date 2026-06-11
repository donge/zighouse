#!/usr/bin/env bash
# Lightweight compact-part integration gate.
#
# This exercises the server write path:
#   RowBinary / RowBinaryWithNamesAndTypes -> CompactPartWriterSession
# and then reads through the normal SELECT path backed by PartScanBridge.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="${ZIGHOUSE_BIN:-$REPO_DIR/zig-out/bin/zighouse}"
PORT="${ZIGHOUSE_COMPACT_PORT:-19171}"
HTTP_PORT=$((PORT + 1))
DATA_DIR="$(mktemp -d /tmp/zh-compact-query-XXXXXX)"
SERVER_PID=""
CURL="curl -sS --noproxy localhost"

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

urlenc() {
    python3 -c 'import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))' "$1"
}

wait_for_server() {
    for i in $(seq 1 40); do
        if $CURL "http://127.0.0.1:$HTTP_PORT/?query=SELECT+1" >/dev/null 2>&1; then
            return
        fi
        sleep 0.2
    done
    echo "ERROR: server did not start"
    cat /tmp/zh-compact-query-server.log 2>/dev/null || true
    exit 1
}

post_sql() {
    $CURL -X POST "http://127.0.0.1:$HTTP_PORT/" --data-binary "$1" >/dev/null
}

post_payload() {
    local sql="$1"
    local file="$2"
    local encoded
    encoded="$(urlenc "$sql")"
    $CURL --data-binary "@$file" "http://127.0.0.1:$HTTP_PORT/?query=$encoded" >/dev/null
}

select_tsv() {
    local sql="$1"
    local encoded
    encoded="$(urlenc "$sql")"
    $CURL "http://127.0.0.1:$HTTP_PORT/?query=$encoded&default_format=TabSeparated"
}

expect_eq() {
    local name="$1"
    local expected="$2"
    local actual="$3"
    if [[ "$actual" != "$expected" ]]; then
        echo "FAIL $name"
        echo "expected:"
        printf '%s\n' "$expected"
        echo "actual:"
        printf '%s\n' "$actual"
        exit 1
    fi
    echo "PASS $name"
}

if [[ ! -x "$BINARY" ]]; then
    echo "Building zighouse..."
    (cd "$REPO_DIR" && zig build -Doptimize=ReleaseFast)
fi

"$BINARY" serve "--data-dir=$DATA_DIR" "--port=$PORT" &>/tmp/zh-compact-query-server.log &
SERVER_PID=$!
wait_for_server

post_sql "CREATE TABLE default.compact_events (id Int32, user_id Int64, category String, score Int32) ENGINE = MergeTree ORDER BY id"

python3 - <<'PY' > /tmp/zh_compact_events_1.bin
import struct, sys

def varuint(n):
    out = bytearray()
    while True:
        b = n & 0x7f
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            return bytes(out)

def string(s):
    b = s.encode()
    return varuint(len(b)) + b

rows = [
    (1, 10, "alpha", 5),
    (2, 20, "", 7),
    (3, 10, "beta", 4),
]
out = bytearray()
for row in rows:
    out += struct.pack("<iq", row[0], row[1])
    out += string(row[2])
    out += struct.pack("<i", row[3])
sys.stdout.buffer.write(out)
PY

python3 - <<'PY' > /tmp/zh_compact_events_2.bin
import struct, sys

def varuint(n):
    out = bytearray()
    while True:
        b = n & 0x7f
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            return bytes(out)

def string(s):
    b = s.encode()
    return varuint(len(b)) + b

rows = [
    (4, 30, "alpha", 9),
    (5, 20, "gamma", 3),
    (6, 20, "beta", 8),
]
out = bytearray()
for row in rows:
    out += struct.pack("<iq", row[0], row[1])
    out += string(row[2])
    out += struct.pack("<i", row[3])
sys.stdout.buffer.write(out)
PY

post_payload "INSERT INTO default.compact_events FORMAT RowBinary" /tmp/zh_compact_events_1.bin
post_payload "INSERT INTO default.compact_events FORMAT RowBinary" /tmp/zh_compact_events_2.bin

parts="$(find "$DATA_DIR/default/compact_events/parts" -name 'all_*_*_0' -type d 2>/dev/null | wc -l | tr -d ' ')"
if [[ "$parts" -lt 2 ]]; then
    echo "FAIL expected at least two compact parts, found $parts"
    exit 1
fi
echo "PASS two compact parts created"

expect_eq "count" "6" "$(select_tsv "SELECT count(*) FROM default.compact_events")"
expect_eq "int filter projection" $'2\n4\n6' "$(select_tsv "SELECT id FROM default.compact_events WHERE score > 6 ORDER BY id")"
expect_eq "string non-empty filter" "5" "$(select_tsv "SELECT count(*) FROM default.compact_events WHERE category <> ''")"
expect_eq "grouped string topK" $'alpha\t2\nbeta\t2\ngamma\t1' "$(select_tsv "SELECT category, count(*) AS c FROM default.compact_events WHERE category <> '' GROUP BY category ORDER BY category")"
expect_eq "count distinct" "3" "$(select_tsv "SELECT count(distinct user_id) FROM default.compact_events")"
expect_eq "limit offset" $'3\n4' "$(select_tsv "SELECT id FROM default.compact_events ORDER BY id LIMIT 2 OFFSET 2")"
expect_eq "sort-key equality" "4" "$(select_tsv "SELECT id FROM default.compact_events WHERE id = 4 ORDER BY id")"
expect_eq "grouped count topK" $'20\t3\n10\t2' "$(select_tsv "SELECT user_id, COUNT(*) FROM default.compact_events GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 2")"

python3 - <<'PY' > /tmp/zh_compact_wnat.bin
import struct, sys

def varuint(n):
    out = bytearray()
    while True:
        b = n & 0x7f
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            return bytes(out)

def string(s):
    b = s.encode()
    return varuint(len(b)) + b

out = bytearray()
out += varuint(2)
out += string("id")
out += string("label")
out += string("Int32")
out += string("String")
for row in [(100, "hello"), (200, "world")]:
    out += struct.pack("<i", row[0])
    out += string(row[1])
sys.stdout.buffer.write(out)
PY

post_payload "INSERT INTO default.compact_auto FORMAT RowBinaryWithNamesAndTypes" /tmp/zh_compact_wnat.bin
if [[ ! -f "$DATA_DIR/default/compact_auto/schema.json" ]]; then
    echo "FAIL RowBinaryWithNamesAndTypes did not persist schema.json"
    exit 1
fi
echo "PASS RowBinaryWithNamesAndTypes schema persisted"
expect_eq "wnat count" "2" "$(select_tsv "SELECT count(*) FROM default.compact_auto")"
expect_eq "wnat string order" $'hello\nworld' "$(select_tsv "SELECT label FROM default.compact_auto ORDER BY id")"

post_sql "CREATE TABLE test.values_events (id Int64, name String) ENGINE=MergeTree() ORDER BY id"
post_sql "INSERT INTO test.values_events VALUES (42, 'hello')"
values_count_file="$(find "$DATA_DIR/test/values_events/parts" -name count.txt -type f | head -n 1)"
if [[ -z "$values_count_file" || "$(cat "$values_count_file")" != "1" ]]; then
    echo "FAIL VALUES insert did not write expected compact part row count"
    exit 1
fi
echo "PASS VALUES non-default compact part created"
expect_eq "non-default db values count" "1" "$(select_tsv "SELECT count(*) FROM test.values_events")"
expect_eq "non-default db values id" "42" "$(select_tsv "SELECT id FROM test.values_events")"
expect_eq "non-default db values string" "hello" "$(select_tsv "SELECT name FROM test.values_events")"

echo "PASS compact part query integration"
