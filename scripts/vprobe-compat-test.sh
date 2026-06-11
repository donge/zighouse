#!/usr/bin/env bash
# scripts/vprobe-compat-test.sh
#
# ZigHouse compatibility test against the vprobe ClickHouse schema.
# For each DDL statement and key SQL pattern, we test whether ZigHouse
# accepts it (HTTP 200) and whether the result looks correct.
#
# Requirements:
#   - zig-out/bin/zighouse built
#
# Usage:
#   bash scripts/vprobe-compat-test.sh
set -euo pipefail

ZH=./zig-out/bin/zighouse
PORT=19900
HTTP_PORT=$((PORT + 1))
DATA=/tmp/zh_vprobe_compat
FAIL=0
PASS_COUNT=0
FAIL_COUNT=0

# ── helpers ──────────────────────────────────────────────────────────────────

pass() { echo "  PASS $1"; PASS_COUNT=$((PASS_COUNT+1)); }
fail() { echo "  FAIL $1"; FAIL_COUNT=$((FAIL_COUNT+1)); FAIL=1; }

ddl() {
  local desc="$1"
  local sql="$2"
  local enc
  enc=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$sql")
  local code
  code=$(curl --noproxy 127.0.0.1 -s -o /tmp/zh_compat_resp.txt -w "%{http_code}" \
    "http://127.0.0.1:$HTTP_PORT/?query=$enc")
  if [ "$code" = "200" ]; then
    pass "$desc"
  else
    fail "$desc (HTTP $code: $(cat /tmp/zh_compat_resp.txt))"
  fi
}

select_eq() {
  local desc="$1"
  local sql="$2"
  local expected="$3"
  local enc
  enc=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$sql")
  local resp
  resp=$(curl --noproxy 127.0.0.1 -s "http://127.0.0.1:$HTTP_PORT/?query=$enc" | tail -1 | tr -d '[:space:]')
  if [ "$resp" = "$expected" ]; then
    pass "$desc (=$expected)"
  else
    fail "$desc (expected '$expected' got '$resp')"
  fi
}

# ── build + start server ──────────────────────────────────────────────────────

echo "=== Build ==="
zig build

echo "=== Start server ==="
rm -rf "$DATA" && mkdir -p "$DATA"
$ZH serve --data-dir="$DATA" --port="$PORT" &
ZH_PID=$!
trap "kill $ZH_PID 2>/dev/null || true" EXIT
sleep 0.5

# ── Section 1: CREATE DATABASE ───────────────────────────────────────────────
echo ""
echo "=== Section 1: CREATE DATABASE ==="

ddl "CREATE DATABASE IF NOT EXISTS vprobe" \
  "CREATE DATABASE IF NOT EXISTS vprobe"

# ── Section 2: probe_heartbeats (simplest table, no complex types) ────────────
echo ""
echo "=== Section 2: probe_heartbeats DDL ==="

ddl "CREATE TABLE probe_heartbeats (DateTime64 + UInt64 + LowCardinality)" \
  "CREATE TABLE vprobe.probe_heartbeats (
    timestamp     DateTime64(3),
    probe_id      LowCardinality(String),
    uptime_secs   UInt64,
    flows_total   UInt64,
    detects_total UInt64,
    probes_total  UInt64
  ) ENGINE = MergeTree()
  PARTITION BY toYYYYMMDD(timestamp)
  ORDER BY (probe_id, timestamp)
  TTL toDateTime(timestamp) + INTERVAL 7 DAY"

# ── Section 3: scoring_rules (ReplacingMergeTree) ────────────────────────────
echo ""
echo "=== Section 3: scoring_rules DDL (ReplacingMergeTree) ==="

ddl "CREATE TABLE scoring_rules (ReplacingMergeTree + UInt8 + Float64)" \
  "CREATE TABLE vprobe.scoring_rules (
    rule_id    String,
    protocol   LowCardinality(String),
    feature    String,
    operator   LowCardinality(String),
    threshold  Float64,
    upper      Float64,
    weight     Float64,
    enabled    UInt8,
    note       String,
    updated_at DateTime64(3),
    version    UInt64
  ) ENGINE = ReplacingMergeTree(version)
  ORDER BY (rule_id)"

# ── Section 4: app_config (ReplacingMergeTree) ───────────────────────────────
echo ""
echo "=== Section 4: app_config DDL ==="

ddl "CREATE TABLE app_config (ReplacingMergeTree + DateTime64)" \
  "CREATE TABLE vprobe.app_config (
    key        String,
    value      String,
    updated_at DateTime64(3),
    version    UInt64
  ) ENGINE = ReplacingMergeTree(version)
  ORDER BY key"

# ── Section 5: intel_entries ─────────────────────────────────────────────────
echo ""
echo "=== Section 5: intel_entries DDL ==="

ddl "CREATE TABLE intel_entries (ReplacingMergeTree + LowCardinality)" \
  "CREATE TABLE vprobe.intel_entries (
    list_name  LowCardinality(String),
    entry      String,
    note       String,
    deleted    UInt8,
    updated_at DateTime64(3),
    version    UInt64
  ) ENGINE = ReplacingMergeTree(version)
  ORDER BY (list_name, entry)"

# ── Section 6: detect_events (complex types) ─────────────────────────────────
echo ""
echo "=== Section 6: detect_events DDL (complex types: IPv6, Array, Map) ==="

ddl "CREATE TABLE detect_events (IPv6 col)" \
  "CREATE TABLE vprobe.detect_events (
    event_type  LowCardinality(String),
    timestamp   DateTime64(3),
    probe_id    LowCardinality(String),
    flow_id     String,
    src_ip      IPv6,
    src_port    UInt16,
    dst_ip      IPv6,
    dst_port    UInt16,
    protocol    LowCardinality(String),
    confidence  Float64,
    evidence    Array(String),
    features    Map(String, Float64),
    data        Map(String, String),
    sni         String,
    up_bytes    UInt64,
    down_bytes  UInt64,
    dst_country LowCardinality(String),
    src_country LowCardinality(String)
  ) ENGINE = MergeTree()
  PARTITION BY toYYYYMMDD(timestamp)
  ORDER BY (protocol, timestamp)
  TTL toDateTime(timestamp) + INTERVAL 90 DAY"

# ── Section 7: Write probe_heartbeats via RowBinaryWithNamesAndTypes ──────────
echo ""
echo "=== Section 7: INSERT probe_heartbeats ==="

python3 - <<'PYEOF' > /tmp/zh_heartbeats.bin
import struct, sys, time

names = ['timestamp', 'probe_id', 'uptime_secs', 'flows_total', 'detects_total', 'probes_total']
types = ['DateTime64(3)', 'LowCardinality(String)', 'UInt64', 'UInt64', 'UInt64', 'UInt64']

buf = bytes([len(names)])
for n in names:
    buf += bytes([len(n)]) + n.encode()
for t in types:
    buf += bytes([len(t)]) + t.encode()

# 3 rows: timestamp as Int64 ms, probe_id as LEN+bytes, uint64s
now_ms = int(time.time() * 1000)
rows = [
    (now_ms,        b'probe-1', 100, 1000, 50, 10),
    (now_ms + 1000, b'probe-2', 200, 2000, 80, 20),
    (now_ms + 2000, b'probe-1', 300, 3000, 90, 30),
]
for (ts, pid, up, fl, de, pr) in rows:
    buf += struct.pack('<q', ts)                  # DateTime64(3) = Int64 ms
    buf += bytes([len(pid)]) + pid               # LowCardinality(String) = RowBinary String
    buf += struct.pack('<Q', up)
    buf += struct.pack('<Q', fl)
    buf += struct.pack('<Q', de)
    buf += struct.pack('<Q', pr)

sys.stdout.buffer.write(buf)
PYEOF

INSERT_CODE=$(curl --noproxy 127.0.0.1 -s -o /tmp/zh_insert_resp.txt -w "%{http_code}" \
  --data-binary @/tmp/zh_heartbeats.bin \
  "http://127.0.0.1:$HTTP_PORT/?query=INSERT+INTO+vprobe.probe_heartbeats+FORMAT+RowBinaryWithNamesAndTypes")
if [ "$INSERT_CODE" = "200" ]; then
  pass "INSERT probe_heartbeats (3 rows)"
else
  fail "INSERT probe_heartbeats (HTTP $INSERT_CODE: $(cat /tmp/zh_insert_resp.txt))"
fi

# ── Section 8: SELECT probe_heartbeats ───────────────────────────────────────
echo ""
echo "=== Section 8: SELECT probe_heartbeats ==="

select_eq "SELECT count(*)" \
  "SELECT count(*) FROM vprobe.probe_heartbeats" "3"

select_eq "SELECT sum(uptime_secs)" \
  "SELECT sum(uptime_secs) FROM vprobe.probe_heartbeats" "600"

select_eq "SELECT count(*) WHERE probe_id = probe-1" \
  "SELECT count(*) FROM vprobe.probe_heartbeats WHERE probe_id = 'probe-1'" "2"

# ── Section 9: Write scoring_rules (ReplacingMergeTree) ──────────────────────
echo ""
echo "=== Section 9: INSERT scoring_rules ==="

python3 - <<'PYEOF' > /tmp/zh_scoring.bin
import struct, sys, time

names = ['rule_id', 'protocol', 'feature', 'operator', 'threshold', 'upper', 'weight', 'enabled', 'note', 'updated_at', 'version']
types = ['String', 'LowCardinality(String)', 'String', 'LowCardinality(String)',
         'Float64', 'Float64', 'Float64', 'UInt8', 'String', 'DateTime64(3)', 'UInt64']

buf = bytes([len(names)])
for n in names:
    buf += bytes([len(n)]) + n.encode()
for t in types:
    buf += bytes([len(t)]) + t.encode()

now_ms = int(time.time() * 1000)

def write_str(s):
    b = s.encode()
    return bytes([len(b)]) + b

def write_f64(v):
    return struct.pack('<d', v)

def write_u64(v):
    return struct.pack('<Q', v)

def write_i64(v):
    return struct.pack('<q', v)

def write_u8(v):
    return struct.pack('B', v)

row = (write_str('rule-1') + write_str('*') + write_str('confidence') +
       write_str('>=') + write_f64(0.9) + write_f64(1.0) + write_f64(1.0) +
       write_u8(1) + write_str('default rule') + write_i64(now_ms) + write_u64(1))

buf += row
sys.stdout.buffer.write(buf)
PYEOF

INSERT_CODE=$(curl --noproxy 127.0.0.1 -s -o /tmp/zh_insert_resp.txt -w "%{http_code}" \
  --data-binary @/tmp/zh_scoring.bin \
  "http://127.0.0.1:$HTTP_PORT/?query=INSERT+INTO+vprobe.scoring_rules+FORMAT+RowBinaryWithNamesAndTypes")
if [ "$INSERT_CODE" = "200" ]; then
  pass "INSERT scoring_rules (1 row)"
else
  fail "INSERT scoring_rules (HTTP $INSERT_CODE: $(cat /tmp/zh_insert_resp.txt))"
fi

select_eq "SELECT count(*) scoring_rules" \
  "SELECT count(*) FROM vprobe.scoring_rules" "1"

# ── Section 10: SELECT scoring_rules FINAL ───────────────────────────────────
echo ""
echo "=== Section 10: SELECT FINAL (ReplacingMergeTree) ==="

select_eq "SELECT count(*) FROM scoring_rules FINAL" \
  "SELECT count(*) FROM vprobe.scoring_rules FINAL" "1"

# ── Section 11: CREATE DICTIONARY (no-op) ────────────────────────────────────
echo ""
echo "=== Section 11: CREATE DICTIONARY (no-op) ==="

ddl "CREATE DICTIONARY dict_intel" \
  "CREATE DICTIONARY IF NOT EXISTS vprobe.dict_intel (
    list_name String,
    entry     String,
    note      String
  ) PRIMARY KEY (list_name, entry)
  SOURCE(CLICKHOUSE(DB 'vprobe' TABLE 'intel_entries' USER 'default' PASSWORD ''))
  LAYOUT(COMPLEX_KEY_HASHED())
  LIFETIME(MIN 5 MAX 30)"

ddl "CREATE DICTIONARY dict_intel_exact" \
  "CREATE DICTIONARY IF NOT EXISTS vprobe.dict_intel_exact (
    kind    String,
    key     String,
    feed_id String,
    score   Float64
  ) PRIMARY KEY (kind, key)
  SOURCE(CLICKHOUSE(DB 'vprobe' TABLE 'intel_entries' USER 'default' PASSWORD ''))
  LAYOUT(COMPLEX_KEY_HASHED())
  LIFETIME(MIN 5 MAX 30)"

# ── Section 12: CREATE VIEW (no-op) ──────────────────────────────────────────
echo ""
echo "=== Section 12: CREATE VIEW (no-op) ==="

ddl "CREATE VIEW intel_exact_current" \
  "CREATE VIEW IF NOT EXISTS vprobe.intel_exact_current AS SELECT kind, entry AS key, note FROM vprobe.intel_entries"

# ── Section 13: detect_events INSERT + SELECT with data['key'] ────────────────
echo ""
echo "=== Section 13: detect_events INSERT + SELECT with data['key'] ==="

python3 - <<'PYEOF' > /tmp/zh_detect.bin
import struct, sys, time

names = ['event_type', 'timestamp', 'probe_id', 'flow_id',
         'src_ip', 'src_port', 'dst_ip', 'dst_port',
         'protocol', 'confidence', 'evidence', 'features', 'data',
         'sni', 'up_bytes', 'down_bytes', 'dst_country', 'src_country']
types = ['LowCardinality(String)', 'DateTime64(3)', 'LowCardinality(String)', 'String',
         'IPv6', 'UInt16', 'IPv6', 'UInt16',
         'LowCardinality(String)', 'Float64', 'Array(String)', 'Map(String, Float64)', 'Map(String, String)',
         'String', 'UInt64', 'UInt64', 'LowCardinality(String)', 'LowCardinality(String)']

buf = bytes([len(names)])
for n in names:
    buf += bytes([len(n)]) + n.encode()
for t in types:
    buf += bytes([len(t)]) + t.encode()

def write_str(s):
    b = s.encode()
    return bytes([len(b)]) + b

def varint(n):
    r = b''
    while n > 0x7f:
        r += bytes([(n & 0x7f) | 0x80])
        n >>= 7
    r += bytes([n])
    return r

def write_array_str(items):
    # RowBinary Array(String): varint count + each String
    r = varint(len(items))
    for item in items:
        r += write_str(item)
    return r

def write_map_str_f64(pairs):
    # RowBinary Map(String,Float64): varint count + each (String,Float64)
    r = varint(len(pairs))
    for k, v in pairs:
        r += write_str(k)
        r += struct.pack('<d', v)
    return r

def write_map_str_str(pairs):
    # RowBinary Map(String,String): varint count + each (String,String)
    r = varint(len(pairs))
    for k, v in pairs:
        r += write_str(k)
        r += write_str(v)
    return r

now_ms = int(time.time() * 1000)
ipv6_zero = b'\x00' * 16

# 2 rows
for i in range(2):
    buf += write_str('network_scan')  # event_type
    buf += struct.pack('<q', now_ms + i * 1000)  # timestamp
    buf += write_str('probe-1')  # probe_id
    buf += write_str(f'flow-{i}')  # flow_id
    buf += ipv6_zero  # src_ip
    buf += struct.pack('<H', 1234)  # src_port
    buf += ipv6_zero  # dst_ip
    buf += struct.pack('<H', 80)  # dst_port
    buf += write_str('TCP')  # protocol
    buf += struct.pack('<d', 0.9 if i == 0 else 0.5)  # confidence
    buf += write_array_str([])  # evidence
    buf += write_map_str_f64([])  # features
    domain = f'evil{i}.com'
    buf += write_map_str_str([('domain', domain), ('is_foreign', 'true' if i == 0 else 'false')])  # data
    buf += write_str('')  # sni
    buf += struct.pack('<Q', 1000)  # up_bytes
    buf += struct.pack('<Q', 2000)  # down_bytes
    buf += write_str('CN')  # dst_country
    buf += write_str('US')  # src_country

sys.stdout.buffer.write(buf)
PYEOF

INSERT_CODE=$(curl --noproxy 127.0.0.1 -s -o /tmp/zh_insert_resp.txt -w "%{http_code}" \
  --data-binary @/tmp/zh_detect.bin \
  "http://127.0.0.1:$HTTP_PORT/?query=INSERT+INTO+vprobe.detect_events+FORMAT+RowBinaryWithNamesAndTypes")
if [ "$INSERT_CODE" = "200" ]; then
  pass "INSERT detect_events (2 rows)"
else
  fail "INSERT detect_events (HTTP $INSERT_CODE: $(cat /tmp/zh_insert_resp.txt))"
fi

select_eq "SELECT count(*) detect_events" \
  "SELECT count(*) FROM vprobe.detect_events" "2"

select_eq "SELECT count(*) high confidence" \
  "SELECT countIf(confidence >= 0.9) FROM vprobe.detect_events" "1"

select_eq "SELECT count(*) WHERE protocol=TCP" \
  "SELECT count(*) FROM vprobe.detect_events WHERE protocol = 'TCP'" "2"

# ── Section 14: complex queries (groupUniqArray, arrayStringConcat) ────────────
echo ""
echo "=== Section 14: groupUniqArray + arrayStringConcat ==="

select_eq "SELECT arrayStringConcat(groupUniqArray)" \
  "SELECT arrayStringConcat(groupUniqArray(protocol), ', ') AS protocols FROM vprobe.detect_events" "TCP"

# ── Section 15: SYSTEM RELOAD DICTIONARY (no-op) ──────────────────────────────
echo ""
echo "=== Section 15: SYSTEM RELOAD DICTIONARY (no-op) ==="

ddl "SYSTEM RELOAD DICTIONARY" \
  "SYSTEM RELOAD DICTIONARY vprobe.dict_intel"

ddl "DROP DICTIONARY" \
  "DROP DICTIONARY IF EXISTS vprobe.dict_intel"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=============================="
echo "Results: $PASS_COUNT passed, $FAIL_COUNT failed"
echo "=============================="

kill $ZH_PID 2>/dev/null || true
trap - EXIT
wait $ZH_PID 2>/dev/null || true

if [ "$FAIL" = "1" ]; then
  echo "COMPAT TEST: FAIL"
  exit 1
fi
echo "COMPAT TEST: PASS"
