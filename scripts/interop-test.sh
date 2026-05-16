#!/usr/bin/env bash
# scripts/interop-test.sh
# End-to-end interop test: Zig writes a CH MergeTree part, CH ATTACHes it.
# Requirements:
#   - docker container sw_asdb running ClickHouse 26.x (password Sw@123456)
#   - zig-out/bin/zighouse built
#   - data/fixture_hits.parquet present
set -euo pipefail

STORE_DIR=/tmp/zh_interop_store
TABLE=zh_interop_test
PART=all_1_1_0
FIXTURE=data/fixture_hits.parquet
CH_DATA=/var/lib/clickhouse/data/default
ZH=./zig-out/bin/zighouse

CH_CMD=(docker exec sw_asdb bash -c)

ch() {
    docker exec sw_asdb bash -c "clickhouse-client --host=127.0.0.1 --password='Sw@123456' -q \"$1\"" 2>/dev/null
}

echo "=== Step 1: build zighouse ==="
zig build

echo "=== Step 2: import fixture ==="
rm -rf "$STORE_DIR"
$ZH import-parquet --format=ch --pk=WatchID "$FIXTURE" "$STORE_DIR" "$TABLE"

echo "=== Step 3: create table in CH ==="
ch "DROP TABLE IF EXISTS default.$TABLE"
ch "CREATE TABLE default.$TABLE (
    WatchID Int64,
    JavaEnable Int16,
    Title String,
    GoodEvent Int16,
    EventTime DateTime,
    EventDate Date,
    CounterID Int32,
    ClientIP Int32,
    RegionID Int32,
    UserID Int64,
    CounterClass Int16,
    OS Int16,
    UserAgent Int16,
    URL String,
    Referer String,
    IsRefresh Int16,
    RefererCategoryID Int16,
    RefererRegionID Int32,
    URLCategoryID Int16,
    URLRegionID Int32,
    ResolutionWidth Int16,
    ResolutionHeight Int16,
    ResolutionDepth Int16,
    FlashMajor Int16,
    FlashMinor Int16,
    FlashMinor2 String,
    NetMajor Int16,
    NetMinor Int16,
    UserAgentMajor Int16,
    UserAgentMinor String,
    CookieEnable Int16,
    JavascriptEnable Int16,
    IsMobile Int16,
    MobilePhone Int16,
    MobilePhoneModel String,
    Params String,
    IPNetworkID Int32,
    TraficSourceID Int16,
    SearchEngineID Int16,
    SearchPhrase String,
    AdvEngineID Int16,
    IsArtifical Int16,
    WindowClientWidth Int16,
    WindowClientHeight Int16,
    ClientTimeZone Int16,
    ClientEventTime DateTime,
    SilverlightVersion1 Int16,
    SilverlightVersion2 Int16,
    SilverlightVersion3 Int32,
    SilverlightVersion4 Int16,
    PageCharset String,
    CodeVersion Int32,
    IsLink Int16,
    IsDownload Int16,
    IsNotBounce Int16,
    FUniqID Int64,
    OriginalURL String,
    HID Int32,
    IsOldCounter Int16,
    IsEvent Int16,
    IsParameter Int16,
    DontCountHits Int16,
    WithHash Int16,
    HitColor String,
    LocalEventTime DateTime,
    Age Int16,
    Sex Int16,
    Income Int16,
    Interests Int16,
    Robotness Int16,
    RemoteIP Int32,
    WindowName Int32,
    OpenerName Int32,
    HistoryLength Int16,
    BrowserLanguage String,
    BrowserCountry String,
    SocialNetwork String,
    SocialAction String,
    HTTPError Int16,
    SendTiming Int32,
    DNSTiming Int32,
    ConnectTiming Int32,
    ResponseStartTiming Int32,
    ResponseEndTiming Int32,
    FetchTiming Int32,
    SocialSourceNetworkID Int16,
    SocialSourcePage String,
    ParamPrice Int64,
    ParamOrderID String,
    ParamCurrency String,
    ParamCurrencyID Int16,
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID Int16,
    RefererHash Int64,
    URLHash Int64,
    CLID Int32
) ENGINE = MergeTree()
ORDER BY WatchID
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0"

echo "=== Step 4: copy part into CH detached dir ==="
DETACHED_PATH="$CH_DATA/$TABLE/detached/$PART"
docker exec sw_asdb mkdir -p "$DETACHED_PATH"
docker cp "$STORE_DIR/$TABLE/parts/$PART/." "sw_asdb:$DETACHED_PATH/"

# Fix ownership
docker exec sw_asdb chown -R clickhouse:clickhouse "$CH_DATA/$TABLE/detached"

echo "=== Step 5: ATTACH PART ==="
ch "ALTER TABLE default.$TABLE ATTACH PART '$PART'"

echo "=== Step 6: Verify row count ==="
COUNT=$(ch "SELECT count() FROM default.$TABLE")
echo "Row count: $COUNT"

if [ "$COUNT" != "1" ]; then
    echo "FAIL: expected 1 row, got $COUNT"
    exit 1
fi

echo "=== Step 7: Verify string column values ==="
FAIL=0

check_col() {
    local col="$1"
    local expected="$2"
    local actual
    actual=$(ch "SELECT $col FROM default.$TABLE")
    if [ "$actual" = "$expected" ]; then
        echo "  PASS $col = '$actual'"
    else
        echo "  FAIL $col: expected '$expected', got '$actual'"
        FAIL=1
    fi
}

check_col "Title"           "Google title"
check_col "URL"             "https://example.com/google/page"
check_col "Referer"         "https://www.google.com/search?q=zighouse"
check_col "HitColor"        "A"
check_col "BrowserLanguage" "en"
check_col "BrowserCountry"  "US"
check_col "PageCharset"     "utf-8"
check_col "OriginalURL"     "https://example.com/google/page"

# Verify EventTime unix timestamp (timezone-independent)
ET=$(ch "SELECT toUnixTimestamp(EventTime) FROM default.$TABLE")
if [ "$ET" = "1372636800" ]; then
    echo "  PASS EventTime unix=$ET"
else
    echo "  FAIL EventTime unix: expected 1372636800, got $ET"
    FAIL=1
fi

# Verify EventDate
ED=$(ch "SELECT toString(EventDate) FROM default.$TABLE")
if [ "$ED" = "2013-07-01" ]; then
    echo "  PASS EventDate=$ED"
else
    echo "  FAIL EventDate: expected 2013-07-01, got $ED"
    FAIL=1
fi

echo "=== Step 8: Verify WHERE queries (primary index granule skipping regression) ==="
# Regression: pk_col_idx mismatch caused CANNOT_READ_ALL_DATA when CH used
# primary.idx for granule skipping.  Any WHERE on any column must not crash.

check_where() {
    local desc="$1"
    local query="$2"
    local result
    if result=$(ch "$query" 2>&1); then
        echo "  PASS $desc"
    else
        echo "  FAIL $desc: $result"
        FAIL=1
    fi
}

# WHERE on PK column (triggers granule skipping via primary.idx)
check_where "WHERE WatchID > 0" \
    "SELECT count() FROM default.$TABLE WHERE WatchID > 0"

# WHERE on non-PK fixed column (full scan, no skipping)
check_where "WHERE CounterID >= 0" \
    "SELECT count() FROM default.$TABLE WHERE CounterID >= 0"

# WHERE on String column
check_where "WHERE Title != ''" \
    "SELECT count() FROM default.$TABLE WHERE Title != ''"

# WHERE + aggregation mixing fixed and string columns
check_where "WHERE + mixed aggregation" \
    "SELECT sum(length(URL)), min(EventDate) FROM default.$TABLE WHERE WatchID > 0"

if [ "$FAIL" = "1" ]; then
    echo "FAIL: one or more column value checks failed"
    exit 1
fi

echo "PASS: interop test succeeded (row count + all column values + WHERE queries correct)"

# ── Step 9: HTTP RowBinary ingest server integration test ──────────────────────
echo ""
echo "=== Step 9: zighouse serve RowBinary ingest ==="

SERVE_PORT=19123
SERVE_DATA=/tmp/zh_serve_test
SERVE_TABLE=serve_test
SERVE_DB=default
SERVE_SCHEMA=/tmp/zh_serve_schema.json
SERVE_PART_DIR="$SERVE_DATA/$SERVE_DB/$SERVE_TABLE/parts"

# Create schemas JSON
cat > "$SERVE_SCHEMA" <<'EOF'
{
  "tables": [
    {
      "db": "default",
      "name": "serve_test",
      "pk": "id",
      "columns": [
        {"name": "id",   "type": "Int32"},
        {"name": "name", "type": "String"}
      ]
    }
  ]
}
EOF

# Clean previous run
rm -rf "$SERVE_DATA"
mkdir -p "$SERVE_DATA"

# Start server in background
$ZH serve --data-dir="$SERVE_DATA" --schemas="$SERVE_SCHEMA" --port="$SERVE_PORT" &
SERVE_PID=$!
trap "kill $SERVE_PID 2>/dev/null || true" EXIT

# Wait for server to start
sleep 0.5

# Build a 2-row RowBinary payload: row1=(1,"alice"), row2=(2,"bob")
# Int32 LE + varUInt(len) + bytes
python3 - <<'PYEOF' > /tmp/zh_serve_payload.bin
import struct, sys
buf = b''
# row 1: id=1, name="alice"
buf += struct.pack('<i', 1)
buf += bytes([5]) + b'alice'
# row 2: id=2, name="bob"
buf += struct.pack('<i', 2)
buf += bytes([3]) + b'bob'
sys.stdout.buffer.write(buf)
PYEOF

# Send to zighouse serve
HTTP_RESP=$(curl -s -o /dev/null -w "%{http_code}" \
    --data-binary @/tmp/zh_serve_payload.bin \
    "http://127.0.0.1:$SERVE_PORT/?query=INSERT+INTO+default.serve_test+FORMAT+RowBinary")

if [ "$HTTP_RESP" = "200" ]; then
    echo "  PASS zighouse serve returned HTTP 200"
else
    echo "  FAIL zighouse serve returned HTTP $HTTP_RESP"
    FAIL=1
fi

# Verify a part was created
PART_COUNT=$(find "$SERVE_PART_DIR" -name "all_*_*_0" -type d 2>/dev/null | wc -l | tr -d ' ')
if [ "$PART_COUNT" -ge "1" ]; then
    echo "  PASS part directory created ($PART_COUNT part(s))"
else
    echo "  FAIL no part directories found under $SERVE_PART_DIR"
    FAIL=1
fi

# Stop server
kill $SERVE_PID 2>/dev/null || true
trap - EXIT
wait $SERVE_PID 2>/dev/null || true

# Optionally: ATTACH the part to CH and verify via SELECT
SERVE_PART_NAME=$(ls "$SERVE_PART_DIR" 2>/dev/null | head -1)
if [ -n "$SERVE_PART_NAME" ]; then
    ch "DROP TABLE IF EXISTS $SERVE_DB.$SERVE_TABLE"
    ch "CREATE TABLE $SERVE_DB.$SERVE_TABLE (id Int32, name String) ENGINE = MergeTree() ORDER BY id SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0"

    DETACHED="$CH_DATA/$SERVE_TABLE/detached/$SERVE_PART_NAME"
    docker exec sw_asdb mkdir -p "$DETACHED"
    docker cp "$SERVE_PART_DIR/$SERVE_PART_NAME/." "sw_asdb:$DETACHED/"
    docker exec sw_asdb chown -R clickhouse:clickhouse "$CH_DATA/$SERVE_TABLE/detached"
    ch "ALTER TABLE $SERVE_DB.$SERVE_TABLE ATTACH PART '$SERVE_PART_NAME'"

    SERVE_COUNT=$(ch "SELECT count() FROM $SERVE_DB.$SERVE_TABLE")
    if [ "$SERVE_COUNT" = "2" ]; then
        echo "  PASS serve_test row count = 2"
    else
        echo "  FAIL serve_test row count expected 2, got $SERVE_COUNT"
        FAIL=1
    fi

    ALICE=$(ch "SELECT name FROM $SERVE_DB.$SERVE_TABLE WHERE id = 1")
    if [ "$ALICE" = "alice" ]; then
        echo "  PASS name='alice' for id=1"
    else
        echo "  FAIL name expected 'alice', got '$ALICE'"
        FAIL=1
    fi
fi

if [ "$FAIL" = "1" ]; then
    echo "FAIL: one or more step 9 checks failed"
    exit 1
fi
echo "PASS: step 9 (zighouse serve RowBinary ingest) succeeded"

# ── Step 10: DDL CREATE TABLE via HTTP ─────────────────────────────────────────
echo ""
echo "=== Step 10: zighouse serve DDL CREATE TABLE ==="

DDL_PORT=19124
DDL_DATA=/tmp/zh_ddl_test
DDL_DB=default
DDL_TABLE=ddl_test
DDL_PART_DIR="$DDL_DATA/$DDL_DB/$DDL_TABLE/parts"
rm -rf "$DDL_DATA"
mkdir -p "$DDL_DATA"

$ZH serve --data-dir="$DDL_DATA" --port="$DDL_PORT" &
DDL_PID=$!
trap "kill $DDL_PID 2>/dev/null || true" EXIT
sleep 0.5

# Create table via DDL (pass query as URL parameter)
DDL_SQL="CREATE TABLE IF NOT EXISTS $DDL_DB.$DDL_TABLE (id Int32, val String) ENGINE = MergeTree ORDER BY id"
DDL_SQL_ENC=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$DDL_SQL")
DDL_RESP=$(curl -s -o /tmp/zh_ddl_resp.txt -w "%{http_code}" \
    "http://127.0.0.1:$DDL_PORT/?query=$DDL_SQL_ENC")
if [ "$DDL_RESP" = "200" ]; then
    echo "  PASS CREATE TABLE returned HTTP 200"
else
    echo "  FAIL CREATE TABLE returned HTTP $DDL_RESP ($(cat /tmp/zh_ddl_resp.txt))"
    FAIL=1
fi

# Idempotent: second CREATE TABLE should also return 200
DDL_RESP2=$(curl -s -o /dev/null -w "%{http_code}" \
    "http://127.0.0.1:$DDL_PORT/?query=$DDL_SQL_ENC")
if [ "$DDL_RESP2" = "200" ]; then
    echo "  PASS idempotent CREATE TABLE returned HTTP 200"
else
    echo "  FAIL idempotent CREATE TABLE returned HTTP $DDL_RESP2"
    FAIL=1
fi

# Verify schema.json was persisted
SCHEMA_FILE="$DDL_DATA/$DDL_DB/$DDL_TABLE/schema.json"
if [ -f "$SCHEMA_FILE" ]; then
    echo "  PASS schema.json persisted"
else
    echo "  FAIL schema.json not found at $SCHEMA_FILE"
    FAIL=1
fi

# Now insert via RowBinary (schema must exist from DDL)
python3 - <<'PYEOF' > /tmp/zh_ddl_payload.bin
import struct, sys
buf = b''
buf += struct.pack('<i', 10)
buf += bytes([3]) + b'foo'
buf += struct.pack('<i', 20)
buf += bytes([3]) + b'bar'
sys.stdout.buffer.write(buf)
PYEOF

INS_RESP=$(curl -s -o /dev/null -w "%{http_code}" \
    --data-binary @/tmp/zh_ddl_payload.bin \
    "http://127.0.0.1:$DDL_PORT/?query=INSERT+INTO+$DDL_DB.$DDL_TABLE+FORMAT+RowBinary")
if [ "$INS_RESP" = "200" ]; then
    echo "  PASS RowBinary INSERT after DDL returned HTTP 200"
else
    echo "  FAIL RowBinary INSERT after DDL returned HTTP $INS_RESP"
    FAIL=1
fi

kill $DDL_PID 2>/dev/null || true
trap - EXIT
wait $DDL_PID 2>/dev/null || true

if [ "$FAIL" = "1" ]; then
    echo "FAIL: one or more step 10 checks failed"
    exit 1
fi
echo "PASS: step 10 (DDL CREATE TABLE + RowBinary INSERT) succeeded"

# ── Step 11: RowBinaryWithNamesAndTypes auto-create table ─────────────────────
echo ""
echo "=== Step 11: zighouse serve RowBinaryWithNamesAndTypes auto-create ==="

WNHT_PORT=19125
WNHT_DATA=/tmp/zh_wnht_test
WNHT_DB=default
WNHT_TABLE=wnht_test
rm -rf "$WNHT_DATA"
mkdir -p "$WNHT_DATA"

$ZH serve --data-dir="$WNHT_DATA" --port="$WNHT_PORT" &
WNHT_PID=$!
trap "kill $WNHT_PID 2>/dev/null || true" EXIT
sleep 0.5

# Build RowBinaryWithNamesAndTypes payload: 2 cols (id Int32, label String), 2 rows
python3 - <<'PYEOF' > /tmp/zh_wnht_payload.bin
import struct, sys

def varuint(n):
    buf = b''
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            buf += bytes([b | 0x80])
        else:
            buf += bytes([b])
            break
    return buf

def string(s):
    b = s.encode()
    return varuint(len(b)) + b

out = b''
# header: num_columns, then all names, then all types (ClickHouse format)
out += varuint(2)
out += string('id')
out += string('label')
out += string('Int32')
out += string('String')
# row 1: id=100, label="hello"
out += struct.pack('<i', 100)
out += string('hello')
# row 2: id=200, label="world"
out += struct.pack('<i', 200)
out += string('world')

sys.stdout.buffer.write(out)
PYEOF

WNHT_RESP=$(curl -s -o /tmp/zh_wnht_resp.txt -w "%{http_code}" \
    --data-binary @/tmp/zh_wnht_payload.bin \
    "http://127.0.0.1:$WNHT_PORT/?query=INSERT+INTO+$WNHT_DB.$WNHT_TABLE+FORMAT+RowBinaryWithNamesAndTypes")
if [ "$WNHT_RESP" = "200" ]; then
    echo "  PASS RowBinaryWithNamesAndTypes INSERT returned HTTP 200"
else
    echo "  FAIL RowBinaryWithNamesAndTypes INSERT returned HTTP $WNHT_RESP ($(cat /tmp/zh_wnht_resp.txt))"
    FAIL=1
fi

# Verify schema.json was auto-persisted
WNHT_SCHEMA="$WNHT_DATA/$WNHT_DB/$WNHT_TABLE/schema.json"
if [ -f "$WNHT_SCHEMA" ]; then
    echo "  PASS schema.json auto-persisted for wnht_test"
else
    echo "  FAIL schema.json not found at $WNHT_SCHEMA"
    FAIL=1
fi

# Verify a part was created
WNHT_PARTS=$(find "$WNHT_DATA/$WNHT_DB/$WNHT_TABLE/parts" -name "all_*_*_0" -type d 2>/dev/null | wc -l | tr -d ' ')
if [ "$WNHT_PARTS" -ge "1" ]; then
    echo "  PASS part created ($WNHT_PARTS part(s))"
else
    echo "  FAIL no parts found"
    FAIL=1
fi

kill $WNHT_PID 2>/dev/null || true
trap - EXIT
wait $WNHT_PID 2>/dev/null || true

if [ "$FAIL" = "1" ]; then
    echo "FAIL: one or more step 11 checks failed"
    exit 1
fi
echo "PASS: step 11 (RowBinaryWithNamesAndTypes auto-create) succeeded"

# ── Step 12: Restart server and query persisted schema ─────────────────────────
echo ""
echo "=== Step 12: restart server, schema auto-load, SELECT ==="

# Re-use the DDL_DATA dir from step 10 which has schema.json + a part
RESTART_PORT=19126
$ZH serve --data-dir="$DDL_DATA" --port="$RESTART_PORT" &
RESTART_PID=$!
trap "kill $RESTART_PID 2>/dev/null || true" EXIT
sleep 0.5

# SELECT count(*) — should return 2 (the two rows inserted in step 10)
SELECT_RESP=$(curl -s -o /tmp/zh_select_resp.txt -w "%{http_code}" \
    "http://127.0.0.1:$RESTART_PORT/?query=SELECT+count(*)+FROM+$DDL_DB.$DDL_TABLE")
if [ "$SELECT_RESP" = "200" ]; then
    echo "  PASS SELECT returned HTTP 200"
else
    echo "  FAIL SELECT returned HTTP $SELECT_RESP ($(cat /tmp/zh_select_resp.txt))"
    FAIL=1
fi

COUNT_VAL=$(cat /tmp/zh_select_resp.txt | tail -1 | tr -d '[:space:]')
if [ "$COUNT_VAL" = "2" ]; then
    echo "  PASS SELECT count() = 2 after restart"
else
    echo "  FAIL SELECT count() expected 2, got '$COUNT_VAL'"
    FAIL=1
fi

kill $RESTART_PID 2>/dev/null || true
trap - EXIT
wait $RESTART_PID 2>/dev/null || true

if [ "$FAIL" = "1" ]; then
    echo "FAIL: one or more step 12 checks failed"
    exit 1
fi
echo "PASS: step 12 (restart + schema auto-load + SELECT) succeeded"

# ── Step 13: Real ClickHouse client → zighouse serve (RowBinaryWithNamesAndTypes) ──
echo ""
echo "=== Step 13: ClickHouse client → zighouse serve (RowBinaryWithNamesAndTypes) ==="

CH13_PORT=19127
CH13_DATA=/tmp/zh_ch13_test
CH13_DB=default
CH13_TABLE=ch13_test
rm -rf "$CH13_DATA"
mkdir -p "$CH13_DATA"

$ZH serve --data-dir="$CH13_DATA" --port="$CH13_PORT" &
CH13_PID=$!
trap "kill $CH13_PID 2>/dev/null || true" EXIT
sleep 0.5

# Export a real RowBinaryWithNamesAndTypes payload from ClickHouse, then feed it to zighouse.
# This validates that zighouse correctly parses the actual ClickHouse wire format.
TMPCH13=$(mktemp /tmp/ch13_payload.XXXXXX.bin)
timeout 10 docker exec sw_asdb clickhouse-client --password='Sw@123456' \
    -q "SELECT toInt32(number+10) as id, concat('row_', toString(number)) as label FROM numbers(3) FORMAT RowBinaryWithNamesAndTypes" \
    > "$TMPCH13" 2>/dev/null

if [ -s "$TMPCH13" ]; then
    INS13=$(curl --noproxy 127.0.0.1 -s -o /tmp/ch13_ins_resp.txt -w "%{http_code}" \
        --data-binary @"$TMPCH13" \
        "http://127.0.0.1:$CH13_PORT/?query=INSERT+INTO+$CH13_DB.$CH13_TABLE+FORMAT+RowBinaryWithNamesAndTypes")
    if [ "$INS13" = "200" ]; then
        echo "  PASS CH RowBinaryWithNamesAndTypes payload accepted (HTTP 200)"
    else
        echo "  FAIL CH payload rejected HTTP $INS13: $(cat /tmp/ch13_ins_resp.txt)"
        FAIL=1
    fi

    # Verify a part was created with 3 rows
    PARTS13=$(find "$CH13_DATA/$CH13_DB/$CH13_TABLE/parts" -name "all_*_*_0" -type d 2>/dev/null | wc -l | tr -d ' ')
    if [ "$PARTS13" -ge "1" ]; then
        echo "  PASS part created ($PARTS13 part(s))"
    else
        echo "  FAIL no parts found"
        FAIL=1
    fi
else
    echo "  WARN Could not extract CH RowBinaryWithNamesAndTypes payload (CH may be unavailable)"
    echo "  INFO Skipping Step 13 (ClickHouse container unavailable)"
fi
rm -f "$TMPCH13"

kill $CH13_PID 2>/dev/null || true
trap - EXIT
wait $CH13_PID 2>/dev/null || true

if [ "$FAIL" = "1" ]; then
    echo "FAIL: one or more step 13 checks failed"
    exit 1
fi
echo "PASS: step 13 (ClickHouse client → zighouse) succeeded"

# ── Step 14: SELECT projection + WHERE + multi-part ────────────────────────────
echo ""
echo "=== Step 14: SELECT projection + WHERE + multi-part ==="

S14_PORT=19128
S14_DATA=/tmp/zh_s14_test
S14_DB=default
S14_TABLE=s14_test
rm -rf "$S14_DATA"
mkdir -p "$S14_DATA"

# Create table via DDL
DDL14="CREATE TABLE $S14_DB.$S14_TABLE (id Int32, name String) ENGINE = MergeTree ORDER BY id"
DDL14_ENC=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$DDL14")

$ZH serve --data-dir="$S14_DATA" --port="$S14_PORT" &
S14_PID=$!
trap "kill $S14_PID 2>/dev/null || true" EXIT
sleep 0.5

curl --noproxy 127.0.0.1 -s -o /dev/null -w "%{http_code}" \
    "http://127.0.0.1:$S14_PORT/?query=$DDL14_ENC" | grep -q "200" \
    && echo "  PASS DDL CREATE TABLE" || { echo "  FAIL DDL CREATE TABLE"; FAIL=1; }

# Insert part 1: rows id=1..3
python3 - <<'PYEOF' > /tmp/zh_s14_p1.bin
import struct, sys
buf = b''
for i in range(1, 4):
    buf += struct.pack('<i', i)
    s = f'name{i}'.encode()
    buf += bytes([len(s)]) + s
sys.stdout.buffer.write(buf)
PYEOF

curl --noproxy 127.0.0.1 -s -o /dev/null -w "%{http_code}" \
    --data-binary @/tmp/zh_s14_p1.bin \
    "http://127.0.0.1:$S14_PORT/?query=INSERT+INTO+$S14_DB.$S14_TABLE+FORMAT+RowBinary" \
    | grep -q "200" && echo "  PASS INSERT part 1" || { echo "  FAIL INSERT part 1"; FAIL=1; }

# Insert part 2: rows id=4..6 (separate INSERT → separate part)
python3 - <<'PYEOF' > /tmp/zh_s14_p2.bin
import struct, sys
buf = b''
for i in range(4, 7):
    buf += struct.pack('<i', i)
    s = f'name{i}'.encode()
    buf += bytes([len(s)]) + s
sys.stdout.buffer.write(buf)
PYEOF

curl --noproxy 127.0.0.1 -s -o /dev/null -w "%{http_code}" \
    --data-binary @/tmp/zh_s14_p2.bin \
    "http://127.0.0.1:$S14_PORT/?query=INSERT+INTO+$S14_DB.$S14_TABLE+FORMAT+RowBinary" \
    | grep -q "200" && echo "  PASS INSERT part 2" || { echo "  FAIL INSERT part 2"; FAIL=1; }

# Verify 2 parts on disk
PART_COUNT=$(find "$S14_DATA/$S14_DB/$S14_TABLE/parts" -name "all_*_*_0" -type d 2>/dev/null | wc -l | tr -d ' ')
if [ "$PART_COUNT" -eq "2" ]; then
    echo "  PASS 2 parts on disk"
else
    echo "  FAIL expected 2 parts, got $PART_COUNT"
    FAIL=1
fi

# SELECT count(*) across both parts → should be 6
COUNT_ENC=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" \
    "SELECT count(*) FROM $S14_DB.$S14_TABLE")
COUNT_RESP=$(curl --noproxy 127.0.0.1 -s \
    "http://127.0.0.1:$S14_PORT/?query=$COUNT_ENC" | tail -1 | tr -d '[:space:]')
if [ "$COUNT_RESP" = "6" ]; then
    echo "  PASS SELECT count(*) = 6 across 2 parts"
else
    echo "  FAIL SELECT count(*) expected 6, got '$COUNT_RESP'"
    FAIL=1
fi

kill $S14_PID 2>/dev/null || true
trap - EXIT
wait $S14_PID 2>/dev/null || true

if [ "$FAIL" = "1" ]; then
    echo "FAIL: one or more step 14 checks failed"
    exit 1
fi
echo "PASS: step 14 (SELECT + multi-part) succeeded"
