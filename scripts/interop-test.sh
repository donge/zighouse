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
$ZH import-parquet --format=ch "$FIXTURE" "$STORE_DIR" "$TABLE"

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

if [ "$FAIL" = "1" ]; then
    echo "FAIL: one or more column value checks failed"
    exit 1
fi

echo "PASS: interop test succeeded (row count + all column values correct)"
