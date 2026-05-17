#!/usr/bin/env python3
"""
Full schema compatibility test for ZigHouse.

Tests all 8 vprobe application tables:
  detect_events, probe_events, probe_heartbeats, scoring_rules,
  app_config, intel_entries, intel_feeds, intel_feed_items

For each table: INSERT (Native Block) → SELECT count(*) → SELECT <cols> → assert.

Types covered:
  String, LowCardinality(String), Float64, UInt8, UInt16, UInt32, UInt64,
  Int32, DateTime64(3), IPv6, Array(String), Map(String,Float64),
  Map(String,String)

Usage:
  python3 scripts/test_all_tables.py [port]
"""

import struct, sys, os, subprocess, time, shutil, urllib.request, urllib.parse

PORT     = int(sys.argv[1]) if len(sys.argv) > 1 else 29904
BASE     = f"http://127.0.0.1:{PORT}"
DATA_DIR = f"/tmp/zh_all_tables_{PORT}"
ZH_BIN   = os.path.join(os.path.dirname(__file__), "../zig-out/bin/zighouse")

PASS = 0; FAIL = 0

def ok(msg):  global PASS; PASS += 1; print(f"  [OK] {msg}")
def fail(msg): global FAIL; FAIL += 1; print(f"  [FAIL] {msg}")

# ── encoding helpers ───────────────────────────────────────────────────────────

def varint(n):
    out = []
    while True:
        b = n & 0x7F; n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n: break
    return bytes(out)

def ch_str(s):
    b = s.encode(); return varint(len(b)) + b

def u8(n):   return struct.pack('B', n)
def u16le(n): return struct.pack('<H', n)
def u32le(n): return struct.pack('<I', n)
def i32le(n): return struct.pack('<i', n)
def u64le(n): return struct.pack('<Q', n)
def i64le(n): return struct.pack('<q', n)
def f64le(f): return struct.pack('<d', f)

def ipv6_bytes(s):
    """Encode IPv6 or IPv4-mapped address as 16 raw bytes (little-endian word order)."""
    import ipaddress
    addr = ipaddress.ip_address(s)
    if addr.version == 4:
        addr = ipaddress.ip_address('::ffff:' + s)
    # CH stores IPv6 as 16 bytes, big-endian
    return addr.packed

def col(name, type_str, data_bytes):
    return ch_str(name) + ch_str(type_str) + data_bytes

# ── column encoders ────────────────────────────────────────────────────────────

def enc_str_col(vals):
    return b"".join(ch_str(v) for v in vals)

def enc_lc_str_col(vals):
    seen = {}
    for v in vals:
        if v not in seen: seen[v] = len(seen)
    dc = list(seen.keys())
    kt = 0 if len(dc) <= 255 else 1
    flags = 0x600 | kt
    out  = u64le(1)               # state_prefix
    out += u64le(flags)
    out += i64le(len(dc))
    for e in dc: out += ch_str(e)
    out += i64le(len(vals))
    for v in vals:
        i = seen[v]
        out += u8(i) if kt == 0 else struct.pack('<H', i)
    return out

def enc_u8_col(vals):   return b"".join(u8(v)    for v in vals)
def enc_u16_col(vals):  return b"".join(u16le(v) for v in vals)
def enc_u32_col(vals):  return b"".join(u32le(v) for v in vals)
def enc_i32_col(vals):  return b"".join(i32le(v) for v in vals)
def enc_u64_col(vals):  return b"".join(u64le(v) for v in vals)
def enc_f64_col(vals):  return b"".join(f64le(v) for v in vals)
def enc_dt64_col(vals): return b"".join(u64le(v) for v in vals)  # ms since epoch
def enc_ipv6_col(vals): return b"".join(ipv6_bytes(v) for v in vals)

def enc_array_str_col(rows):
    """Array(String): ClickHouse Native offset format.
    Wire: uint64[num_rows] cumulative end-offsets, then all element strings concatenated.
    """
    # Compute cumulative offsets
    offsets = b""
    elements = b""
    cumulative = 0
    for arr in rows:
        cumulative += len(arr)
        offsets += u64le(cumulative)
        for s in arr:
            elements += ch_str(s)
    return offsets + elements

def enc_map_str_f64_col(rows):
    """Map(String, Float64): ClickHouse Native offset format.
    Wire: uint64[num_rows] cumulative pair-offsets, then all keys concatenated, then all values concatenated.
    """
    offsets = b""
    keys = b""
    vals = b""
    cumulative = 0
    for d in rows:
        cumulative += len(d)
        offsets += u64le(cumulative)
        for k, v in d.items():
            keys += ch_str(k)
            vals += f64le(v)
    return offsets + keys + vals

def enc_map_str_str_col(rows):
    """Map(String, String): ClickHouse Native offset format."""
    offsets = b""
    keys = b""
    vals = b""
    cumulative = 0
    for d in rows:
        cumulative += len(d)
        offsets += u64le(cumulative)
        for k, v in d.items():
            keys += ch_str(k)
            vals += ch_str(v)
    return offsets + keys + vals

def block_header(num_cols, num_rows):
    return varint(num_cols) + varint(num_rows)

# ── HTTP helpers ───────────────────────────────────────────────────────────────

def post(path, body, ct="application/octet-stream"):
    req = urllib.request.Request(BASE+path, data=body,
          headers={"Content-Type": ct}, method="POST")
    with urllib.request.urlopen(req) as r:
        return r.read()

def get(path):
    with urllib.request.urlopen(BASE+path) as r:
        return r.read().decode()

def select(sql):
    return get("/?query=" + urllib.parse.quote(sql))

def insert_native(db_table, body_sql, block_bytes):
    sql = f"INSERT INTO {db_table} FORMAT Native"
    return post("/?query=" + urllib.parse.quote(sql), block_bytes)

def ddl(sql):
    post("/?query=" + urllib.parse.quote(sql), b"")

def wait_ping(timeout=10):
    for _ in range(timeout * 10):
        try:
            with urllib.request.urlopen(f"{BASE}/ping", timeout=1) as r:
                if r.read().strip() == b"Ok.": return True
        except: pass
        time.sleep(0.1)
    return False

# ── table test helpers ─────────────────────────────────────────────────────────

def check_count(table, expected):
    r = select(f"SELECT count(*) FROM {table}")
    lines = [l for l in r.strip().split("\n") if l]
    got = int(lines[-1])
    if got == expected:
        ok(f"{table}: count(*) = {expected}")
    else:
        fail(f"{table}: count(*) = {got}, want {expected}")

def check_col(table, col_name, expected_vals, select_sql=None):
    sql = select_sql or f"SELECT {col_name} FROM {table}"
    r = select(sql)
    lines = r.split("\n")
    # The header is line 0; there are len(expected_vals) data rows, each ending with \n.
    # An empty-string value produces an empty line indistinguishable from trailing \n,
    # so use expected count to take exactly the right number of rows.
    n = len(expected_vals)
    rows = lines[1:1 + n]  # skip header, take exactly n rows
    got = [l.strip() for l in rows]
    exp = [str(v) for v in expected_vals]
    if set(got) == set(exp):
        ok(f"{table}.{col_name} values match {exp}")
    else:
        fail(f"{table}.{col_name}: got={got}, want={exp}")

# ── Table 1: probe_heartbeats ─────────────────────────────────────────────────

def test_probe_heartbeats():
    print("\n── probe_heartbeats ──")
    T = "test.probe_heartbeats"
    ddl(f"""CREATE TABLE IF NOT EXISTS {T} (
        timestamp     DateTime64(3),
        probe_id      LowCardinality(String),
        uptime_secs   UInt64,
        flows_total   UInt64,
        detects_total UInt64,
        probes_total  UInt64
    ) ENGINE = MergeTree() ORDER BY (probe_id, timestamp)""")

    rows = [
        (1700000000000, "probe-A", 3600, 10000, 42, 7),
        (1700000060000, "probe-B", 7200, 20000, 88, 3),
    ]
    n = len(rows)
    blk  = block_header(6, n)
    blk += col("timestamp",     "DateTime64(3)",          enc_dt64_col([r[0] for r in rows]))
    blk += col("probe_id",      "LowCardinality(String)", enc_lc_str_col([r[1] for r in rows]))
    blk += col("uptime_secs",   "UInt64",                 enc_u64_col([r[2] for r in rows]))
    blk += col("flows_total",   "UInt64",                 enc_u64_col([r[3] for r in rows]))
    blk += col("detects_total", "UInt64",                 enc_u64_col([r[4] for r in rows]))
    blk += col("probes_total",  "UInt64",                 enc_u64_col([r[5] for r in rows]))
    insert_native(T, "", blk)

    check_count(T, n)
    check_col(T, "probe_id", ["probe-A", "probe-B"])
    check_col(T, "uptime_secs", [3600, 7200])

# ── Table 2: scoring_rules ────────────────────────────────────────────────────

def test_scoring_rules():
    print("\n── scoring_rules ──")
    T = "test.scoring_rules"
    ddl(f"""CREATE TABLE IF NOT EXISTS {T} (
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
    ) ENGINE = ReplacingMergeTree(version) ORDER BY (rule_id)""")

    rows = [
        ("id-0001", "tcp", "stat.byte_ratio", ">=",      0.8, 1.0, 2.5, 1, "rule one", 1700000000000, 1),
        ("id-0002", "udp", "stat.pkt_count",  "between", 10., 100., 1.0, 1, "rule two", 1700000001000, 2),
        ("id-0003", "tcp", "time.dur_ms",      "<=",     500., 0.,  0.5, 0, "",         1700000002000, 3),
    ]
    n = len(rows)
    blk  = block_header(11, n)
    blk += col("rule_id",    "String",                    enc_str_col([r[0] for r in rows]))
    blk += col("protocol",   "LowCardinality(String)",    enc_lc_str_col([r[1] for r in rows]))
    blk += col("feature",    "String",                    enc_str_col([r[2] for r in rows]))
    blk += col("operator",   "LowCardinality(String)",    enc_lc_str_col([r[3] for r in rows]))
    blk += col("threshold",  "Float64",                   enc_f64_col([r[4] for r in rows]))
    blk += col("upper",      "Float64",                   enc_f64_col([r[5] for r in rows]))
    blk += col("weight",     "Float64",                   enc_f64_col([r[6] for r in rows]))
    blk += col("enabled",    "UInt8",                     enc_u8_col([r[7] for r in rows]))
    blk += col("note",       "String",                    enc_str_col([r[8] for r in rows]))
    blk += col("updated_at", "DateTime64(3)",             enc_dt64_col([r[9] for r in rows]))
    blk += col("version",    "UInt64",                    enc_u64_col([r[10] for r in rows]))
    insert_native(T, "", blk)

    check_count(T, n)
    check_col(T, "rule_id",  ["id-0001", "id-0002", "id-0003"])
    check_col(T, "protocol", ["tcp", "udp", "tcp"])
    check_col(T, "operator", [">=", "between", "<="])
    check_col(T, "feature",  ["stat.byte_ratio", "stat.pkt_count", "time.dur_ms"])

# ── Table 3: app_config ───────────────────────────────────────────────────────

def test_app_config():
    print("\n── app_config ──")
    T = "test.app_config"
    ddl(f"""CREATE TABLE IF NOT EXISTS {T} (
        key        String,
        value      String,
        updated_at DateTime64(3),
        version    UInt64
    ) ENGINE = ReplacingMergeTree(version) ORDER BY key""")

    rows = [
        ("theme",   '{"color":"dark"}',  1700000000000, 1),
        ("timeout", "30",                1700000001000, 1),
    ]
    n = len(rows)
    blk  = block_header(4, n)
    blk += col("key",        "String",        enc_str_col([r[0] for r in rows]))
    blk += col("value",      "String",        enc_str_col([r[1] for r in rows]))
    blk += col("updated_at", "DateTime64(3)", enc_dt64_col([r[2] for r in rows]))
    blk += col("version",    "UInt64",        enc_u64_col([r[3] for r in rows]))
    insert_native(T, "", blk)

    check_count(T, n)
    check_col(T, "key", ["theme", "timeout"])

# ── Table 4: intel_entries ────────────────────────────────────────────────────

def test_intel_entries():
    print("\n── intel_entries ──")
    T = "test.intel_entries"
    ddl(f"""CREATE TABLE IF NOT EXISTS {T} (
        list_name  LowCardinality(String),
        entry      String,
        note       String,
        deleted    UInt8,
        updated_at DateTime64(3),
        version    UInt64
    ) ENGINE = ReplacingMergeTree(version) ORDER BY (list_name, entry)""")

    rows = [
        ("blacklist", "1.2.3.4",   "bad ip",  0, 1700000000000, 1),
        ("blacklist", "evil.com",  "bad host",0, 1700000001000, 1),
        ("whitelist", "8.8.8.8",   "google",  0, 1700000002000, 1),
    ]
    n = len(rows)
    blk  = block_header(6, n)
    blk += col("list_name",  "LowCardinality(String)", enc_lc_str_col([r[0] for r in rows]))
    blk += col("entry",      "String",                 enc_str_col([r[1] for r in rows]))
    blk += col("note",       "String",                 enc_str_col([r[2] for r in rows]))
    blk += col("deleted",    "UInt8",                  enc_u8_col([r[3] for r in rows]))
    blk += col("updated_at", "DateTime64(3)",          enc_dt64_col([r[4] for r in rows]))
    blk += col("version",    "UInt64",                 enc_u64_col([r[5] for r in rows]))
    insert_native(T, "", blk)

    check_count(T, n)
    check_col(T, "entry", ["1.2.3.4", "evil.com", "8.8.8.8"])

# ── Table 5: detect_events (Array + Map + IPv6) ───────────────────────────────

def test_detect_events():
    print("\n── detect_events (Array/Map/IPv6) ──")
    T = "test.detect_events"
    ddl(f"""CREATE TABLE IF NOT EXISTS {T} (
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
    ) ENGINE = MergeTree() ORDER BY (protocol, timestamp)""")

    rows = [
        {
            "event_type":  "detect",
            "timestamp":   1700000000000,
            "probe_id":    "probe-A",
            "flow_id":     "abc123",
            "src_ip":      "192.168.1.1",
            "src_port":    12345,
            "dst_ip":      "8.8.8.8",
            "dst_port":    443,
            "protocol":    "tls",
            "confidence":  0.9,
            "evidence":    ["ja3_proxy", "long_conn"],
            "features":    {"stat.byte_ratio": 0.55, "stat.pkt_count": 32.0},
            "data":        {"sni_hint": "example.com"},
            "sni":         "example.com",
            "up_bytes":    1024,
            "down_bytes":  2048,
            "dst_country": "US",
            "src_country": "CN",
        },
        {
            "event_type":  "detect",
            "timestamp":   1700000060000,
            "probe_id":    "probe-B",
            "flow_id":     "def456",
            "src_ip":      "10.0.0.1",
            "src_port":    54321,
            "dst_ip":      "1.1.1.1",
            "dst_port":    80,
            "protocol":    "http",
            "confidence":  0.7,
            "evidence":    ["user_agent"],
            "features":    {"stat.pkt_count": 5.0},
            "data":        {},
            "sni":         "",
            "up_bytes":    512,
            "down_bytes":  4096,
            "dst_country": "US",
            "src_country": "DE",
        },
    ]
    n = len(rows)
    blk  = block_header(18, n)
    blk += col("event_type",  "LowCardinality(String)",  enc_lc_str_col([r["event_type"]  for r in rows]))
    blk += col("timestamp",   "DateTime64(3)",           enc_dt64_col([r["timestamp"]   for r in rows]))
    blk += col("probe_id",    "LowCardinality(String)",  enc_lc_str_col([r["probe_id"]    for r in rows]))
    blk += col("flow_id",     "String",                  enc_str_col([r["flow_id"]     for r in rows]))
    blk += col("src_ip",      "IPv6",                    enc_ipv6_col([r["src_ip"]      for r in rows]))
    blk += col("src_port",    "UInt16",                  enc_u16_col([r["src_port"]    for r in rows]))
    blk += col("dst_ip",      "IPv6",                    enc_ipv6_col([r["dst_ip"]      for r in rows]))
    blk += col("dst_port",    "UInt16",                  enc_u16_col([r["dst_port"]    for r in rows]))
    blk += col("protocol",    "LowCardinality(String)",  enc_lc_str_col([r["protocol"]    for r in rows]))
    blk += col("confidence",  "Float64",                 enc_f64_col([r["confidence"]  for r in rows]))
    blk += col("evidence",    "Array(String)",           enc_array_str_col([r["evidence"]    for r in rows]))
    blk += col("features",    "Map(String, Float64)",    enc_map_str_f64_col([r["features"]    for r in rows]))
    blk += col("data",        "Map(String, String)",     enc_map_str_str_col([r["data"]        for r in rows]))
    blk += col("sni",         "String",                  enc_str_col([r["sni"]         for r in rows]))
    blk += col("up_bytes",    "UInt64",                  enc_u64_col([r["up_bytes"]    for r in rows]))
    blk += col("down_bytes",  "UInt64",                  enc_u64_col([r["down_bytes"]  for r in rows]))
    blk += col("dst_country", "LowCardinality(String)",  enc_lc_str_col([r["dst_country"] for r in rows]))
    blk += col("src_country", "LowCardinality(String)",  enc_lc_str_col([r["src_country"] for r in rows]))
    insert_native(T, "", blk)

    check_count(T, n)
    check_col(T, "flow_id",  ["abc123", "def456"])
    check_col(T, "protocol", ["tls", "http"])
    check_col(T, "sni",      ["example.com", ""])
    check_col(T, "up_bytes", [1024, 512])

# ── Table 6: probe_events (Array + UInt16 + Int32) ───────────────────────────

def test_probe_events():
    print("\n── probe_events (Array/UInt16/Int32) ──")
    T = "test.probe_events"
    ddl(f"""CREATE TABLE IF NOT EXISTS {T} (
        timestamp        DateTime64(3),
        probe_id         LowCardinality(String),
        flow_id          String,
        src_ip           IPv6,
        src_port         UInt16,
        dst_ip           IPv6,
        dst_port         UInt16,
        protocol         LowCardinality(String),
        confidence       Float64,
        evidence         Array(String),
        passive_conf     Float64,
        probe_boost      Float64,
        dns_sni          String,
        dns_resolved_ips Array(String),
        dns_consistent   UInt8,
        tls_self_signed  UInt8,
        tls_issuer       String,
        tls_valid_days   Int32,
        tls_sans         Array(String),
        tls_matches_sni  UInt8,
        tls_is_letsencrypt UInt8,
        ss_is_ss_pattern UInt8
    ) ENGINE = MergeTree() ORDER BY (protocol, timestamp)""")

    rows = [
        {
            "timestamp": 1700000000000, "probe_id": "probe-A",
            "flow_id": "ghi789", "src_ip": "192.168.2.1", "src_port": 11111,
            "dst_ip": "93.184.216.34", "dst_port": 443, "protocol": "tls",
            "confidence": 0.85, "evidence": ["tls_probe"],
            "passive_conf": 0.7, "probe_boost": 0.15,
            "dns_sni": "example.com", "dns_resolved_ips": ["93.184.216.34"],
            "dns_consistent": 1,
            "tls_self_signed": 0, "tls_issuer": "DigiCert", "tls_valid_days": 365,
            "tls_sans": ["example.com", "www.example.com"],
            "tls_matches_sni": 1, "tls_is_letsencrypt": 0, "ss_is_ss_pattern": 0,
        },
    ]
    n = len(rows)
    blk  = block_header(22, n)
    blk += col("timestamp",          "DateTime64(3)",          enc_dt64_col([r["timestamp"]          for r in rows]))
    blk += col("probe_id",           "LowCardinality(String)", enc_lc_str_col([r["probe_id"]           for r in rows]))
    blk += col("flow_id",            "String",                 enc_str_col([r["flow_id"]            for r in rows]))
    blk += col("src_ip",             "IPv6",                   enc_ipv6_col([r["src_ip"]             for r in rows]))
    blk += col("src_port",           "UInt16",                 enc_u16_col([r["src_port"]           for r in rows]))
    blk += col("dst_ip",             "IPv6",                   enc_ipv6_col([r["dst_ip"]             for r in rows]))
    blk += col("dst_port",           "UInt16",                 enc_u16_col([r["dst_port"]           for r in rows]))
    blk += col("protocol",           "LowCardinality(String)", enc_lc_str_col([r["protocol"]           for r in rows]))
    blk += col("confidence",         "Float64",                enc_f64_col([r["confidence"]         for r in rows]))
    blk += col("evidence",           "Array(String)",          enc_array_str_col([r["evidence"]    for r in rows]))
    blk += col("passive_conf",       "Float64",                enc_f64_col([r["passive_conf"]       for r in rows]))
    blk += col("probe_boost",        "Float64",                enc_f64_col([r["probe_boost"]        for r in rows]))
    blk += col("dns_sni",            "String",                 enc_str_col([r["dns_sni"]            for r in rows]))
    blk += col("dns_resolved_ips",   "Array(String)",          enc_array_str_col([r["dns_resolved_ips"] for r in rows]))
    blk += col("dns_consistent",     "UInt8",                  enc_u8_col([r["dns_consistent"]     for r in rows]))
    blk += col("tls_self_signed",    "UInt8",                  enc_u8_col([r["tls_self_signed"]    for r in rows]))
    blk += col("tls_issuer",         "String",                 enc_str_col([r["tls_issuer"]         for r in rows]))
    blk += col("tls_valid_days",     "Int32",                  enc_i32_col([r["tls_valid_days"]     for r in rows]))
    blk += col("tls_sans",           "Array(String)",          enc_array_str_col([r["tls_sans"]    for r in rows]))
    blk += col("tls_matches_sni",    "UInt8",                  enc_u8_col([r["tls_matches_sni"]    for r in rows]))
    blk += col("tls_is_letsencrypt", "UInt8",                  enc_u8_col([r["tls_is_letsencrypt"] for r in rows]))
    blk += col("ss_is_ss_pattern",   "UInt8",                  enc_u8_col([r["ss_is_ss_pattern"]   for r in rows]))
    insert_native(T, "", blk)

    check_count(T, n)
    check_col(T, "flow_id",    ["ghi789"])
    check_col(T, "tls_issuer", ["DigiCert"])
    check_col(T, "dns_sni",    ["example.com"])

# ── Table 7: intel_feeds (Map(String,String) + many String cols) ──────────────

def test_intel_feeds():
    print("\n── intel_feeds ──")
    T = "test.intel_feeds"
    ddl(f"""CREATE TABLE IF NOT EXISTS {T} (
        feed_id         String,
        name            String,
        kind            LowCardinality(String),
        resource_type   LowCardinality(String),
        apply_target    LowCardinality(String),
        source_url      String,
        info_url        String,
        enabled         UInt8,
        current_version String,
        pending_version String,
        status          LowCardinality(String),
        entry_count     UInt64,
        checksum        String,
        last_checked_at DateTime64(3),
        last_updated_at DateTime64(3),
        last_error      String,
        data            Map(String, String),
        updated_at      DateTime64(3),
        version         UInt64
    ) ENGINE = ReplacingMergeTree(version) ORDER BY feed_id""")

    rows = [
        {
            "feed_id": "feed-001", "name": "Threat Intel A",
            "kind": "ip", "resource_type": "table", "apply_target": "dictionary",
            "source_url": "https://example.com/feed.csv", "info_url": "https://example.com",
            "enabled": 1, "current_version": "v1", "pending_version": "",
            "status": "ok", "entry_count": 1000, "checksum": "abc123",
            "last_checked_at": 1700000000000, "last_updated_at": 1700000000000,
            "last_error": "", "data": {"format": "csv"}, "updated_at": 1700000000000,
            "version": 1,
        },
    ]
    n = len(rows)
    blk  = block_header(19, n)
    blk += col("feed_id",         "String",                  enc_str_col([r["feed_id"]         for r in rows]))
    blk += col("name",            "String",                  enc_str_col([r["name"]            for r in rows]))
    blk += col("kind",            "LowCardinality(String)",  enc_lc_str_col([r["kind"]           for r in rows]))
    blk += col("resource_type",   "LowCardinality(String)",  enc_lc_str_col([r["resource_type"]  for r in rows]))
    blk += col("apply_target",    "LowCardinality(String)",  enc_lc_str_col([r["apply_target"]   for r in rows]))
    blk += col("source_url",      "String",                  enc_str_col([r["source_url"]      for r in rows]))
    blk += col("info_url",        "String",                  enc_str_col([r["info_url"]        for r in rows]))
    blk += col("enabled",         "UInt8",                   enc_u8_col([r["enabled"]         for r in rows]))
    blk += col("current_version", "String",                  enc_str_col([r["current_version"] for r in rows]))
    blk += col("pending_version", "String",                  enc_str_col([r["pending_version"] for r in rows]))
    blk += col("status",          "LowCardinality(String)",  enc_lc_str_col([r["status"]         for r in rows]))
    blk += col("entry_count",     "UInt64",                  enc_u64_col([r["entry_count"]     for r in rows]))
    blk += col("checksum",        "String",                  enc_str_col([r["checksum"]        for r in rows]))
    blk += col("last_checked_at", "DateTime64(3)",           enc_dt64_col([r["last_checked_at"] for r in rows]))
    blk += col("last_updated_at", "DateTime64(3)",           enc_dt64_col([r["last_updated_at"] for r in rows]))
    blk += col("last_error",      "String",                  enc_str_col([r["last_error"]      for r in rows]))
    blk += col("data",            "Map(String, String)",     enc_map_str_str_col([r["data"]   for r in rows]))
    blk += col("updated_at",      "DateTime64(3)",           enc_dt64_col([r["updated_at"]      for r in rows]))
    blk += col("version",         "UInt64",                  enc_u64_col([r["version"]         for r in rows]))
    insert_native(T, "", blk)

    check_count(T, n)
    check_col(T, "feed_id", ["feed-001"])
    check_col(T, "name",    ["Threat Intel A"])
    check_col(T, "status",  ["ok"])

# ── Table 8: intel_feed_items (Array(String) + Map(String,String) + UInt64) ──

def test_intel_feed_items():
    print("\n── intel_feed_items ──")
    T = "test.intel_feed_items"
    ddl(f"""CREATE TABLE IF NOT EXISTS {T} (
        feed_id    String,
        version    String,
        kind       LowCardinality(String),
        key        String,
        tags       Array(String),
        score      Float64,
        data       Map(String, String),
        enabled    UInt8,
        updated_at DateTime64(3),
        row_hash   UInt64
    ) ENGINE = ReplacingMergeTree(row_hash) ORDER BY (kind, key, feed_id, version)""")

    rows = [
        {
            "feed_id": "feed-001", "version": "v1", "kind": "ip",
            "key": "1.2.3.4", "tags": ["malware", "c2"],
            "score": 0.95, "data": {"asn": "AS12345"},
            "enabled": 1, "updated_at": 1700000000000, "row_hash": 111111,
        },
        {
            "feed_id": "feed-001", "version": "v1", "kind": "domain",
            "key": "evil.example.com", "tags": ["phishing"],
            "score": 0.8, "data": {},
            "enabled": 1, "updated_at": 1700000001000, "row_hash": 222222,
        },
    ]
    n = len(rows)
    blk  = block_header(10, n)
    blk += col("feed_id",    "String",                  enc_str_col([r["feed_id"]    for r in rows]))
    blk += col("version",    "String",                  enc_str_col([r["version"]    for r in rows]))
    blk += col("kind",       "LowCardinality(String)",  enc_lc_str_col([r["kind"]       for r in rows]))
    blk += col("key",        "String",                  enc_str_col([r["key"]        for r in rows]))
    blk += col("tags",       "Array(String)",           enc_array_str_col([r["tags"] for r in rows]))
    blk += col("score",      "Float64",                 enc_f64_col([r["score"]      for r in rows]))
    blk += col("data",       "Map(String, String)",     enc_map_str_str_col([r["data"] for r in rows]))
    blk += col("enabled",    "UInt8",                   enc_u8_col([r["enabled"]    for r in rows]))
    blk += col("updated_at", "DateTime64(3)",           enc_dt64_col([r["updated_at"] for r in rows]))
    blk += col("row_hash",   "UInt64",                  enc_u64_col([r["row_hash"]   for r in rows]))
    insert_native(T, "", blk)

    check_count(T, n)
    check_col(T, "key",     ["1.2.3.4", "evil.example.com"])
    check_col(T, "feed_id", ["feed-001", "feed-001"])

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    if os.path.exists(DATA_DIR): shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR)
    zh = subprocess.Popen([ZH_BIN, "serve", f"--data-dir={DATA_DIR}", f"--port={PORT}"],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert wait_ping(), "ZigHouse did not start"
        print(f"[OK] ZigHouse started on port {PORT}")
        ddl("CREATE DATABASE IF NOT EXISTS test")

        test_probe_heartbeats()
        test_scoring_rules()
        test_app_config()
        test_intel_entries()
        test_detect_events()
        test_probe_events()
        test_intel_feeds()
        test_intel_feed_items()

        print(f"\n{'='*50}")
        print(f"Results: {PASS} passed, {FAIL} failed")
        if FAIL: sys.exit(1)
        else: print("✓ All tests passed")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
    finally:
        zh.terminate(); zh.wait()
        shutil.rmtree(DATA_DIR, ignore_errors=True)

if __name__ == "__main__":
    main()
