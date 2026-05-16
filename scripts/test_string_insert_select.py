#!/usr/bin/env python3
"""
Local read/write test for ZigHouse string columns.

Constructs a ClickHouse Native Block (as clickhouse-go HTTP sends it),
POSTs it as INSERT, then SELECTs back and verifies string values are intact.

Schema mirrors vprobe.scoring_rules:
  rule_id      String
  protocol     LowCardinality(String)
  feature      String
  operator     LowCardinality(String)
  threshold    Float64
  weight       Float64
  enabled      UInt8
  note         String

Usage:
  python3 scripts/test_string_insert_select.py [port]
"""

import struct
import sys
import urllib.request
import urllib.parse
import os
import subprocess
import time
import signal
import shutil

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 29903
BASE = f"http://127.0.0.1:{PORT}"
DATA_DIR = f"/tmp/zh_test_str_{PORT}"
ZH_BIN = os.path.join(os.path.dirname(__file__), "../zig-out/bin/zighouse")

# ── Native Block encoding helpers ─────────────────────────────────────────────

def varint(n: int) -> bytes:
    out = []
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)

def ch_string(s: str) -> bytes:
    b = s.encode()
    return varint(len(b)) + b

def u64le(n: int) -> bytes:
    return struct.pack('<Q', n)

def i64le(n: int) -> bytes:
    return struct.pack('<q', n)

def f64le(f: float) -> bytes:
    return struct.pack('<d', f)

def u8(n: int) -> bytes:
    return struct.pack('B', n)

def encode_lowcardinality_column(values: list[str]) -> bytes:
    """
    Encode a LowCardinality(String) column as clickhouse-go HTTP sends it.

    Wire layout (encodeRevision=0, writeStatePrefix=True):
      uint64  state_prefix = 1   (sharedDictionariesWithAdditionalKeys)
      uint64  flags = 0x602      (updateAll=0x600 | key_type=UInt8=0x2? no: UInt16=1)
      Actually: flags = 0x600 | key_type
        key_type 0 → UInt8  (dict_size <= 255)
        key_type 1 → UInt16 (dict_size <= 65535)
      int64   dict_count
      dict_count × ch_string
      int64   key_count
      key_count × uint8/16 indices
    """
    # Build dictionary (deduplicated, preserving insertion order)
    seen = {}
    for v in values:
        if v not in seen:
            seen[v] = len(seen)
    dict_entries = list(seen.keys())
    dict_count = len(dict_entries)

    # Choose key type
    if dict_count <= 255:
        key_type = 0  # UInt8
    else:
        key_type = 1  # UInt16

    flags = 0x600 | key_type  # updateAll | key_type

    out = b""
    out += u64le(1)           # state_prefix
    out += u64le(flags)
    out += i64le(dict_count)
    for entry in dict_entries:
        out += ch_string(entry)
    out += i64le(len(values))
    for v in values:
        idx = seen[v]
        if key_type == 0:
            out += u8(idx)
        else:
            out += struct.pack('<H', idx)
    return out

def encode_string_column(values: list[str]) -> bytes:
    """Encode a plain String column (columnar, no prefix)."""
    out = b""
    for v in values:
        out += ch_string(v)
    return out

def encode_f64_column(values: list[float]) -> bytes:
    out = b""
    for v in values:
        out += f64le(v)
    return out

def encode_u8_column(values: list[int]) -> bytes:
    out = b""
    for v in values:
        out += u8(v)
    return out

def encode_u64_column(values: list[int]) -> bytes:
    out = b""
    for v in values:
        out += u64le(v)
    return out

def build_native_block(rows: list[dict]) -> bytes:
    """
    Build a Native Block for scoring_rules (all 11 columns).
    clickhouse-go HTTP: NO BlockInfo prefix, just num_cols + num_rows + columns.
    """
    num_cols = 11
    num_rows = len(rows)

    rule_ids   = [r["rule_id"]    for r in rows]
    protocols  = [r["protocol"]   for r in rows]
    features   = [r["feature"]    for r in rows]
    operators  = [r["operator"]   for r in rows]
    thresholds = [r["threshold"]  for r in rows]
    uppers     = [r["upper"]      for r in rows]
    weights    = [r["weight"]     for r in rows]
    enableds   = [r["enabled"]    for r in rows]
    notes      = [r["note"]       for r in rows]
    updated_at = [r["updated_at"] for r in rows]
    versions   = [r["version"]    for r in rows]

    block = b""
    block += varint(num_cols)
    block += varint(num_rows)

    # rule_id  String
    block += ch_string("rule_id")
    block += ch_string("String")
    block += encode_string_column(rule_ids)

    # protocol  LowCardinality(String)
    block += ch_string("protocol")
    block += ch_string("LowCardinality(String)")
    block += encode_lowcardinality_column(protocols)

    # feature  String
    block += ch_string("feature")
    block += ch_string("String")
    block += encode_string_column(features)

    # operator  LowCardinality(String)
    block += ch_string("operator")
    block += ch_string("LowCardinality(String)")
    block += encode_lowcardinality_column(operators)

    # threshold  Float64
    block += ch_string("threshold")
    block += ch_string("Float64")
    block += encode_f64_column(thresholds)

    # upper  Float64
    block += ch_string("upper")
    block += ch_string("Float64")
    block += encode_f64_column(uppers)

    # weight  Float64
    block += ch_string("weight")
    block += ch_string("Float64")
    block += encode_f64_column(weights)

    # enabled  UInt8
    block += ch_string("enabled")
    block += ch_string("UInt8")
    block += encode_u8_column(enableds)

    # note  String
    block += ch_string("note")
    block += ch_string("String")
    block += encode_string_column(notes)

    # updated_at  DateTime64(3)
    block += ch_string("updated_at")
    block += ch_string("DateTime64(3)")
    block += encode_u64_column(updated_at)

    # version  UInt64
    block += ch_string("version")
    block += ch_string("UInt64")
    block += encode_u64_column(versions)

    return block

# ── HTTP helpers ──────────────────────────────────────────────────────────────

def post(path: str, body: bytes, content_type: str = "application/octet-stream") -> bytes:
    req = urllib.request.Request(
        BASE + path,
        data=body,
        headers={"Content-Type": content_type},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return resp.read()

def get(path: str) -> str:
    with urllib.request.urlopen(BASE + path) as resp:
        return resp.read().decode()

# ── Test data ──────────────────────────────────────────────────────────────────

TEST_ROWS = [
    {
        "rule_id":    "aaaaaaaa-0001-0001-0001-000000000001",
        "protocol":   "tcp",
        "feature":    "stat.byte_ratio",
        "operator":   ">=",
        "threshold":  0.8,
        "upper":      1.0,
        "weight":     2.5,
        "enabled":    1,
        "note":       "test rule one",
        "updated_at": 1700000000000,
        "version":    1,
    },
    {
        "rule_id":    "aaaaaaaa-0002-0002-0002-000000000002",
        "protocol":   "udp",
        "feature":    "stat.pkt_count",
        "operator":   "between",
        "threshold":  10.0,
        "upper":      100.0,
        "weight":     1.0,
        "enabled":    1,
        "note":       "test rule two",
        "updated_at": 1700000001000,
        "version":    2,
    },
    {
        "rule_id":    "aaaaaaaa-0003-0003-0003-000000000003",
        "protocol":   "tcp",
        "feature":    "time.stream_dur_ms",
        "operator":   "<=",
        "threshold":  500.0,
        "upper":      0.0,
        "weight":     0.5,
        "enabled":    0,
        "note":       "",
        "updated_at": 1700000002000,
        "version":    3,
    },
]

# ── Main ──────────────────────────────────────────────────────────────────────

def wait_ping(timeout=10):
    for _ in range(timeout * 10):
        try:
            with urllib.request.urlopen(f"{BASE}/ping", timeout=1) as r:
                if r.read().strip() == b"Ok.":
                    return True
        except Exception:
            pass
        time.sleep(0.1)
    return False

def main():
    # 1. Start ZigHouse
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR)

    zh = subprocess.Popen(
        [ZH_BIN, "serve", f"--data-dir={DATA_DIR}", f"--port={PORT}"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        assert wait_ping(), "ZigHouse did not start"
        print(f"[OK] ZigHouse started on port {PORT}")

        # 2. CREATE TABLE (DDL)
        ddl = (
            "CREATE TABLE IF NOT EXISTS test.scoring_rules ("
            "  rule_id String,"
            "  protocol LowCardinality(String),"
            "  feature String,"
            "  operator LowCardinality(String),"
            "  threshold Float64,"
            "  upper Float64,"
            "  weight Float64,"
            "  enabled UInt8,"
            "  note String,"
            "  updated_at DateTime64(3),"
            "  version UInt64"
            ") ENGINE = ReplacingMergeTree(version) ORDER BY (rule_id)"
        )
        post("/?query=" + urllib.parse.quote(ddl), b"")
        print("[OK] CREATE TABLE")

        # 3. INSERT via Native Block
        block = build_native_block(TEST_ROWS)
        sql = "INSERT INTO test.scoring_rules FORMAT Native"
        post(f"/?query={urllib.parse.quote(sql)}", block)
        print(f"[OK] INSERT {len(TEST_ROWS)} rows ({len(block)} bytes Native Block)")

        # 4. SELECT count(*)
        result = get("/?query=" + urllib.parse.quote(
            "SELECT count(*) FROM test.scoring_rules"
        ))
        lines = [l for l in result.strip().split("\n") if l]
        count = int(lines[-1])
        assert count == len(TEST_ROWS), f"count={count} want {len(TEST_ROWS)}"
        print(f"[OK] SELECT count(*) = {count}")

        # 5. SELECT string columns and verify
        result = get("/?query=" + urllib.parse.quote(
            "SELECT rule_id, protocol, feature, operator, note FROM test.scoring_rules"
        ))
        print("\n[DEBUG] Raw SELECT result:")
        print(result)

        rows = [l.split(",") for l in result.strip().split("\n") if l]
        header = rows[0]
        data_rows = rows[1:]
        assert len(data_rows) == len(TEST_ROWS), \
            f"got {len(data_rows)} data rows, want {len(TEST_ROWS)}"

        # Build expected by matching rule_id
        expected_by_id = {r["rule_id"]: r for r in TEST_ROWS}

        errors = []
        for row in data_rows:
            row_dict = dict(zip(header, row))
            rid = row_dict.get("rule_id", "").strip()
            if rid not in expected_by_id:
                errors.append(f"Unexpected rule_id: {repr(rid)}")
                continue
            exp = expected_by_id[rid]
            for col in ("protocol", "feature", "operator", "note"):
                got = row_dict.get(col, "").strip()
                want = exp[col]
                if got != want:
                    errors.append(f"rule_id={rid} col={col}: got={repr(got)} want={repr(want)}")

        if errors:
            print("\n[FAIL] String column mismatches:")
            for e in errors:
                print("  " + e)
            sys.exit(1)
        else:
            print("[OK] All string columns match!")

        # 6. SELECT numeric columns
        result2 = get("/?query=" + urllib.parse.quote(
            "SELECT rule_id, threshold, weight, enabled FROM test.scoring_rules"
        ))
        rows2 = [l.split(",") for l in result2.strip().split("\n") if l]
        header2 = rows2[0]
        data2 = rows2[1:]
        num_errors = []
        for row in data2:
            rd = dict(zip(header2, row))
            rid = rd.get("rule_id", "").strip()
            if rid not in expected_by_id:
                continue
            exp = expected_by_id[rid]
            for col, want in [("threshold", exp["threshold"]),
                               ("weight",    exp["weight"]),
                               ("enabled",   exp["enabled"])]:
                got_s = rd.get(col, "").strip()
                try:
                    got_f = float(got_s)
                except ValueError:
                    num_errors.append(f"rule_id={rid} col={col}: cannot parse {repr(got_s)}")
                    continue
                if abs(got_f - float(want)) > 1e-9:
                    num_errors.append(f"rule_id={rid} col={col}: got={got_f} want={want}")

        if num_errors:
            print("\n[FAIL] Numeric column mismatches:")
            for e in num_errors:
                print("  " + e)
            sys.exit(1)
        else:
            print("[OK] All numeric columns match!")

        print("\n✓ All tests passed")

    finally:
        zh.terminate()
        zh.wait()
        shutil.rmtree(DATA_DIR, ignore_errors=True)

if __name__ == "__main__":
    main()
