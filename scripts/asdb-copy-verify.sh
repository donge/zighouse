#!/usr/bin/env bash
# Copy active ClickHouse parts from a remote ClickHouse/asdb instance into a
# ZigHouse data dir, then run a small read-only verification suite.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${ZIGHOUSE_BIN:-$REPO_DIR/zig-out/bin/zighouse}"
REMOTE="${ASDB_REMOTE:-ubuntu@49.235.47.102}"
CONTAINER="${ASDB_CONTAINER:-sw_asdb}"
SOURCE_MODE="${ASDB_SOURCE_MODE:-auto}" # auto | docker | direct
CH_HOST="${ASDB_CH_HOST:-127.0.0.1}"
CH_PORT="${ASDB_CH_PORT:-9000}"
CH_PASSWORD="${ASDB_CH_PASSWORD:-}"
DB_FILTER="${ASDB_DB:-}"
DATA_DIR="${ASDB_DATA_DIR:-/tmp/zighouse-asdb-copy}"
PORT="${ASDB_ZH_PORT:-19910}"
HTTP_PORT=$((PORT + 1))
TABLE_FILTER="${ASDB_TABLES:-}"
COLUMN_FILTER="${ASDB_COLUMNS:-}"
CURL="curl -sS --noproxy localhost"
SERVER_PID=""
DIRECT_ROOT=""

ssh_q() {
    ssh -n "$REMOTE" "$@"
}

detect_source_mode() {
    if [[ "$SOURCE_MODE" != "auto" ]]; then
        echo "$SOURCE_MODE"
        return
    fi
    if ssh_q "sudo -n docker exec $CONTAINER clickhouse-client --query 'SELECT 1' >/dev/null 2>&1"; then
        echo docker
        return
    fi
    echo direct
}

SOURCE_MODE="$(detect_source_mode)"
if [[ "$SOURCE_MODE" == "direct" ]]; then
    ASDB_PID="$(ssh_q "pgrep -f 'asdb server|clickhouse-server' | head -1" | tr -d '\r')"
    if [[ -z "$ASDB_PID" ]]; then
        echo "ERROR: direct mode could not find asdb/clickhouse server pid on $REMOTE" >&2
        exit 1
    fi
    DIRECT_ROOT="/proc/$ASDB_PID/root"
fi

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

remote_ch() {
    local sql="$1"
    if [[ "$SOURCE_MODE" == "docker" ]]; then
        printf '%s\n' "$sql" |
            ssh "$REMOTE" "sudo -n docker exec -i $CONTAINER clickhouse-client --format TSVRaw"
    else
        local pass_arg=()
        if [[ -n "$CH_PASSWORD" ]]; then
            pass_arg=(--password="$CH_PASSWORD")
        fi
        printf '%s\n' "$sql" |
            ssh "$REMOTE" "clickhouse-client --host='$CH_HOST' --port='$CH_PORT' ${pass_arg[*]} --format TSVRaw"
    fi
}

remote_cat() {
    local path="$1"
    if [[ "$SOURCE_MODE" == "docker" ]]; then
        ssh -n "$REMOTE" "sudo -n docker exec $CONTAINER cat '$path'"
    else
        ssh -n "$REMOTE" "sudo -n cat '$DIRECT_ROOT$path'"
    fi
}

copy_part() {
    local src="$1"
    local dest="$2"
    local parent part
    parent="$(dirname "$src")"
    part="$(basename "$src")"
    mkdir -p "$dest"
    if [[ -z "$COLUMN_FILTER" ]]; then
        if [[ "$SOURCE_MODE" == "docker" ]]; then
            ssh -n "$REMOTE" "sudo -n docker exec $CONTAINER tar -C '$parent' -cf - '$part'" |
                tar -C "$dest" -xf -
        else
            ssh -n "$REMOTE" "sudo -n tar -C '$DIRECT_ROOT$parent' -cf - '$part'" |
                tar -C "$dest" -xf -
        fi
    else
        local remote_filter='
            cd "$parent"
            files=""
            for f in "$PART/count.txt" "$PART/columns.txt" "$PART/columns_substreams.txt" "$PART/metadata_version.txt" "$PART/default_compression_codec.txt" "$PART/serialization.json" "$PART/primary.cidx" "$PART/primary.idx" "$PART/data.bin" "$PART/data.cmrk2" "$PART/data.cmrk3" "$PART/data.cmrk4"; do
                [ -e "$f" ] && files="$files $f"
            done
            for c in ${COLS//,/ }; do
                enc="${c//./%2E}"
                for base in "$c" "$enc"; do
                for suffix in .bin .cmrk2 .mrk2 .size.bin .size.cmrk2 .size.mrk2 .size0.bin .size0.cmrk2 .size0.mrk2 .dict.bin .dict.cmrk2 .dict.mrk2 .dict_prefix.bin .dict_prefix.cmrk2 .null.bin .null.cmrk2 .sparse.idx.bin .sparse.idx.cmrk2; do
                    f="$PART/$base$suffix"
                    [ -e "$f" ] && files="$files $f"
                done
                done
            done
            tar -cf - $files
        '
        if [[ "$SOURCE_MODE" == "docker" ]]; then
            ssh -n "$REMOTE" "sudo -n docker exec -e PART='$part' -e COLS='$COLUMN_FILTER' $CONTAINER bash -lc '$remote_filter'" |
                tar -C "$dest" -xf -
        else
            ssh -n "$REMOTE" "sudo -n env PART='$part' COLS='$COLUMN_FILTER' parent='$DIRECT_ROOT$parent' bash -lc '$remote_filter'" |
                tar -C "$dest" -xf -
        fi
    fi
}

urlenc() {
    python3 -c 'import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))' "$1"
}

select_zh() {
    local sql="$1"
    local encoded
    encoded="$(urlenc "$sql")"
    $CURL "http://127.0.0.1:$HTTP_PORT/?query=$encoded&default_format=TabSeparated"
}

wait_for_server() {
    for _ in $(seq 1 80); do
        if $CURL "http://127.0.0.1:$HTTP_PORT/?query=SELECT+1" >/dev/null 2>&1; then
            return
        fi
        sleep 0.25
    done
    echo "ERROR: ZigHouse server did not start"
    cat /tmp/zighouse-asdb-copy-server.log 2>/dev/null || true
    exit 1
}

if [[ ! -x "$BINARY" ]]; then
    (cd "$REPO_DIR" && zig build -Doptimize=ReleaseFast)
fi

rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR/metadata"

echo "== Fetching table manifest from $REMOTE ($SOURCE_MODE) =="
TABLES_TSV="$DATA_DIR/tables.tsv"
PARTS_TSV="$DATA_DIR/parts.tsv"

DB_PRED="database NOT IN ('system','INFORMATION_SCHEMA','information_schema')"
if [[ -n "$DB_FILTER" ]]; then
    DB_PRED="database = '$DB_FILTER'"
fi
remote_ch "SELECT database, name, engine, metadata_path FROM system.tables WHERE $DB_PRED AND engine IN ('MergeTree','ReplacingMergeTree') ORDER BY database, name FORMAT TSVRaw" > "$TABLES_TSV"
remote_ch "SELECT database, table, name, path, rows FROM system.parts WHERE active AND $DB_PRED ORDER BY database, table, name FORMAT TSVRaw" > "$PARTS_TSV"
if [[ -n "$TABLE_FILTER" ]]; then
    awk -F'\t' -v keep="$TABLE_FILTER" '
        BEGIN { split(keep, a, ","); for (i in a) ok[a[i]]=1 }
        ok[$2]
    ' "$TABLES_TSV" > "$TABLES_TSV.filtered"
    mv "$TABLES_TSV.filtered" "$TABLES_TSV"
    awk -F'\t' -v keep="$TABLE_FILTER" '
        BEGIN { split(keep, a, ","); for (i in a) ok[a[i]]=1 }
        ok[$2]
    ' "$PARTS_TSV" > "$PARTS_TSV.filtered"
    mv "$PARTS_TSV.filtered" "$PARTS_TSV"
fi

echo "== Copying metadata SQL =="
while IFS=$'\t' read -r db table engine meta_path; do
    [[ -z "${db:-}" ]] && continue
    mkdir -p "$DATA_DIR/metadata/$db"
    remote_cat "/var/lib/clickhouse/$meta_path" > "$DATA_DIR/metadata/$db/$table.sql"
    echo "metadata $db.$table ($engine)"
done < "$TABLES_TSV"

echo "== Copying active parts =="
while IFS=$'\t' read -r db table part path rows; do
    [[ -z "${db:-}" ]] && continue
    copy_part "${path%/}" "$DATA_DIR/$db/$table/parts"
    echo "part $db.$table/$part rows=$rows"
done < "$PARTS_TSV"

echo "== Starting ZigHouse on $PORT =="
"$BINARY" serve "--data-dir=$DATA_DIR" "--port=$PORT" &>/tmp/zighouse-asdb-copy-server.log &
SERVER_PID=$!
wait_for_server

echo "== Comparing counts =="
failed=0
while IFS=$'\t' read -r db table engine meta_path; do
    [[ -z "${db:-}" ]] && continue
    copied_count="$(awk -F'\t' -v db="$db" -v table="$table" '$1 == db && $2 == table { sum += $5 } END { print sum + 0 }' "$PARTS_TSV")"
    ch_count="$(remote_ch "SELECT count(*) FROM $db.$table FORMAT TSVRaw" | tr -d '\r')"
    zh_count="$(select_zh "SELECT count(*) FROM $db.$table" | tr -d '\r')"
    if [[ "$copied_count" != "$zh_count" ]]; then
        echo "FAIL count $db.$table copied_parts=$copied_count zighouse=$zh_count clickhouse_now=$ch_count"
        failed=1
    else
        if [[ "$ch_count" != "$zh_count" ]]; then
            echo "PASS count $db.$table = $zh_count (clickhouse_now=$ch_count live drift)"
        else
            echo "PASS count $db.$table = $zh_count"
        fi
    fi
done < "$TABLES_TSV"

echo "== Smoke queries =="
while IFS=$'\t' read -r db table engine meta_path; do
    [[ -z "${db:-}" ]] && continue
    cols="$(remote_ch "SELECT groupArray(name) FROM system.columns WHERE database='$db' AND table='$table' FORMAT TSVRaw" | tr -d '[]'\''\r')"
    if [[ "$cols" == *"code"* && "$cols" == *"date"* ]]; then
        query="SELECT code, date FROM $db.$table LIMIT 5"
    elif [[ "$cols" == *"key"* ]]; then
        query="SELECT key FROM $db.$table LIMIT 5"
    elif [[ "$cols" == *"probe_id"* ]]; then
        query="SELECT probe_id FROM $db.$table LIMIT 5"
    elif [[ "$cols" == *"rule_id"* ]]; then
        query="SELECT rule_id FROM $db.$table LIMIT 5"
    else
        query="SELECT count(*) FROM $db.$table"
    fi
    echo "-- $query"
    ch_rows="$(remote_ch "$query FORMAT TSVRaw" | tr -d '\r')"
    zh_rows="$(select_zh "$query" | tr -d '\r')"
    if [[ "$ch_rows" != "$zh_rows" ]]; then
        echo "FAIL smoke $db.$table"
        echo "clickhouse:"
        printf '%s\n' "$ch_rows"
        echo "zighouse:"
        printf '%s\n' "$zh_rows"
        failed=1
    else
        echo "PASS smoke $db.$table"
    fi
done < "$TABLES_TSV"

if grep -q $'vprobe\tdetect_events\t' "$TABLES_TSV"; then
    echo "-- vprobe.detect_events ARRAY JOIN mapKeys/mapValues"
    query="SELECT fk, fv FROM vprobe.detect_events ARRAY JOIN mapKeys(features) AS fk, mapValues(features) AS fv LIMIT 5"
    zh_rows="$(select_zh "$query" | tr -d '\r')"
    if [[ -z "$zh_rows" ]]; then
        echo "FAIL array join vprobe.detect_events returned no rows"
        printf '%s\n' "$zh_rows"
        failed=1
    else
        echo "PASS array join vprobe.detect_events"
    fi
fi

if grep -q $'default\tfund_flow\t' "$TABLES_TSV"; then
    echo "-- default.fund_flow count already checked; compact cmrk3 column reads may be unsupported"
fi

if [[ "$failed" != 0 ]]; then
    echo "asdb copy verify FAILED"
    exit 1
fi
echo "asdb copy verify PASS"
