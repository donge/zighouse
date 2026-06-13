#!/usr/bin/env bash
# Copy active ClickHouse parts from the remote asdb container into a ZigHouse
# data dir, then run a small read-only verification suite.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${ZIGHOUSE_BIN:-$REPO_DIR/zig-out/bin/zighouse}"
REMOTE="${ASDB_REMOTE:-ubuntu@49.235.47.102}"
CONTAINER="${ASDB_CONTAINER:-sw_asdb}"
DATA_DIR="${ASDB_DATA_DIR:-/tmp/zighouse-asdb-copy}"
PORT="${ASDB_ZH_PORT:-19910}"
HTTP_PORT=$((PORT + 1))
TABLE_FILTER="${ASDB_TABLES:-}"
COLUMN_FILTER="${ASDB_COLUMNS:-}"
CURL="curl -sS --noproxy localhost"
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

remote_ch() {
    local sql="$1"
    printf '%s\n' "$sql" |
        ssh "$REMOTE" "sudo -n docker exec -i $CONTAINER clickhouse-client --format TSVRaw"
}

remote_cat() {
    local path="$1"
    ssh -n "$REMOTE" "sudo -n docker exec $CONTAINER cat '$path'"
}

copy_part() {
    local src="$1"
    local dest="$2"
    local parent part
    parent="$(dirname "$src")"
    part="$(basename "$src")"
    mkdir -p "$dest"
    if [[ -z "$COLUMN_FILTER" ]]; then
        ssh -n "$REMOTE" "sudo -n docker exec $CONTAINER tar -C '$parent' -cf - '$part'" |
            tar -C "$dest" -xf -
    else
        ssh -n "$REMOTE" "sudo -n docker exec -e PART='$part' -e COLS='$COLUMN_FILTER' $CONTAINER bash -lc '
            cd \"$parent\"
            files=\"\"
            for f in \"\$PART/count.txt\" \"\$PART/columns.txt\" \"\$PART/metadata_version.txt\" \"\$PART/default_compression_codec.txt\" \"\$PART/serialization.json\" \"\$PART/primary.cidx\" \"\$PART/primary.idx\" \"\$PART/data.bin\" \"\$PART/data.cmrk2\" \"\$PART/data.cmrk3\" \"\$PART/data.cmrk4\"; do
                [ -e \"\$f\" ] && files=\"\$files \$f\"
            done
            for c in \${COLS//,/ }; do
                for suffix in .bin .cmrk2 .mrk2 .null.bin .null.cmrk2 .sparse.idx.bin .sparse.idx.cmrk2; do
                    f=\"\$PART/\$c\$suffix\"
                    [ -e \"\$f\" ] && files=\"\$files \$f\"
                done
            done
            tar -cf - \$files
        '" | tar -C "$dest" -xf -
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

echo "== Fetching table manifest from $REMOTE/$CONTAINER =="
TABLES_TSV="$DATA_DIR/tables.tsv"
PARTS_TSV="$DATA_DIR/parts.tsv"

remote_ch "SELECT database, name, engine, metadata_path FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') AND engine IN ('MergeTree','ReplacingMergeTree') ORDER BY database, name FORMAT TSVRaw" > "$TABLES_TSV"
remote_ch "SELECT database, table, name, path, rows FROM system.parts WHERE active AND database NOT IN ('system','INFORMATION_SCHEMA','information_schema') ORDER BY database, table, name FORMAT TSVRaw" > "$PARTS_TSV"
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
    ch_count="$(remote_ch "SELECT count(*) FROM $db.$table FORMAT TSVRaw" | tr -d '\r')"
    zh_count="$(select_zh "SELECT count(*) FROM $db.$table" | tr -d '\r')"
    if [[ "$ch_count" != "$zh_count" ]]; then
        echo "FAIL count $db.$table clickhouse=$ch_count zighouse=$zh_count"
        failed=1
    else
        echo "PASS count $db.$table = $zh_count"
    fi
done < "$TABLES_TSV"

echo "== Smoke queries =="
for table in day day_old day_p60 day_hc day_next_map; do
    if grep -q $'default\t'"$table"$'\t' "$TABLES_TSV"; then
        echo "-- default.$table LIMIT"
        ch_rows="$(remote_ch "SELECT code, date FROM default.$table LIMIT 5 FORMAT TSVRaw" | tr -d '\r')"
        zh_rows="$(select_zh "SELECT code, date FROM default.$table LIMIT 5" | tr -d '\r')"
        if [[ "$ch_rows" != "$zh_rows" ]]; then
            echo "FAIL limit default.$table"
            echo "clickhouse:"
            printf '%s\n' "$ch_rows"
            echo "zighouse:"
            printf '%s\n' "$zh_rows"
            failed=1
        else
            echo "PASS limit default.$table"
        fi
    fi
done

if grep -q $'default\tfund_flow\t' "$TABLES_TSV"; then
    echo "-- default.fund_flow count already checked; compact cmrk3 column reads may be unsupported"
fi

if [[ "$failed" != 0 ]]; then
    echo "asdb copy verify FAILED"
    exit 1
fi
echo "asdb copy verify PASS"
