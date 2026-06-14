# Usage

## Start the Server

```bash
zighouse serve /var/lib/zighouse
```

Or with a custom port:

```bash
zighouse serve /var/lib/zighouse --port=19902
```

The server listens on:
- **HTTP**: port `8123` (or `port + 1` for non-default ports)
- **TCP** (ClickHouse Native protocol): port `9000`

## Import Data

Import a Parquet file:

```bash
zighouse import hits.parquet --table=hits
```

Specify database and primary key:

```bash
zighouse import events.parquet --db=analytics --table=events --pk=timestamp
```

Import to a ClickHouse-compatible format:

```bash
zighouse import data.parquet --format=ch-compact --table=metrics
```

## Run Queries

### Via curl (HTTP API)

```bash
curl 'http://localhost:8123/?query=SELECT+count(*)+FROM+hits'
```

### Via the query command (generic store)

```bash
zighouse query ./store hits 'SELECT count(*) FROM hits'
```

## Run Benchmarks

Run all 43 ClickBench queries:

```bash
zighouse bench --store=./store hits clickbench-submit/zighouse/queries.sql
```

Run a single query:

```bash
zighouse bench --store=./store hits clickbench-submit/zighouse/queries.sql --query=9
```

Run a range:

```bash
zighouse bench --store=./store hits clickbench-submit/zighouse/queries.sql --from=1 --limit=5
```

## Integration with vcontrol

zighouse can be used as the backend database for
[vcontrol](https://github.com/donge/vcontrol) — a VPN detection control plane.

```bash
# Start zighouse on the port vcontrol expects
zighouse serve /var/lib/zighouse --port=19902

# vcontrol connects via:
#   DSN: http://default:@127.0.0.1:19903/vprobe
```

All 111 vcontrol compatibility tests pass.

## HTTP API

### Health check

```bash
curl http://localhost:8123/ping
# → Ok.
```

### SELECT

```bash
curl -G 'http://localhost:8123/' --data-urlencode 'query=SELECT protocol, confidence FROM vprobe.detect_events WHERE event_type = '"'"'detect'"'"' LIMIT 5'
```

### INSERT (RowBinary)

```bash
curl -X POST 'http://localhost:8123/?query=INSERT+INTO+vprobe.detect_events+FORMAT+RowBinaryWithNamesAndTypes' \
  --data-binary @data.bin
```

### DESCRIBE TABLE

```bash
curl -G 'http://localhost:8123/' --data-urlencode 'query=DESCRIBE TABLE vprobe.detect_events'
```

## Data Maintenance

### Compact parts (merge small parts)

```bash
zighouse compact --data-dir=/var/lib/zighouse --once
```

Run the compactor continuously:

```bash
zighouse compact --data-dir=/var/lib/zighouse --interval=3600
```

### View store info

```bash
zighouse info /var/lib/zighouse
```

### Inspect Parquet metadata

```bash
zighouse inspect data.parquet
```
