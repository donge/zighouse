/// ZigHouse Compactor — background part merge process.
///
/// Runs as a separate process (zighouse compactor --data-dir=<dir>).
/// Periodically scans all tables under data_dir and merges small parts
/// into larger ones, reducing per-query scan overhead.
///
/// Merge protocol (atomic, safe with concurrent serve):
///   1. Write merged data into parts/tmp_<pid>_<ts>/
///   2. Atomic rename → parts/all_{min}_{max}_{level+1}/
///   3. Delete original parts
///
/// The serve process's part_scanner ignores "tmp_" directories and handles
/// overlap-filtered results correctly, so the window between rename and
/// delete is always safe.

const std           = @import("std");
const schema_config = @import("schema_config");
const schema_persist = @import("schema_persist");
const ch_part       = @import("ch_part");
const part_scanner  = @import("part_scanner");

/// Global counter for unique tmp dir names.
var tmp_counter: u64 = 0;

pub const Config = struct {
    data_dir:          []const u8,
    /// Seconds between full-scan passes.
    interval_s:        u64 = 30,
    /// Trigger a merge when a table has at least this many parts.
    min_parts_to_merge: usize = 8,
    /// Maximum number of parts to merge in a single operation.
    max_parts_per_merge: usize = 32,
    /// Maximum total rows in a merged part (0 = unlimited).
    max_rows_per_merge: u64 = 0,
    /// Run one pass and exit instead of looping forever.
    once: bool = false,
    /// Compression codec for merged parts: 0x82=LZ4 (default), 0x90=ZSTD.
    codec: u8 = 0x82, // METHOD_LZ4
};

/// Run the compactor loop forever.  Call from main.
pub fn run(allocator: std.mem.Allocator, io: std.Io, config: Config) !void {
    std.debug.print("compactor: starting, data_dir={s} interval={d}s min_parts={d}\n", .{
        config.data_dir,
        config.interval_s,
        config.min_parts_to_merge,
    });
    while (true) {
        runOnce(allocator, io, config) catch |err| {
            std.debug.print("compactor: runOnce error: {}\n", .{err});
        };
        if (config.once) return;
        const sleep_dur = std.Io.Clock.Duration{
            .raw = .{ .nanoseconds = @as(i96, config.interval_s) * std.time.ns_per_s },
            .clock = .awake,
        };
        std.Io.Clock.Duration.sleep(sleep_dur, io) catch {};
    }
}

/// Run one compaction pass over all tables.
pub fn runOnce(allocator: std.mem.Allocator, io: std.Io, config: Config) !void {
    const cwd = std.Io.Dir.cwd();

    var data_dir = cwd.openDir(io, config.data_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return,
        else => return err,
    };
    defer data_dir.close(io);

    var db_iter = data_dir.iterate();
    while (try db_iter.next(io)) |db_entry| {
        if (db_entry.kind != .directory) continue;
        const db = db_entry.name;

        const db_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ config.data_dir, db });
        defer allocator.free(db_path);

        var db_dir = cwd.openDir(io, db_path, .{ .iterate = true }) catch continue;
        defer db_dir.close(io);

        var tbl_iter = db_dir.iterate();
        while (try tbl_iter.next(io)) |tbl_entry| {
            if (tbl_entry.kind != .directory) continue;
            const table_name = tbl_entry.name;

            maybeCompact(allocator, io, config, db, table_name) catch |err| {
                std.debug.print("compactor: {s}.{s}: error {}\n", .{ db, table_name, err });
            };
        }
    }
}

/// Check and compact one table if it has enough parts.
fn maybeCompact(
    allocator:  std.mem.Allocator,
    io:         std.Io,
    config:     Config,
    db:         []const u8,
    table_name: []const u8,
) !void {
    var parts = try part_scanner.scan(allocator, io, config.data_dir, db, table_name);
    defer parts.deinit();

    const ms = parts.metas();
    if (ms.len < config.min_parts_to_merge) return;

    // Load schema so we know column types.
    var schema_cfg = (try schema_persist.load(allocator, io, config.data_dir, db, table_name)) orelse {
        std.debug.print("compactor: {s}.{s}: no schema.json, skipping\n", .{ db, table_name });
        return;
    };
    defer schema_cfg.deinit();

    const entry = schema_cfg.find(db, table_name) orelse return;

    // Select candidates: the first min(len, max_parts_per_merge) parts.
    // Optionally also enforce max_rows_per_merge.
    var n_candidates = @min(ms.len, config.max_parts_per_merge);
    if (config.max_rows_per_merge > 0) {
        // Count rows and trim candidates to stay under the limit.
        var total_rows: u64 = 0;
        var ci: usize = 0;
        while (ci < n_candidates) : (ci += 1) {
            const rows = readRowCount(io, allocator, ms[ci].full_path) catch break;
            total_rows += rows;
            if (total_rows > config.max_rows_per_merge) {
                n_candidates = if (ci == 0) 1 else ci;
                break;
            }
        }
    }
    if (n_candidates < 2) return; // nothing to merge

    const candidates = ms[0..n_candidates];
    try mergeParts(allocator, io, config.data_dir, db, table_name, entry.table, candidates, config.codec);

    std.debug.print("compactor: {s}.{s}: merged {d} parts into all_{d}_{d}_{d}\n", .{
        db, table_name, n_candidates,
        candidates[0].min_seq,
        candidates[n_candidates - 1].max_seq,
        maxLevel(candidates) + 1,
    });
}

/// Read row count from a part's count.txt.
fn readRowCount(io: std.Io, allocator: std.mem.Allocator, part_dir: []const u8) !u64 {
    const path = try std.fmt.allocPrint(allocator, "{s}/count.txt", .{part_dir});
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(64));
    defer allocator.free(bytes);
    const trimmed = std.mem.trim(u8, bytes, " \t\r\n");
    return std.fmt.parseInt(u64, trimmed, 10);
}

fn maxLevel(candidates: []const part_scanner.PartMeta) u32 {
    var m: u32 = 0;
    for (candidates) |c| if (c.level > m) { m = c.level; };
    return m;
}

/// Merge `candidates` parts into a new compact part, then atomically
/// replace them.
fn mergeParts(
    allocator:  std.mem.Allocator,
    io:         std.Io,
    data_dir:   []const u8,
    db:         []const u8,
    table_name: []const u8,
    table:      @import("schema").Table,
    candidates: []const part_scanner.PartMeta,
    codec:      u8,
) !void {
    const cwd = std.Io.Dir.cwd();

    // ── 1. Choose paths ───────────────────────────────────────────────────────
    const min_seq   = candidates[0].min_seq;
    const max_seq   = candidates[candidates.len - 1].max_seq;
    const new_level = maxLevel(candidates) + 1;

    const tmp_dir = try std.fmt.allocPrint(
        allocator,
        "{s}/{s}/{s}/parts/tmp_{d}_{d}",
        .{ data_dir, db, table_name, std.c.getpid(), @atomicRmw(u64, &tmp_counter, .Add, 1, .monotonic) },
    );
    defer allocator.free(tmp_dir);

    const final_dir = try std.fmt.allocPrint(
        allocator,
        "{s}/{s}/{s}/parts/all_{d}_{d}_{d}",
        .{ data_dir, db, table_name, min_seq, max_seq, new_level },
    );
    defer allocator.free(final_dir);

    // ── 2. Open writer into tmp_dir ───────────────────────────────────────────
    var writer = try ch_part.CompactPart.open(io, allocator, tmp_dir, table, codec);
    defer writer.deinit();

    var total_rows: u64 = 0;

    // ── 3. Stream all candidate parts into the writer ─────────────────────────
    for (candidates) |cand| {
        var opened = try ch_part.OpenedPartAny.open(io, allocator, cand.full_path, table);
        defer opened.deinit();

        const n_rows = opened.rowCount();
        if (n_rows == 0) continue;
        total_rows += n_rows;

        const n_cols = table.columns.len;

        // We need to feed rows into CompactPart column by column.
        // CompactPart accumulates all data in memory, so we can feed
        // each column in full before moving to the next.
        for (0..n_cols) |col_idx| {
            const col = table.columns[col_idx];
            var cr = try opened.columnReader(col_idx);
            defer cr.deinit();

            switch (col.ty) {
                .text, .char => {
                    // Read all strings and append.
                    const ReadCtx = struct {
                        part_writer: *ch_part.CompactPart,
                        ci: usize,
                        err: ?anyerror = null,
                    };
                    var ctx = ReadCtx{ .part_writer = &writer, .ci = col_idx };
                    _ = try cr.readStrings(n_rows, &ctx, struct {
                        fn cb(c: *ReadCtx, s: []const u8) anyerror!void {
                            c.part_writer.appendString(c.ci, s) catch |e| {
                                c.err = e;
                                return e;
                            };
                        }
                    }.cb);
                    if (ctx.err) |e| return e;
                },
                else => {
                    // Read fixed values in chunks.
                    var buf: [4096]i64 = undefined;
                    var rows_left = n_rows;
                    while (rows_left > 0) {
                        const chunk = @min(rows_left, buf.len);
                        const got = try cr.readFixed(buf[0..chunk]);
                        if (got == 0) break;
                        try writer.appendFixedBatch(col_idx, buf[0..got]);
                        rows_left -= got;
                    }
                },
            }
        }
    }

    // ── 4. Finish (flush to tmp_dir) ──────────────────────────────────────────
    writer.setRowCount(total_rows);
    try writer.finish();

    // ── 5. Atomic rename tmp → final ─────────────────────────────────────────
    try std.Io.Dir.rename(cwd, tmp_dir, cwd, final_dir, io);

    // ── 6. Delete original parts ──────────────────────────────────────────────
    for (candidates) |cand| {
        cwd.deleteTree(io, cand.full_path) catch |err| {
            std.debug.print("compactor: warning: could not delete {s}: {}\n", .{ cand.full_path, err });
        };
    }
}
