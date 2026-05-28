/// Part writer session for HTTP ingest.
///
/// Manages a single in-progress ClickHouse MergeTree part.
/// One session = one INSERT request = one part directory.
///
/// Part naming: all_<seq>_<seq>_0
///   seq is a monotonically increasing counter (per-process, global across tables).
///
/// Write protocol (atomic, crash-safe):
///   open()   → creates parts/tmp_<seq>/  (invisible to part_scanner)
///   finish() → flushes data, then renames tmp_<seq>/ → all_<seq>_<seq>_0/
///
/// If the process crashes between open() and finish() the tmp_ directory is
/// left behind but will be ignored by part_scanner (it filters "tmp_" prefix).
///
/// Usage:
///   var sess = try CompactPartWriterSession.open(io, allocator, data_dir, db, table, schema, seq);
///   defer sess.deinit();
///   try sess.writeColumns(dec.columns);
///   try sess.finish();   // atomic rename → all_<seq>_<seq>_0

const std       = @import("std");
const schema    = @import("schema");
const ch_part   = @import("ch_part");
const row_binary_decoder = @import("row_binary_decoder");

// ── Wide-format session (legacy; kept for import-parquet --format=ch) ─────────

pub const PartWriterSession = struct {
    allocator: std.mem.Allocator,
    io:        std.Io,
    part:      ch_part.Part,
    /// Current (temporary) directory while writing.
    tmp_dir:   []u8,
    /// Final directory name (set after finish()).
    part_dir:  []u8,
    row_count: u64,

    /// Open a new wide-format part for writing.
    /// Writes to parts/tmp_<seq>/ until finish() renames it.
    pub fn open(
        allocator:    std.mem.Allocator,
        io:           std.Io,
        data_dir:     []const u8,
        db:           []const u8,
        table_name:   []const u8,
        table:        schema.Table,
        pk_col_name:  ?[]const u8,
        seq:          u64,
        codec:        u8,
    ) !PartWriterSession {
        const tmp_dir = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}/{s}/parts/tmp_{d}",
            .{ data_dir, db, table_name, seq },
        );
        errdefer allocator.free(tmp_dir);

        const part_dir = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}/{s}/parts/all_{d}_{d}_0",
            .{ data_dir, db, table_name, seq, seq },
        );
        errdefer allocator.free(part_dir);

        const part = try ch_part.Part.open(io, allocator, tmp_dir, table, pk_col_name, codec);
        return .{
            .allocator = allocator,
            .io        = io,
            .part      = part,
            .tmp_dir   = tmp_dir,
            .part_dir  = part_dir,
            .row_count = 0,
        };
    }

    pub fn deinit(self: *PartWriterSession) void {
        self.part.deinit();
        self.allocator.free(self.tmp_dir);
        self.allocator.free(self.part_dir);
    }

    /// Write all column buffers from a decoded RowBinary batch.
    pub fn writeColumns(self: *PartWriterSession, columns: []const row_binary_decoder.ColumnBuffer) !void {
        if (columns.len == 0) return;
        const n_rows = columns[0].rowCount();
        if (n_rows == 0) return;

        for (columns, 0..) |*col_buf, col_idx| {
            switch (col_buf.col.ty) {
                .text, .char => {
                    for (col_buf.str_vals.items) |s| {
                        try self.part.appendStrOne(col_idx, s);
                    }
                },
                else => {
                    try self.part.appendFixedBatch(col_idx, col_buf.fixed_vals.items);
                },
            }
        }
        self.row_count += n_rows;
    }

    /// Flush part data, then atomically rename tmp_ → all_<seq>_<seq>_0.
    pub fn finish(self: *PartWriterSession) !void {
        self.part.setRowCount(self.row_count);
        try self.part.finish();
        // Atomic rename: tmp_<seq>/ → all_<seq>_<seq>_0/
        const cwd = std.Io.Dir.cwd();
        try std.Io.Dir.rename(cwd, self.tmp_dir, cwd, self.part_dir, self.io);
    }
};

// ── Compact-format session (default for HTTP ingest) ──────────────────────────

/// Like PartWriterSession but writes a ClickHouse Compact MergeTree part.
///
/// Compact parts store all columns in a single data.bin file (interleaved by
/// granule), which is the format CH uses for freshly-inserted small parts before
/// merging.  Directly ATTACH-able to ClickHouse and readable via CompactOpenedPart.
pub const CompactPartWriterSession = struct {
    allocator: std.mem.Allocator,
    io:        std.Io,
    part:      ch_part.CompactPart,
    /// Temporary directory (parts/tmp_<seq>/).
    tmp_dir:   []u8,
    /// Final directory (parts/all_<seq>_<seq>_0/).
    part_dir:  []u8,
    row_count: u64,

    /// Open a new compact part for writing.
    /// Writes to parts/tmp_<seq>/ until finish() renames it.
    pub fn open(
        allocator:  std.mem.Allocator,
        io:         std.Io,
        data_dir:   []const u8,
        db:         []const u8,
        table_name: []const u8,
        table:      schema.Table,
        seq:        u64,
        codec:      u8,
    ) !CompactPartWriterSession {
        const tmp_dir = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}/{s}/parts/tmp_{d}",
            .{ data_dir, db, table_name, seq },
        );
        errdefer allocator.free(tmp_dir);

        const part_dir = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}/{s}/parts/all_{d}_{d}_0",
            .{ data_dir, db, table_name, seq, seq },
        );
        errdefer allocator.free(part_dir);

        const part = try ch_part.CompactPart.open(io, allocator, tmp_dir, table, codec);
        return .{
            .allocator = allocator,
            .io        = io,
            .part      = part,
            .tmp_dir   = tmp_dir,
            .part_dir  = part_dir,
            .row_count = 0,
        };
    }

    pub fn deinit(self: *CompactPartWriterSession) void {
        self.part.deinit();
        self.allocator.free(self.tmp_dir);
        self.allocator.free(self.part_dir);
    }

    /// Write all column buffers from a decoded RowBinary batch.
    pub fn writeColumns(self: *CompactPartWriterSession, columns: []const row_binary_decoder.ColumnBuffer) !void {
        if (columns.len == 0) return;
        const n_rows = columns[0].rowCount();
        if (n_rows == 0) return;

        for (columns, 0..) |*col_buf, col_idx| {
            switch (col_buf.col.ty) {
                .text, .char, .low_card => {
                    for (col_buf.str_vals.items) |s| {
                        try self.part.appendString(col_idx, s);
                    }
                },
                else => {
                    try self.part.appendFixedBatch(col_idx, col_buf.fixed_vals.items);
                },
            }
        }
        self.row_count += n_rows;
    }

    /// Flush compact part data, then atomically rename tmp_ → all_<seq>_<seq>_0.
    pub fn finish(self: *CompactPartWriterSession) !void {
        self.part.setRowCount(self.row_count);
        try self.part.finish();
        // Atomic rename: tmp_<seq>/ → all_<seq>_<seq>_0/
        const cwd = std.Io.Dir.cwd();
        try std.Io.Dir.rename(cwd, self.tmp_dir, cwd, self.part_dir, self.io);
    }
};
