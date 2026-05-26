/// Part writer session for HTTP ingest.
///
/// Manages a single in-progress ClickHouse MergeTree part.
/// One session = one INSERT request = one part directory.
///
/// Part naming: all_<seq>_<seq>_0
///   seq is a monotonically increasing counter per (database, table).
///
/// Usage:
///   var sess = try PartWriterSession.open(io, allocator, data_dir, db, table, schema, pk_col, seq);
///   defer sess.deinit();
///   try sess.writeColumns(dec.columns);  // from RowBinaryDecoder
///   try sess.finish();                    // flushes part, returns part_dir path

const std = @import("std");
const schema = @import("schema");
const ch_part = @import("ch_part");
const row_binary_decoder = @import("row_binary_decoder");

pub const PartWriterSession = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    part: ch_part.Part,
    part_dir: []u8,
    row_count: u64,

    /// Open a new part for writing.
    ///
    /// part_dir = <data_dir>/<db>/<table>/parts/all_<seq>_<seq>_0
    pub fn open(
        allocator: std.mem.Allocator,
        io: std.Io,
        data_dir: []const u8,
        db: []const u8,
        table_name: []const u8,
        table: schema.Table,
        pk_col_name: ?[]const u8,
        seq: u64,
    ) !PartWriterSession {
        const part_dir = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}/{s}/parts/all_{d}_{d}_0",
            .{ data_dir, db, table_name, seq, seq },
        );
        errdefer allocator.free(part_dir);

        const part = try ch_part.Part.open(io, allocator, part_dir, table, pk_col_name);
        return .{
            .allocator = allocator,
            .io = io,
            .part = part,
            .part_dir = part_dir,
            .row_count = 0,
        };
    }

    pub fn deinit(self: *PartWriterSession) void {
        self.part.deinit();
        self.allocator.free(self.part_dir);
    }

    /// Write all column buffers from a decoded RowBinary batch.
    /// columns must match the schema order and types of the Part.
    pub fn writeColumns(self: *PartWriterSession, columns: []const row_binary_decoder.ColumnBuffer) !void {
        if (columns.len == 0) return;

        // Number of rows in this batch
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

    /// Finalise and flush the part to disk.
    /// After finish() the part is complete and can be used by ClickHouse ATTACH.
    pub fn finish(self: *PartWriterSession) !void {
        self.part.setRowCount(self.row_count);
        try self.part.finish();
    }
};

/// Like PartWriterSession but writes a ClickHouse Compact MergeTree part.
///
/// Compact parts store all columns in a single data.bin file (interleaved by granule),
/// which is the format CH uses for freshly-inserted small parts before merging.
/// The resulting part is directly ATTACH-able to CH and readable via CompactOpenedPart.
pub const CompactPartWriterSession = struct {
    allocator: std.mem.Allocator,
    io:        std.Io,
    part:      ch_part.CompactPart,
    part_dir:  []u8,
    row_count: u64,

    /// Open a new compact part for writing.
    pub fn open(
        allocator:  std.mem.Allocator,
        io:         std.Io,
        data_dir:   []const u8,
        db:         []const u8,
        table_name: []const u8,
        table:      schema.Table,
        seq:        u64,
    ) !CompactPartWriterSession {
        const part_dir = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}/{s}/parts/all_{d}_{d}_0",
            .{ data_dir, db, table_name, seq, seq },
        );
        errdefer allocator.free(part_dir);

        const part = try ch_part.CompactPart.open(io, allocator, part_dir, table);
        return .{
            .allocator = allocator,
            .io        = io,
            .part      = part,
            .part_dir  = part_dir,
            .row_count = 0,
        };
    }

    pub fn deinit(self: *CompactPartWriterSession) void {
        self.part.deinit();
        self.allocator.free(self.part_dir);
    }

    /// Write all column buffers from a decoded RowBinary batch.
    pub fn writeColumns(self: *CompactPartWriterSession, columns: []const row_binary_decoder.ColumnBuffer) !void {
        if (columns.len == 0) return;
        const n_rows = columns[0].rowCount();
        if (n_rows == 0) return;

        for (columns, 0..) |*col_buf, col_idx| {
            switch (col_buf.col.ty) {
                .text, .char => {
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

    /// Finalise and flush the compact part to disk.
    pub fn finish(self: *CompactPartWriterSession) !void {
        self.part.setRowCount(self.row_count);
        try self.part.finish();
    }
};
