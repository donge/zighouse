/// ResultSet — the final typed result of a query execution.
///
/// A ResultSet is the materialized output of a Pipeline. It owns its memory
/// via an ArenaAllocator and is the handoff point between the execution engine
/// and the serializer layer (e.g. ClickHouse Native block encoder).
///
/// Relationship to DataChunk:
///   - DataChunk: ephemeral, flows through the pipeline, may be overwritten.
///   - ResultSet: final, stable, owned. The serializer reads from ResultSet.
///
/// The ResultSet stores data in the same column-major layout as DataChunk,
/// but all chunks have been concatenated into a single flat column array.
const std = @import("std");
const types = @import("types.zig");
const chunk = @import("chunk.zig");

pub const ColumnType = types.ColumnType;
pub const Value      = types.Value;
pub const Column     = chunk.Column;
pub const DataChunk  = chunk.DataChunk;

// ── ColMeta ───────────────────────────────────────────────────────────────────

/// Metadata for one output column of the result.
pub const ColMeta = struct {
    /// Column name as it appears in the response (may be an alias).
    name:      []const u8,
    /// The definitive type — set by the executor, not guessed by the serializer.
    col_type:  ColumnType,
    /// Optional wire type override (e.g. "UInt16", "UInt32") used by the
    /// serializer to emit the exact ClickHouse type name instead of the
    /// canonical col_type name. Null means use col_type's default name.
    ch_type:   ?[]const u8 = null,
};

// ── ResultSet ─────────────────────────────────────────────────────────────────

/// The complete, typed result of a query.
///
/// Layout:
///   - `metas[i]`          — metadata for column i
///   - `columns[i].data`   — the typed slice for column i (len == num_rows)
///   - `columns[i].null_mask` — NULL bitmap for column i
///
/// Invariants:
///   - metas.len == columns.len
///   - For all i: columns[i].len == num_rows
///   - columns[i].data tag matches metas[i].col_type
pub const ResultSet = struct {
    metas:    []ColMeta,
    columns:  []Column,
    num_rows: usize,
    arena:    std.heap.ArenaAllocator,

    /// Free all memory owned by this ResultSet.
    pub fn deinit(self: *ResultSet) void {
        self.arena.deinit();
    }

    pub fn allocator(self: *ResultSet) std.mem.Allocator {
        return self.arena.allocator();
    }

    /// Return the column index for a given name, or null.
    pub fn findColumn(self: ResultSet, name: []const u8) ?usize {
        for (self.metas, 0..) |m, i| {
            if (std.mem.eql(u8, m.name, name)) return i;
        }
        return null;
    }

    /// Read one value from column `col_idx`, row `row_idx`.
    /// Returns null if the cell is NULL.
    pub fn get(self: ResultSet, col_idx: usize, row_idx: usize) ?Value {
        const col = self.columns[col_idx];
        if (chunk.isNull(col.null_mask, row_idx)) return null;
        return col.data.get(row_idx);
    }

    /// Number of columns.
    pub fn numCols(self: ResultSet) usize {
        return self.metas.len;
    }
};

// ── Builder ───────────────────────────────────────────────────────────────────

/// Accumulates DataChunks produced by the pipeline and materialises them
/// into a single ResultSet.
///
/// Usage:
///   var sink = ResultSink.init(child_alloc);
///   while (pipeline.nextChunk(&c)) try sink.consume(c);
///   const rs = try sink.finish();
pub const ResultSink = struct {
    /// Incoming chunks buffered until finish().
    chunks:   std.ArrayListUnmanaged(DataChunk),
    /// Total rows seen so far.
    total_rows: usize,
    /// Column metadata inferred from the first chunk.
    metas:    std.ArrayListUnmanaged(ColMeta),
    /// The result arena (outlives this struct after finish() is called).
    result_arena: std.heap.ArenaAllocator,
    /// Parent allocator used to init result_arena.
    parent_alloc: std.mem.Allocator,

    pub fn init(parent_alloc: std.mem.Allocator) ResultSink {
        return .{
            .chunks       = .empty,
            .total_rows   = 0,
            .metas        = .empty,
            .result_arena = std.heap.ArenaAllocator.init(parent_alloc),
            .parent_alloc = parent_alloc,
        };
    }

    /// Hand a chunk to the sink. The sink takes ownership of the chunk's arena.
    pub fn consume(self: *ResultSink, c: DataChunk) !void {
        if (self.metas.items.len == 0 and c.columns.len > 0) {
            // Record column metadata from the first non-empty chunk.
            const ra = self.result_arena.allocator();
            for (c.columns) |col| {
                try self.metas.append(self.parent_alloc, .{
                    .name     = try ra.dupe(u8, col.name),
                    .col_type = @as(ColumnType, col.data),
                });
            }
        }
        self.total_rows += c.num_rows;
        try self.chunks.append(self.parent_alloc, c);
    }

    /// Materialise all buffered chunks into a single ResultSet.
    /// The caller owns the returned ResultSet and must call rs.deinit().
    pub fn finish(self: *ResultSink) !ResultSet {
        defer {
            // Free the chunk list (the chunks themselves were deinit'd during
            // concatenation below, or are empty — arena owns data).
            self.chunks.deinit(self.parent_alloc);
            self.metas.deinit(self.parent_alloc);
        }

        const num_rows = self.total_rows;
        const num_cols = self.metas.items.len;
        const ra       = self.result_arena.allocator();

        if (num_cols == 0 or num_rows == 0) {
            // Return an empty-row ResultSet, but preserve column metadata
            // so callers can still inspect column names and types.
            const out_metas: []ColMeta = if (num_cols > 0)
                try ra.dupe(ColMeta, self.metas.items)
            else
                try ra.alloc(ColMeta, 0);
            // Allocate a column entry per meta so that len(metas) == len(columns).
            // Each column has an empty data slice (0 rows).
            const out_cols = try ra.alloc(Column, num_cols);
            for (out_cols, out_metas) |*c, meta| {
                c.* = .{
                    .name      = meta.name,
                    .null_mask = &.{},
                    .len       = 0,
                    .data      = switch (meta.col_type) {
                        .bool_u8       => .{ .bool_u8       = &.{} },
                        .int64         => .{ .int64         = &.{} },
                        .uint64        => .{ .uint64        = &.{} },
                        .float64       => .{ .float64       = &.{} },
                        .string        => .{ .string        = &.{} },
                        .date_u16      => .{ .date_u16      = &.{} },
                        .datetime64_ms => .{ .datetime64_ms = &.{} },
                        .array_string  => .{ .array_string  = &.{} },
                    },
                };
            }
            return ResultSet{
                .metas    = out_metas,
                .columns  = out_cols,
                .num_rows = 0,
                .arena    = self.result_arena,
            };
        }

        // Allocate flat column arrays in the result arena.
        const out_metas = try ra.dupe(ColMeta, self.metas.items);
        const out_cols  = try ra.alloc(Column, num_cols);

        for (out_cols, out_metas, 0..) |*out_col, meta, _ci| {
            _ = _ci;
            const nw = chunk.nullMaskWords(num_rows);
            const null_mask = try ra.alloc(u64, nw);
            @memset(null_mask, 0);

            const data: chunk.ColumnData = switch (meta.col_type) {
                .bool_u8       => .{ .bool_u8       = try ra.alloc(u8,           num_rows) },
                .int64         => .{ .int64         = try ra.alloc(i64,          num_rows) },
                .uint64        => .{ .uint64        = try ra.alloc(u64,          num_rows) },
                .float64       => .{ .float64       = try ra.alloc(f64,          num_rows) },
                .date_u16      => .{ .date_u16      = try ra.alloc(u16,          num_rows) },
                .datetime64_ms => .{ .datetime64_ms = try ra.alloc(i64,          num_rows) },
                .string        => .{ .string        = try ra.alloc([]const u8,   num_rows) },
                .array_string  => .{ .array_string  = try ra.alloc([][]const u8, num_rows) },
            };
            out_col.* = .{
                .name      = meta.name,
                .data      = data,
                .null_mask = null_mask,
                .len       = num_rows,
            };
        }

        // Copy rows from each buffered chunk.
        var row_offset: usize = 0;
        for (self.chunks.items) |*c2| {
            defer c2.deinit();
            const n = c2.num_rows;
            for (c2.columns, 0..) |src_col, ci| {
                const dst = &out_cols[ci];
                // Copy null bits
                for (0..n) |r| {
                    if (chunk.isNull(src_col.null_mask, r)) {
                        chunk.setNull(dst.null_mask, row_offset + r);
                    }
                }
                // Copy data
                try copyColumnSlice(dst.data, row_offset, src_col.data, n, ra);
            }
            row_offset += n;
        }

        return ResultSet{
            .metas    = out_metas,
            .columns  = out_cols,
            .num_rows = num_rows,
            .arena    = self.result_arena,
        };
    }
};

/// Copy `n` elements from `src` starting at offset 0 into `dst` at
/// `dst_offset`. String slices are re-duped into `ra` so they survive
/// the source chunk's deinit.
fn copyColumnSlice(
    dst: chunk.ColumnData,
    dst_offset: usize,
    src: chunk.ColumnData,
    n: usize,
    ra: std.mem.Allocator,
) !void {
    switch (dst) {
        .bool_u8       => |d| @memcpy(d[dst_offset..][0..n], src.bool_u8[0..n]),
        .int64         => |d| @memcpy(d[dst_offset..][0..n], src.int64[0..n]),
        .uint64        => |d| @memcpy(d[dst_offset..][0..n], src.uint64[0..n]),
        .float64       => |d| @memcpy(d[dst_offset..][0..n], src.float64[0..n]),
        .date_u16      => |d| @memcpy(d[dst_offset..][0..n], src.date_u16[0..n]),
        .datetime64_ms => |d| @memcpy(d[dst_offset..][0..n], src.datetime64_ms[0..n]),
        .string        => |d| {
            for (0..n) |r| {
                d[dst_offset + r] = try ra.dupe(u8, src.string[r]);
            }
        },
        .array_string  => |d| {
            for (0..n) |r| {
                const src_arr = src.array_string[r];
                const dst_arr = try ra.alloc([]const u8, src_arr.len);
                for (src_arr, 0..) |s, si| {
                    dst_arr[si] = try ra.dupe(u8, s);
                }
                d[dst_offset + r] = dst_arr;
            }
        },
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "ResultSink empty" {
    var sink = ResultSink.init(std.testing.allocator);
    var rs = try sink.finish();
    defer rs.deinit();
    try std.testing.expectEqual(@as(usize, 0), rs.num_rows);
}

test "ResultSink single chunk" {
    const alloc = std.testing.allocator;
    var sink = ResultSink.init(alloc);

    // Build a small chunk: 2 rows, column "x" Int64
    var b = chunk.ChunkBuilder.init(alloc, 2);
    const ci = try b.addColumn("x", .int64);
    b.chunk.columns[ci].data.int64[0] = 100;
    b.chunk.columns[ci].data.int64[1] = 200;
    try sink.consume(b.finish());

    var rs = try sink.finish();
    defer rs.deinit();

    try std.testing.expectEqual(@as(usize, 2), rs.num_rows);
    try std.testing.expectEqual(@as(usize, 1), rs.numCols());
    try std.testing.expectEqualStrings("x", rs.metas[0].name);
    try std.testing.expectEqual(Value{ .int64 = 100 }, rs.get(0, 0).?);
    try std.testing.expectEqual(Value{ .int64 = 200 }, rs.get(0, 1).?);
}
