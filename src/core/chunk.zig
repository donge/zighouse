/// Column-oriented DataChunk — the unit of data flowing through the pipeline.
///
/// Design goals:
///   1. Cache-friendly: data stored as typed slices ([]i64, []f64, ...) not
///      row-of-structs. Each column's values are contiguous in memory.
///   2. Type-safe: ColumnData is a tagged union — no runtime type guessing.
///   3. NULL-efficient: a compact bit-mask (1 bit per row) tracks nulls
///      independently of the value array.
///   4. Arena-backed: the chunk owns an ArenaAllocator. Caller calls
///      chunk.deinit() once; all column buffers are freed in one shot.
///
/// Chunk size: CHUNK_SIZE rows per chunk (default 2048). The last chunk in a
/// scan may contain fewer rows (num_rows < CHUNK_SIZE).
const std = @import("std");
const types = @import("types.zig");

pub const ColumnType = types.ColumnType;
pub const Value = types.Value;

/// Default number of rows per DataChunk. Tuned for L1/L2 cache residency on
/// typical OLAP workloads. Can be overridden at comptime via build options.
pub const CHUNK_SIZE: usize = 2048;

// ── NULL bitmap helpers ───────────────────────────────────────────────────────

/// Number of u64 words needed for a null_mask covering `n` rows.
pub fn nullMaskWords(n: usize) usize {
    return (n + 63) / 64;
}

/// Test whether row `i` is NULL in a null_mask.
pub fn isNull(null_mask: []const u64, i: usize) bool {
    return (null_mask[i / 64] >> @intCast(i % 64)) & 1 == 1;
}

/// Mark row `i` as NULL.
pub fn setNull(null_mask: []u64, i: usize) void {
    null_mask[i / 64] |= @as(u64, 1) << @intCast(i % 64);
}

/// Mark row `i` as non-NULL.
pub fn clearNull(null_mask: []u64, i: usize) void {
    null_mask[i / 64] &= ~(@as(u64, 1) << @intCast(i % 64));
}

/// True if all rows in `null_mask` are non-NULL (fast check).
pub fn allNonNull(null_mask: []const u64) bool {
    for (null_mask) |w| if (w != 0) return false;
    return true;
}

// ── ColumnData ────────────────────────────────────────────────────────────────

/// The actual data buffer for one column in a chunk.
/// Each variant holds a slice of exactly `Column.len` elements.
///
/// String slices point into the chunk's ArenaAllocator; they are valid
/// for the lifetime of the owning DataChunk.
pub const ColumnData = union(ColumnType) {
    bool_u8: []u8,
    int64: []i64,
    uint64: []u64,
    float64: []f64,
    date_u16: []u16,
    datetime64_ms: []i64,
    string: [][]const u8,
    array_string: [][][]const u8,

    /// Return the number of elements stored.
    pub fn len(self: ColumnData) usize {
        return switch (self) {
            inline else => |s| s.len,
        };
    }

    /// Read the value at index `i` as a tagged Value.
    pub fn get(self: ColumnData, i: usize) Value {
        return switch (self) {
            .bool_u8 => |s| .{ .bool_u8 = s[i] },
            .int64 => |s| .{ .int64 = s[i] },
            .uint64 => |s| .{ .uint64 = s[i] },
            .float64 => |s| .{ .float64 = s[i] },
            .date_u16 => |s| .{ .date_u16 = s[i] },
            .datetime64_ms => |s| .{ .datetime64_ms = s[i] },
            .string => |s| .{ .string = s[i] },
            .array_string => |s| .{ .array_string = s[i] },
        };
    }

    /// Write the value `v` at index `i`. Panics on type mismatch.
    pub fn set(self: *ColumnData, i: usize, v: Value) void {
        switch (self.*) {
            .bool_u8 => |s| s[i] = v.bool_u8,
            .int64 => |s| s[i] = v.int64,
            .uint64 => |s| s[i] = v.uint64,
            .float64 => |s| s[i] = v.float64,
            .date_u16 => |s| s[i] = v.date_u16,
            .datetime64_ms => |s| s[i] = v.datetime64_ms,
            .string => |s| s[i] = v.string,
            .array_string => |s| s[i] = v.array_string,
        }
    }

    /// Copy one value within the same typed column buffer.
    pub fn copyWithin(self: ColumnData, from: usize, to: usize) void {
        switch (self) {
            inline else => |s| s[to] = s[from],
        }
    }
};

// ── SelectionVector ──────────────────────────────────────────────────────────

/// Row ids that survived a filter. Keeping this as a first-class value lets
/// filters hand downstream operators a compact row set without immediately
/// materialising a physically compacted chunk.
pub const SelectionVector = struct {
    indices: []u32,
    len: usize,

    pub fn init(buffer: []u32) SelectionVector {
        return .{ .indices = buffer, .len = 0 };
    }

    pub fn append(self: *SelectionVector, row_idx: usize) void {
        std.debug.assert(self.len < self.indices.len);
        self.indices[self.len] = @intCast(row_idx);
        self.len += 1;
    }

    pub fn slice(self: SelectionVector) []const u32 {
        return self.indices[0..self.len];
    }

    pub fn isEmpty(self: SelectionVector) bool {
        return self.len == 0;
    }

    pub fn isFull(self: SelectionVector, n: usize) bool {
        if (self.len != n) return false;
        for (self.slice(), 0..) |idx, i| {
            if (idx != i) return false;
        }
        return true;
    }

    pub fn rowAt(self: SelectionVector, logical_row: usize) usize {
        std.debug.assert(logical_row < self.len);
        return @intCast(self.indices[logical_row]);
    }

    pub fn full(buffer: []u32, n: usize) SelectionVector {
        std.debug.assert(buffer.len >= n);
        var sel = SelectionVector.init(buffer);
        for (0..n) |i| sel.append(i);
        return sel;
    }

    pub fn fromI16Mask(buffer: []u32, mask: []const i16) SelectionVector {
        std.debug.assert(buffer.len >= mask.len);
        var sel = SelectionVector.init(buffer);
        for (mask, 0..) |m, i| {
            if (m != 0) sel.append(i);
        }
        return sel;
    }
};

/// A logical view over a DataChunk plus a row selection.
///
/// This is the migration bridge toward selection-aware operators: filters can
/// hand downstream code a selected view without requiring immediate physical
/// compaction. Existing operators may still call `materialize()` until they are
/// taught to consume row ids directly.
pub const SelectedChunk = struct {
    chunk: *DataChunk,
    selection: SelectionVector,

    pub fn len(self: SelectedChunk) usize {
        return self.selection.len;
    }

    pub fn isEmpty(self: SelectedChunk) bool {
        return self.selection.isEmpty();
    }

    pub fn physicalRow(self: SelectedChunk, logical_row: usize) usize {
        return self.selection.rowAt(logical_row);
    }

    pub fn getOpt(self: SelectedChunk, col_idx: usize, logical_row: usize) ?Value {
        return self.chunk.columns[col_idx].getOpt(self.physicalRow(logical_row));
    }

    pub inline fn fillRow(self: SelectedChunk, logical_row: usize, out: []?Value) void {
        self.chunk.fillRow(self.physicalRow(logical_row), out);
    }

    pub fn readRow(self: SelectedChunk, logical_row: usize, out_alloc: std.mem.Allocator) ![]?Value {
        return self.chunk.readRow(self.physicalRow(logical_row), out_alloc);
    }

    pub fn materialize(self: SelectedChunk) void {
        self.chunk.compactSelection(self.selection);
    }
};

// ── Column ────────────────────────────────────────────────────────────────────

/// One column in a DataChunk.
pub const Column = struct {
    /// Column name (or alias) — slice into the chunk's arena.
    name: []const u8,
    /// The column type and data buffer.
    data: ColumnData,
    /// NULL bitmap: bit[i] = 1 means row i is NULL.
    /// Length: nullMaskWords(len) u64 words.
    null_mask: []u64,
    /// Number of rows in this column (== DataChunk.num_rows).
    len: usize,
    /// True if this column was pruned (not needed by query). Its data
    /// buffer may point to a shared read-only zero buffer; copyRow must
    /// not write to it.
    pruned: bool = false,

    /// Read value at row `i`, or null if masked.
    pub fn getOpt(self: Column, i: usize) ?Value {
        if (isNull(self.null_mask, i)) return null;
        return self.data.get(i);
    }

    /// True if row `i` is NULL.
    pub fn isRowNull(self: Column, i: usize) bool {
        return isNull(self.null_mask, i);
    }
};

// ── DataChunk ─────────────────────────────────────────────────────────────────

/// A batch of `num_rows` rows stored in column-major order.
///
/// Allocation strategy:
///   All column buffers (data slices, null_masks, string slices, name strings)
///   are allocated from `arena`. Call `deinit()` to free everything at once.
pub const DataChunk = struct {
    columns: []Column, // mutable slice; owned by arena
    num_rows: usize,
    arena: std.heap.ArenaAllocator,

    /// Release all memory owned by this chunk.
    pub fn deinit(self: *DataChunk) void {
        self.arena.deinit();
    }

    /// Allocator backed by this chunk's arena (for building column buffers).
    pub fn allocator(self: *DataChunk) std.mem.Allocator {
        return self.arena.allocator();
    }

    /// Return column index by name, or null if not found.
    pub fn findColumn(self: DataChunk, name: []const u8) ?usize {
        for (self.columns, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, name)) return i;
        }
        return null;
    }

    /// Return a row as a slice of Values (allocated from `out_alloc`).
    /// Null columns yield the zero value for their type (callers check
    /// Column.isRowNull if they need to distinguish NULL from zero).
    pub fn readRow(self: DataChunk, row: usize, out_alloc: std.mem.Allocator) ![]?Value {
        const vals = try out_alloc.alloc(?Value, self.columns.len);
        self.fillRow(row, vals);
        return vals;
    }

    pub inline fn fillRow(self: DataChunk, row: usize, out: []?Value) void {
        std.debug.assert(out.len >= self.columns.len);
        for (self.columns, 0..) |col, ci| {
            out[ci] = if (col.isRowNull(row)) null else col.data.get(row);
        }
    }

    /// Physically compact this chunk in-place according to `selection`.
    /// This is the compatibility bridge while operators are migrated to
    /// consume SelectionVector directly. Pruned columns are skipped because
    /// they may point at shared read-only zero buffers.
    pub fn compactSelection(self: *DataChunk, selection: SelectionVector) void {
        const rows = selection.slice();
        for (rows, 0..) |src_u32, dst| {
            const src: usize = @intCast(src_u32);
            if (src == dst) continue;
            for (self.columns) |*col| {
                if (col.pruned) continue;
                col.data.copyWithin(src, dst);
                if (isNull(col.null_mask, src)) {
                    setNull(col.null_mask, dst);
                } else {
                    clearNull(col.null_mask, dst);
                }
            }
        }
        self.num_rows = rows.len;
        for (self.columns) |*col| col.len = rows.len;
    }

    pub fn selected(self: *DataChunk, selection: SelectionVector) SelectedChunk {
        return .{ .chunk = self, .selection = selection };
    }
};

/// Return the zero/empty value for a given ColumnData's type.
fn zeroValue(data: ColumnData) Value {
    return switch (data) {
        .bool_u8 => .{ .bool_u8 = 0 },
        .int64 => .{ .int64 = 0 },
        .uint64 => .{ .uint64 = 0 },
        .float64 => .{ .float64 = 0.0 },
        .date_u16 => .{ .date_u16 = 0 },
        .datetime64_ms => .{ .datetime64_ms = 0 },
        .string => .{ .string = "" },
        .array_string => .{ .array_string = &.{} },
    };
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Helper for constructing a DataChunk incrementally.
/// Typical usage (e.g. in a TableScan or test):
///
///   var b = ChunkBuilder.init(child_alloc, num_rows);
///   try b.addColumn("ts",    .datetime64_ms);
///   try b.addColumn("value", .float64);
///   // ... fill b.chunk.columns[i].data ...
///   const chunk = b.finish();
pub const ChunkBuilder = struct {
    chunk: DataChunk,

    pub fn init(child_alloc: std.mem.Allocator, num_rows: usize) ChunkBuilder {
        return .{
            .chunk = .{
                .columns = &.{},
                .num_rows = num_rows,
                .arena = std.heap.ArenaAllocator.init(child_alloc),
            },
        };
    }

    /// Allocate a new column buffer of the given type and append it.
    /// Returns the index of the new column. Use `self.chunk.columns[idx]`
    /// to fill the data buffer.
    pub fn addColumn(
        self: *ChunkBuilder,
        name: []const u8,
        col_type: ColumnType,
    ) !usize {
        const alloc = self.chunk.arena.allocator();
        const n = self.chunk.num_rows;
        const nw = nullMaskWords(n);

        const null_mask = try alloc.alloc(u64, nw);
        @memset(null_mask, 0); // all non-NULL by default

        const name_copy = try alloc.dupe(u8, name);

        const data: ColumnData = switch (col_type) {
            .bool_u8 => .{ .bool_u8 = try alloc.alloc(u8, n) },
            .int64 => .{ .int64 = try alloc.alloc(i64, n) },
            .uint64 => .{ .uint64 = try alloc.alloc(u64, n) },
            .float64 => .{ .float64 = try alloc.alloc(f64, n) },
            .date_u16 => .{ .date_u16 = try alloc.alloc(u16, n) },
            .datetime64_ms => .{ .datetime64_ms = try alloc.alloc(i64, n) },
            .string => .{ .string = try alloc.alloc([]const u8, n) },
            .array_string => .{ .array_string = try alloc.alloc([][]const u8, n) },
        };

        // Grow columns slice by 1. Both old and new slices come from the
        // same arena so realloc is safe and no @constCast is needed.
        const old_len = self.chunk.columns.len;
        const new_cols = try alloc.realloc(self.chunk.columns, old_len + 1);
        new_cols[old_len] = .{
            .name = name_copy,
            .data = data,
            .null_mask = null_mask,
            .len = n,
        };
        self.chunk.columns = new_cols;
        return old_len; // index of the newly added column
    }

    pub fn finish(self: *ChunkBuilder) DataChunk {
        return self.chunk;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "null_mask bit operations" {
    var mask = [_]u64{0};
    try std.testing.expect(!isNull(&mask, 0));
    setNull(&mask, 3);
    try std.testing.expect(isNull(&mask, 3));
    try std.testing.expect(!isNull(&mask, 2));
    clearNull(&mask, 3);
    try std.testing.expect(!isNull(&mask, 3));
}

test "ChunkBuilder basic" {
    var b = ChunkBuilder.init(std.testing.allocator, 3);
    defer b.chunk.deinit();

    const ci = try b.addColumn("score", .int64);
    b.chunk.columns[ci].data.int64[0] = 10;
    b.chunk.columns[ci].data.int64[1] = 20;
    b.chunk.columns[ci].data.int64[2] = 30;

    const chunk = b.finish();
    try std.testing.expectEqual(@as(usize, 3), chunk.num_rows);
    try std.testing.expectEqual(@as(usize, 1), chunk.columns.len);
    try std.testing.expectEqual(Value{ .int64 = 20 }, chunk.columns[0].data.get(1));
}

test "DataChunk.findColumn" {
    var b = ChunkBuilder.init(std.testing.allocator, 1);
    defer b.chunk.deinit();
    _ = try b.addColumn("alpha", .string);
    _ = try b.addColumn("beta", .float64);
    const chunk = b.finish();
    try std.testing.expectEqual(@as(?usize, 0), chunk.findColumn("alpha"));
    try std.testing.expectEqual(@as(?usize, 1), chunk.findColumn("beta"));
    try std.testing.expectEqual(@as(?usize, null), chunk.findColumn("gamma"));
}

test "SelectionVector from mask and compactSelection" {
    var b = ChunkBuilder.init(std.testing.allocator, 5);
    defer b.chunk.deinit();

    const ci = try b.addColumn("score", .int64);
    for (b.chunk.columns[ci].data.int64, 0..) |*v, i| v.* = @intCast(i + 10);
    setNull(b.chunk.columns[ci].null_mask, 3);

    var buf: [5]u32 = undefined;
    const mask = [_]i16{ 0, 1, 0, 1, 1 };
    const sel = SelectionVector.fromI16Mask(&buf, &mask);
    try std.testing.expectEqual(@as(usize, 3), sel.len);
    try std.testing.expectEqual(@as(u32, 1), sel.slice()[0]);
    try std.testing.expectEqual(@as(u32, 3), sel.slice()[1]);
    try std.testing.expectEqual(@as(u32, 4), sel.slice()[2]);

    b.chunk.compactSelection(sel);
    try std.testing.expectEqual(@as(usize, 3), b.chunk.num_rows);
    try std.testing.expectEqual(@as(i64, 11), b.chunk.columns[ci].data.int64[0]);
    try std.testing.expectEqual(@as(i64, 13), b.chunk.columns[ci].data.int64[1]);
    try std.testing.expectEqual(@as(i64, 14), b.chunk.columns[ci].data.int64[2]);
    try std.testing.expect(!b.chunk.columns[ci].isRowNull(0));
    try std.testing.expect(b.chunk.columns[ci].isRowNull(1));
    try std.testing.expect(!b.chunk.columns[ci].isRowNull(2));
}

test "SelectedChunk reads logical rows before materialization" {
    var b = ChunkBuilder.init(std.testing.allocator, 4);
    defer b.chunk.deinit();

    const ci = try b.addColumn("score", .int64);
    for (b.chunk.columns[ci].data.int64, 0..) |*v, i| v.* = @intCast((i + 1) * 10);

    var buf: [4]u32 = undefined;
    const mask = [_]i16{ 0, 1, 0, 1 };
    const sel = SelectionVector.fromI16Mask(&buf, &mask);
    try std.testing.expect(!sel.isEmpty());
    try std.testing.expect(!sel.isFull(4));
    try std.testing.expectEqual(@as(usize, 1), sel.rowAt(0));
    try std.testing.expectEqual(@as(usize, 3), sel.rowAt(1));

    const view = b.chunk.selected(sel);
    try std.testing.expectEqual(@as(usize, 2), view.len());
    try std.testing.expectEqual(Value{ .int64 = 20 }, view.getOpt(ci, 0).?);
    try std.testing.expectEqual(Value{ .int64 = 40 }, view.getOpt(ci, 1).?);

    var row_buf: [1]?Value = undefined;
    view.fillRow(1, &row_buf);
    try std.testing.expectEqual(Value{ .int64 = 40 }, row_buf[0].?);

    const row = try view.readRow(1, std.testing.allocator);
    defer std.testing.allocator.free(row);
    try std.testing.expectEqual(Value{ .int64 = 40 }, row[ci].?);

    view.materialize();
    try std.testing.expectEqual(@as(usize, 2), b.chunk.num_rows);
    try std.testing.expectEqual(@as(i64, 20), b.chunk.columns[ci].data.int64[0]);
    try std.testing.expectEqual(@as(i64, 40), b.chunk.columns[ci].data.int64[1]);
}

test "compactSelection handles empty and full selections" {
    var b = ChunkBuilder.init(std.testing.allocator, 3);
    defer b.chunk.deinit();

    const ci = try b.addColumn("score", .int64);
    b.chunk.columns[ci].data.int64[0] = 7;
    b.chunk.columns[ci].data.int64[1] = 8;
    b.chunk.columns[ci].data.int64[2] = 9;

    var full_buf: [3]u32 = undefined;
    const full_sel = SelectionVector.full(&full_buf, 3);
    try std.testing.expect(full_sel.isFull(3));
    b.chunk.compactSelection(full_sel);
    try std.testing.expectEqual(@as(usize, 3), b.chunk.num_rows);
    try std.testing.expectEqual(@as(i64, 7), b.chunk.columns[ci].data.int64[0]);
    try std.testing.expectEqual(@as(i64, 8), b.chunk.columns[ci].data.int64[1]);
    try std.testing.expectEqual(@as(i64, 9), b.chunk.columns[ci].data.int64[2]);

    var empty_buf: [3]u32 = undefined;
    const empty_sel = SelectionVector.init(&empty_buf);
    b.chunk.compactSelection(empty_sel);
    try std.testing.expectEqual(@as(usize, 0), b.chunk.num_rows);
    try std.testing.expectEqual(@as(usize, 0), b.chunk.columns[ci].len);
}
