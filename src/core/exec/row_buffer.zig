//! Row-oriented buffer retained for breaker internals.
const std = @import("std");
const types = @import("../types.zig");
const chunk = @import("../chunk.zig");
const result = @import("../result.zig");

pub const Value = types.Value;
pub const ColumnType = types.ColumnType;
pub const ResultSet = result.ResultSet;

pub const RowBuffer = struct {
    metas: []result.ColMeta,
    rows: std.ArrayListUnmanaged([]?Value),

    pub fn init(metas: []result.ColMeta) RowBuffer {
        return .{ .metas = metas, .rows = .empty };
    }

    pub fn append(self: *RowBuffer, alloc: std.mem.Allocator, row: []?Value) !void {
        try self.rows.append(alloc, row);
    }

    /// Materialise into a ResultSet.  All values are duped into a fresh arena.
    pub fn toResultSet(self: RowBuffer, parent_alloc: std.mem.Allocator) !ResultSet {
        var arena = std.heap.ArenaAllocator.init(parent_alloc);
        const ra = arena.allocator();

        const num_rows = self.rows.items.len;
        const num_cols = self.metas.len;

        const out_metas = try ra.dupe(result.ColMeta, self.metas);
        if (num_rows == 0 or num_cols == 0) {
            var out_cols: []chunk.Column = &.{};
            if (num_cols > 0 and num_rows == 0) {
                out_cols = try ra.alloc(chunk.Column, num_cols);
                for (out_cols, out_metas) |*c, meta| {
                    c.* = .{
                        .name = meta.name,
                        .null_mask = &.{},
                        .len = 0,
                        .data = switch (meta.col_type) {
                            .bool_u8 => .{ .bool_u8 = &.{} },
                            .int64 => .{ .int64 = &.{} },
                            .uint64 => .{ .uint64 = &.{} },
                            .float64 => .{ .float64 = &.{} },
                            .string => .{ .string = &.{} },
                            .date_u16 => .{ .date_u16 = &.{} },
                            .datetime64_ms => .{ .datetime64_ms = &.{} },
                            .array_string => .{ .array_string = &.{} },
                        },
                    };
                }
            }
            return ResultSet{
                .metas = out_metas,
                .columns = out_cols,
                .num_rows = 0,
                .arena = arena,
            };
        }

        const out_cols = try ra.alloc(chunk.Column, num_cols);
        for (out_cols, out_metas) |*col, meta| {
            const nw = chunk.nullMaskWords(num_rows);
            const null_mask = try ra.alloc(u64, nw);
            @memset(null_mask, 0);
            col.* = .{
                .name = meta.name,
                .data = try allocColumnDataRA(meta.col_type, num_rows, ra),
                .null_mask = null_mask,
                .len = num_rows,
            };
        }

        for (self.rows.items, 0..) |row, r| {
            for (row, 0..) |v_opt, ci| {
                if (v_opt) |v| {
                    setColValue(&out_cols[ci].data, r, v, ra);
                } else {
                    chunk.setNull(out_cols[ci].null_mask, r);
                }
            }
        }

        return ResultSet{
            .metas = out_metas,
            .columns = out_cols,
            .num_rows = num_rows,
            .arena = arena,
        };
    }
};

fn allocColumnDataRA(col_type: ColumnType, n: usize, ra: std.mem.Allocator) !chunk.ColumnData {
    return switch (col_type) {
        .bool_u8 => .{ .bool_u8 = try ra.alloc(u8, n) },
        .int64 => .{ .int64 = try ra.alloc(i64, n) },
        .uint64 => .{ .uint64 = try ra.alloc(u64, n) },
        .float64 => .{ .float64 = try ra.alloc(f64, n) },
        .date_u16 => .{ .date_u16 = try ra.alloc(u16, n) },
        .datetime64_ms => .{ .datetime64_ms = try ra.alloc(i64, n) },
        .string => .{ .string = try ra.alloc([]const u8, n) },
        .array_string => .{ .array_string = try ra.alloc([][]const u8, n) },
    };
}

fn setColValue(data: *chunk.ColumnData, r: usize, v: Value, ra: std.mem.Allocator) void {
    switch (data.*) {
        .bool_u8 => |s| s[r] = switch (v) {
            .bool_u8 => |x| x,
            else => @intCast(v.toI64() orelse 0),
        },
        .int64 => |s| s[r] = v.toI64() orelse 0,
        .uint64 => |s| s[r] = v.toU64() orelse 0,
        .float64 => |s| s[r] = v.toF64() orelse 0.0,
        .date_u16 => |s| s[r] = switch (v) {
            .date_u16 => |x| x,
            .uint64 => |u| @truncate(u),
            else => @as(u16, @intCast(@max(0, v.toI64() orelse 0))),
        },
        .datetime64_ms => |s| s[r] = v.toI64() orelse 0,
        .string => |s| s[r] = ra.dupe(u8, v.toStr() orelse "") catch (v.toStr() orelse ""),
        .array_string => |s| s[r] = switch (v) {
            .array_string => |a| a,
            else => &.{},
        },
    }
}
