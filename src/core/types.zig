/// Core type system shared by ZigDB and ZigHouse.
///
/// These types flow through the entire execution engine:
///   Planner → PhysicalPlan → Pipeline → DataChunk → ResultSet → Serializer
///
/// Design principle: type information is never lost. A Value always carries
/// its exact type tag, eliminating the need for heuristic type guessing
/// (the root cause of the CSV-intermediate bug class).
const std = @import("std");

// ── Column-level type ────────────────────────────────────────────────────────

/// The logical type of a column or expression result.
/// Determines how data is stored in ColumnData and serialized to wire format.
pub const ColumnType = enum {
    /// UInt8 — boolean results from predicates, dictHas, etc.
    bool_u8,
    /// Int64 — default integer type.
    int64,
    /// UInt64 — non-negative counters, counts.
    uint64,
    /// Float64 — floating-point values.
    float64,
    /// UInt16 — days since 1970-01-01 (ClickHouse Date).
    date_u16,
    /// Int64 — milliseconds since epoch (ClickHouse DateTime64(3)).
    datetime64_ms,
    /// Variable-length UTF-8 string.
    string,
    /// Array of UTF-8 strings — groupUniqArray, arrayStringConcat output.
    array_string,

    /// Return a human-readable ClickHouse type name for this column type.
    pub fn chTypeName(self: ColumnType) []const u8 {
        return switch (self) {
            .bool_u8       => "UInt8",
            .int64         => "Int64",
            .uint64        => "UInt64",
            .float64       => "Float64",
            .date_u16      => "Date",
            .datetime64_ms => "DateTime64(3)",
            .string        => "String",
            .array_string  => "Array(String)",
        };
    }

    /// Whether this type is numeric (supports arithmetic aggregation).
    pub fn isNumeric(self: ColumnType) bool {
        return switch (self) {
            .bool_u8, .int64, .uint64, .float64, .date_u16, .datetime64_ms => true,
            .string, .array_string => false,
        };
    }
};

// ── Row-level value ──────────────────────────────────────────────────────────

/// A single typed value. Used in expression evaluation kernels and as the
/// intermediate representation when processing individual rows.
///
/// Column-oriented storage (DataChunk) uses typed slices instead of []Value
/// for cache efficiency. Value is used for:
///   - expression evaluation results
///   - GROUP BY key storage
///   - scalar aggregate accumulators
pub const Value = union(ColumnType) {
    bool_u8:       u8,
    int64:         i64,
    uint64:        u64,
    float64:       f64,
    date_u16:      u16,
    datetime64_ms: i64,
    /// Slice into an arena; valid for the lifetime of the query.
    string:        []const u8,
    /// Slice of string slices; elements point into the same arena.
    array_string:  [][]const u8,

    // ── Null sentinel ────────────────────────────────────────────────────────
    // NULL is NOT a Value tag — it is represented by the null_mask bitmap
    // in Column. This avoids the "nullable everything" overhead at the value
    // level while keeping the hot path branch-free.

    // ── Conversions ──────────────────────────────────────────────────────────

    pub fn toI64(self: Value) ?i64 {
        return switch (self) {
            .int64         => |v| v,
            .uint64        => |v| @intCast(v),
            .float64       => |v| @intFromFloat(v),
            .date_u16      => |v| @as(i64, v),
            .datetime64_ms => |v| v,
            .bool_u8       => |v| @as(i64, v),
            else           => null,
        };
    }

    pub fn toU64(self: Value) ?u64 {
        return switch (self) {
            .uint64        => |v| v,
            .int64         => |v| if (v >= 0) @intCast(v) else null,
            .float64       => |v| if (v >= 0) @intFromFloat(v) else null,
            .date_u16      => |v| @as(u64, v),
            .datetime64_ms => |v| if (v >= 0) @intCast(v) else null,
            .bool_u8       => |v| @as(u64, v),
            else           => null,
        };
    }

    pub fn toF64(self: Value) ?f64 {
        return switch (self) {
            .float64       => |v| v,
            .int64         => |v| @floatFromInt(v),
            .uint64        => |v| @floatFromInt(v),
            .date_u16      => |v| @floatFromInt(v),
            .datetime64_ms => |v| @floatFromInt(v),
            .bool_u8       => |v| @floatFromInt(v),
            else           => null,
        };
    }

    pub fn toStr(self: Value) ?[]const u8 {
        return switch (self) {
            .string => |s| s,
            else    => null,
        };
    }

    /// Returns the ColumnType tag of this value.
    pub fn columnType(self: Value) ColumnType {
        return @as(ColumnType, self);
    }

    // ── Comparison ───────────────────────────────────────────────────────────

    pub fn order(a: Value, b: Value) std.math.Order {
        switch (a) {
            .int64 => |av| {
                if (b.toI64()) |bv| return std.math.order(av, bv);
                return .lt;
            },
            .uint64 => |av| {
                if (b.toU64()) |bv| return std.math.order(av, bv);
                return .lt;
            },
            .float64 => |av| {
                if (b.toF64()) |bv| return std.math.order(av, bv);
                return .lt;
            },
            .date_u16 => |av| switch (b) {
                .date_u16 => |bv| return std.math.order(av, bv),
                else => if (b.toI64()) |bv| return std.math.order(@as(i64, av), bv) else return .lt,
            },
            .datetime64_ms => |av| switch (b) {
                .datetime64_ms => |bv| return std.math.order(av, bv),
                else => if (b.toI64()) |bv| return std.math.order(av, bv) else return .lt,
            },
            .bool_u8 => |av| switch (b) {
                .bool_u8 => |bv| return std.math.order(av, bv),
                else => if (b.toI64()) |bv| return std.math.order(@as(i64, av), bv) else return .lt,
            },
            .string => |av| switch (b) {
                .string => |bv| return std.mem.order(u8, av, bv),
                else    => return .gt,
            },
            .array_string => return .lt,
        }
    }

    pub fn eql(a: Value, b: Value) bool {
        return order(a, b) == .eq;
    }

    // ── Hash (for GROUP BY keys) ──────────────────────────────────────────────

    pub fn hash(self: Value) u64 {
        var h = std.hash.Wyhash.init(0);
        // Mix in the type tag first so that int64(42) != float64(42.0) in hash,
        // matching the eql() contract (cross-type equality only via order()).
        const tag: u8 = @intFromEnum(@as(ColumnType, self));
        h.update(&[1]u8{tag});
        switch (self) {
            .int64         => |v| h.update(std.mem.asBytes(&v)),
            .uint64        => |v| h.update(std.mem.asBytes(&v)),
            .float64       => |v| {
                // Normalise -0.0 → +0.0 so equal values hash equally.
                const norm: f64 = if (v == 0.0) 0.0 else v;
                h.update(std.mem.asBytes(&norm));
            },
            .date_u16      => |v| h.update(std.mem.asBytes(&v)),
            .datetime64_ms => |v| h.update(std.mem.asBytes(&v)),
            .bool_u8       => |v| h.update(std.mem.asBytes(&v)),
            .string        => |v| h.update(v),
            .array_string  => |arr| {
                for (arr) |s| h.update(s);
            },
        }
        return h.final();
    }

    // ── Debug ─────────────────────────────────────────────────────────────────

    pub fn format(
        self: Value,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        switch (self) {
            .bool_u8       => |v| try writer.print("{d}", .{v}),
            .int64         => |v| try writer.print("{d}", .{v}),
            .uint64        => |v| try writer.print("{d}", .{v}),
            .float64       => |v| try writer.print("{d}", .{v}),
            .date_u16      => |v| try writer.print("date({d})", .{v}),
            .datetime64_ms => |v| try writer.print("dt({d})", .{v}),
            .string        => |v| try writer.print("{s}", .{v}),
            .array_string  => |arr| {
                try writer.writeByte('[');
                for (arr, 0..) |s, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try writer.print("{s}", .{s});
                }
                try writer.writeByte(']');
            },
        }
    }
};

// ── Aggregate accumulator ─────────────────────────────────────────────────────

/// State carried by a single aggregate function instance during GROUP BY or
/// scalar aggregation. The accumulator is updated once per input row.
pub const AggAccum = union(enum) {
    /// sum / count — integer accumulation
    i64_sum:   i64,
    u64_sum:   u64,
    f64_sum:   f64,
    /// avg accumulator: tracks both sum and count for correct AVG finalization
    f64_avg:   struct { sum: f64, count: u64 },
    /// count(*) / count(col)
    count:     u64,
    /// min / max — track current extremum
    i64_min:   i64,
    i64_max:   i64,
    u64_min:   u64,
    u64_max:   u64,
    f64_min:   f64,
    f64_max:   f64,
    str_min:   ?[]const u8,
    str_max:   ?[]const u8,
    /// groupUniqArray — collect unique strings
    uniq_strs: std.StringHashMapUnmanaged(void),
    /// count(distinct col) — track unique values via u64 hash set
    distinct_u64: std.AutoHashMapUnmanaged(u64, void),
    /// any() — first non-null value seen
    any_val: ?Value,

    /// Convert a scalar accumulator to a Value.
    /// For uniq_strs/any_val(array), call toArrayValue() instead.
    pub fn toValue(self: AggAccum) error{UseToArrayValue}!Value {
        return switch (self) {
            .i64_sum   => |v| .{ .int64   = v },
            .u64_sum   => |v| .{ .uint64  = v },
            .f64_sum   => |v| .{ .float64 = v },
            .f64_avg   => |v| .{ .float64 = if (v.count > 0) v.sum / @as(f64, @floatFromInt(v.count)) else 0.0 },
            .count     => |v| .{ .uint64  = v },
            .i64_min   => |v| .{ .int64   = v },
            .i64_max   => |v| .{ .int64   = v },
            .u64_min   => |v| .{ .uint64  = v },
            .u64_max   => |v| .{ .uint64  = v },
            .f64_min   => |v| .{ .float64 = v },
            .f64_max   => |v| .{ .float64 = v },
            .str_min   => |v| .{ .string  = v orelse "" },
            .str_max   => |v| .{ .string  = v orelse "" },
            .uniq_strs => return error.UseToArrayValue,
            .distinct_u64 => |m| .{ .uint64 = m.count() },
            .any_val   => |v| if (v) |val| blk: {
                // If the stored value is an array, direct callers to toArrayValue.
                if (val == .array_string) return error.UseToArrayValue;
                break :blk val;
            } else .{ .string = "" },
        };
    }

    /// Convert a uniq_strs or any_val(array) accumulator to an array_string Value.
    /// Strings are duped into `alloc`.
    pub fn toArrayValue(self: AggAccum, alloc: std.mem.Allocator) !Value {
        switch (self) {
            .uniq_strs => |m| {
                const arr = try alloc.alloc([]const u8, m.count());
                var it = m.keyIterator();
                var i: usize = 0;
                while (it.next()) |k| : (i += 1) {
                    arr[i] = try alloc.dupe(u8, k.*);
                }
                return Value{ .array_string = arr };
            },
            .any_val => |v| {
                if (v) |val| {
                    if (val == .array_string) return val;
                }
                return Value{ .array_string = &.{} };
            },
            else => return error.NotUniqStrs,
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "Value.order numeric cross-type" {
    const a = Value{ .int64 = 42 };
    const b = Value{ .float64 = 42.0 };
    try std.testing.expectEqual(std.math.Order.eq, Value.order(a, b));
}

test "Value.hash stability" {
    const v1 = Value{ .string = "hello" };
    const v2 = Value{ .string = "hello" };
    try std.testing.expectEqual(v1.hash(), v2.hash());
}

test "ColumnType.chTypeName" {
    try std.testing.expectEqualStrings("UInt8",         ColumnType.bool_u8.chTypeName());
    try std.testing.expectEqualStrings("Date",          ColumnType.date_u16.chTypeName());
    try std.testing.expectEqualStrings("DateTime64(3)", ColumnType.datetime64_ms.chTypeName());
    try std.testing.expectEqualStrings("Array(String)", ColumnType.array_string.chTypeName());
}
