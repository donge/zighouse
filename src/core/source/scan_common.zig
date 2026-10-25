//! Small shared pieces used by both physical SourceIface backends.
//!
//! The Store V2 and MergeTree readers have different column storage but the
//! same scan window, column-pruning, and column-lookup semantics. Keeping those
//! here prevents the two bridges from drifting apart.

const std = @import("std");
const schema = @import("schema");
const core = @import("core");

pub const ColumnType = core.chunk.ColumnType;

pub fn toCoreColType(ty: schema.ColumnType) ColumnType {
    return switch (ty) {
        .int8, .int16, .int32, .int64 => .int64,
        .float32, .float64 => .float64,
        .date => .date_u16,
        .timestamp => .datetime64_ms,
        .text, .char, .low_card => .string,
    };
}

pub fn findColumn(columns: []const schema.Column, name: []const u8) ?usize {
    for (columns, 0..) |col, i| {
        if (std.mem.eql(u8, col.name, name)) return i;
    }
    return null;
}

pub inline fn effectiveLo(scan_lo: u64, row_count: u64) u64 {
    return @min(scan_lo, row_count);
}

pub inline fn effectiveHi(scan_hi: u64, row_count: u64) u64 {
    return @min(scan_hi, row_count);
}

pub inline fn effectiveCount(scan_lo: u64, scan_hi: u64, row_count: u64) u64 {
    const lo = effectiveLo(scan_lo, row_count);
    const hi = effectiveHi(scan_hi, row_count);
    return if (hi > lo) hi - lo else 0;
}

pub inline fn rangeActive(scan_lo: u64, scan_hi: u64, row_count: u64) bool {
    return effectiveLo(scan_lo, row_count) != 0 or
        effectiveHi(scan_hi, row_count) != row_count;
}

pub fn buildNeededMask(
    alloc: std.mem.Allocator,
    columns: []const schema.Column,
    pruned_columns: []const []const u8,
) !?[]bool {
    if (pruned_columns.len == 0) return null;
    const mask = try alloc.alloc(bool, columns.len);
    @memset(mask, false);
    for (pruned_columns) |name| {
        if (findColumn(columns, name)) |ci| mask[ci] = true;
    }
    return mask;
}

pub fn resetNeededMask(
    alloc: std.mem.Allocator,
    needed: *?[]bool,
    columns: []const schema.Column,
    pruned_columns: []const []const u8,
) void {
    if (pruned_columns.len == 0) {
        if (needed.*) |mask| alloc.free(mask);
        needed.* = null;
        return;
    }
    if (needed.* == null) {
        needed.* = alloc.alloc(bool, columns.len) catch return;
    }
    @memset(needed.*.?, false);
    for (pruned_columns) |name| {
        if (findColumn(columns, name)) |ci| needed.*.?[ci] = true;
    }
}

pub inline fn isNeeded(
    needed: ?[]const bool,
    override_active: bool,
    override: *const [256]bool,
    ci: usize,
) bool {
    if (override_active) return ci < 256 and override[ci];
    if (needed) |mask| return ci < mask.len and mask[ci];
    return true;
}

pub fn setNeededOverride(
    override: *[256]bool,
    active: *bool,
    columns: []const schema.Column,
    col_names: ?[]const []const u8,
) void {
    if (col_names == null) {
        active.* = false;
        return;
    }
    @memset(override, false);
    for (col_names.?) |name| {
        if (findColumn(columns, name)) |ci| {
            if (ci < 256) override[ci] = true;
        }
    }
    active.* = true;
}

pub fn setNonEmptyBool(
    flags: *[256]bool,
    columns: []const schema.Column,
    col_name: ?[]const u8,
) void {
    if (col_name == null) {
        @memset(flags, false);
        return;
    }
    if (findColumn(columns, col_name.?)) |ci| {
        if (ci < 256) flags[ci] = true;
    }
}
