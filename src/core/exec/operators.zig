//! Chunk operators and shared scan/filter state.
const std = @import("std");
const types = @import("../types.zig");
const chunk = @import("../chunk.zig");
const result = @import("../result.zig");
const plan = @import("plan.zig");
const kernels = @import("kernels.zig");
const ht = @import("hash_table.zig");
const simd_ops = @import("../simd_ops.zig");
const simd_batch = @import("../simd_batch.zig");
const context = @import("context.zig");

pub const Value = types.Value;
pub const ColumnType = types.ColumnType;
pub const DataChunk = chunk.DataChunk;
pub const QueryContext = context.QueryContext;
pub const SourceIface = context.SourceIface;
const simd = simd_ops;

pub const LikeGuard = struct {
    col_idx: usize,
    pattern: []const u8,
    negate: bool,
    matcher: kernels.LikeMatcher,
};

/// A single int comparison extracted from a pure-AND predicate tree.
/// Enables a fast vectorized filter path that avoids per-row kernels.evalExpr.
pub const IntCmpCond = struct {
    col_idx: usize,
    op: enum(u8) { eq, neq, lt, lte, gt, gte, in2 },
    val: i64,
    val2: i64 = 0, // only used for in2
};

/// A single string comparison extracted from a pure-AND predicate tree.
/// Enables a fast filter path for str_col != 'literal' (e.g. MobilePhoneModel <> '').
pub const StrCmpCond = struct {
    col_idx: usize,
    op: enum(u8) { eq, neq },
    val: []const u8,
};

/// Raw typed column slice backed by the mmap'd store (no fetchRange widening).
/// Enables SIMD filter and key extraction directly on narrow on-disk formats
/// (i16: not widened to i64; i32/date: not widened to i64; i64: already full width).
/// Used by the raw-slice fast path in ParHashCtx.runWork and ScatterCtx.runWork
/// to bypass the i16→i64 / i32→i64 widening that wastes 2-4× memory bandwidth.
pub const RawColSlice = union(enum) {
    i16s: []const i16,
    i32s: []const i32,
    i64s: []const i64,

    pub fn resolve(source: SourceIface, schema: []const result.ColMeta, col_idx: usize) ?RawColSlice {
        if (col_idx >= schema.len) return null;
        const name = schema[col_idx].name;
        if (source.getRawInt16Col(name)) |s| return .{ .i16s = s };
        if (source.getRawInt32Col(name)) |s| return .{ .i32s = s };
        if (source.getRawInt64Col(name)) |s| return .{ .i64s = s };
        return null;
    }

    /// Apply one IntCmpCond to rows [start, start+n) using SIMD.
    /// ANDs the comparison result into mask[0..n] in place.
    /// tmp_a/tmp_b are i16 scratch buffers (length >= n).
    pub fn applyMaskSIMD(
        self: RawColSlice,
        start: usize,
        n: usize,
        cond: IntCmpCond,
        mask: []i16,
        tmp_a: []i16,
        tmp_b: []i16,
    ) void {
        switch (self) {
            .i16s => |a| {
                if (std.math.cast(i16, cond.val)) |rhs| {
                    const rhs2: i16 = if (cond.op == .in2)
                        (std.math.cast(i16, cond.val2) orelse rhs)
                    else
                        0;
                    cmpBatchDispatch(i16, a[start..][0..n], cond.op, rhs, rhs2, tmp_a[0..n], tmp_b[0..n]);
                } else @memset(tmp_a[0..n], 0);
            },
            .i32s => |a| {
                if (std.math.cast(i32, cond.val)) |rhs| {
                    const rhs2: i32 = if (cond.op == .in2)
                        (std.math.cast(i32, cond.val2) orelse rhs)
                    else
                        0;
                    cmpBatchDispatch(i32, a[start..][0..n], cond.op, rhs, rhs2, tmp_a[0..n], tmp_b[0..n]);
                } else @memset(tmp_a[0..n], 0);
            },
            .i64s => |a| {
                cmpBatchDispatch(i64, a[start..][0..n], cond.op, cond.val, cond.val2, tmp_a[0..n], tmp_b[0..n]);
            },
        }
        simd_batch.andMasks(mask[0..n], tmp_a[0..n], mask[0..n]);
    }

    /// Get i64 representation of element at absolute row index (sign-extends narrow types).
    pub fn getI64(self: RawColSlice, row: usize) i64 {
        return switch (self) {
            .i16s => |a| @as(i64, a[row]),
            .i32s => |a| @as(i64, a[row]),
            .i64s => |a| a[row],
        };
    }

    pub fn getAggPartial(self: RawColSlice, row: usize, kind: ht.CompactAggKind) u64 {
        const v = self.getI64(row);
        return switch (kind) {
            .i64_sum, .count_distinct_u64 => @bitCast(v),
            .u64_sum => @as(u64, @bitCast(v)),
            .f64_sum => @bitCast(@as(f64, @floatFromInt(v))),
            else => 0,
        };
    }

    pub fn minMax(self: RawColSlice) struct { min: i64, max: i64 } {
        return switch (self) {
            .i16s => |a| blk: {
                const mm = simd.minMax(i16, a);
                break :blk .{ .min = mm.min, .max = mm.max };
            },
            .i32s => |a| blk: {
                const mm = simd.minMax(i32, a);
                break :blk .{ .min = mm.min, .max = mm.max };
            },
            .i64s => |a| blk: {
                const mm = simd.minMax(i64, a);
                break :blk .{ .min = mm.min, .max = mm.max };
            },
        };
    }

    pub fn sumI64Range(self: RawColSlice, start: usize, end: usize) i64 {
        var total: i64 = 0;
        switch (self) {
            .i16s => |a| total = simd.sumI16ToI64(a[start..end]),
            .i32s => |a| total = simd.sumI32ToI64(a[start..end]),
            .i64s => |a| total = simd.sumI64(a[start..end]),
        }
        return total;
    }

    pub fn sumF64Range(self: RawColSlice, start: usize, end: usize) f64 {
        var total: f64 = 0;
        switch (self) {
            .i16s => |a| total = @floatFromInt(simd.sumI16ToI64(a[start..end])),
            .i32s => |a| total = @floatFromInt(simd.sumI32ToI64(a[start..end])),
            .i64s => |a| {
                for (a[start..end]) |v| total += @floatFromInt(v);
            },
        }
        return total;
    }
};

/// A CASE WHEN key that evaluates to a string inline (no evalExpr needed).
/// Supports: CASE WHEN <int_AND_conditions> THEN <str_col_ref> ELSE <lit_str> END
pub const CaseWhenStrKey = struct {
    when_ic: [4]IntCmpCond,
    when_ic_n: usize,
    then_col_idx: usize, // string column for THEN branch
    else_str: []const u8, // literal for ELSE (default "")

    /// Evaluate this CASE WHEN for row r in chunk c.
    pub fn eval(self: *const CaseWhenStrKey, c: *const DataChunk, r: usize) []const u8 {
        for (self.when_ic[0..self.when_ic_n]) |cond| {
            if (cond.col_idx >= c.columns.len) return self.else_str;
            const col = c.columns[cond.col_idx];
            const v: i64 = switch (col.data) {
                .int64 => |a| a[r],
                .uint64 => |a| @bitCast(a[r]),
                .bool_u8 => |a| @as(i64, a[r]),
                .date_u16 => |a| @as(i64, a[r]),
                .datetime64_ms => |a| a[r],
                else => return self.else_str,
            };
            const pass = switch (cond.op) {
                .eq => v == cond.val,
                .neq => v != cond.val,
                .lt => v < cond.val,
                .lte => v <= cond.val,
                .gt => v > cond.val,
                .gte => v >= cond.val,
                .in2 => v == cond.val or v == cond.val2,
            };
            if (!pass) return self.else_str;
        }
        // All WHEN conditions passed: return THEN column value.
        if (self.then_col_idx >= c.columns.len) return self.else_str;
        const tc = c.columns[self.then_col_idx];
        if (tc.isRowNull(r)) return self.else_str;
        return switch (tc.data) {
            .string => |a| a[r],
            else => self.else_str,
        };
    }
};

/// Try to extract a simple CASE WHEN → string key from a ProjectItem expression.
pub fn extractCaseWhenStrKey(expr: plan.Expr) ?CaseWhenStrKey {
    if (expr != .case_when) return null;
    const cw = expr.case_when;
    if (cw.when.len != 1) return null;
    if (cw.then[0] != .col_ref) return null;
    const else_str: []const u8 = if (cw.else_expr) |e| switch (e) {
        .lit_str => |s| s,
        else => return null,
    } else "";
    var ic_buf: [4]IntCmpCond = undefined;
    var ic_n: usize = 0;
    if (!extractAndIntConds(cw.when[0], &ic_buf, &ic_n, false)) return null;
    if (ic_n == 0 or ic_n > 4) return null;
    var cw_result = CaseWhenStrKey{
        .when_ic = undefined,
        .when_ic_n = ic_n,
        .then_col_idx = cw.then[0].col_ref.index,
        .else_str = else_str,
    };
    @memcpy(cw_result.when_ic[0..ic_n], ic_buf[0..ic_n]);
    return cw_result;
}

/// Extract leaf int-comparison conditions from a pure-AND predicate tree.
/// Returns false if the predicate contains any non-int or non-AND node.
/// When `best_effort=true`, partial extraction is allowed; returns true with partial set.
/// Dispatch cmpBatch for type T with a runtime op from IntCmpCond.
/// For .in2: ORs two cmpBatch(eq) results into `out`.
/// `rhs2` and `tmp_b` are only used for .in2.
pub fn cmpBatchDispatch(
    comptime T: type,
    vals: []const T,
    op: anytype, // IntCmpCond.op (runtime enum)
    rhs: T,
    rhs2: T,
    out: []i16,
    tmp_b: []i16,
) void {
    switch (op) {
        .eq => simd_batch.cmpBatch(T, vals, .eq, rhs, out),
        .neq => simd_batch.cmpBatch(T, vals, .neq, rhs, out),
        .lt => simd_batch.cmpBatch(T, vals, .lt, rhs, out),
        .lte => simd_batch.cmpBatch(T, vals, .lte, rhs, out),
        .gt => simd_batch.cmpBatch(T, vals, .gt, rhs, out),
        .gte => simd_batch.cmpBatch(T, vals, .gte, rhs, out),
        .in2 => {
            simd_batch.cmpBatch(T, vals, .eq, rhs, out);
            simd_batch.cmpBatch(T, vals, .eq, rhs2, tmp_b);
            simd_batch.orMasks(out, tmp_b, out);
        },
    }
}

/// Apply one IntCmpCond to n rows of a DataChunk column using SIMD vectorization.
/// Updates `mask` (AND-combines with the new condition result) in-place.
/// `tmp_a` and `tmp_b` are scratch i16 slices; `tmp_b` is only used for .in2.
/// All slices must have length >= n.
/// Returns false if the column type is unsupported — caller should fall back to scalar.
pub fn applyIntCondSIMD(
    c: *const DataChunk,
    cond: IntCmpCond,
    n: usize,
    mask: []i16,
    tmp_a: []i16,
    tmp_b: []i16,
) bool {
    if (cond.col_idx >= c.columns.len) {
        @memset(mask[0..n], 0);
        return true;
    }
    const col = c.columns[cond.col_idx];
    switch (col.data) {
        .int64 => |a| {
            cmpBatchDispatch(i64, a[0..n], cond.op, cond.val, cond.val2, tmp_a[0..n], tmp_b[0..n]);
        },
        .uint64 => |a| {
            cmpBatchDispatch(u64, a[0..n], cond.op, @bitCast(cond.val), @bitCast(cond.val2), tmp_a[0..n], tmp_b[0..n]);
        },
        .date_u16 => |a| {
            if (cond.val < 0 or cond.val > 65535) return false;
            if (cond.op == .in2 and (cond.val2 < 0 or cond.val2 > 65535)) return false;
            const rhs: u16 = @intCast(cond.val);
            const rhs2: u16 = if (cond.op == .in2) @intCast(cond.val2) else 0;
            cmpBatchDispatch(u16, a[0..n], cond.op, rhs, rhs2, tmp_a[0..n], tmp_b[0..n]);
        },
        .datetime64_ms => |a| {
            cmpBatchDispatch(i64, a[0..n], cond.op, cond.val, cond.val2, tmp_a[0..n], tmp_b[0..n]);
        },
        .bool_u8 => |a| {
            if (cond.val < 0 or cond.val > 255) return false;
            if (cond.op == .in2 and (cond.val2 < 0 or cond.val2 > 255)) return false;
            const rhs: u8 = @intCast(cond.val);
            const rhs2: u8 = if (cond.op == .in2) @intCast(cond.val2) else 0;
            cmpBatchDispatch(u8, a[0..n], cond.op, rhs, rhs2, tmp_a[0..n], tmp_b[0..n]);
        },
        else => return false,
    }
    simd_batch.andMasks(mask[0..n], tmp_a[0..n], mask[0..n]);
    return true;
}

pub fn extractAndIntConds(
    expr: plan.Expr,
    out: []IntCmpCond,
    n: *usize,
    best_effort: bool,
) bool {
    switch (expr) {
        .@"and" => |op| {
            const l_ok = extractAndIntConds(op.left, out, n, best_effort);
            const r_ok = extractAndIntConds(op.right, out, n, best_effort);
            if (best_effort) return l_ok or r_ok;
            return l_ok and r_ok;
        },
        // AND represented as fn_call{name="and", args=[l, r]} by the planner.
        .fn_call => |fc| {
            if (!std.mem.eql(u8, fc.name, "and")) return false;
            if (fc.args.len < 2) return false;
            var all_ok = true;
            for (fc.args) |arg| {
                const ok = extractAndIntConds(arg, out, n, best_effort);
                if (!ok) {
                    if (!best_effort) {
                        all_ok = false;
                    }
                }
            }
            return all_ok;
        },
        .eq, .neq, .lt, .lte, .gt, .gte => {
            const op: *plan.BinOp = switch (expr) {
                .eq => |o| o,
                .neq => |o| o,
                .lt => |o| o,
                .lte => |o| o,
                .gt => |o| o,
                .gte => |o| o,
                else => unreachable,
            };
            if (op.left != .col_ref) return false;
            const col_idx = op.left.col_ref.index;
            const val: i64 = switch (op.right) {
                .lit_i64 => |v| v,
                .lit_u64 => |v| @bitCast(v),
                else => return false,
            };
            const kind: @TypeOf(@as(IntCmpCond, undefined).op) = switch (expr) {
                .eq => .eq,
                .neq => .neq,
                .lt => .lt,
                .lte => .lte,
                .gt => .gt,
                .gte => .gte,
                else => unreachable,
            };
            if (n.* >= out.len) return false;
            out[n.*] = .{ .col_idx = col_idx, .op = kind, .val = val };
            n.* += 1;
            return true;
        },
        // a IN (b, c) → OR(a==b, a==c) — detect 2-value IN list on same column.
        .@"or" => |op| {
            const le = op.left;
            const re = op.right;
            if (le == .eq and re == .eq) {
                const lop = le.eq;
                const rop = re.eq;
                if (lop.left == .col_ref and rop.left == .col_ref and
                    lop.left.col_ref.index == rop.left.col_ref.index)
                {
                    const col_idx = lop.left.col_ref.index;
                    const v1: i64 = switch (lop.right) {
                        .lit_i64 => |v| v,
                        .lit_u64 => |v| @bitCast(v),
                        else => return false,
                    };
                    const v2: i64 = switch (rop.right) {
                        .lit_i64 => |v| v,
                        .lit_u64 => |v| @bitCast(v),
                        else => return false,
                    };
                    if (n.* >= out.len) return false;
                    out[n.*] = .{ .col_idx = col_idx, .op = .in2, .val = v1, .val2 = v2 };
                    n.* += 1;
                    return true;
                }
            }
            return false;
        },
        else => return false,
    }
}

/// A single-term col_ref eq/neq lit_str filter that can be inlined cheaply in scatter loops
/// without per-row arena allocation or evalExpr overhead.
pub const SimpleStrFilter = struct {
    col_idx: usize,
    value: []const u8,
    is_neq: bool, // true = keep row when s != value; false = keep when s == value

    pub fn passes(self: SimpleStrFilter, col: chunk.Column, r: usize) bool {
        // Fast path: when the source decoded this column as bool_u8 (1=non-empty, 0=empty)
        // via setStringNonEmptyBool, we only support the eq/neq-empty pattern.
        if (col.data == .bool_u8) {
            const non_empty = col.data.bool_u8[r] != 0;
            // is_neq=true, value="" → keep non-empty rows → pass when non_empty
            // is_neq=false, value="" → keep empty rows → pass when !non_empty
            return if (self.is_neq) non_empty else !non_empty;
        }
        const s: []const u8 = if (col.isRowNull(r)) "" else col.data.string[r];
        return if (self.is_neq) !std.mem.eql(u8, s, self.value) else std.mem.eql(u8, s, self.value);
    }

    pub fn passesRaw(self: SimpleStrFilter, offsets: []const u64, bytes: ?[]const u8, row: usize) bool {
        const lo: usize = @intCast(offsets[row]);
        const hi: usize = @intCast(offsets[row + 1]);
        if (self.value.len == 0) {
            const non_empty = hi != lo;
            return if (self.is_neq) non_empty else !non_empty;
        }
        const s = (bytes orelse return false)[lo..hi];
        return if (self.is_neq) !std.mem.eql(u8, s, self.value) else std.mem.eql(u8, s, self.value);
    }
};

/// Try to extract a single-term col_ref eq/neq lit_str filter from a plan.Expr.
/// Returns null if the expression is not exactly that shape.
pub fn tryExtractSimpleStrFilter(expr: plan.Expr) ?SimpleStrFilter {
    switch (expr) {
        .eq, .neq => {
            const op: *plan.BinOp = if (expr == .eq) expr.eq else expr.neq;
            if (op.left != .col_ref) return null;
            const val: []const u8 = switch (op.right) {
                .lit_str => |s| s,
                else => return null,
            };
            return .{ .col_idx = op.left.col_ref.index, .value = val, .is_neq = (expr == .neq) };
        },
        else => return null,
    }
}

/// Extract pure-AND string comparison conditions (eq/neq of col_ref vs lit_str).
/// Returns true if the entire predicate is covered by string conditions (complete).
pub fn extractAndStrConds(
    expr: plan.Expr,
    out: []StrCmpCond,
    n: *usize,
    best_effort: bool,
) bool {
    switch (expr) {
        .@"and" => |op| {
            const l_ok = extractAndStrConds(op.left, out, n, best_effort);
            const r_ok = extractAndStrConds(op.right, out, n, best_effort);
            if (best_effort) return l_ok or r_ok;
            return l_ok and r_ok;
        },
        // AND as fn_call{name="and"} from native Zig planner.
        .fn_call => |fc| {
            if (!std.mem.eql(u8, fc.name, "and")) return false;
            var all_ok = true;
            for (fc.args) |arg| {
                if (!extractAndStrConds(arg, out, n, best_effort)) {
                    if (!best_effort) all_ok = false;
                }
            }
            return all_ok;
        },
        .eq, .neq => {
            const op: *plan.BinOp = if (expr == .eq) expr.eq else expr.neq;
            if (op.left != .col_ref) return false;
            const lit: []const u8 = switch (op.right) {
                .lit_str => |s| s,
                else => return false,
            };
            const kind: @TypeOf(@as(StrCmpCond, undefined).op) = if (expr == .eq) .eq else .neq;
            if (n.* >= out.len) return false;
            out[n.*] = .{ .col_idx = op.left.col_ref.index, .op = kind, .val = lit };
            n.* += 1;
            return true;
        },
        else => return false,
    }
}

/// Extract an AND-only predicate into separate int and str condition lists.
/// Returns true if the predicate is FULLY covered (no fn_call, no regexp, etc).
pub fn extractMixedAndConds(
    expr: plan.Expr,
    ic_out: []IntCmpCond,
    ic_n: *usize,
    sc_out: []StrCmpCond,
    sc_n: *usize,
) bool {
    switch (expr) {
        .@"and" => |op| {
            const l_ok = extractMixedAndConds(op.left, ic_out, ic_n, sc_out, sc_n);
            const r_ok = extractMixedAndConds(op.right, ic_out, ic_n, sc_out, sc_n);
            return l_ok and r_ok;
        },
        // AND as fn_call{name="and"} from native Zig planner.
        .fn_call => |fc| {
            if (!std.mem.eql(u8, fc.name, "and")) return false;
            var all_ok = true;
            for (fc.args) |arg| {
                if (!extractMixedAndConds(arg, ic_out, ic_n, sc_out, sc_n)) all_ok = false;
            }
            return all_ok;
        },
        .eq, .neq, .lt, .lte, .gt, .gte => {
            const op: *plan.BinOp = switch (expr) {
                .eq => |o| o,
                .neq => |o| o,
                .lt => |o| o,
                .lte => |o| o,
                .gt => |o| o,
                .gte => |o| o,
                else => unreachable,
            };
            if (op.left != .col_ref) return false;
            // Try int literal first.
            const int_val: ?i64 = switch (op.right) {
                .lit_i64 => |v| v,
                .lit_u64 => |v| @bitCast(v),
                else => null,
            };
            if (int_val) |val| {
                const kind: @TypeOf(@as(IntCmpCond, undefined).op) = switch (expr) {
                    .eq => .eq,
                    .neq => .neq,
                    .lt => .lt,
                    .lte => .lte,
                    .gt => .gt,
                    .gte => .gte,
                    else => unreachable,
                };
                if (ic_n.* >= ic_out.len) return false;
                ic_out[ic_n.*] = .{ .col_idx = op.left.col_ref.index, .op = kind, .val = val };
                ic_n.* += 1;
                return true;
            }
            // Try str literal.
            const str_val: ?[]const u8 = switch (op.right) {
                .lit_str => |s| s,
                else => null,
            };
            if (str_val) |val| {
                if (expr != .eq and expr != .neq) return false;
                const kind: @TypeOf(@as(StrCmpCond, undefined).op) = if (expr == .eq) .eq else .neq;
                if (sc_n.* >= sc_out.len) return false;
                sc_out[sc_n.*] = .{ .col_idx = op.left.col_ref.index, .op = kind, .val = val };
                sc_n.* += 1;
                return true;
            }
            return false;
        },
        .@"or" => |op| {
            // Only handle 2-value int IN (same as extractAndIntConds).
            const le = op.left;
            const re = op.right;
            if (le == .eq and re == .eq) {
                const lop = le.eq;
                const rop = re.eq;
                if (lop.left == .col_ref and rop.left == .col_ref and
                    lop.left.col_ref.index == rop.left.col_ref.index)
                {
                    const v1: i64 = switch (lop.right) {
                        .lit_i64 => |v| v,
                        .lit_u64 => |v| @bitCast(v),
                        else => return false,
                    };
                    const v2: i64 = switch (rop.right) {
                        .lit_i64 => |v| v,
                        .lit_u64 => |v| @bitCast(v),
                        else => return false,
                    };
                    if (ic_n.* >= ic_out.len) return false;
                    ic_out[ic_n.*] = .{ .col_idx = lop.left.col_ref.index, .op = .in2, .val = v1, .val2 = v2 };
                    ic_n.* += 1;
                    return true;
                }
            }
            return false;
        },
        else => return false,
    }
}

pub const FilterState = struct {
    predicate: plan.Expr,
    /// Column indices referenced by the predicate; populated lazily on first apply().
    ref_indices: ?[]usize = null,
    /// Row buffer reused across chunk calls (allocated on first apply).
    row_buf: ?[]?Value = null,
    /// LIKE guards: col_ref LIKE/NOT_LIKE lit_str checks extracted from the predicate.
    /// Checked cheaply before full evalExpr to short-circuit expensive rows early.
    /// null = not yet initialized; empty slice = no LIKE guards in predicate.
    like_guards: ?[]LikeGuard = null,
    /// Set to true after first chunk if all guard columns are .string type.
    guards_verified: bool = false,

    /// When true, the pure-LIKE fast path skips copyRow and just counts matching rows.
    /// Safe only when downstream only reads c.num_rows (e.g. COUNT(*) aggregation).
    count_only_mode: bool = false,

    /// Vectorized integer condition fast path.
    /// null = not yet initialized; empty slice = predicate is NOT a pure-AND int filter.
    int_conds: ?[]IntCmpCond = null,
    /// True if int_conds covers ALL filter conditions (can skip evalExpr entirely).
    /// False means int_conds is only a partial pre-filter (evalExpr still runs after).
    int_conds_complete: bool = false,

    /// SIMD batch mask buffer reused across chunk calls (size = chunk_rows).
    /// Used by the evalExprBatch fast path for predicates that don't decompose to IntCmpCond.
    simd_mask_buf: ?[]i16 = null,
    /// Scratch buffers for applyIntCondSIMD: tmp_a for cmpBatch output, tmp_b for .in2 second pass.
    simd_tmp_a: ?[]i16 = null,
    simd_tmp_b: ?[]i16 = null,

    /// Precomputed list of non-pruned column indices in the DataChunk.
    /// Used by copyRowActive() to skip the O(all_cols) loop in copyRow() when most columns
    /// are pruned (narrow scan). null = not yet initialized (computed on first apply()).
    active_col_indices: ?[]usize = null,

    pub fn apply(self: *FilterState, c: *DataChunk, ctx: *QueryContext) !void {
        const alloc = ctx.allocator();
        // Build ref_indices, row_buf, and like_guards on first call (once per query).
        if (self.ref_indices == null) {
            const mask = try alloc.alloc(bool, c.columns.len);
            @memset(mask, false);
            collectColRefs(self.predicate, mask);
            var count: usize = 0;
            for (mask) |m| {
                if (m) count += 1;
            }
            const indices = try alloc.alloc(usize, count);
            var wi: usize = 0;
            for (mask, 0..) |m, j| {
                if (m) {
                    indices[wi] = j;
                    wi += 1;
                }
            }
            self.ref_indices = indices;
            const row = try alloc.alloc(?Value, c.columns.len);
            @memset(row, null);
            self.row_buf = row;
            // Collect LIKE/NOT_LIKE guards; only keep if all guard columns are string type.
            var guards_list = std.ArrayListUnmanaged(LikeGuard).empty;
            collectLikeGuards(self.predicate, &guards_list, alloc);
            const raw_guards = try guards_list.toOwnedSlice(alloc);
            var guards_ok = true;
            for (raw_guards) |lg| {
                if (lg.col_idx >= c.columns.len or c.columns[lg.col_idx].data != .string) {
                    guards_ok = false;
                    break;
                }
            }
            self.like_guards = if (guards_ok) raw_guards else &.{};
            self.guards_verified = true;
            // Try to extract pure-AND int conditions for vectorized fast path.
            var ic_buf: [16]IntCmpCond = undefined;
            var ic_n: usize = 0;
            const ic_complete = extractAndIntConds(self.predicate, &ic_buf, &ic_n, false);
            if (ic_complete and ic_n > 0) {
                self.int_conds = try alloc.dupe(IntCmpCond, ic_buf[0..ic_n]);
                self.int_conds_complete = true;
            } else {
                // Try partial extraction (best_effort=true): use as inline guard before evalExpr.
                ic_n = 0;
                _ = extractAndIntConds(self.predicate, &ic_buf, &ic_n, true);
                if (ic_n > 0) {
                    self.int_conds = try alloc.dupe(IntCmpCond, ic_buf[0..ic_n]);
                    // int_conds_complete stays false: used as inline guard, not compaction.
                } else {
                    self.int_conds = &.{}; // mark as not applicable
                }
            }
            // Allocate SIMD mask buffer for evalExprBatch fast path.
            self.simd_mask_buf = try alloc.alloc(i16, c.num_rows);
            self.simd_tmp_a = try alloc.alloc(i16, c.num_rows);
            self.simd_tmp_b = try alloc.alloc(i16, c.num_rows);
            // Precompute active (non-pruned) column indices for fast copyRow.
            var active_count: usize = 0;
            for (c.columns) |col| {
                if (!col.pruned) active_count += 1;
            }
            const aci = try alloc.alloc(usize, active_count);
            var ai: usize = 0;
            for (c.columns, 0..) |col, ci| {
                if (!col.pruned) {
                    aci[ai] = ci;
                    ai += 1;
                }
            }
            self.active_col_indices = aci;
        }
        const ref = self.ref_indices.?;
        const row = self.row_buf.?;
        const guards = self.like_guards.?;
        // Use precomputed active column list for fast copyRow when available.
        const active_cols: ?[]const usize = self.active_col_indices;
        // Inline helper: copy row using active list if available (O(active) vs O(all)).
        const CopyHelper = struct {
            c: *DataChunk,
            active: ?[]const usize,
            inline fn copy(self2: @This(), from: usize, to: usize) void {
                if (self2.active) |ac| {
                    copyRowActive(self2.c, from, to, ac);
                } else {
                    copyRow(self2.c, from, to);
                }
            }
        };
        const cr = CopyHelper{ .c = c, .active = active_cols };

        // ── Vectorized int-only fast path ─────────────────────────────────────
        // If predicate is a pure AND of integer comparisons, apply each condition
        // as a tight loop without boxing rows into []?Value.
        if (self.int_conds) |conds| {
            if (conds.len > 0) {
                // Verify all referenced columns are int64/uint64 (check on first call).
                var all_int = true;
                for (conds) |cond| {
                    if (cond.col_idx >= c.columns.len) {
                        all_int = false;
                        break;
                    }
                    switch (c.columns[cond.col_idx].data) {
                        .int64, .uint64, .date_u16, .datetime64_ms, .bool_u8 => {},
                        else => {
                            all_int = false;
                            break;
                        },
                    }
                }
                if (all_int) {
                    if (self.int_conds_complete) {
                        // SIMD fast path: build a pass-mask with cmpBatch, then compact.
                        const n = c.num_rows;
                        const i16_mask = self.simd_mask_buf.?[0..n];
                        const i16_tmp_a = self.simd_tmp_a.?[0..n];
                        const i16_tmp_b = self.simd_tmp_b.?[0..n];
                        @memset(i16_mask, 1);
                        var simd_ok = true;
                        for (conds) |cond| {
                            if (!applyIntCondSIMD(c, cond, n, i16_mask, i16_tmp_a, i16_tmp_b)) {
                                simd_ok = false;
                                break;
                            }
                        }
                        var write_pos: usize = 0;
                        if (simd_ok) {
                            for (0..n) |r| {
                                if (i16_mask[r] != 0) {
                                    if (write_pos != r) cr.copy(r, write_pos);
                                    write_pos += 1;
                                }
                            }
                        } else {
                            // Scalar fallback (rare: only if a column type is unsupported).
                            row_loop: for (0..n) |r| {
                                for (conds) |cond| {
                                    const col = c.columns[cond.col_idx];
                                    if (col.isRowNull(r)) continue :row_loop;
                                    const v: i64 = switch (col.data) {
                                        .int64 => |a| a[r],
                                        .uint64 => |a| @bitCast(a[r]),
                                        .date_u16 => |a| @as(i64, a[r]),
                                        .datetime64_ms => |a| a[r],
                                        .bool_u8 => |a| @as(i64, a[r]),
                                        else => continue :row_loop,
                                    };
                                    const pass = switch (cond.op) {
                                        .eq => v == cond.val,
                                        .neq => v != cond.val,
                                        .lt => v < cond.val,
                                        .lte => v <= cond.val,
                                        .gt => v > cond.val,
                                        .gte => v >= cond.val,
                                        .in2 => v == cond.val or v == cond.val2,
                                    };
                                    if (!pass) continue :row_loop;
                                }
                                if (write_pos != r) cr.copy(r, write_pos);
                                write_pos += 1;
                            }
                        }
                        c.num_rows = write_pos;
                        for (c.columns) |*col2| col2.len = write_pos;
                        return;
                    } else {
                        // Partial inline-guard path: check int conditions per-row inline
                        // before calling evalExpr. No copyRow compaction here — we fall
                        // through to the general evalExpr loop below, which handles the
                        // actual row compaction. Int conds re-read via self.int_conds below.
                    }
                }
            }
        }

        // Pure-LIKE fast path: predicate is exactly col_ref LIKE/NOT_LIKE lit_str.
        if (guards.len == 1) {
            switch (self.predicate) {
                .like, .not_like => {
                    const lg = guards[0];
                    const col = c.columns[lg.col_idx];
                    if (self.count_only_mode) {
                        // Count-only: skip copyRow entirely — caller only needs c.num_rows.
                        var count: usize = 0;
                        for (0..c.num_rows) |r| {
                            const s = if (col.isRowNull(r)) "" else col.data.string[r];
                            if (lg.matcher.match(s) != lg.negate) count += 1;
                        }
                        c.num_rows = count;
                        for (c.columns) |*col2| col2.len = count;
                        return;
                    }
                    var write_pos: usize = 0;
                    for (0..c.num_rows) |r| {
                        const s = if (col.isRowNull(r)) "" else col.data.string[r];
                        const keep = lg.matcher.match(s) != lg.negate;
                        if (keep and write_pos == r) {
                            write_pos += 1;
                        } else if (keep) {
                            cr.copy(r, write_pos);
                            write_pos += 1;
                        }
                    }
                    c.num_rows = write_pos;
                    for (c.columns) |*col2| col2.len = write_pos;
                    return;
                },
                else => {},
            }
        }

        // Fast path: col_ref != lit_str (e.g. "Referer <> ''").
        // Avoids row_buf boxing and full evalExpr dispatch.
        switch (self.predicate) {
            .neq => |op| {
                if (op.left == .col_ref and op.right == .lit_str) {
                    const col_idx = op.left.col_ref.index;
                    const lit = op.right.lit_str;
                    if (col_idx < c.columns.len and c.columns[col_idx].data == .string) {
                        const col = c.columns[col_idx];
                        var write_pos: usize = 0;
                        for (0..c.num_rows) |r| {
                            const s = if (col.isRowNull(r)) "" else col.data.string[r];
                            if (!std.mem.eql(u8, s, lit)) {
                                if (write_pos != r) cr.copy(r, write_pos);
                                write_pos += 1;
                            }
                        }
                        c.num_rows = write_pos;
                        for (c.columns) |*col2| col2.len = write_pos;
                        return;
                    }
                }
            },
            .eq => |op| {
                if (op.left == .col_ref and op.right == .lit_str) {
                    const col_idx = op.left.col_ref.index;
                    const lit = op.right.lit_str;
                    if (col_idx < c.columns.len and c.columns[col_idx].data == .string) {
                        const col = c.columns[col_idx];
                        var write_pos: usize = 0;
                        for (0..c.num_rows) |r| {
                            const s = if (col.isRowNull(r)) "" else col.data.string[r];
                            if (std.mem.eql(u8, s, lit)) {
                                if (write_pos != r) cr.copy(r, write_pos);
                                write_pos += 1;
                            }
                        }
                        c.num_rows = write_pos;
                        for (c.columns) |*col2| col2.len = write_pos;
                        return;
                    }
                }
            },
            else => {},
        }

        // Multi-LIKE guard short-circuit: check all LIKE guards before boxing row_buf.
        if (guards.len > 0) {
            var write_pos: usize = 0;
            row_loop: for (0..c.num_rows) |r| {
                for (guards) |lg| {
                    const col = c.columns[lg.col_idx];
                    const s = if (col.isRowNull(r)) "" else col.data.string[r];
                    if (lg.matcher.match(s) == lg.negate) continue :row_loop;
                }
                // All LIKE guards passed — fill row_buf and evaluate full predicate.
                for (ref) |j| {
                    const col = c.columns[j];
                    row[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                const v = try kernels.evalExpr(self.predicate, row, null, alloc);
                const keep = if (v) |val| val.bool_u8 != 0 else false;
                if (keep and write_pos == r) {
                    write_pos += 1;
                } else if (keep) {
                    cr.copy(r, write_pos);
                    write_pos += 1;
                }
            }
            c.num_rows = write_pos;
            for (c.columns) |*col| col.len = write_pos;
            return;
        }

        // evalExprBatch SIMD fast path: fires when no partial int guards and no LIKE guards.
        // Evaluates the full predicate over all rows at once using SIMD mask, then compacts.
        const has_partial_int_guards = if (self.int_conds) |ic| (!self.int_conds_complete and ic.len > 0) else false;
        if (!has_partial_int_guards and guards.len == 0) batch_path: {
            const mask_buf = self.simd_mask_buf orelse break :batch_path;
            const mask = mask_buf[0..c.num_rows];
            kernels.evalExprBatch(self.predicate, c.*, mask, alloc) catch break :batch_path;
            var write_pos_b: usize = 0;
            for (0..c.num_rows) |r| {
                if (mask[r] != 0) {
                    if (write_pos_b != r) cr.copy(r, write_pos_b);
                    write_pos_b += 1;
                }
            }
            c.num_rows = write_pos_b;
            for (c.columns) |*col| col.len = write_pos_b;
            return;
        }

        var write_pos: usize = 0;
        // When int_conds is set but not complete (partial guards), check them inline
        // before calling evalExpr to skip rows that definitely fail int conditions.
        const partial_guards: []const IntCmpCond = if (self.int_conds) |ic|
            (if (!self.int_conds_complete) ic else &.{})
        else
            &.{};

        outer: for (0..c.num_rows) |r| {
            // Inline int guard check: skip evalExpr for rows that fail int conditions.
            if (partial_guards.len > 0) {
                for (partial_guards) |cond| {
                    if (cond.col_idx >= c.columns.len) continue;
                    const col = c.columns[cond.col_idx];
                    if (col.isRowNull(r)) continue :outer;
                    const v: i64 = switch (col.data) {
                        .int64 => |a| a[r],
                        .uint64 => |a| @bitCast(a[r]),
                        .date_u16 => |a| @as(i64, a[r]),
                        .datetime64_ms => |a| a[r],
                        .bool_u8 => |a| @as(i64, a[r]),
                        else => continue,
                    };
                    const pass = switch (cond.op) {
                        .eq => v == cond.val,
                        .neq => v != cond.val,
                        .lt => v < cond.val,
                        .lte => v <= cond.val,
                        .gt => v > cond.val,
                        .gte => v >= cond.val,
                        .in2 => v == cond.val or v == cond.val2,
                    };
                    if (!pass) continue :outer;
                }
            }
            for (ref) |j| {
                const col = c.columns[j];
                row[j] = if (col.isRowNull(r)) null else col.data.get(r);
            }
            const v = try kernels.evalExpr(self.predicate, row, null, alloc);
            const keep = if (v) |val| val.bool_u8 != 0 else false;
            if (keep and write_pos == r) {
                write_pos += 1;
            } else if (keep) {
                cr.copy(r, write_pos);
                write_pos += 1;
            }
        }
        c.num_rows = write_pos;
        for (c.columns) |*col| col.len = write_pos;
    }
};

/// Recursively collect all col_ref LIKE/NOT_LIKE lit_str guards from an AND-chained predicate.
/// These guards can be evaluated cheaply before full expression eval to short-circuit rows.
pub fn collectLikeGuards(expr: plan.Expr, guards: *std.ArrayListUnmanaged(LikeGuard), alloc: std.mem.Allocator) void {
    switch (expr) {
        .like, .not_like => |op| {
            if (op.left == .col_ref and op.right == .lit_str) {
                guards.append(alloc, .{
                    .col_idx = op.left.col_ref.index,
                    .pattern = op.right.lit_str,
                    .negate = expr == .not_like,
                    .matcher = kernels.LikeMatcher.compile(op.right.lit_str, if (op.escape) |e| if (e.len > 0) e[0] else null else null),
                }) catch {};
            }
        },
        .@"and" => |op| {
            collectLikeGuards(op.left, guards, alloc);
            collectLikeGuards(op.right, guards, alloc);
        },
        // AND represented as fn_call{name="and"} by the native Zig planner.
        .fn_call => |fc| {
            if (std.mem.eql(u8, fc.name, "and")) {
                for (fc.args) |arg| collectLikeGuards(arg, guards, alloc);
            }
        },
        else => {},
    }
}

pub fn exprCanUseBatchSelection(expr: plan.Expr) bool {
    return switch (expr) {
        .eq, .neq, .lt, .lte, .gt, .gte => |op| blk: {
            if (op.left == .col_ref) {
                break :blk op.right == .lit_i64 or op.right == .lit_u64;
            }
            if (op.right == .col_ref) {
                break :blk op.left == .lit_i64 or op.left == .lit_u64;
            }
            break :blk false;
        },
        .@"and", .@"or" => |op| exprCanUseBatchSelection(op.left) and exprCanUseBatchSelection(op.right),
        .not => |op| exprCanUseBatchSelection(op.operand),
        else => false,
    };
}

/// Recursively collect column reference indices from an expression into a mask.
pub fn collectColRefs(expr: plan.Expr, mask: []bool) void {
    switch (expr) {
        .col_ref => |cr| if (cr.index < mask.len) {
            mask[cr.index] = true;
        },
        .add, .sub, .mul, .div, .mod => |op| {
            collectColRefs(op.left, mask);
            collectColRefs(op.right, mask);
        },
        .eq, .neq, .lt, .lte, .gt, .gte => |op| {
            collectColRefs(op.left, mask);
            collectColRefs(op.right, mask);
        },
        .@"and", .@"or" => |op| {
            collectColRefs(op.left, mask);
            collectColRefs(op.right, mask);
        },
        .not => |inner| collectColRefs(inner.operand, mask),
        .like, .not_like, .concat => |op| {
            collectColRefs(op.left, mask);
            collectColRefs(op.right, mask);
        },
        .is_null, .is_not_null => |inner| collectColRefs(inner.operand, mask),
        .cast => |c| collectColRefs(c.expr, mask),
        .fn_call => |fc| for (fc.args) |arg| collectColRefs(arg, mask),
        .agg_call => |ac| if (ac.arg) |arg| collectColRefs(arg, mask),
        .case_when => |cw| {
            for (cw.when, cw.then) |wh, th| {
                collectColRefs(wh, mask);
                collectColRefs(th, mask);
            }
            if (cw.else_expr) |e| collectColRefs(e, mask);
        },
        else => {},
    }
}

pub const ScanPrunePolicy = enum { any_reduction, half };

/// Declarative column request shared by scan-based physical implementations.
/// SourceIface currently caps runtime column overrides at 256 columns, so wide
/// schemas outside that contract conservatively skip pruning.
pub const ScanRequest = struct {
    needed: [256]bool = @as([256]bool, @splat(false)),

    pub fn addExpr(self: *ScanRequest, expr: plan.Expr) void {
        collectColRefs(expr, &self.needed);
    }

    pub fn addItems(self: *ScanRequest, items: []const plan.ProjectItem) void {
        for (items) |item| self.addExpr(item.expr);
    }

    pub fn addIndex(self: *ScanRequest, index: usize) void {
        if (index < self.needed.len) self.needed[index] = true;
    }

    pub fn apply(self: *const ScanRequest, source: SourceIface, policy: ScanPrunePolicy) ScanScope {
        const sm = source.schema();
        if (sm.len == 0 or sm.len > self.needed.len) return .{ .source = source };

        var count: usize = 0;
        for (self.needed[0..sm.len]) |is_needed| count += @intFromBool(is_needed);
        const worthwhile = count > 0 and count < sm.len and switch (policy) {
            .any_reduction => true,
            .half => count * 2 < sm.len,
        };
        if (!worthwhile) return .{ .source = source };

        var names: [256][]const u8 = undefined;
        var names_len: usize = 0;
        for (self.needed[0..sm.len], 0..) |is_needed, i| {
            if (is_needed) {
                names[names_len] = sm[i].name;
                names_len += 1;
            }
        }
        source.setNeededCols(names[0..names_len]);
        return .{ .source = source };
    }
};

pub const ScanScope = struct {
    source: SourceIface,

    pub fn deinit(self: ScanScope) void {
        self.source.setNeededCols(null);
    }
};

pub fn copyRow(c: *DataChunk, from: usize, to: usize) void {
    for (c.columns) |*col| {
        if (col.pruned) continue; // shared read-only zero buffer; skip
        const v = col.data.get(from);
        col.data.set(to, v);
        if (chunk.isNull(col.null_mask, from)) {
            chunk.setNull(col.null_mask, to);
        } else {
            chunk.clearNull(col.null_mask, to);
        }
    }
}

/// Faster copyRow when the set of active (non-pruned) column indices is pre-known.
/// O(active_cols) instead of O(all_cols) — critical for narrow-scan performance.
inline fn copyRowActive(c: *DataChunk, from: usize, to: usize, active: []const usize) void {
    for (active) |ci| {
        const col = &c.columns[ci];
        const v = col.data.get(from);
        col.data.set(to, v);
        if (chunk.isNull(col.null_mask, from)) {
            chunk.setNull(col.null_mask, to);
        } else {
            chunk.clearNull(col.null_mask, to);
        }
    }
}

/// Project: evaluate SELECT list expressions, producing a new DataChunk.
pub const ProjectState = struct {
    items: []const plan.ProjectItem,
    row_buf: ?[]?Value = null,

    pub fn apply(self: *ProjectState, c: *DataChunk, ctx: *QueryContext) !void {
        const alloc = ctx.allocator();
        const n = c.num_rows;
        const out_cols = try allocProjectColumns(self.items, n, alloc);
        if (self.row_buf == null or self.row_buf.?.len < c.columns.len) {
            const row_buf = try alloc.alloc(?Value, c.columns.len);
            @memset(row_buf, null);
            self.row_buf = row_buf;
        }
        const row_buf = self.row_buf.?;

        for (0..n) |r| {
            c.fillRow(r, row_buf);
            try evalProjectRow(self.items, out_cols, r, row_buf, alloc);
        }

        // Replace chunk columns (arena owns both old and new allocations).
        c.columns = out_cols;
    }

    pub fn applySelected(
        self: *ProjectState,
        selected: chunk.SelectedChunk,
        out: *DataChunk,
        ctx: *QueryContext,
    ) !void {
        const alloc = ctx.allocator();
        const n = selected.len();
        const out_cols = try allocProjectColumns(self.items, n, alloc);
        if (self.row_buf == null or self.row_buf.?.len < selected.chunk.columns.len) {
            const row_buf = try alloc.alloc(?Value, selected.chunk.columns.len);
            @memset(row_buf, null);
            self.row_buf = row_buf;
        }
        const row_buf = self.row_buf.?;

        for (0..n) |r| {
            selected.fillRow(r, row_buf);
            try evalProjectRow(self.items, out_cols, r, row_buf, alloc);
        }

        out.columns = out_cols;
        out.num_rows = n;
    }
};

pub fn allocProjectColumns(items: []const plan.ProjectItem, n: usize, alloc: std.mem.Allocator) ![]chunk.Column {
    var out_cols = try alloc.alloc(chunk.Column, items.len);
    for (items, 0..) |item, ci| {
        const nw = chunk.nullMaskWords(n);
        const null_mask = try alloc.alloc(u64, nw);
        @memset(null_mask, 0);
        const data = try allocColumnData(item.out_type, n, alloc);
        out_cols[ci] = .{
            .name = item.alias,
            .data = data,
            .null_mask = null_mask,
            .len = n,
        };
    }
    return out_cols;
}

pub fn evalProjectRow(
    items: []const plan.ProjectItem,
    out_cols: []chunk.Column,
    row_idx: usize,
    row: []const ?Value,
    alloc: std.mem.Allocator,
) !void {
    for (items, 0..) |item, ci| {
        const v_opt = try kernels.evalExpr(item.expr, row, null, alloc);
        if (v_opt) |v| {
            setColumnValue(&out_cols[ci].data, row_idx, v);
        } else {
            chunk.setNull(out_cols[ci].null_mask, row_idx);
            setColumnZero(&out_cols[ci].data, row_idx);
        }
    }
}

pub fn allocColumnData(col_type: ColumnType, n: usize, alloc: std.mem.Allocator) !chunk.ColumnData {
    return switch (col_type) {
        .bool_u8 => .{ .bool_u8 = try alloc.alloc(u8, n) },
        .int64 => .{ .int64 = try alloc.alloc(i64, n) },
        .uint64 => .{ .uint64 = try alloc.alloc(u64, n) },
        .float64 => .{ .float64 = try alloc.alloc(f64, n) },
        .date_u16 => .{ .date_u16 = try alloc.alloc(u16, n) },
        .datetime64_ms => .{ .datetime64_ms = try alloc.alloc(i64, n) },
        .string => .{ .string = try alloc.alloc([]const u8, n) },
        .array_string => .{ .array_string = try alloc.alloc([][]const u8, n) },
    };
}

pub fn setColumnValue(data: *chunk.ColumnData, r: usize, v: Value) void {
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
            else => @truncate(@as(u16, @intCast(v.toI64() orelse 0))),
        },
        .datetime64_ms => |s| s[r] = v.toI64() orelse 0,
        .string => |s| s[r] = v.toStr() orelse "",
        .array_string => |s| s[r] = switch (v) {
            .array_string => |a| a,
            else => &.{},
        },
    }
}

pub fn setColumnZero(data: *chunk.ColumnData, r: usize) void {
    switch (data.*) {
        .bool_u8 => |s| s[r] = 0,
        .int64 => |s| s[r] = 0,
        .uint64 => |s| s[r] = 0,
        .float64 => |s| s[r] = 0.0,
        .date_u16 => |s| s[r] = 0,
        .datetime64_ms => |s| s[r] = 0,
        .string => |s| s[r] = "",
        .array_string => |s| s[r] = &.{},
    }
}

/// LimitState: tracks how many rows have been emitted, truncates chunks.
pub const LimitState = struct {
    limit: u64,
    offset: u64,
    emitted: u64 = 0,
    skipped: u64 = 0,

    pub fn done(self: LimitState) bool {
        return self.emitted >= self.limit;
    }

    pub fn apply(self: *LimitState, c: *DataChunk) void {
        // Handle offset.
        if (self.skipped < self.offset) {
            const skip = @min(c.num_rows, self.offset - self.skipped);
            self.skipped += skip;
            // Compact out the skipped rows.
            const remaining = c.num_rows - skip;
            if (remaining == 0) {
                c.num_rows = 0;
                return;
            }
            for (0..remaining) |i| copyRow(c, i + skip, i);
            c.num_rows = remaining;
            for (c.columns) |*col| col.len = remaining;
        }
        // Truncate to limit.
        if (self.emitted >= self.limit) {
            c.num_rows = 0;
        } else {
            const take = @min(c.num_rows, self.limit - self.emitted);
            self.emitted += take;
            c.num_rows = take;
            for (c.columns) |*col| col.len = take;
        }
    }
};

// ── PhysicalOperator ──────────────────────────────────────────────────────────

/// A single operator node. Operators are applied in sequence to each chunk.
/// Pipeline breakers (hash_agg, order_by, hash_join) are not handled here
/// directly — see executePlan() for how breakers split the pipeline.
pub const PhysicalOperator = union(enum) {
    filter: FilterState,
    project: ProjectState,
    limit: LimitState,
};

// ── executePlan ───────────────────────────────────────────────────────────────
