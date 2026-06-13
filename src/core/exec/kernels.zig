/// Scalar expression kernels and aggregate update functions.
///
/// Kernels operate on individual Values (not yet vectorized over DataChunk
/// columns). This is intentional for the initial implementation — correctness
/// first. The interface is designed so that the hot `evalExpr` function can
/// later be replaced with a SIMD batch kernel without changing callers.
///
/// Null handling: if any input to a binary/unary operator is null, the result
/// is null. Callers check null_mask bits before calling evalExpr.
const std = @import("std");
const types = @import("../types.zig");
const plan = @import("plan.zig");
const chunk = @import("../chunk.zig");
const simd = @import("../simd_batch.zig");

pub const Value = types.Value;
pub const AggAccum = types.AggAccum;
pub const ColumnType = types.ColumnType;
pub const Expr = plan.Expr;
pub const DataChunk = chunk.DataChunk;

// ── Dict vtable (set by server before each IR query) ─────────────────────────

/// Opaque pointer to the dict store (server.DictStore).
pub var dict_store: ?*anyopaque = null;

/// dictHas(store, dict_name, keys_ptr, n_keys) → 0/1
pub var dict_has_fn: ?*const fn (
    store: *anyopaque,
    dict_name: [*:0]const u8,
    keys_ptr: [*]const [*:0]const u8,
    n_keys: usize,
) u8 = null;

/// dictGet(store, dict_name, attr_name, keys_ptr, n_keys) → null-terminated string or null
pub var dict_get_fn: ?*const fn (
    store: *anyopaque,
    dict_name: [*:0]const u8,
    attr_name: [*:0]const u8,
    keys_ptr: [*]const [*:0]const u8,
    n_keys: usize,
) ?[*:0]const u8 = null;

// ── Scalar expression evaluation ──────────────────────────────────────────────

/// Evaluate a scalar Expr against a single row of values.
///
/// `row`: the input values for this row (col_ref indices into this slice).
/// `arena`: short-lived allocator for intermediate string allocations.
///          The lifetime of the returned Value matches `arena`.
///
/// Returns null if the expression evaluates to SQL NULL.
pub fn evalExpr(expr: Expr, row: []const ?Value, lambda_val: ?Value, arena: std.mem.Allocator) anyerror!?Value {
    return evalExprFull(expr, row, lambda_val, null, arena);
}

fn evalExprFull(expr: Expr, row: []const ?Value, lambda_val: ?Value, lambda_val2: ?Value, arena: std.mem.Allocator) anyerror!?Value {
    switch (expr) {
        // Literals
        .lit_i64 => |v| return Value{ .int64 = v },
        .lit_u64 => |v| return Value{ .uint64 = v },
        .lit_f64 => |v| return Value{ .float64 = v },
        .lit_str => |v| return Value{ .string = v },
        .lit_bool => |v| return Value{ .bool_u8 = if (v) 1 else 0 },
        .lit_null => return null,
        .lit_array => |arr| return Value{ .array_string = arr },

        // Lambda param — return the currently-bound element
        .lambda_param => return lambda_val,
        // Second lambda param for (x,y)->body
        .lambda_param2 => return lambda_val2,

        // Lambda expression itself — should only appear as arg to arrayMap/Filter/Exists
        // If evaluated standalone, return null.
        .lambda => return null,

        // Column reference
        .col_ref => |ref| {
            if (ref.index >= row.len) return null;
            return row[ref.index];
        },

        // Arithmetic
        .add => |op| {
            const l = (try evalExprFull(op.left, row, lambda_val, lambda_val2, arena)) orelse return null;
            const r = (try evalExprFull(op.right, row, lambda_val, lambda_val2, arena)) orelse return null;
            return numericBinOp(l, r, .add);
        },
        .sub => |op| {
            const l = (try evalExprFull(op.left, row, lambda_val, lambda_val2, arena)) orelse return null;
            const r = (try evalExprFull(op.right, row, lambda_val, lambda_val2, arena)) orelse return null;
            return numericBinOp(l, r, .sub);
        },
        .mul => |op| {
            const l = (try evalExprFull(op.left, row, lambda_val, lambda_val2, arena)) orelse return null;
            const r = (try evalExprFull(op.right, row, lambda_val, lambda_val2, arena)) orelse return null;
            return numericBinOp(l, r, .mul);
        },
        .div => |op| {
            const l = (try evalExprFull(op.left, row, lambda_val, lambda_val2, arena)) orelse return null;
            const r = (try evalExprFull(op.right, row, lambda_val, lambda_val2, arena)) orelse return null;
            return numericBinOp(l, r, .div);
        },
        .mod => |op| {
            const l = (try evalExprFull(op.left, row, lambda_val, lambda_val2, arena)) orelse return null;
            const r = (try evalExprFull(op.right, row, lambda_val, lambda_val2, arena)) orelse return null;
            return numericBinOp(l, r, .mod);
        },

        // Comparisons — result is always bool_u8
        .eq => |op| return cmpOp(try evalExprFull(op.left, row, lambda_val, lambda_val2, arena), try evalExprFull(op.right, row, lambda_val, lambda_val2, arena), .eq),
        .neq => |op| return cmpOp(try evalExprFull(op.left, row, lambda_val, lambda_val2, arena), try evalExprFull(op.right, row, lambda_val, lambda_val2, arena), .neq),
        .lt => |op| return cmpOp(try evalExprFull(op.left, row, lambda_val, lambda_val2, arena), try evalExprFull(op.right, row, lambda_val, lambda_val2, arena), .lt),
        .lte => |op| return cmpOp(try evalExprFull(op.left, row, lambda_val, lambda_val2, arena), try evalExprFull(op.right, row, lambda_val, lambda_val2, arena), .lte),
        .gt => |op| return cmpOp(try evalExprFull(op.left, row, lambda_val, lambda_val2, arena), try evalExprFull(op.right, row, lambda_val, lambda_val2, arena), .gt),
        .gte => |op| return cmpOp(try evalExprFull(op.left, row, lambda_val, lambda_val2, arena), try evalExprFull(op.right, row, lambda_val, lambda_val2, arena), .gte),

        // Logical
        .@"and" => |op| {
            const l = try evalExprFull(op.left, row, lambda_val, lambda_val2, arena);
            if (l) |lv| if (lv.bool_u8 == 0) return Value{ .bool_u8 = 0 };
            const r = try evalExprFull(op.right, row, lambda_val, lambda_val2, arena);
            if (l == null or r == null) return null;
            return Value{ .bool_u8 = if (l.?.bool_u8 != 0 and r.?.bool_u8 != 0) 1 else 0 };
        },
        .@"or" => |op| {
            const l = try evalExprFull(op.left, row, lambda_val, lambda_val2, arena);
            if (l) |lv| if (lv.bool_u8 != 0) return Value{ .bool_u8 = 1 };
            const r = try evalExprFull(op.right, row, lambda_val, lambda_val2, arena);
            // SQL three-valued OR truth table:
            //   TRUE  OR anything = TRUE  (handled by short-circuit above)
            //   FALSE OR FALSE    = FALSE
            //   FALSE OR NULL     = NULL
            //   NULL  OR FALSE    = NULL
            //   NULL  OR NULL     = NULL
            if (l == null or r == null) return null;
            const lv: u8 = l.?.bool_u8;
            const rv: u8 = r.?.bool_u8;
            return Value{ .bool_u8 = if (lv != 0 or rv != 0) 1 else 0 };
        },
        .not => |op| {
            const v = try evalExprFull(op.operand, row, lambda_val, lambda_val2, arena) orelse return null;
            return Value{ .bool_u8 = if (v.bool_u8 == 0) 1 else 0 };
        },

        // IS NULL / IS NOT NULL
        .is_null => |op| {
            const v = try evalExprFull(op.operand, row, lambda_val, lambda_val2, arena);
            return Value{ .bool_u8 = if (v == null) 1 else 0 };
        },
        .is_not_null => |op| {
            const v = try evalExprFull(op.operand, row, lambda_val, lambda_val2, arena);
            return Value{ .bool_u8 = if (v != null) 1 else 0 };
        },

        // String
        .like => |op| return strLike(try evalExprFull(op.left, row, lambda_val, lambda_val2, arena), try evalExprFull(op.right, row, lambda_val, lambda_val2, arena), false, op.escape),
        .not_like => |op| return strLike(try evalExprFull(op.left, row, lambda_val, lambda_val2, arena), try evalExprFull(op.right, row, lambda_val, lambda_val2, arena), true, op.escape),
        .concat => |op| {
            const l = (try evalExprFull(op.left, row, lambda_val, lambda_val2, arena)) orelse return null;
            const r = (try evalExprFull(op.right, row, lambda_val, lambda_val2, arena)) orelse return null;
            const ls = l.toStr() orelse return null;
            const rs = r.toStr() orelse return null;
            const out = try std.fmt.allocPrint(arena, "{s}{s}", .{ ls, rs });
            return Value{ .string = out };
        },

        // CASE WHEN
        .case_when => |cw| {
            for (cw.when, cw.then) |when_expr, then_expr| {
                const cond = try evalExprFull(when_expr, row, lambda_val, lambda_val2, arena) orelse continue;
                if (cond.bool_u8 != 0) return evalExprFull(then_expr, row, lambda_val, lambda_val2, arena);
            }
            if (cw.else_expr) |e| return evalExprFull(e, row, lambda_val, lambda_val2, arena);
            return null;
        },

        // Aggregate calls should not appear in row-level eval.
        .agg_call => return error.AggCallInScalarContext,

        // Scalar function calls — dispatch by name.
        .fn_call => |fc| return evalFnCallFull(fc, row, lambda_val, lambda_val2, arena),

        // Cast
        .cast => |c| {
            const v = (try evalExprFull(c.expr, row, lambda_val, lambda_val2, arena)) orelse return null;
            return castValue(v, c.to_type, arena);
        },

        // Dictionary function calls
        .dict_call => |dc| return evalDictCall(dc, row, lambda_val, arena),
    }
}

// ── Numeric binary ops ────────────────────────────────────────────────────────

const NumOp = enum { add, sub, mul, div, mod };

fn numericBinOp(l: Value, r: Value, op: NumOp) ?Value {
    // Prefer float if either side is float or a numeric string (e.g. array elements).
    if (l == .float64 or r == .float64 or l == .string or r == .string) {
        const lf: f64 = switch (l) {
            .string => |s| std.fmt.parseFloat(f64, s) catch return null,
            else => l.toF64() orelse return null,
        };
        const rf: f64 = switch (r) {
            .string => |s| std.fmt.parseFloat(f64, s) catch return null,
            else => r.toF64() orelse return null,
        };
        return Value{ .float64 = switch (op) {
            .add => lf + rf,
            .sub => lf - rf,
            .mul => lf * rf,
            .div => if (rf == 0.0) null else lf / rf,
            .mod => @mod(lf, rf),
        } orelse return null };
    }
    // Integer path.
    const li = l.toI64() orelse return null;
    const ri = r.toI64() orelse return null;
    return Value{ .int64 = switch (op) {
        .add => li +% ri,
        .sub => li -% ri,
        .mul => li *% ri,
        .div => if (ri == 0) return null else @divTrunc(li, ri),
        .mod => if (ri == 0) return null else @rem(li, ri),
    } };
}

// ── Comparison ops ────────────────────────────────────────────────────────────

const CmpOp = enum { eq, neq, lt, lte, gt, gte };

fn cmpOp(l_opt: ?Value, r_opt: ?Value, op: CmpOp) ?Value {
    const l = l_opt orelse return null;
    const r = r_opt orelse return null;
    const ord = Value.order(l, r);
    const result: bool = switch (op) {
        .eq => ord == .eq,
        .neq => ord != .eq,
        .lt => ord == .lt,
        .lte => ord == .lt or ord == .eq,
        .gt => ord == .gt,
        .gte => ord == .gt or ord == .eq,
    };
    return Value{ .bool_u8 = if (result) 1 else 0 };
}

// ── LIKE pattern matching ─────────────────────────────────────────────────────

fn strLike(l_opt: ?Value, r_opt: ?Value, negate: bool, escape: ?[]const u8) ?Value {
    const l = l_opt orelse return null;
    const r = r_opt orelse return null;
    const s = l.toStr() orelse return null;
    const pat = r.toStr() orelse return null;
    const esc = if (escape != null and escape.?.len > 0) escape.?[0] else null;
    const matched = likeMatch(s, pat, esc);
    return Value{ .bool_u8 = if (matched != negate) 1 else 0 };
}

/// Simple SQL LIKE matcher: `%` matches any sequence, `_` matches any char.
/// If `escape` is non-null, the escape character makes the next char literal
/// (e.g. `LIKE '50\%' ESCAPE '\'` matches `50%`).
pub fn likeMatch(s: []const u8, pattern: []const u8, escape: ?u8) bool {
    var si: usize = 0;
    var pi: usize = 0;
    var star_pi: usize = std.math.maxInt(usize);
    var star_si: usize = 0;

    while (si < s.len) {
        if (pi < pattern.len and escape != null and pattern[pi] == escape.?) {
            pi += 1;
            if (pi < pattern.len and pattern[pi] == s[si]) {
                si += 1; pi += 1;
            } else if (star_pi != std.math.maxInt(usize)) {
                star_si += 1; si = star_si; pi = star_pi + 1;
            } else {
                return false;
            }
        } else if (pi < pattern.len and pattern[pi] != '%' and (pattern[pi] == '_' or pattern[pi] == s[si])) {
            si += 1;
            pi += 1;
        } else if (pi < pattern.len and pattern[pi] == '%') {
            star_pi = pi;
            star_si = si;
            pi += 1;
        } else if (star_pi != std.math.maxInt(usize)) {
            star_si += 1;
            si = star_si;
            pi = star_pi + 1;
        } else {
            return false;
        }
    }
    while (pi < pattern.len and pattern[pi] == '%') pi += 1;
    return pi == pattern.len;
}

/// Compiled LIKE pattern for repeated matching in a hot loop.
/// Call once per query; use matchCompiled per row.
pub const LikeMatcher = struct {
    pattern: []const u8,
    // Optimization kind:
    kind: Kind,
    needle: []const u8, // for .contains / .prefix / .suffix
    // Boyer-Moore-Horspool skip table for .contains with needle.len >= 2.
    bmh_skip: [256]usize,
    // ESCAPE character (nil if none). Fast paths skipped when escape is set.
    escape: ?u8 = null,

    pub const Kind = enum { contains, prefix, suffix, generic };

    pub fn compile(pattern: []const u8, escape: ?u8) LikeMatcher {
        if (escape != null) {
            return .{ .pattern = pattern, .kind = .generic, .needle = "", .bmh_skip = undefined, .escape = escape };
        }
        // '%needle%' — contains search (no wildcards in needle)
        if (pattern.len >= 2 and pattern[0] == '%' and pattern[pattern.len - 1] == '%') {
            const inner = pattern[1 .. pattern.len - 1];
            if (inner.len > 0 and std.mem.indexOfAny(u8, inner, "%_") == null) {
                var skip: [256]usize = undefined;
                bm_preprocess(inner, &skip);
                return .{ .pattern = pattern, .kind = .contains, .needle = inner, .bmh_skip = skip };
            }
        }
        // 'prefix%' — starts-with
        if (pattern.len >= 1 and pattern[pattern.len - 1] == '%') {
            const prefix = pattern[0 .. pattern.len - 1];
            if (std.mem.indexOfAny(u8, prefix, "%_") == null) {
                return .{ .pattern = pattern, .kind = .prefix, .needle = prefix, .bmh_skip = undefined };
            }
        }
        // '%suffix' — ends-with
        if (pattern.len >= 1 and pattern[0] == '%') {
            const suffix = pattern[1..];
            if (std.mem.indexOfAny(u8, suffix, "%_") == null) {
                return .{ .pattern = pattern, .kind = .suffix, .needle = suffix, .bmh_skip = undefined };
            }
        }
        return .{ .pattern = pattern, .kind = .generic, .needle = "", .bmh_skip = undefined };
    }

    fn bm_preprocess(needle: []const u8, skip: *[256]usize) void {
        @memset(skip, needle.len);
        for (0..needle.len - 1) |i| {
            skip[needle[i]] = needle.len - 1 - i;
        }
    }

    pub inline fn match(self: *const LikeMatcher, s: []const u8) bool {
        switch (self.kind) {
            .contains => {
                const n = self.needle;
                if (s.len < n.len) return false;
                if (n.len == 0) return true;
                if (n.len == 1) return std.mem.indexOfScalar(u8, s, n[0]) != null;
                // SIMD first-byte scan: scan 16 bytes at a time for needle[0],
                // then verify the rest of the needle on each match position.
                // Outperforms BMH for short needles (< ~8 bytes) in long strings because
                // it processes 16 bytes/cycle in the no-match case vs ~2-4 bytes/cycle BMH.
                const VL: usize = 16;
                const Vt = @Vector(VL, u8);
                const first = n[0];
                const rest = n[1..];
                const vfirst: Vt = @splat(first);
                var si: usize = 0;
                // Vectorised scan: no-match cost = 1 compare + 1 Or-reduce per 16 bytes.
                while (si + VL <= s.len) : (si += VL) {
                    const vec16: Vt = s[si..][0..VL].*;
                    const eq: @Vector(VL, bool) = vec16 == vfirst;
                    if (@reduce(.Or, eq)) {
                        inline for (0..VL) |bi| {
                            if (eq[bi]) {
                                const pos = si + bi;
                                if (pos + n.len <= s.len and
                                    std.mem.eql(u8, s[pos + 1 .. pos + n.len], rest))
                                    return true;
                            }
                        }
                    }
                }
                // Scalar tail for remaining bytes.
                while (si + n.len <= s.len) : (si += 1) {
                    if (s[si] == first and std.mem.eql(u8, s[si + 1 .. si + n.len], rest))
                        return true;
                }
                return false;
            },
            .prefix => return std.mem.startsWith(u8, s, self.needle),
            .suffix => return std.mem.endsWith(u8, s, self.needle),
            .generic => return likeMatch(s, self.pattern, self.escape),
        }
    }
};

// ── Scalar function dispatch ──────────────────────────────────────────────────

fn evalFnCallFull(fc: *const plan.FnCall, row: []const ?Value, lambda_val: ?Value, lambda_val2: ?Value, arena: std.mem.Allocator) !?Value {
    const name = fc.name;

    // ── Lambda-aware array functions (must NOT pre-eval the lambda arg) ────────
    if (std.mem.eql(u8, name, "arrayMap") or
        std.mem.eql(u8, name, "arrayFilter") or
        std.mem.eql(u8, name, "arrayExists"))
    {
        if (fc.args.len < 2) return null;
        // args[0] must be a lambda expr; args[1] is the array expression
        const lam = switch (fc.args[0]) {
            .lambda => |l| l,
            else => return null,
        };

        if (std.mem.eql(u8, name, "arrayMap")) {
            // 2-param lambda: arrayMap((x,y)->body, arr1, arr2)
            if (fc.args.len >= 3) {
                const arr1_val = (try evalExprFull(fc.args[1], row, lambda_val, lambda_val2, arena)) orelse return null;
                const arr2_val = (try evalExprFull(fc.args[2], row, lambda_val, lambda_val2, arena)) orelse return null;
                const arr1 = switch (arr1_val) {
                    .array_string => |s| s,
                    else => return null,
                };
                const arr2 = switch (arr2_val) {
                    .array_string => |s| s,
                    else => return null,
                };
                const len = @min(arr1.len, arr2.len);
                const out = try arena.alloc([]const u8, len);
                for (0..len) |i| {
                    const e1: ?Value = Value{ .string = arr1[i] };
                    const e2: ?Value = Value{ .string = arr2[i] };
                    const mapped = (try evalExprFull(lam.body.*, row, e1, e2, arena)) orelse Value{ .float64 = 0.0 };
                    out[i] = mapped.toStr() orelse try std.fmt.allocPrint(arena, "{d}", .{mapped.toF64() orelse 0.0});
                }
                return Value{ .array_string = out };
            }
        }

        const arr_val = (try evalExprFull(fc.args[1], row, lambda_val, lambda_val2, arena)) orelse return null;
        const arr = switch (arr_val) {
            .array_string => |s| blk: {
                // Convert [][]const u8 to []?Value for uniform processing
                const vals = try arena.alloc(?Value, s.len);
                for (s, 0..) |elem, i| vals[i] = Value{ .string = elem };
                break :blk vals;
            },
            else => return null,
        };

        if (std.mem.eql(u8, name, "arrayMap")) {
            const out = try arena.alloc([]const u8, arr.len);
            for (arr, 0..) |elem_v, i| {
                const mapped = (try evalExprFull(lam.body.*, row, elem_v, null, arena)) orelse Value{ .string = "" };
                out[i] = mapped.toStr() orelse try std.fmt.allocPrint(arena, "{d}", .{mapped.toF64() orelse 0.0});
            }
            return Value{ .array_string = out };
        }

        if (std.mem.eql(u8, name, "arrayFilter")) {
            var out: std.ArrayListUnmanaged([]const u8) = .empty;
            for (arr) |elem_v| {
                const cond = (try evalExprFull(lam.body.*, row, elem_v, null, arena)) orelse continue;
                const keep = switch (cond) {
                    .bool_u8 => |b| b != 0,
                    .int64 => |i| i != 0,
                    .string => |s| s.len > 0,
                    else => false,
                };
                if (keep) {
                    const s = (elem_v orelse continue).toStr() orelse continue;
                    try out.append(arena, s);
                }
            }
            return Value{ .array_string = try out.toOwnedSlice(arena) };
        }

        if (std.mem.eql(u8, name, "arrayExists")) {
            for (arr) |elem_v| {
                const cond = (try evalExprFull(lam.body.*, row, elem_v, null, arena)) orelse continue;
                const hit = switch (cond) {
                    .bool_u8 => |b| b != 0,
                    .int64 => |i| i != 0,
                    .string => |s| s.len > 0,
                    else => false,
                };
                if (hit) return Value{ .bool_u8 = 1 };
            }
            return Value{ .bool_u8 = 0 };
        }
    }

    // Evaluate all arguments first (non-lambda path).
    const args = try arena.alloc(?Value, fc.args.len);
    for (fc.args, 0..) |arg, i| {
        args[i] = try evalExprFull(arg, row, lambda_val, lambda_val2, arena);
    }

    // Logical operators encoded as fn_call by the planner
    if (std.mem.eql(u8, name, "and")) {
        if (args.len < 2) return null;
        const isTruthy = struct {
            fn check(v: ?Value) bool {
                return switch (v orelse return false) {
                    .bool_u8 => |b| b != 0,
                    .int64 => |i| i != 0,
                    .uint64 => |u| u != 0,
                    .float64 => |f| f != 0.0,
                    .string => |s| s.len > 0,
                    else => false,
                };
            }
        }.check;
        for (args) |a| {
            if (!isTruthy(a)) return Value{ .bool_u8 = 0 };
        }
        return Value{ .bool_u8 = 1 };
    }
    if (std.mem.eql(u8, name, "or")) {
        if (args.len < 2) return null;
        const isTruthy = struct {
            fn check(v: ?Value) bool {
                return switch (v orelse return false) {
                    .bool_u8 => |b| b != 0,
                    .int64 => |i| i != 0,
                    .uint64 => |u| u != 0,
                    .float64 => |f| f != 0.0,
                    .string => |s| s.len > 0,
                    else => false,
                };
            }
        }.check;
        for (args) |a| {
            if (isTruthy(a)) return Value{ .bool_u8 = 1 };
        }
        return Value{ .bool_u8 = 0 };
    }

    // String functions
    if (std.mem.eql(u8, name, "length") or std.mem.eql(u8, name, "char_length")) {
        const v = args[0] orelse return null;
        switch (v) {
            .array_string => |arr| return Value{ .int64 = @intCast(arr.len) },
            else => {
                const s = v.toStr() orelse return null;
                return Value{ .int64 = @intCast(s.len) };
            },
        }
    }
    if (std.mem.eql(u8, name, "lower")) {
        const v = args[0] orelse return null;
        const s = v.toStr() orelse return null;
        const out = try arena.dupe(u8, s);
        for (out) |*c| c.* = std.ascii.toLower(c.*);
        return Value{ .string = out };
    }
    if (std.mem.eql(u8, name, "upper")) {
        const v = args[0] orelse return null;
        const s = v.toStr() orelse return null;
        const out = try arena.dupe(u8, s);
        for (out) |*c| c.* = std.ascii.toUpper(c.*);
        return Value{ .string = out };
    }
    if (std.mem.eql(u8, name, "toString") or std.mem.eql(u8, name, "CAST_str")) {
        const v = args[0] orelse return null;
        // FixedString(16) IPv6: raw 16 bytes → format as IPv6 address string
        if (v == .string and v.string.len == 16) {
            return Value{ .string = try ipv6BytesToStr(v.string, arena) };
        }
        if (v.toStr()) |s| return Value{ .string = s };
        return Value{ .string = try valueToArrayString(v, arena) };
    }
    if (std.mem.eql(u8, name, "CAST_array_string")) {
        const v = args[0] orelse return Value{ .array_string = &.{} };
        switch (v) {
            .array_string => |arr| return Value{ .array_string = arr },
            .string => |s| {
                if (s.len == 0) return Value{ .array_string = &.{} };
                const out = try arena.alloc([]const u8, 1);
                out[0] = s;
                return Value{ .array_string = out };
            },
            else => {
                const out = try arena.alloc([]const u8, 1);
                out[0] = try valueToArrayString(v, arena);
                return Value{ .array_string = out };
            },
        }
    }
    if (std.mem.eql(u8, name, "toInt64") or std.mem.eql(u8, name, "toInt32")) {
        const v = args[0] orelse return null;
        return Value{ .int64 = v.toI64() orelse 0 };
    }
    if (std.mem.eql(u8, name, "toFloat64") or std.mem.eql(u8, name, "toFloat32")) {
        const v = args[0] orelse return null;
        return Value{ .float64 = v.toF64() orelse 0.0 };
    }
    if (std.mem.eql(u8, name, "if")) {
        const cond = args[0] orelse return null;
        return if (cond.bool_u8 != 0) args[1] else args[2];
    }
    if (std.mem.eql(u8, name, "coalesce") or std.mem.eql(u8, name, "ifNull")) {
        for (args) |a| if (a != null) return a;
        return null;
    }
    if (std.mem.eql(u8, name, "abs")) {
        const v = args[0] orelse return null;
        return switch (v) {
            .int64 => |i| Value{ .int64 = if (i < 0) -i else i },
            .float64 => |f| Value{ .float64 = @abs(f) },
            else => v,
        };
    }

    // ── Numeric ───────────────────────────────────────────────────────────────
    if (std.mem.eql(u8, name, "floor") or std.mem.eql(u8, name, "toInt64OrZero")) {
        const v = args[0] orelse return null;
        return switch (v) {
            .float64 => |f| Value{ .int64 = @intFromFloat(@floor(f)) },
            .int64 => v,
            .uint64 => v,
            else => Value{ .int64 = @intFromFloat(@floor(v.toF64() orelse 0.0)) },
        };
    }
    if (std.mem.eql(u8, name, "ceil") or std.mem.eql(u8, name, "ceiling")) {
        const v = args[0] orelse return null;
        return switch (v) {
            .float64 => |f| Value{ .int64 = @intFromFloat(@ceil(f)) },
            .int64 => v,
            .uint64 => v,
            else => Value{ .int64 = @intFromFloat(@ceil(v.toF64() orelse 0.0)) },
        };
    }
    if (std.mem.eql(u8, name, "round")) {
        const v = args[0] orelse return null;
        const scale: i64 = if (args.len >= 2) (if (args[1]) |s| s.toI64() orelse 0 else 0) else 0;
        const f = v.toF64() orelse return null;
        if (scale == 0) return Value{ .float64 = @round(f) };
        var factor: f64 = 1.0;
        var i: i64 = 0;
        while (i < @abs(scale)) : (i += 1) factor *= 10.0;
        return if (scale > 0)
            Value{ .float64 = @round(f * factor) / factor }
        else
            Value{ .float64 = @round(f / factor) * factor };
    }
    if (std.mem.eql(u8, name, "greatest")) {
        var best: ?Value = null;
        for (args) |a| {
            const v = a orelse continue;
            if (best == null or Value.order(v, best.?) == .gt) best = v;
        }
        return best;
    }
    if (std.mem.eql(u8, name, "least")) {
        var best: ?Value = null;
        for (args) |a| {
            const v = a orelse continue;
            if (best == null or Value.order(v, best.?) == .lt) best = v;
        }
        return best;
    }
    if (std.mem.eql(u8, name, "intDiv") or std.mem.eql(u8, name, "intdiv")) {
        const a = (args[0] orelse return null).toI64() orelse return null;
        const b = (args[1] orelse return null).toI64() orelse return null;
        if (b == 0) return null;
        return Value{ .int64 = @divTrunc(a, b) };
    }
    if (std.mem.eql(u8, name, "modulo") or std.mem.eql(u8, name, "mod")) {
        const a = (args[0] orelse return null).toI64() orelse return null;
        const b = (args[1] orelse return null).toI64() orelse return null;
        if (b == 0) return null;
        return Value{ .int64 = @rem(a, b) };
    }
    if (std.mem.eql(u8, name, "multiply") or std.mem.eql(u8, name, "times")) {
        const a = args[0] orelse return null;
        const b = args[1] orelse return null;
        if (a == .float64 or b == .float64)
            return Value{ .float64 = (a.toF64() orelse 0.0) * (b.toF64() orelse 0.0) };
        return Value{ .int64 = (a.toI64() orelse 0) *% (b.toI64() orelse 0) };
    }
    if (std.mem.eql(u8, name, "divide")) {
        const a = args[0] orelse return null;
        const b = args[1] orelse return null;
        const bv = b.toF64() orelse return null;
        if (bv == 0.0) return null;
        return Value{ .float64 = (a.toF64() orelse 0.0) / bv };
    }
    if (std.mem.eql(u8, name, "minus")) {
        const a = args[0] orelse return null;
        const b = args[1] orelse return null;
        if (a == .float64 or b == .float64)
            return Value{ .float64 = (a.toF64() orelse 0.0) - (b.toF64() orelse 0.0) };
        return Value{ .int64 = (a.toI64() orelse 0) -% (b.toI64() orelse 0) };
    }
    if (std.mem.eql(u8, name, "plus")) {
        const a = args[0] orelse return null;
        const b = args[1] orelse return null;
        if (a == .float64 or b == .float64)
            return Value{ .float64 = (a.toF64() orelse 0.0) + (b.toF64() orelse 0.0) };
        return Value{ .int64 = (a.toI64() orelse 0) +% (b.toI64() orelse 0) };
    }

    // ── Type conversion ───────────────────────────────────────────────────────
    if (std.mem.eql(u8, name, "toUInt64") or std.mem.eql(u8, name, "toUInt32") or
        std.mem.eql(u8, name, "toUInt16") or std.mem.eql(u8, name, "toUInt8"))
    {
        const v = args[0] orelse return null;
        return Value{ .uint64 = v.toU64() orelse 0 };
    }
    if (std.mem.eql(u8, name, "toUInt8OrZero") or std.mem.eql(u8, name, "toUInt64OrZero") or
        std.mem.eql(u8, name, "toUInt32OrZero"))
    {
        const v = args[0] orelse return Value{ .uint64 = 0 };
        return Value{ .uint64 = v.toU64() orelse 0 };
    }
    if (std.mem.eql(u8, name, "toFloat32") or std.mem.eql(u8, name, "toFloat64OrZero")) {
        const v = args[0] orelse return null;
        return Value{ .float64 = v.toF64() orelse 0.0 };
    }
    if (std.mem.eql(u8, name, "toStringCutToZero")) {
        const v = args[0] orelse return null;
        const s = v.toStr() orelse return Value{ .string = "" };
        // Cut at first null byte if any.
        const cut = std.mem.indexOfScalar(u8, s, 0) orelse s.len;
        return Value{ .string = s[0..cut] };
    }
    if (std.mem.eql(u8, name, "assumeNotNull") or std.mem.eql(u8, name, "identity")) {
        return args[0];
    }
    if (std.mem.eql(u8, name, "nullIf")) {
        const a = args[0] orelse return null;
        const b = args[1] orelse return null;
        return if (Value.eql(a, b)) null else a;
    }

    // ── String functions ──────────────────────────────────────────────────────
    if (std.mem.eql(u8, name, "regexp_replace") or std.mem.eql(u8, name, "replaceRegexpOne")) {
        if (args.len < 3) return null;
        const s = (args[0] orelse return null).toStr() orelse return null;
        const pattern = (args[1] orelse return null).toStr() orelse return null;
        // Fast path: the ClickBench Q29 URL-domain extraction pattern.
        // REGEXP_REPLACE(url, '^https?://(?:www\.)?([^/]+)/.*$', '\1')
        // Equivalent string logic: strip http(s)://, strip optional www., take up to first '/'.
        if (std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*$") or
            std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*"))
        {
            const after_proto = if (std.mem.startsWith(u8, s, "https://"))
                s[8..]
            else if (std.mem.startsWith(u8, s, "http://"))
                s[7..]
            else
                return Value{ .string = s }; // no match → return unchanged
            // Find the first slash (ends the host part).
            const slash = std.mem.indexOfScalar(u8, after_proto, '/') orelse
                return Value{ .string = s }; // no path → pattern doesn't match
            var host = after_proto[0..slash];
            // Strip optional leading "www."
            if (std.mem.startsWith(u8, host, "www.")) host = host[4..];
            return Value{ .string = host };
        }
        // Generic fallback: return the input string unchanged (pattern not implemented).
        return Value{ .string = s };
    }
    if (std.mem.eql(u8, name, "substring") or std.mem.eql(u8, name, "substr") or
        std.mem.eql(u8, name, "mid"))
    {
        const s = (args[0] orelse return null).toStr() orelse return null;
        const pos_raw = (args[1] orelse return null).toI64() orelse return null;
        // ClickHouse uses 1-based indexing; 0 is treated as 1.
        const start: usize = if (pos_raw <= 0) 0 else @as(usize, @intCast(pos_raw - 1));
        if (start >= s.len) return Value{ .string = "" };
        const rest = s[start..];
        if (args.len >= 3) {
            const len_raw = (args[2] orelse return null).toI64() orelse return null;
            if (len_raw <= 0) return Value{ .string = "" };
            const take = @min(rest.len, @as(usize, @intCast(len_raw)));
            return Value{ .string = rest[0..take] };
        }
        return Value{ .string = rest };
    }
    if (std.mem.eql(u8, name, "startsWith") or std.mem.eql(u8, name, "hasPrefix")) {
        const s = (args[0] orelse return null).toStr() orelse return null;
        const pfx = (args[1] orelse return null).toStr() orelse return null;
        return Value{ .bool_u8 = if (std.mem.startsWith(u8, s, pfx)) 1 else 0 };
    }
    if (std.mem.eql(u8, name, "endsWith") or std.mem.eql(u8, name, "hasSuffix")) {
        const s = (args[0] orelse return null).toStr() orelse return null;
        const sfx = (args[1] orelse return null).toStr() orelse return null;
        return Value{ .bool_u8 = if (std.mem.endsWith(u8, s, sfx)) 1 else 0 };
    }
    if (std.mem.eql(u8, name, "trimLeft") or std.mem.eql(u8, name, "ltrim")) {
        const s = (args[0] orelse return null).toStr() orelse return null;
        var i: usize = 0;
        while (i < s.len and (s[i] == ' ' or s[i] == '\t' or s[i] == '\r' or s[i] == '\n')) i += 1;
        return Value{ .string = s[i..] };
    }
    if (std.mem.eql(u8, name, "trimRight") or std.mem.eql(u8, name, "rtrim")) {
        const s = (args[0] orelse return null).toStr() orelse return null;
        var e: usize = s.len;
        while (e > 0 and (s[e - 1] == ' ' or s[e - 1] == '\t' or s[e - 1] == '\r' or s[e - 1] == '\n')) e -= 1;
        return Value{ .string = s[0..e] };
    }
    if (std.mem.eql(u8, name, "trim") or std.mem.eql(u8, name, "btrim")) {
        const s = (args[0] orelse return null).toStr() orelse return null;
        return Value{ .string = std.mem.trim(u8, s, " \t\r\n") };
    }
    if (std.mem.eql(u8, name, "replaceAll") or std.mem.eql(u8, name, "replace")) {
        const s = (args[0] orelse return null).toStr() orelse return null;
        const pat = (args[1] orelse return null).toStr() orelse return null;
        const rep = (args[2] orelse return null).toStr() orelse return null;
        if (pat.len == 0) return args[0];
        const out = try std.mem.replaceOwned(u8, arena, s, pat, rep);
        return Value{ .string = out };
    }
    if (std.mem.eql(u8, name, "position") or std.mem.eql(u8, name, "locate")) {
        // position(needle, haystack) — 1-based, 0 if not found
        if (args.len < 2) return null;
        const needle = (args[0] orelse return null).toStr() orelse return null;
        const haystack = (args[1] orelse return null).toStr() orelse return null;
        const idx = std.mem.indexOf(u8, haystack, needle);
        return Value{ .int64 = if (idx) |i| @as(i64, @intCast(i + 1)) else 0 };
    }
    if (std.mem.eql(u8, name, "positionCaseInsensitive")) {
        if (args.len < 2) return null;
        const needle = (args[0] orelse return null).toStr() orelse return null;
        const haystack = (args[1] orelse return null).toStr() orelse return null;
        const needle_l = try std.ascii.allocLowerString(arena, needle);
        const haystack_l = try std.ascii.allocLowerString(arena, haystack);
        const idx = std.mem.indexOf(u8, haystack_l, needle_l);
        return Value{ .int64 = if (idx) |i| @as(i64, @intCast(i + 1)) else 0 };
    }
    if (std.mem.eql(u8, name, "empty")) {
        const s = (args[0] orelse return null).toStr() orelse return null;
        return Value{ .bool_u8 = if (s.len == 0) 1 else 0 };
    }
    if (std.mem.eql(u8, name, "notEmpty")) {
        const s = (args[0] orelse return null).toStr() orelse return null;
        return Value{ .bool_u8 = if (s.len > 0) 1 else 0 };
    }
    if (std.mem.eql(u8, name, "concat")) {
        var out: std.ArrayListUnmanaged(u8) = .empty;
        for (args) |a| {
            const s = (a orelse continue).toStr() orelse continue;
            try out.appendSlice(arena, s);
        }
        return Value{ .string = try out.toOwnedSlice(arena) };
    }
    // format('{} {}{}', arg1, arg2, arg3, ...) — ClickHouse {} placeholder interpolation
    if (std.mem.eql(u8, name, "format")) {
        if (args.len < 1) return null;
        const fmt_str = (args[0] orelse return null).toStr() orelse return null;
        var out: std.ArrayListUnmanaged(u8) = .empty;
        var arg_idx: usize = 1;
        var i: usize = 0;
        while (i < fmt_str.len) {
            if (i + 1 < fmt_str.len and fmt_str[i] == '{' and fmt_str[i + 1] == '}') {
                // {} placeholder — substitute next arg
                if (arg_idx < args.len) {
                    const av = args[arg_idx] orelse Value{ .string = "" };
                    const s = av.toStr() orelse blk: {
                        const f = av.toF64() orelse 0.0;
                        // Print integer-valued floats without decimal point
                        if (f == @trunc(f) and @abs(f) < 1e15) {
                            break :blk try std.fmt.allocPrint(arena, "{d}", .{@as(i64, @intFromFloat(f))});
                        }
                        break :blk try std.fmt.allocPrint(arena, "{d}", .{f});
                    };
                    try out.appendSlice(arena, s);
                    arg_idx += 1;
                }
                i += 2;
            } else {
                try out.append(arena, fmt_str[i]);
                i += 1;
            }
        }
        return Value{ .string = try out.toOwnedSlice(arena) };
    }
    if (std.mem.eql(u8, name, "lowerUTF8") or std.mem.eql(u8, name, "lower")) {
        const v = args[0] orelse return null;
        const s = v.toStr() orelse return null;
        const out = try arena.dupe(u8, s);
        for (out) |*c| c.* = std.ascii.toLower(c.*);
        return Value{ .string = out };
    }
    if (std.mem.eql(u8, name, "upperUTF8")) {
        const v = args[0] orelse return null;
        const s = v.toStr() orelse return null;
        const out = try arena.dupe(u8, s);
        for (out) |*c| c.* = std.ascii.toUpper(c.*);
        return Value{ .string = out };
    }
    // tuple(...) — used as dict key wrapper; return first arg as-is (kernels just passes through)
    if (std.mem.eql(u8, name, "tuple")) {
        return if (args.len > 0) args[0] else null;
    }
    if (std.mem.eql(u8, name, "mapGet")) {
        // Blob format: varint N | N×(varint+key_bytes) | N×(varint+value_bytes)
        const blob = (args[0] orelse return Value{ .string = "" }).toStr() orelse return Value{ .string = "" };
        const key = (args[1] orelse return Value{ .string = "" }).toStr() orelse return Value{ .string = "" };
        if (blob.len == 0) return Value{ .string = "" };
        // Read count N
        const readVarUInt = struct {
            fn f(data: []const u8) ?struct { val: u64, adv: usize } {
                var v: u64 = 0;
                var shift: u6 = 0;
                var i: usize = 0;
                while (i < data.len and i < 9) {
                    const b = data[i];
                    i += 1;
                    v |= (@as(u64, b & 0x7F)) << shift;
                    if (b & 0x80 == 0) return .{ .val = v, .adv = i };
                    shift += 7;
                }
                return null;
            }
        }.f;
        const cnt_r = readVarUInt(blob) orelse return Value{ .string = "" };
        const count = cnt_r.val;
        var kp: usize = cnt_r.adv;
        var match_idx: ?u64 = null;
        for (0..count) |i| {
            const kr = readVarUInt(blob[kp..]) orelse return Value{ .string = "" };
            const klen = @as(usize, @intCast(kr.val));
            kp += kr.adv;
            if (kp + klen > blob.len) return Value{ .string = "" };
            const k = blob[kp .. kp + klen];
            if (match_idx == null and std.mem.eql(u8, k, key)) match_idx = @intCast(i);
            kp += klen;
        }
        if (match_idx == null) return Value{ .string = "" };
        var vp: usize = kp;
        for (0..count) |i| {
            const vr = readVarUInt(blob[vp..]) orelse return Value{ .string = "" };
            const vlen = @as(usize, @intCast(vr.val));
            vp += vr.adv;
            if (vp + vlen > blob.len) return Value{ .string = "" };
            if (i == match_idx.?) return Value{ .string = blob[vp .. vp + vlen] };
            vp += vlen;
        }
        return Value{ .string = "" };
    }
    // Map(String,Float64) blob: varint(N) + N×(varint_klen+key) + N×f64_le
    if (std.mem.eql(u8, name, "mapGetFloat64")) {
        const blob = (args[0] orelse return Value{ .float64 = 0.0 }).toStr() orelse return Value{ .float64 = 0.0 };
        const key = (args[1] orelse return Value{ .float64 = 0.0 }).toStr() orelse return Value{ .float64 = 0.0 };
        if (blob.len == 0) return Value{ .float64 = 0.0 };
        const readVarUInt = struct {
            fn f(data: []const u8) ?struct { val: u64, adv: usize } {
                var v: u64 = 0;
                var shift: u6 = 0;
                var i: usize = 0;
                while (i < data.len and i < 9) {
                    const b = data[i];
                    i += 1;
                    v |= (@as(u64, b & 0x7F)) << shift;
                    if (b & 0x80 == 0) return .{ .val = v, .adv = i };
                    shift += 7;
                }
                return null;
            }
        }.f;
        const cnt_r = readVarUInt(blob) orelse return Value{ .float64 = 0.0 };
        const count = @as(usize, @intCast(cnt_r.val));
        var kp: usize = cnt_r.adv;
        var match_idx: ?usize = null;
        for (0..count) |i| {
            const kr = readVarUInt(blob[kp..]) orelse return Value{ .float64 = 0.0 };
            const klen = @as(usize, @intCast(kr.val));
            kp += kr.adv;
            if (kp + klen > blob.len) return Value{ .float64 = 0.0 };
            const k = blob[kp .. kp + klen];
            if (match_idx == null and std.mem.eql(u8, k, key)) match_idx = i;
            kp += klen;
        }
        if (match_idx == null) return Value{ .float64 = 0.0 };
        // values start at kp; each is 8 bytes f64
        const val_offset = kp + match_idx.? * 8;
        if (val_offset + 8 > blob.len) return Value{ .float64 = 0.0 };
        const bits = std.mem.readInt(u64, blob[val_offset..][0..8], .little);
        return Value{ .float64 = @bitCast(bits) };
    }
    if (std.mem.eql(u8, name, "mapKeysFloat64")) {
        const blob = (args[0] orelse return Value{ .array_string = &.{} }).toStr() orelse return Value{ .array_string = &.{} };
        if (blob.len == 0) return Value{ .array_string = &.{} };
        const readVarUInt = struct {
            fn f(data: []const u8) ?struct { val: u64, adv: usize } {
                var v: u64 = 0;
                var shift: u6 = 0;
                var i: usize = 0;
                while (i < data.len and i < 9) {
                    const b = data[i];
                    i += 1;
                    v |= (@as(u64, b & 0x7F)) << shift;
                    if (b & 0x80 == 0) return .{ .val = v, .adv = i };
                    shift += 7;
                }
                return null;
            }
        }.f;
        const cnt_r = readVarUInt(blob) orelse return Value{ .array_string = &.{} };
        const count = @as(usize, @intCast(cnt_r.val));
        const keys = try arena.alloc([]const u8, count);
        var kp: usize = cnt_r.adv;
        for (0..count) |i| {
            const kr = readVarUInt(blob[kp..]) orelse return Value{ .array_string = &.{} };
            const klen = @as(usize, @intCast(kr.val));
            kp += kr.adv;
            if (kp + klen > blob.len) return Value{ .array_string = &.{} };
            keys[i] = blob[kp .. kp + klen];
            kp += klen;
        }
        return Value{ .array_string = keys };
    }
    if (std.mem.eql(u8, name, "mapValuesFloat64")) {
        const blob = (args[0] orelse return Value{ .array_string = &.{} }).toStr() orelse return Value{ .array_string = &.{} };
        if (blob.len == 0) return Value{ .array_string = &.{} };
        const readVarUInt = struct {
            fn f(data: []const u8) ?struct { val: u64, adv: usize } {
                var v: u64 = 0;
                var shift: u6 = 0;
                var i: usize = 0;
                while (i < data.len and i < 9) {
                    const b = data[i];
                    i += 1;
                    v |= (@as(u64, b & 0x7F)) << shift;
                    if (b & 0x80 == 0) return .{ .val = v, .adv = i };
                    shift += 7;
                }
                return null;
            }
        }.f;
        const cnt_r = readVarUInt(blob) orelse return Value{ .array_string = &.{} };
        const count = @as(usize, @intCast(cnt_r.val));
        var kp: usize = cnt_r.adv;
        for (0..count) |_| {
            const kr = readVarUInt(blob[kp..]) orelse return Value{ .array_string = &.{} };
            const klen = @as(usize, @intCast(kr.val));
            kp += kr.adv;
            if (kp + klen > blob.len) return Value{ .array_string = &.{} };
            kp += klen;
        }
        if (kp + count * 8 > blob.len) return Value{ .array_string = &.{} };
        const vals = try arena.alloc([]const u8, count);
        for (0..count) |i| {
            const bits = std.mem.readInt(u64, blob[kp + i * 8 ..][0..8], .little);
            vals[i] = try std.fmt.allocPrint(arena, "{d}", .{@as(f64, @bitCast(bits))});
        }
        return Value{ .array_string = vals };
    }
    if (std.mem.eql(u8, name, "splitByChar")) {
        const delim = (args[0] orelse return null).toStr() orelse return null;
        const s = (args[1] orelse return null).toStr() orelse return null;
        var parts: std.ArrayListUnmanaged([]const u8) = .empty;
        if (delim.len == 0) {
            try parts.append(arena, s);
        } else {
            var it = std.mem.splitSequence(u8, s, delim);
            while (it.next()) |p| try parts.append(arena, p);
        }
        return Value{ .array_string = try parts.toOwnedSlice(arena) };
    }
    if (std.mem.eql(u8, name, "splitByString")) {
        const delim = (args[0] orelse return null).toStr() orelse return null;
        const s = (args[1] orelse return null).toStr() orelse return null;
        var parts: std.ArrayListUnmanaged([]const u8) = .empty;
        if (delim.len == 0) {
            try parts.append(arena, s);
        } else {
            var it = std.mem.splitSequence(u8, s, delim);
            while (it.next()) |p| try parts.append(arena, p);
        }
        return Value{ .array_string = try parts.toOwnedSlice(arena) };
    }
    if (std.mem.eql(u8, name, "arrayStringConcat") or std.mem.eql(u8, name, "array_to_string")) {
        const arr = (args[0] orelse return Value{ .string = "" });
        const sep: []const u8 = if (args.len >= 2)
            (if (args[1]) |sv| sv.toStr() orelse "" else "")
        else
            "";
        switch (arr) {
            .array_string => |elems| {
                var out: std.ArrayListUnmanaged(u8) = .empty;
                for (elems, 0..) |e, i| {
                    if (i > 0) try out.appendSlice(arena, sep);
                    try out.appendSlice(arena, e);
                }
                return Value{ .string = try out.toOwnedSlice(arena) };
            },
            else => return Value{ .string = arr.toStr() orelse "" },
        }
    }
    if (std.mem.eql(u8, name, "has")) {
        const arr = (args[0] orelse return Value{ .bool_u8 = 0 });
        const needle = args[1] orelse return Value{ .bool_u8 = 0 };
        switch (arr) {
            .array_string => |elems| {
                const ns = needle.toStr() orelse return Value{ .bool_u8 = 0 };
                for (elems) |e| if (std.mem.eql(u8, e, ns)) return Value{ .bool_u8 = 1 };
                return Value{ .bool_u8 = 0 };
            },
            else => return Value{ .bool_u8 = 0 },
        }
    }
    if (std.mem.eql(u8, name, "indexOf")) {
        const arr = (args[0] orelse return Value{ .int64 = 0 });
        const needle = args[1] orelse return Value{ .int64 = 0 };
        switch (arr) {
            .array_string => |elems| {
                const ns = needle.toStr() orelse return Value{ .int64 = 0 };
                for (elems, 0..) |e, i| if (std.mem.eql(u8, e, ns)) return Value{ .int64 = @intCast(i + 1) };
                return Value{ .int64 = 0 };
            },
            else => return Value{ .int64 = 0 },
        }
    }
    if (std.mem.eql(u8, name, "arrayReverse")) {
        const arr = args[0] orelse return null;
        switch (arr) {
            .array_string => |elems| {
                const copy = try arena.dupe([]const u8, elems);
                std.mem.reverse([]const u8, copy);
                return Value{ .array_string = copy };
            },
            else => return arr,
        }
    }
    if (std.mem.eql(u8, name, "arrayDistinct")) {
        const arr = args[0] orelse return null;
        switch (arr) {
            .array_string => |elems| {
                var seen = std.StringHashMapUnmanaged(void){};
                var out: std.ArrayListUnmanaged([]const u8) = .empty;
                for (elems) |e| {
                    if (seen.contains(e)) continue;
                    try seen.put(arena, e, {});
                    try out.append(arena, e);
                }
                return Value{ .array_string = try out.toOwnedSlice(arena) };
            },
            else => return arr,
        }
    }
    if (std.mem.eql(u8, name, "arraySlice")) {
        const arr = args[0] orelse return null;
        const off_raw = (args[1] orelse return null).toI64() orelse return null;
        switch (arr) {
            .array_string => |elems| {
                const start: usize = if (off_raw <= 0) 0 else @min(elems.len, @as(usize, @intCast(off_raw - 1)));
                const rest = elems[start..];
                if (args.len >= 3) {
                    const len_raw = (args[2] orelse return null).toI64() orelse return null;
                    if (len_raw <= 0) return Value{ .array_string = &.{} };
                    const take = @min(rest.len, @as(usize, @intCast(len_raw)));
                    return Value{ .array_string = rest[0..take] };
                }
                return Value{ .array_string = rest };
            },
            else => return arr,
        }
    }
    if (std.mem.eql(u8, name, "length") or std.mem.eql(u8, name, "char_length")) {
        const v = args[0] orelse return null;
        return switch (v) {
            .string => |s| Value{ .int64 = @intCast(s.len) },
            .array_string => |a| Value{ .int64 = @intCast(a.len) },
            else => Value{ .int64 = 0 },
        };
    }

    // ── Date functions ────────────────────────────────────────────────────────
    // Date values are stored as days-since-epoch (u16) or ms-since-epoch (i64).
    if (std.mem.eql(u8, name, "toYear")) {
        const v = args[0] orelse return null;
        const days: i64 = switch (v) {
            .date_u16 => |d| @intCast(d),
            .datetime64_ms => |ms| @divTrunc(ms, 86400000),
            .int64 => |i| i,
            .uint64 => |u| @intCast(u),
            else => return null,
        };
        const epoch_year: i64 = 1970;
        var y = epoch_year;
        var remaining = days;
        while (true) {
            const leap = (@mod(y, 4) == 0 and (@mod(y, 100) != 0 or @mod(y, 400) == 0));
            const days_in_year: i64 = if (leap) 366 else 365;
            if (remaining < days_in_year) break;
            remaining -= days_in_year;
            y += 1;
        }
        return Value{ .uint64 = @intCast(y) };
    }
    if (std.mem.eql(u8, name, "toYYYYMMDD") or std.mem.eql(u8, name, "toyyyymmdd")) {
        const v = args[0] orelse return null;
        const days: i64 = switch (v) {
            .date_u16 => |d| @intCast(d),
            .datetime64_ms => |ms| @divTrunc(ms, 86400000),
            .int64 => |i| i,
            .uint64 => |u| @intCast(u),
            else => return null,
        };
        const ymd = daysToYMD(days);
        const result: u64 = @as(u64, @intCast(ymd[0])) * 10000 + @as(u64, @intCast(ymd[1])) * 100 + @as(u64, @intCast(ymd[2]));
        return Value{ .uint64 = result };
    }
    if (std.mem.eql(u8, name, "toMonth")) {
        const v = args[0] orelse return null;
        const days: i64 = switch (v) {
            .date_u16 => |d| @intCast(d),
            .datetime64_ms => |ms| @divTrunc(ms, 86400000),
            .int64 => |i| i,
            else => return null,
        };
        const ymd = daysToYMD(days);
        return Value{ .uint64 = @intCast(ymd[1]) };
    }
    if (std.mem.eql(u8, name, "toDayOfMonth") or std.mem.eql(u8, name, "toDayOfWeek")) {
        const v = args[0] orelse return null;
        const days: i64 = switch (v) {
            .date_u16 => |d| @intCast(d),
            .datetime64_ms => |ms| @divTrunc(ms, 86400000),
            .int64 => |i| i,
            else => return null,
        };
        if (std.mem.eql(u8, name, "toDayOfMonth")) {
            const ymd = daysToYMD(days);
            return Value{ .uint64 = @intCast(ymd[2]) };
        }
        // toDayOfWeek: 1=Monday … 7=Sunday (ISO)
        const dow = @mod(days + 3, 7) + 1; // epoch (1970-01-01) was Thursday=4
        return Value{ .uint64 = @intCast(dow) };
    }
    if (std.mem.eql(u8, name, "toUnixTimestamp")) {
        const v = args[0] orelse return null;
        return switch (v) {
            .datetime64_ms => |ms| Value{ .int64 = @divTrunc(ms, 1000) },
            .date_u16 => |d| Value{ .int64 = @as(i64, d) * 86400 },
            .int64 => v,
            .uint64 => |u| Value{ .int64 = @intCast(u) },
            else => null,
        };
    }
    if (std.mem.eql(u8, name, "toDate") or std.mem.eql(u8, name, "toDateOrZero")) {
        const v = args[0] orelse return Value{ .date_u16 = 0 };
        return switch (v) {
            .date_u16 => v,
            .datetime64_ms => |ms| Value{ .date_u16 = @truncate(@as(u64, @intCast(@max(0, @divTrunc(ms, 86400000))))) },
            .int64 => |i| if (i < 0 or i > 65535) Value{ .date_u16 = 0 } else Value{ .date_u16 = @intCast(i) },
            .string, .bool_u8 => blk: {
                const s = v.toStr() orelse break :blk Value{ .date_u16 = 0 };
                const d = parseDateStr(s) orelse break :blk Value{ .date_u16 = 0 };
                break :blk Value{ .date_u16 = d };
            },
            else => Value{ .date_u16 = 0 },
        };
    }
    if (std.mem.eql(u8, name, "toStartOfHour")) {
        const v = args[0] orelse return null;
        const ms: i64 = switch (v) {
            .datetime64_ms => |m| m,
            .int64 => |i| i * 1000,
            else => return null,
        };
        const hour_ms: i64 = 3600 * 1000;
        return Value{ .datetime64_ms = @divTrunc(ms, hour_ms) * hour_ms };
    }
    if (std.mem.eql(u8, name, "toStartOfMinute")) {
        const v = args[0] orelse return null;
        const ms: i64 = switch (v) {
            .datetime64_ms => |m| m,
            .int64 => |i| i * 1000,
            else => return null,
        };
        const minute_ms: i64 = 60 * 1000;
        return Value{ .datetime64_ms = @divTrunc(ms, minute_ms) * minute_ms };
    }
    if (std.mem.eql(u8, name, "toStartOfDay")) {
        const v = args[0] orelse return null;
        const ms: i64 = switch (v) {
            .datetime64_ms => |m| m,
            .int64 => |i| i * 1000,
            else => return null,
        };
        const day_ms: i64 = 86400 * 1000;
        return Value{ .datetime64_ms = @divTrunc(ms, day_ms) * day_ms };
    }
    // date_part(unit_str, ts) / extract(unit FROM ts) → integer component
    if (std.mem.eql(u8, name, "date_part") and args.len == 2) {
        const unit_val = args[0] orelse return null;
        const ts_val = args[1] orelse return null;
        const unit_str: []const u8 = switch (unit_val) {
            .string => |s| s,
            else => return null,
        };
        const ms: i64 = switch (ts_val) {
            .datetime64_ms => |m| m,
            .int64 => |i| i * 1000,
            else => return null,
        };
        const secs = @divTrunc(ms, 1000);
        if (std.mem.eql(u8, unit_str, "minute") or std.mem.eql(u8, unit_str, "min")) {
            return Value{ .int64 = @mod(@divTrunc(secs, 60), 60) };
        }
        if (std.mem.eql(u8, unit_str, "hour")) {
            return Value{ .int64 = @mod(@divTrunc(secs, 3600), 24) };
        }
        if (std.mem.eql(u8, unit_str, "day") or std.mem.eql(u8, unit_str, "dayofmonth")) {
            const days = @divTrunc(ms, 86400 * 1000);
            const ymd = daysToYMD(if (days >= 0) days else 0);
            return Value{ .int64 = @intCast(ymd[2]) };
        }
        if (std.mem.eql(u8, unit_str, "month")) {
            const days = @divTrunc(ms, 86400 * 1000);
            const ymd = daysToYMD(if (days >= 0) days else 0);
            return Value{ .int64 = @intCast(ymd[1]) };
        }
        if (std.mem.eql(u8, unit_str, "year")) {
            const days = @divTrunc(ms, 86400 * 1000);
            const ymd = daysToYMD(if (days >= 0) days else 0);
            return Value{ .int64 = @intCast(ymd[0]) };
        }
        return null;
    }
    if (std.mem.eql(u8, name, "now")) {
        var ts: std.posix.timespec = undefined;
        switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
            .SUCCESS => return Value{ .datetime64_ms = @as(i64, ts.sec) * 1000 + @divTrunc(ts.nsec, 1_000_000) },
            else => return Value{ .datetime64_ms = 0 },
        }
    }
    if (std.mem.eql(u8, name, "today")) {
        var ts: std.posix.timespec = undefined;
        const secs: i64 = switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
            .SUCCESS => ts.sec,
            else => 0,
        };
        const days: u16 = @truncate(@as(u64, @intCast(@max(0, @divTrunc(secs, 86400)))));
        return Value{ .date_u16 = days };
    }

    // ── Math ──────────────────────────────────────────────────────────────────
    if (std.mem.eql(u8, name, "sqrt")) {
        const v = args[0] orelse return null;
        const x = v.toF64() orelse return null;
        return Value{ .float64 = @sqrt(x) };
    }

    // ── Time component extractors ─────────────────────────────────────────────
    if (std.mem.eql(u8, name, "toHour") or std.mem.eql(u8, name, "hour")) {
        const v = args[0] orelse return null;
        const ms: i64 = switch (v) {
            .datetime64_ms => |m| m,
            .int64 => |i| i * 1000,
            .date_u16 => |d| @as(i64, d) * 86400000,
            else => return null,
        };
        const secs = @divTrunc(ms, 1000);
        return Value{ .int64 = @mod(@divTrunc(secs, 3600), 24) };
    }
    if (std.mem.eql(u8, name, "toMinute")) {
        const v = args[0] orelse return null;
        const ms: i64 = switch (v) {
            .datetime64_ms => |m| m,
            .int64 => |i| i * 1000,
            else => return null,
        };
        const secs = @divTrunc(ms, 1000);
        return Value{ .int64 = @mod(@divTrunc(secs, 60), 60) };
    }
    if (std.mem.eql(u8, name, "toSecond")) {
        const v = args[0] orelse return null;
        const ms: i64 = switch (v) {
            .datetime64_ms => |m| m,
            .int64 => |i| i * 1000,
            else => return null,
        };
        return Value{ .int64 = @mod(@divTrunc(ms, 1000), 60) };
    }

    // ── Interval constructors (return ms as int64) ────────────────────────────
    if (std.mem.eql(u8, name, "toIntervalSecond")) {
        const v = args[0] orelse return null;
        const n = v.toI64() orelse return null;
        return Value{ .int64 = n * 1_000 };
    }
    if (std.mem.eql(u8, name, "toIntervalMinute")) {
        const v = args[0] orelse return null;
        const n = v.toI64() orelse return null;
        return Value{ .int64 = n * 60_000 };
    }
    if (std.mem.eql(u8, name, "toIntervalHour")) {
        const v = args[0] orelse return null;
        const n = v.toI64() orelse return null;
        return Value{ .int64 = n * 3_600_000 };
    }
    if (std.mem.eql(u8, name, "toIntervalDay")) {
        const v = args[0] orelse return null;
        const n = v.toI64() orelse return null;
        return Value{ .int64 = n * 86_400_000 };
    }
    if (std.mem.eql(u8, name, "toIntervalWeek")) {
        const v = args[0] orelse return null;
        const n = v.toI64() orelse return null;
        return Value{ .int64 = n * 7 * 86_400_000 };
    }
    if (std.mem.eql(u8, name, "toIntervalMonth")) {
        const v = args[0] orelse return null;
        const n = v.toI64() orelse return null;
        return Value{ .int64 = n * 30 * 86_400_000 };
    }
    if (std.mem.eql(u8, name, "toIntervalYear")) {
        const v = args[0] orelse return null;
        const n = v.toI64() orelse return null;
        return Value{ .int64 = n * 365 * 86_400_000 };
    }

    // ── Array element access ───────────────────────────────────────────────────
    if (std.mem.eql(u8, name, "arrayElement")) {
        if (args.len < 2) return null;
        const arr_val = args[0] orelse return null;
        const idx_val = args[1] orelse return null;
        const arr = switch (arr_val) {
            .array_string => |s| s,
            else => return null,
        };
        const idx = idx_val.toI64() orelse return null;
        // 1-based index; negative indexes from end
        const len: i64 = @intCast(arr.len);
        const pos: i64 = if (idx > 0) idx - 1 else len + idx;
        if (pos < 0 or pos >= len) return null;
        return Value{ .string = arr[@intCast(pos)] };
    }

    // ── Array scalar aggregates ────────────────────────────────────────────────
    if (std.mem.eql(u8, name, "arraySum")) {
        const v = args[0] orelse return null;
        const arr = switch (v) {
            .array_string => |s| s,
            else => return null,
        };
        var total: f64 = 0;
        for (arr) |elem| total += std.fmt.parseFloat(f64, elem) catch 0;
        return Value{ .float64 = total };
    }
    if (std.mem.eql(u8, name, "arrayAvg")) {
        const v = args[0] orelse return null;
        const arr = switch (v) {
            .array_string => |s| s,
            else => return null,
        };
        if (arr.len == 0) return Value{ .float64 = 0 };
        var total: f64 = 0;
        for (arr) |elem| total += std.fmt.parseFloat(f64, elem) catch 0;
        return Value{ .float64 = total / @as(f64, @floatFromInt(arr.len)) };
    }

    // ── Array constructor: array(v1, v2, ...) ─────────────────────────────────
    if (std.mem.eql(u8, name, "array")) {
        const items = try arena.alloc([]const u8, args.len);
        for (args, 0..) |av, i| {
            if (av) |vv| {
                items[i] = vv.toStr() orelse try std.fmt.allocPrint(arena, "{d}", .{vv.toF64() orelse 0.0});
            } else {
                items[i] = "";
            }
        }
        return Value{ .array_string = items };
    }

    // ── arrayIntersect(arr1, arr2) ────────────────────────────────────────────
    if (std.mem.eql(u8, name, "arrayIntersect")) {
        if (args.len < 2) return Value{ .array_string = &.{} };
        const a1 = switch (args[0] orelse return Value{ .array_string = &.{} }) {
            .array_string => |s| s,
            else => return Value{ .array_string = &.{} },
        };
        const a2 = switch (args[1] orelse return Value{ .array_string = &.{} }) {
            .array_string => |s| s,
            else => return Value{ .array_string = &.{} },
        };
        var out: std.ArrayListUnmanaged([]const u8) = .empty;
        for (a1) |elem| {
            for (a2) |e2| {
                if (std.mem.eql(u8, elem, e2)) {
                    try out.append(arena, elem);
                    break;
                }
            }
        }
        return Value{ .array_string = try out.toOwnedSlice(arena) };
    }

    // ── Conditional ───────────────────────────────────────────────────────────
    if (std.mem.eql(u8, name, "multiIf")) {
        // multiIf(cond1, then1, cond2, then2, ..., else)
        var i: usize = 0;
        while (i + 1 < args.len) : (i += 2) {
            const cond = args[i] orelse continue;
            const is_true = switch (cond) {
                .bool_u8 => |b| b != 0,
                .int64 => |v2| v2 != 0,
                .uint64 => |v2| v2 != 0,
                else => false,
            };
            if (is_true) return args[i + 1];
        }
        // last arg is the else branch
        return if (args.len % 2 == 1) args[args.len - 1] else null;
    }

    // ── IP functions ──────────────────────────────────────────────────────────
    // risk_score(protocol, features) — stub returning 0.0 (matches generic_executor behavior)
    if (std.mem.eql(u8, name, "risk_score")) return Value{ .float64 = 0.0 };

    if (std.mem.eql(u8, name, "IPv4StringToNumOrDefault") or
        std.mem.eql(u8, name, "IPv4StringToNumOrZero"))
    {
        const s = (args[0] orelse return Value{ .uint64 = 0 }).toStr() orelse return Value{ .uint64 = 0 };
        return Value{ .uint64 = parseIPv4(s) orelse 0 };
    }
    if (std.mem.eql(u8, name, "isIPv4String")) {
        const s = (args[0] orelse return Value{ .bool_u8 = 0 }).toStr() orelse return Value{ .bool_u8 = 0 };
        return Value{ .bool_u8 = if (parseIPv4(s) != null) 1 else 0 };
    }
    if (std.mem.eql(u8, name, "isIPv6String")) {
        const s = (args[0] orelse return Value{ .bool_u8 = 0 }).toStr() orelse return Value{ .bool_u8 = 0 };
        return Value{ .bool_u8 = if (isIPv6(s)) 1 else 0 };
    }
    if (std.mem.eql(u8, name, "IPv6StringToNumOrDefault") or
        std.mem.eql(u8, name, "IPv6StringToNumOrZero"))
    {
        // Stub: return 0 (16-byte representation not implemented yet)
        return Value{ .uint64 = 0 };
    }
    if (std.mem.eql(u8, name, "toString")) {
        const v = args[0] orelse return null;
        if (v.toStr()) |s| return Value{ .string = s };
        return Value{ .string = try valueToArrayString(v, arena) };
    }

    if (std.mem.eql(u8, name, "hasAny")) {
        const arr = args[0] orelse return Value{ .bool_u8 = 0 };
        const need = args[1] orelse return Value{ .bool_u8 = 0 };
        const haystack = switch (arr) {
            .array_string => |a| a,
            else => return Value{ .bool_u8 = 0 },
        };
        const needles = switch (need) {
            .array_string => |a| a,
            else => return Value{ .bool_u8 = 0 },
        };
        for (needles) |n| for (haystack) |h| if (std.mem.eql(u8, h, n)) return Value{ .bool_u8 = 1 };
        return Value{ .bool_u8 = 0 };
    }
    if (std.mem.eql(u8, name, "hasAll")) {
        const arr = args[0] orelse return Value{ .bool_u8 = 0 };
        const need = args[1] orelse return Value{ .bool_u8 = 0 };
        const haystack = switch (arr) {
            .array_string => |a| a,
            else => return Value{ .bool_u8 = 0 },
        };
        const needles = switch (need) {
            .array_string => |a| a,
            else => return Value{ .bool_u8 = 0 },
        };
        for (needles) |n| {
            var found = false;
            for (haystack) |h| if (std.mem.eql(u8, h, n)) {
                found = true;
                break;
            };
            if (!found) return Value{ .bool_u8 = 0 };
        }
        return Value{ .bool_u8 = 1 };
    }
    if (std.mem.eql(u8, name, "arrayConcat")) {
        var out: std.ArrayListUnmanaged([]const u8) = .empty;
        for (args) |a| {
            if (a == null) continue;
            switch (a.?) {
                .array_string => |elems| try out.appendSlice(arena, elems),
                else => {},
            }
        }
        return Value{ .array_string = try out.toOwnedSlice(arena) };
    }
    if (std.mem.eql(u8, name, "arrayFlatten")) {
        // Flatten one level: array of arrays → flat array (we store as array_string)
        const arr = args[0] orelse return Value{ .array_string = &.{} };
        return arr; // already flat in our representation
    }
    if (std.mem.eql(u8, name, "arrayMax")) {
        const arr = args[0] orelse return null;
        switch (arr) {
            .array_string => |elems| {
                if (elems.len == 0) return null;
                var all_numeric = true;
                var best_num = std.fmt.parseFloat(f64, elems[0]) catch blk: {
                    all_numeric = false;
                    break :blk 0.0;
                };
                var best: []const u8 = elems[0];
                if (all_numeric) {
                    for (elems[1..]) |e| {
                        const n = std.fmt.parseFloat(f64, e) catch {
                            all_numeric = false;
                            break;
                        };
                        if (n > best_num) {
                            best_num = n;
                            best = e;
                        }
                    }
                }
                if (!all_numeric) {
                    best = elems[0];
                    for (elems[1..]) |e| if (std.mem.lessThan(u8, best, e)) {
                        best = e;
                    };
                }
                return Value{ .string = best };
            },
            else => return null,
        }
    }
    if (std.mem.eql(u8, name, "arrayMin")) {
        const arr = args[0] orelse return null;
        switch (arr) {
            .array_string => |elems| {
                if (elems.len == 0) return null;
                var best: []const u8 = elems[0];
                for (elems[1..]) |e| if (std.mem.lessThan(u8, e, best)) {
                    best = e;
                };
                return Value{ .string = best };
            },
            else => return null,
        }
    }
    // mapKeys / mapValues — our Map blob is raw bytes; return empty array
    // (real parsing of the Map blob is complex; stub with empty for now)
    if (std.mem.eql(u8, name, "mapKeys") or std.mem.eql(u8, name, "mapValues")) {
        return Value{ .array_string = &.{} };
    }

    // Unknown function — return an error so callers can surface it.
    return error.UnknownFunction;
}
// ── Date helpers ──────────────────────────────────────────────────────────────

/// Convert days-since-epoch (1970-01-01) to [year, month, day].
pub fn daysToYMD(days: i64) [3]u32 {
    // Proleptic Gregorian calendar algorithm.
    const n = days + 719468; // shift to era starting 2000-03-01
    const era: i64 = if (n >= 0) @divFloor(n, 146097) else @divFloor(n - 146096, 146097);
    const doe: u64 = @intCast(n - era * 146097); // day-of-era [0, 146096]
    const yoe: u64 = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const y: i64 = @as(i64, @intCast(yoe)) + era * 400;
    const doy: u64 = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const mp: u64 = (5 * doy + 2) / 153;
    const d: u32 = @intCast(doy - (153 * mp + 2) / 5 + 1);
    const m: u32 = if (mp < 10) @intCast(mp + 3) else @intCast(mp - 9);
    const yr: i64 = y + (if (m <= 2) @as(i64, 1) else 0);
    return .{ @intCast(yr), m, d };
}

/// Parse "YYYY-MM-DD" → days since 1970-01-01, or null.
fn parseDateStr(s: []const u8) ?u16 {
    if (s.len < 10 or s[4] != '-' or s[7] != '-') return null;
    const y = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
    const m = std.fmt.parseInt(u32, s[5..7], 10) catch return null;
    const d = std.fmt.parseInt(u32, s[8..10], 10) catch return null;
    // Compute days since 1970-01-01 using Gregorian formula.
    var yr: i32 = y;
    var mo: i32 = @intCast(m);
    if (mo <= 2) {
        yr -= 1;
        mo += 9;
    } else {
        mo -= 3;
    }
    const era: i32 = @divFloor(yr, 400);
    const yoe: i32 = yr - era * 400;
    const doy: i32 = @divFloor(153 * mo + 2, 5) + @as(i32, @intCast(d)) - 1;
    const doe: i32 = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    const days: i32 = era * 146097 + doe - 719468;
    if (days < 0 or days > 65535) return null;
    return @intCast(days);
}

/// Parse dotted decimal IPv4 string, return packed u32 as u64.
fn parseIPv4(s: []const u8) ?u64 {
    var octets: [4]u8 = undefined;
    var it = std.mem.splitScalar(u8, s, '.');
    var i: usize = 0;
    while (it.next()) |octet| {
        if (i >= 4) return null;
        octets[i] = std.fmt.parseInt(u8, octet, 10) catch return null;
        i += 1;
    }
    if (i != 4) return null;
    const n: u32 = (@as(u32, octets[0]) << 24) | (@as(u32, octets[1]) << 16) |
        (@as(u32, octets[2]) << 8) | @as(u32, octets[3]);
    return @intCast(n);
}

/// Very basic IPv6 check: contains ':' and no '.'.
fn isIPv6(s: []const u8) bool {
    return std.mem.indexOfScalar(u8, s, ':') != null and
        std.mem.indexOfScalar(u8, s, '.') == null;
}

/// Format 16 raw bytes as an IPv6 address string (RFC 5952 / ClickHouse format).
/// IPv4-mapped addresses (::ffff:x.x.x.x) use mixed notation.
fn ipv6BytesToStr(bytes: []const u8, arena: std.mem.Allocator) ![]const u8 {
    if (bytes.len != 16) return bytes;

    // IPv4-mapped: first 10 bytes zero, bytes 10-11 = 0xff 0xff
    const is_ipv4_mapped = blk: {
        for (bytes[0..10]) |b| if (b != 0) break :blk false;
        break :blk bytes[10] == 0xff and bytes[11] == 0xff;
    };
    if (is_ipv4_mapped) {
        return std.fmt.allocPrint(arena, "::ffff:{d}.{d}.{d}.{d}", .{
            bytes[12], bytes[13], bytes[14], bytes[15],
        });
    }

    // Build groups of 16-bit words
    var groups: [8]u16 = undefined;
    for (&groups, 0..) |*g, i| {
        g.* = (@as(u16, bytes[i * 2]) << 8) | bytes[i * 2 + 1];
    }
    // Find longest run of consecutive zero groups (length >= 2) for :: compression
    var best_start: usize = 8; // 8 = none
    var best_len: usize = 0;
    var run_start: usize = 0;
    var run_len: usize = 0;
    for (groups, 0..) |g, i| {
        if (g == 0) {
            if (run_len == 0) run_start = i;
            run_len += 1;
            if (run_len > best_len and run_len >= 2) {
                best_len = run_len;
                best_start = run_start;
            }
        } else {
            run_len = 0;
        }
    }
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    var i: usize = 0;
    var after_dc = false;
    while (i < 8) {
        if (best_len >= 2 and i == best_start) {
            try buf.appendSlice(arena, "::");
            i += best_len;
            after_dc = true;
            continue;
        }
        if (i > 0 and !after_dc) try buf.append(arena, ':');
        after_dc = false;
        const hex = try std.fmt.allocPrint(arena, "{x}", .{groups[i]});
        try buf.appendSlice(arena, hex);
        i += 1;
    }
    return buf.toOwnedSlice(arena);
}

fn castValue(v: Value, to: ColumnType, arena: std.mem.Allocator) !?Value {
    return switch (to) {
        .int64 => if (v.toI64()) |i| Value{ .int64 = i } else null,
        .uint64 => if (v.toU64()) |u| Value{ .uint64 = u } else null,
        .float64 => if (v.toF64()) |f| Value{ .float64 = f } else null,
        .bool_u8 => if (v.toI64()) |i| Value{ .bool_u8 = if (i != 0) 1 else 0 } else null,
        .date_u16 => if (v.toI64()) |i| blk: {
            // Days since epoch must fit in u16 (0..65535).
            // Negative values and out-of-range values produce NULL.
            if (i < 0 or i > 65535) break :blk null;
            break :blk Value{ .date_u16 = @intCast(i) };
        } else null,
        .datetime64_ms => if (v.toI64()) |i| Value{ .datetime64_ms = i } else null,
        .string => Value{ .string = try valueToArrayString(v, arena) },
        .array_string => null, // no scalar cast to array
    };
}

pub fn valueToArrayString(v: Value, arena: std.mem.Allocator) ![]const u8 {
    if (v.toStr()) |s| return s;
    return switch (v) {
        .bool_u8 => |b| if (b != 0) "1" else "0",
        .int64 => |i| try std.fmt.allocPrint(arena, "{d}", .{i}),
        .uint64 => |u| try std.fmt.allocPrint(arena, "{d}", .{u}),
        .float64 => |f| try std.fmt.allocPrint(arena, "{d}", .{f}),
        .date_u16 => |d| try std.fmt.allocPrint(arena, "{d}", .{d}),
        .datetime64_ms => |ts| try std.fmt.allocPrint(arena, "{d}", .{ts}),
        .array_string => "",
        .string => unreachable,
    };
}

// ── Aggregate update ──────────────────────────────────────────────────────────

/// Update a single accumulator with a new value `v`.
/// `v` is null if the row's value is NULL (most aggregates skip NULLs).
pub fn updateAccum(accum: *AggAccum, v: ?Value, arena: std.mem.Allocator) !void {
    switch (accum.*) {
        .count => accum.count += 1, // count(*) always increments
        .i64_sum => if (v) |val| {
            if (val.toI64()) |i| accum.i64_sum +%= i;
        },
        .u64_sum => if (v) |val| {
            if (val.toU64()) |u| accum.u64_sum +%= u;
        },
        .f64_sum => if (v) |val| {
            if (val.toF64()) |f| accum.f64_sum += f;
        },
        .f64_avg => if (v) |val| {
            if (val.toF64()) |f| {
                accum.f64_avg.sum += f;
                accum.f64_avg.count += 1;
            }
        },
        .i64_min => if (v) |val| {
            if (val.toI64()) |i| {
                if (i < accum.i64_min) accum.i64_min = i;
            }
        },
        .i64_max => if (v) |val| {
            if (val.toI64()) |i| {
                if (i > accum.i64_max) accum.i64_max = i;
            }
        },
        .u64_min => if (v) |val| {
            if (val.toU64()) |u| {
                if (u < accum.u64_min) accum.u64_min = u;
            }
        },
        .u64_max => if (v) |val| {
            if (val.toU64()) |u| {
                if (u > accum.u64_max) accum.u64_max = u;
            }
        },
        .f64_min => if (v) |val| {
            if (val.toF64()) |f| {
                if (f < accum.f64_min) accum.f64_min = f;
            }
        },
        .f64_max => if (v) |val| {
            if (val.toF64()) |f| {
                if (f > accum.f64_max) accum.f64_max = f;
            }
        },
        .str_min => if (v) |val| {
            if (val.toStr()) |s| {
                // Borrow the pointer directly — string slices from DataChunk columns
                // point into store memory (mmapped) or chunk arenas that are not freed
                // during hash_agg / scalar_agg processing.  No dupe needed.
                if (accum.str_min) |cur| {
                    if (std.mem.lessThan(u8, s, cur)) accum.str_min = s;
                } else accum.str_min = s;
            }
        },
        .str_max => if (v) |val| {
            if (val.toStr()) |s| {
                if (accum.str_max) |cur| {
                    if (std.mem.lessThan(u8, cur, s)) accum.str_max = s;
                } else accum.str_max = s;
            }
        },
        .uniq_strs => if (v) |val| {
            const s = try valueToArrayString(val, arena);
            const owned = try arena.dupe(u8, s);
            try accum.uniq_strs.put(arena, owned, {});
        },
        .array_strs => if (v) |val| {
            const s = try valueToArrayString(val, arena);
            try accum.array_strs.append(arena, try arena.dupe(u8, s));
        },
        .any_val => if (accum.any_val == null) {
            accum.any_val = v;
        },
        .distinct_u64 => if (v) |val| {
            // Hash the value to a u64 key for distinct tracking.
            // For numeric types, use the raw bits; for strings, use Wyhash.
            // IMPORTANT: use c_allocator (not the arena) so that the hashmap
            // backing storage outlives any arena deinit in the parallel path.
            const key: u64 = switch (val) {
                .int64 => |i| @bitCast(i),
                .uint64 => |u| u,
                .float64 => |f| @bitCast(f),
                .string => |s| std.hash.Wyhash.hash(0, s),
                else => 0,
            };
            try accum.distinct_u64.put(std.heap.c_allocator, key, {});
        },
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test {
    _ = @import("kernels_test.zig");
}

// ── Dictionary function evaluation ───────────────────────────────────────────

fn evalDictCall(dc: *const plan.DictCall, row: []const ?Value, lambda_val: ?Value, arena: std.mem.Allocator) !?Value {
    const store = dict_store orelse {
        // No dict store — return safe defaults.
        if (std.mem.eql(u8, dc.fn_name, "dictHas")) return Value{ .bool_u8 = 0 };
        if (dc.default_expr) |de| return evalExpr(de, row, lambda_val, arena);
        return Value{ .string = "" };
    };

    // Evaluate key expressions to strings.
    const key_strs = try arena.alloc([:0]const u8, dc.keys.len);
    for (dc.keys, 0..) |key_expr, i| {
        const kv = try evalExpr(key_expr, row, lambda_val, arena);
        const ks = if (kv) |v| v.toStr() orelse "" else "";
        key_strs[i] = try arena.dupeZ(u8, ks);
    }
    // Build C-string pointer array.
    const keys_ptrs = try arena.alloc([*:0]const u8, key_strs.len);
    for (key_strs, 0..) |ks, i| keys_ptrs[i] = ks.ptr;

    const dict_namez = try arena.dupeZ(u8, dc.dict_name);

    if (std.mem.eql(u8, dc.fn_name, "dictHas")) {
        const has_fn = dict_has_fn orelse return Value{ .bool_u8 = 0 };
        const result = has_fn(store, dict_namez.ptr, keys_ptrs.ptr, key_strs.len);
        return Value{ .bool_u8 = result };
    }

    // dictGet / dictGetOrDefault / dictGetOrNull
    const get_fn = dict_get_fn orelse {
        if (dc.default_expr) |de| return evalExpr(de, row, lambda_val, arena);
        return null;
    };
    const attr_namez = try arena.dupeZ(u8, dc.attr_name orelse "");
    const result_ptr = get_fn(store, dict_namez.ptr, attr_namez.ptr, keys_ptrs.ptr, key_strs.len);

    if (result_ptr) |ptr| {
        const s = std.mem.sliceTo(ptr, 0);
        if (dc.attr_name) |attr| {
            if (std.ascii.eqlIgnoreCase(attr, "score")) {
                return Value{ .float64 = std.fmt.parseFloat(f64, s) catch 0.0 };
            }
        }
        const out = try arena.dupe(u8, s);
        return Value{ .string = out };
    }
    // No result — return default or null.
    if (dc.default_expr) |de| return evalExpr(de, row, lambda_val, arena);
    return null;
}

// ── Batch predicate evaluation ────────────────────────────────────────────────

/// Evaluate a boolean Expr over all rows in `dc`, writing 1/0 into `out`.
///
/// Fast path: a simple comparison (col op literal) on a fixed-width integer
/// column is dispatched to SIMD kernels. All other shapes fall back to the
/// scalar evalExpr path row-by-row.
///
/// `out` must be pre-allocated with length == dc.num_rows.
/// Returns an error only if the scalar fallback path does.
pub fn evalExprBatch(
    expr: Expr,
    dc: DataChunk,
    out: []i16,
    arena: std.mem.Allocator,
) anyerror!void {
    std.debug.assert(out.len == dc.num_rows);
    if (dc.num_rows == 0) return;

    // Try to match a simple col_ref cmp literal pattern for SIMD fast path.
    switch (expr) {
        .eq, .neq, .lt, .lte, .gt, .gte => |binop| {
            const cmp_kind: simd.CmpKind = switch (expr) {
                .eq => .eq,
                .neq => .neq,
                .lt => .lt,
                .lte => .lte,
                .gt => .gt,
                .gte => .gte,
                else => unreachable,
            };
            if (tryCmpBatch(binop, cmp_kind, dc, out)) return;
        },
        .@"and" => |binop| {
            // Allocate two temporary masks and AND them.
            const tmp_a = try arena.alloc(i16, dc.num_rows);
            const tmp_b = try arena.alloc(i16, dc.num_rows);
            try evalExprBatch(binop.left, dc, tmp_a, arena);
            try evalExprBatch(binop.right, dc, tmp_b, arena);
            simd.andMasks(tmp_a, tmp_b, out);
            return;
        },
        .@"or" => |binop| {
            const tmp_a = try arena.alloc(i16, dc.num_rows);
            const tmp_b = try arena.alloc(i16, dc.num_rows);
            try evalExprBatch(binop.left, dc, tmp_a, arena);
            try evalExprBatch(binop.right, dc, tmp_b, arena);
            simd.orMasks(tmp_a, tmp_b, out);
            return;
        },
        .not => |unop| {
            try evalExprBatch(unop.operand, dc, out, arena);
            simd.notMask(out);
            return;
        },
        else => {},
    }

    // Scalar fallback: evaluate row-by-row.
    var row_arena = std.heap.ArenaAllocator.init(arena);
    defer row_arena.deinit();
    for (0..dc.num_rows) |i| {
        _ = row_arena.reset(.retain_capacity);
        const row = try dc.readRow(i, row_arena.allocator());
        const val = try evalExpr(expr, row, null, row_arena.allocator());
        out[i] = if (val) |v| switch (v) {
            .bool_u8 => @intCast(v.bool_u8),
            .int64 => if (v.int64 != 0) @as(i16, 1) else 0,
            .uint64 => if (v.uint64 != 0) @as(i16, 1) else 0,
            else => 1,
        } else 0;
    }
}

/// Evaluate a boolean Expr over `dc` and return the row ids that pass.
/// This is the SelectionVector-facing companion to evalExprBatch. It keeps the
/// mask representation private so downstream operators can gradually migrate
/// from compacted chunks to selection-aware execution.
pub fn evalExprSelection(
    expr: Expr,
    dc: DataChunk,
    selection_buf: []u32,
    mask_buf: []i16,
    arena: std.mem.Allocator,
) anyerror!chunk.SelectionVector {
    std.debug.assert(selection_buf.len >= dc.num_rows);
    std.debug.assert(mask_buf.len >= dc.num_rows);
    const mask = mask_buf[0..dc.num_rows];
    try evalExprBatch(expr, dc, mask, arena);
    return chunk.SelectionVector.fromI16Mask(selection_buf[0..dc.num_rows], mask);
}

/// Dispatch cmpBatch with a runtime CmpKind by switching to comptime variants.
fn cmpBatchRuntime(comptime T: type, vals: []const T, op: simd.CmpKind, rhs: T, out: []i16) void {
    switch (op) {
        .eq => simd.cmpBatch(T, vals, .eq, rhs, out),
        .neq => simd.cmpBatch(T, vals, .neq, rhs, out),
        .lt => simd.cmpBatch(T, vals, .lt, rhs, out),
        .lte => simd.cmpBatch(T, vals, .lte, rhs, out),
        .gt => simd.cmpBatch(T, vals, .gt, rhs, out),
        .gte => simd.cmpBatch(T, vals, .gte, rhs, out),
    }
}

/// Attempt a SIMD comparison dispatch. Returns true if the fast path fired.
fn tryCmpBatch(
    binop: *const plan.BinOp,
    op: simd.CmpKind,
    dc: DataChunk,
    out: []i16,
) bool {
    // Pattern: col_ref cmp lit  OR  lit cmp col_ref (flipped).
    const Resolved = struct { col_idx: usize, lit_expr: plan.Expr, effective_op: simd.CmpKind };
    const resolved: Resolved = switch (binop.left) {
        .col_ref => |ref| .{ .col_idx = ref.index, .lit_expr = binop.right, .effective_op = op },
        else => switch (binop.right) {
            .col_ref => |ref| .{ .col_idx = ref.index, .lit_expr = binop.left, .effective_op = flipCmp(op) },
            else => return false,
        },
    };
    const col_idx = resolved.col_idx;
    const lit_expr = resolved.lit_expr;
    const effective_op = resolved.effective_op;

    if (col_idx >= dc.columns.len) return false;
    const col = dc.columns[col_idx];

    switch (col.data) {
        .int64 => |vals| {
            const rhs: i64 = switch (lit_expr) {
                .lit_i64 => |v| v,
                .lit_u64 => |v| @as(i64, @bitCast(v)),
                else => return false,
            };
            cmpBatchRuntime(i64, vals, effective_op, rhs, out);
            applyNullMask(col, out);
            return true;
        },
        .datetime64_ms => |vals| {
            const rhs: i64 = switch (lit_expr) {
                .lit_i64 => |v| v,
                .lit_u64 => |v| @as(i64, @bitCast(v)),
                else => return false,
            };
            cmpBatchRuntime(i64, vals, effective_op, rhs, out);
            applyNullMask(col, out);
            return true;
        },
        .uint64 => |vals| {
            const rhs: u64 = switch (lit_expr) {
                .lit_u64 => |v| v,
                .lit_i64 => |v| if (v < 0) return false else @intCast(v),
                else => return false,
            };
            cmpBatchRuntime(u64, vals, effective_op, rhs, out);
            applyNullMask(col, out);
            return true;
        },
        .date_u16 => |vals| {
            const rhs: u16 = switch (lit_expr) {
                .lit_i64 => |v| if (v < 0 or v > 65535) return false else @intCast(v),
                .lit_u64 => |v| if (v > 65535) return false else @intCast(v),
                else => return false,
            };
            cmpBatchRuntime(u16, vals, effective_op, rhs, out);
            applyNullMask(col, out);
            return true;
        },
        else => return false,
    }
}

fn flipCmp(op: simd.CmpKind) simd.CmpKind {
    return switch (op) {
        .eq => .eq,
        .neq => .neq,
        .lt => .gt,
        .lte => .gte,
        .gt => .lt,
        .gte => .lte,
    };
}

fn applyNullMask(col: chunk.Column, out: []i16) void {
    if (chunk.allNonNull(col.null_mask)) return;
    for (0..out.len) |i| {
        if (chunk.isNull(col.null_mask, i)) out[i] = 0;
    }
}
