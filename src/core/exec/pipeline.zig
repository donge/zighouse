/// Push-based Pipeline execution engine.
///
/// A Pipeline connects a Source, zero or more Transform operators, and a Sink.
/// Data flows in DataChunks from source → transforms → sink.
///
/// Pipeline breakers (HashAgg, OrderBy, HashJoin) are handled by splitting
/// execution into multiple pipeline segments. The breaker's build phase
/// consumes its input pipeline fully, then the probe/read phase feeds the
/// next pipeline segment.
///
/// Thread model: morsel-parallel for scalar_agg and hash_agg when the source
/// supports fetchRange. Falls back to single-threaded otherwise.
const std    = @import("std");
const types  = @import("../types.zig");
const chunk  = @import("../chunk.zig");
const result = @import("../result.zig");
const plan   = @import("plan.zig");
const kernels = @import("kernels.zig");
const ht     = @import("hash_table.zig");
const simd   = @import("../simd_ops.zig");
const parallel = @import("parallel");

pub const Value      = types.Value;
pub const AggAccum   = types.AggAccum;
pub const ColumnType = types.ColumnType;
pub const DataChunk  = chunk.DataChunk;
pub const ResultSet  = result.ResultSet;
pub const ResultSink = result.ResultSink;

// ── QueryContext ──────────────────────────────────────────────────────────────

/// Per-query execution context. Holds the arena for all intermediate
/// allocations during one query's lifetime.
pub const QueryContext = struct {
    /// All transient allocations (intermediate chunks, hash tables, etc.)
    /// are made from this arena. Freed when the query finishes.
    arena: std.heap.ArenaAllocator,
    /// Injected source implementations (set before executing a plan).
    source: SourceIface,

    pub fn init(parent_alloc: std.mem.Allocator, source: SourceIface) QueryContext {
        return .{
            .arena  = std.heap.ArenaAllocator.init(parent_alloc),
            .source = source,
        };
    }

    pub fn deinit(self: *QueryContext) void {
        self.arena.deinit();
    }

    pub fn allocator(self: *QueryContext) std.mem.Allocator {
        return self.arena.allocator();
    }
};

// ── Source interface ──────────────────────────────────────────────────────────

/// A type-erased source. Concrete implementations live in src/core/source/.
/// Using a vtable (function pointers) here rather than comptime generics
/// because the source type is selected at runtime (config / query routing).
pub const SourceIface = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Fill `out` with the next chunk of rows. Returns false when exhausted.
        nextChunk: *const fn (ptr: *anyopaque, out: *DataChunk, ctx: *QueryContext) anyerror!bool,
        /// Reset the source to re-scan from the beginning.
        reset: *const fn (ptr: *anyopaque) void,
        /// Return column metadata for this source's schema.
        schema: *const fn (ptr: *anyopaque) []const result.ColMeta,
        /// Return an upper-bound row count estimate (0 = unknown).
        rowCount: *const fn (ptr: *anyopaque) u64,
        /// Optional: fetch a specific row range [start, start+n) into `out`.
        /// If null, the source does not support random-access range reads.
        /// `alloc` is used for the chunk's column buffers.
        fetchRange: ?*const fn (ptr: *anyopaque, start: u64, n: usize, out: *DataChunk, alloc: std.mem.Allocator) anyerror!void = null,
        /// Optional: restrict which columns are decoded during nextChunk / fetchRange.
        /// Pass null to restore all columns. col_names is borrowed (caller owns).
        setNeededCols: ?*const fn (ptr: *anyopaque, col_names: ?[]const []const u8) void = null,
        /// Optional: mark a string column to be decoded as bool_u8 (1=non-empty, 0=empty)
        /// instead of full string slices. Use for columns only needed for `!= ''` checks.
        /// Pass null col_name to clear all such marks.
        setStringNonEmptyBool: ?*const fn (ptr: *anyopaque, col_name: ?[]const u8) void = null,
    };

    pub fn nextChunk(self: SourceIface, out: *DataChunk, ctx: *QueryContext) !bool {
        return self.vtable.nextChunk(self.ptr, out, ctx);
    }

    pub fn reset(self: SourceIface) void {
        self.vtable.reset(self.ptr);
    }

    pub fn schema(self: SourceIface) []const result.ColMeta {
        return self.vtable.schema(self.ptr);
    }

    pub fn rowCount(self: SourceIface) u64 {
        return self.vtable.rowCount(self.ptr);
    }

    /// Returns true if this source supports parallel range reads.
    pub fn supportsRange(self: SourceIface) bool {
        return self.vtable.fetchRange != null;
    }

    pub fn fetchRange(self: SourceIface, start: u64, n: usize, out: *DataChunk, alloc: std.mem.Allocator) !void {
        return self.vtable.fetchRange.?(self.ptr, start, n, out, alloc);
    }

    /// Temporarily restrict which columns are decoded during nextChunk / fetchRange.
    /// Pass null to restore all columns. Only has effect if source supports it.
    pub fn setNeededCols(self: SourceIface, col_names: ?[]const []const u8) void {
        if (self.vtable.setNeededCols) |f| f(self.ptr, col_names);
    }

    /// Mark a string column for lightweight bool_u8 decoding (1=non-empty, 0=empty).
    /// Use when the column is only needed for a `!= ''` filter (not projected/keyed).
    /// Pass null col_name to clear all marks. No-op if source doesn't support it.
    pub fn setStringNonEmptyBool(self: SourceIface, col_name: ?[]const u8) void {
        if (self.vtable.setStringNonEmptyBool) |f| f(self.ptr, col_name);
    }
};

// ── Operator state ────────────────────────────────────────────────────────────

/// Filter: evaluates predicate on each row, zeroes out non-matching rows.
/// Non-matching rows are compacted out — chunk.num_rows shrinks.
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
    val:  i64,
    val2: i64 = 0,  // only used for in2
};

/// A single string comparison extracted from a pure-AND predicate tree.
/// Enables a fast filter path for str_col != 'literal' (e.g. MobilePhoneModel <> '').
pub const StrCmpCond = struct {
    col_idx: usize,
    op: enum(u8) { eq, neq },
    val: []const u8,
};

/// A CASE WHEN key that evaluates to a string inline (no evalExpr needed).
/// Supports: CASE WHEN <int_AND_conditions> THEN <str_col_ref> ELSE <lit_str> END
pub const CaseWhenStrKey = struct {
    when_ic:     [4]IntCmpCond,
    when_ic_n:   usize,
    then_col_idx: usize,    // string column for THEN branch
    else_str:    []const u8, // literal for ELSE (default "")

    /// Evaluate this CASE WHEN for row r in chunk c.
    pub fn eval(self: *const CaseWhenStrKey, c: *const DataChunk, r: usize) []const u8 {
        for (self.when_ic[0..self.when_ic_n]) |cond| {
            if (cond.col_idx >= c.columns.len) return self.else_str;
            const col = c.columns[cond.col_idx];
            const v: i64 = switch (col.data) {
                .int64 => |a| a[r], .uint64 => |a| @bitCast(a[r]),
                .bool_u8 => |a| @as(i64, a[r]), .date_u16 => |a| @as(i64, a[r]),
                .datetime64_ms => |a| a[r],
                else => return self.else_str,
            };
            const pass = switch (cond.op) {
                .eq => v == cond.val, .neq => v != cond.val,
                .lt => v < cond.val,  .lte => v <= cond.val,
                .gt => v > cond.val,  .gte => v >= cond.val,
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
fn extractCaseWhenStrKey(expr: plan.Expr) ?CaseWhenStrKey {
    if (expr != .case_when) return null;
    const cw = expr.case_when;
    if (cw.when.len != 1) return null;
    if (cw.then[0] != .col_ref) return null;
    const else_str: []const u8 = if (cw.else_expr) |e| switch (e) {
        .lit_str => |s| s, else => return null,
    } else "";
    var ic_buf: [4]IntCmpCond = undefined;
    var ic_n: usize = 0;
    if (!extractAndIntConds(cw.when[0], &ic_buf, &ic_n, false)) return null;
    if (ic_n == 0 or ic_n > 4) return null;
    var cw_result = CaseWhenStrKey{
        .when_ic     = undefined,
        .when_ic_n   = ic_n,
        .then_col_idx = cw.then[0].col_ref.index,
        .else_str    = else_str,
    };
    @memcpy(cw_result.when_ic[0..ic_n], ic_buf[0..ic_n]);
    return cw_result;
}

/// Parse "YYYY-MM-DD" string to days-since-epoch (i64), or null if not a date string.
fn parseDateStrToI64(s: []const u8) ?i64 {
    if (s.len < 10 or s[4] != '-' or s[7] != '-') return null;
    const y = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
    const m = std.fmt.parseInt(u32, s[5..7], 10) catch return null;
    const d = std.fmt.parseInt(u32, s[8..10], 10) catch return null;
    var yr: i32 = y;
    var mo: i32 = @intCast(m);
    if (mo <= 2) { yr -= 1; mo += 9; } else { mo -= 3; }
    const era: i32 = @divFloor(yr, 400);
    const yoe: i32 = yr - era * 400;
    const doy: i32 = @divFloor(153 * mo + 2, 5) + @as(i32, @intCast(d)) - 1;
    const doe: i32 = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    const days: i32 = era * 146097 + doe - 719468;
    return @as(i64, days);
}

/// Extract leaf int-comparison conditions from a pure-AND predicate tree.
/// Returns false if the predicate contains any non-int or non-AND node.
/// When `best_effort=true`, partial extraction is allowed; returns true with partial set.
fn extractAndIntConds(
    expr: plan.Expr,
    out:  []IntCmpCond,
    n:    *usize,
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
                if (!ok) { if (!best_effort) { all_ok = false; } }
            }
            return all_ok;
        },
        .eq, .neq, .lt, .lte, .gt, .gte => {
            const op: *plan.BinOp = switch (expr) {
                .eq  => |o| o, .neq => |o| o, .lt => |o| o,
                .lte => |o| o, .gt  => |o| o, .gte => |o| o,
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
                .eq  => .eq,  .neq => .neq, .lt => .lt,
                .lte => .lte, .gt  => .gt,  .gte => .gte,
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
                    const v1: i64 = switch (lop.right) { .lit_i64 => |v| v, .lit_u64 => |v| @bitCast(v), else => return false };
                    const v2: i64 = switch (rop.right) { .lit_i64 => |v| v, .lit_u64 => |v| @bitCast(v), else => return false };
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
const SimpleStrFilter = struct {
    col_idx: usize,
    value:   []const u8,
    is_neq:  bool,  // true = keep row when s != value; false = keep when s == value

    fn passes(self: SimpleStrFilter, col: chunk.Column, r: usize) bool {
        // Fast path: when the source decoded this column as bool_u8 (1=non-empty, 0=empty)
        // via setStringNonEmptyBool, we only support the eq/neq-empty pattern.
        if (col.data == .bool_u8) {
            const non_empty = col.data.bool_u8[r] != 0;
            // is_neq=true, value="" → keep non-empty rows → pass when non_empty
            // is_neq=false, value="" → keep empty rows → pass when !non_empty
            return if (self.is_neq) non_empty else !non_empty;
        }
        const s: []const u8 = if (col.isRowNull(r)) "" else col.data.string[r];
        return if (self.is_neq) !std.mem.eql(u8, s, self.value)
               else              std.mem.eql(u8, s, self.value);
    }
};

/// Try to extract a single-term col_ref eq/neq lit_str filter from a plan.Expr.
/// Returns null if the expression is not exactly that shape.
fn tryExtractSimpleStrFilter(expr: plan.Expr) ?SimpleStrFilter {
    switch (expr) {
        .eq, .neq => {
            const op: *plan.BinOp = if (expr == .eq) expr.eq else expr.neq;
            if (op.left != .col_ref) return null;
            const val: []const u8 = switch (op.right) { .lit_str => |s| s, else => return null };
            return .{ .col_idx = op.left.col_ref.index, .value = val, .is_neq = (expr == .neq) };
        },
        else => return null,
    }
}

/// Extract pure-AND string comparison conditions (eq/neq of col_ref vs lit_str).
/// Returns true if the entire predicate is covered by string conditions (complete).
fn extractAndStrConds(
    expr: plan.Expr,
    out:  []StrCmpCond,
    n:    *usize,
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
            for (fc.args) |arg| { if (!extractAndStrConds(arg, out, n, best_effort)) { if (!best_effort) all_ok = false; } }
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
fn extractMixedAndConds(
    expr:   plan.Expr,
    ic_out: []IntCmpCond, ic_n: *usize,
    sc_out: []StrCmpCond, sc_n: *usize,
) bool {
    switch (expr) {
        .@"and" => |op| {
            const l_ok = extractMixedAndConds(op.left,  ic_out, ic_n, sc_out, sc_n);
            const r_ok = extractMixedAndConds(op.right, ic_out, ic_n, sc_out, sc_n);
            return l_ok and r_ok;
        },
        // AND as fn_call{name="and"} from native Zig planner.
        .fn_call => |fc| {
            if (!std.mem.eql(u8, fc.name, "and")) return false;
            var all_ok = true;
            for (fc.args) |arg| { if (!extractMixedAndConds(arg, ic_out, ic_n, sc_out, sc_n)) all_ok = false; }
            return all_ok;
        },
        .eq, .neq, .lt, .lte, .gt, .gte => {
            const op: *plan.BinOp = switch (expr) {
                .eq  => |o| o, .neq => |o| o, .lt => |o| o,
                .lte => |o| o, .gt  => |o| o, .gte => |o| o,
                else => unreachable,
            };
            if (op.left != .col_ref) return false;
            // Try int literal first.
            const int_val: ?i64 = switch (op.right) {
                .lit_i64 => |v| v, .lit_u64 => |v| @bitCast(v), else => null,
            };
            if (int_val) |val| {
                const kind: @TypeOf(@as(IntCmpCond, undefined).op) = switch (expr) {
                    .eq  => .eq,  .neq => .neq, .lt => .lt,
                    .lte => .lte, .gt  => .gt,  .gte => .gte,
                    else => unreachable,
                };
                if (ic_n.* >= ic_out.len) return false;
                ic_out[ic_n.*] = .{ .col_idx = op.left.col_ref.index, .op = kind, .val = val };
                ic_n.* += 1;
                return true;
            }
            // Try str literal.
            const str_val: ?[]const u8 = switch (op.right) {
                .lit_str => |s| s, else => null,
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
                    const v1: i64 = switch (lop.right) { .lit_i64 => |v| v, .lit_u64 => |v| @bitCast(v), else => return false };
                    const v2: i64 = switch (rop.right) { .lit_i64 => |v| v, .lit_u64 => |v| @bitCast(v), else => return false };
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
            for (mask) |m| { if (m) count += 1; }
            const indices = try alloc.alloc(usize, count);
            var wi: usize = 0;
            for (mask, 0..) |m, j| { if (m) { indices[wi] = j; wi += 1; } }
            self.ref_indices = indices;
            const row = try alloc.alloc(?Value, c.columns.len);
            @memset(row, null);
            self.row_buf = row;
            // Collect LIKE/NOT_LIKE guards; only keep if all guard columns are string type.
            var guards_list = std.ArrayListUnmanaged(LikeGuard){ .items = &.{}, .capacity = 0 };
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
            // Precompute active (non-pruned) column indices for fast copyRow.
            var active_count: usize = 0;
            for (c.columns) |col| { if (!col.pruned) active_count += 1; }
            const aci = try alloc.alloc(usize, active_count);
            var ai: usize = 0;
            for (c.columns, 0..) |col, ci| { if (!col.pruned) { aci[ai] = ci; ai += 1; } }
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
                    if (cond.col_idx >= c.columns.len) { all_int = false; break; }
                    switch (c.columns[cond.col_idx].data) {
                        .int64, .uint64, .date_u16, .datetime64_ms, .bool_u8 => {},
                        else => { all_int = false; break; },
                    }
                }
                if (all_int) {
                    if (self.int_conds_complete) {
                        // Complete fast path: skip evalExpr entirely.
                        var write_pos: usize = 0;
                        row_loop: for (0..c.num_rows) |r| {
                            for (conds) |cond| {
                                const col = c.columns[cond.col_idx];
                                if (col.isRowNull(r)) continue :row_loop;
                                const v: i64 = switch (col.data) {
                                    .int64          => |a| a[r],
                                    .uint64         => |a| @bitCast(a[r]),
                                    .date_u16       => |a| @as(i64, a[r]),
                                    .datetime64_ms  => |a| a[r],
                                    .bool_u8        => |a| @as(i64, a[r]),
                                    else            => continue :row_loop,
                                };
                                const pass = switch (cond.op) {
                                    .eq  => v == cond.val,
                                    .neq => v != cond.val,
                                    .lt  => v <  cond.val,
                                    .lte => v <= cond.val,
                                    .gt  => v >  cond.val,
                                    .gte => v >= cond.val,
                                    .in2 => v == cond.val or v == cond.val2,
                                };
                                if (!pass) continue :row_loop;
                            }
                            if (write_pos != r) cr.copy(r, write_pos);
                            write_pos += 1;
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
        else &.{};

        outer: for (0..c.num_rows) |r| {
            // Inline int guard check: skip evalExpr for rows that fail int conditions.
            if (partial_guards.len > 0) {
                for (partial_guards) |cond| {
                    if (cond.col_idx >= c.columns.len) continue;
                    const col = c.columns[cond.col_idx];
                    if (col.isRowNull(r)) continue :outer;
                    const v: i64 = switch (col.data) {
                        .int64         => |a| a[r],
                        .uint64        => |a| @bitCast(a[r]),
                        .date_u16      => |a| @as(i64, a[r]),
                        .datetime64_ms => |a| a[r],
                        .bool_u8       => |a| @as(i64, a[r]),
                        else           => continue,
                    };
                    const pass = switch (cond.op) {
                        .eq  => v == cond.val,
                        .neq => v != cond.val,
                        .lt  => v <  cond.val,
                        .lte => v <= cond.val,
                        .gt  => v >  cond.val,
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
fn collectLikeGuards(expr: plan.Expr, guards: *std.ArrayListUnmanaged(LikeGuard), alloc: std.mem.Allocator) void {
    switch (expr) {
        .like, .not_like => |op| {
            if (op.left == .col_ref and op.right == .lit_str) {
                guards.append(alloc, .{
                    .col_idx = op.left.col_ref.index,
                    .pattern = op.right.lit_str,
                    .negate  = expr == .not_like,
                    .matcher = kernels.LikeMatcher.compile(op.right.lit_str),
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

/// Recursively collect column reference indices from an expression into a mask.
fn collectColRefs(expr: plan.Expr, mask: []bool) void {
    switch (expr) {
        .col_ref => |cr| if (cr.index < mask.len) { mask[cr.index] = true; },
        .add, .sub, .mul, .div, .mod => |op| { collectColRefs(op.left, mask); collectColRefs(op.right, mask); },
        .eq, .neq, .lt, .lte, .gt, .gte => |op| { collectColRefs(op.left, mask); collectColRefs(op.right, mask); },
        .@"and", .@"or" => |op| { collectColRefs(op.left, mask); collectColRefs(op.right, mask); },
        .not => |inner| collectColRefs(inner.operand, mask),
        .like, .not_like, .concat => |op| { collectColRefs(op.left, mask); collectColRefs(op.right, mask); },
        .is_null, .is_not_null => |inner| collectColRefs(inner.operand, mask),
        .cast => |c| collectColRefs(c.expr, mask),
        .fn_call => |fc| for (fc.args) |arg| collectColRefs(arg, mask),
        .agg_call => |ac| if (ac.arg) |arg| collectColRefs(arg, mask),
        .case_when => |cw| {
            for (cw.when, cw.then) |wh, th| { collectColRefs(wh, mask); collectColRefs(th, mask); }
            if (cw.else_expr) |e| collectColRefs(e, mask);
        },
        else => {},
    }
}

fn copyRow(c: *DataChunk, from: usize, to: usize) void {
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
    items: []plan.ProjectItem,

    pub fn apply(self: *ProjectState, c: *DataChunk, ctx: *QueryContext) !void {
        const alloc = ctx.allocator();
        const n     = c.num_rows;

        // Build output column buffers.
        var out_cols = try alloc.alloc(chunk.Column, self.items.len);
        for (self.items, 0..) |item, ci| {
            const nw        = chunk.nullMaskWords(n);
            const null_mask = try alloc.alloc(u64, nw);
            @memset(null_mask, 0);
            const data = allocColumnData(item.out_type, n, alloc) catch continue;
            out_cols[ci] = .{
                .name      = item.alias,
                .data      = data,
                .null_mask = null_mask,
                .len       = n,
            };
        }

        // Evaluate each row.
        for (0..n) |r| {
            const row = try c.readRow(r, alloc);
            for (self.items, 0..) |item, ci| {
                const v_opt = try kernels.evalExpr(item.expr, row, null, alloc);
                if (v_opt) |v| {
                    setColumnValue(&out_cols[ci].data, r, v);
                } else {
                    chunk.setNull(out_cols[ci].null_mask, r);
                    setColumnZero(&out_cols[ci].data, r);
                }
            }
        }

        // Replace chunk columns (arena owns both old and new allocations).
        c.columns = out_cols;
    }
};

fn allocColumnData(col_type: ColumnType, n: usize, alloc: std.mem.Allocator) !chunk.ColumnData {
    return switch (col_type) {
        .bool_u8       => .{ .bool_u8       = try alloc.alloc(u8,           n) },
        .int64         => .{ .int64         = try alloc.alloc(i64,          n) },
        .uint64        => .{ .uint64        = try alloc.alloc(u64,          n) },
        .float64       => .{ .float64       = try alloc.alloc(f64,          n) },
        .date_u16      => .{ .date_u16      = try alloc.alloc(u16,          n) },
        .datetime64_ms => .{ .datetime64_ms = try alloc.alloc(i64,          n) },
        .string        => .{ .string        = try alloc.alloc([]const u8,   n) },
        .array_string  => .{ .array_string  = try alloc.alloc([][]const u8, n) },
    };
}

fn setColumnValue(data: *chunk.ColumnData, r: usize, v: Value) void {
    switch (data.*) {
        .bool_u8       => |s| s[r] = switch (v) { .bool_u8 => |x| x, else => @intCast(v.toI64() orelse 0) },
        .int64         => |s| s[r] = v.toI64() orelse 0,
        .uint64        => |s| s[r] = v.toU64() orelse 0,
        .float64       => |s| s[r] = v.toF64() orelse 0.0,
        .date_u16      => |s| s[r] = switch (v) { .date_u16 => |x| x, else => @truncate(@as(u16, @intCast(v.toI64() orelse 0))) },
        .datetime64_ms => |s| s[r] = v.toI64() orelse 0,
        .string        => |s| s[r] = v.toStr() orelse "",
        .array_string  => |s| s[r] = switch (v) { .array_string => |a| a, else => &.{} },
    }
}

fn setColumnZero(data: *chunk.ColumnData, r: usize) void {
    switch (data.*) {
        .bool_u8       => |s| s[r] = 0,
        .int64         => |s| s[r] = 0,
        .uint64        => |s| s[r] = 0,
        .float64       => |s| s[r] = 0.0,
        .date_u16      => |s| s[r] = 0,
        .datetime64_ms => |s| s[r] = 0,
        .string        => |s| s[r] = "",
        .array_string  => |s| s[r] = &.{},
    }
}

/// LimitState: tracks how many rows have been emitted, truncates chunks.
pub const LimitState = struct {
    limit:   u64,
    offset:  u64,
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
            if (remaining == 0) { c.num_rows = 0; return; }
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
    filter:  FilterState,
    project: ProjectState,
    limit:   LimitState,
};

// ── executePlan ───────────────────────────────────────────────────────────────

/// Internal row list used during plan execution.
/// Memory owned by the QueryContext arena.
pub const RowList = struct {
    metas: []result.ColMeta,
    rows:  std.ArrayListUnmanaged([]?Value),

    pub fn init(metas: []result.ColMeta) RowList {
        return .{ .metas = metas, .rows = .empty };
    }

    pub fn append(self: *RowList, alloc: std.mem.Allocator, row: []?Value) !void {
        try self.rows.append(alloc, row);
    }

    /// Materialise into a ResultSet.  All values are duped into a fresh arena.
    pub fn toResultSet(self: RowList, parent_alloc: std.mem.Allocator) !ResultSet {
        var arena = std.heap.ArenaAllocator.init(parent_alloc);
        const ra  = arena.allocator();

        const num_rows = self.rows.items.len;
        const num_cols = self.metas.len;

        const out_metas = try ra.dupe(result.ColMeta, self.metas);
        if (num_rows == 0 or num_cols == 0) {
            return ResultSet{
                .metas    = out_metas,
                .columns  = &.{},
                .num_rows = 0,
                .arena    = arena,
            };
        }

        const out_cols = try ra.alloc(chunk.Column, num_cols);
        for (out_cols, out_metas) |*col, meta| {
            const nw        = chunk.nullMaskWords(num_rows);
            const null_mask = try ra.alloc(u64, nw);
            @memset(null_mask, 0);
            col.* = .{
                .name      = meta.name,
                .data      = try allocColumnDataRA(meta.col_type, num_rows, ra),
                .null_mask = null_mask,
                .len       = num_rows,
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
            .metas    = out_metas,
            .columns  = out_cols,
            .num_rows = num_rows,
            .arena    = arena,
        };
    }
};

fn allocColumnDataRA(col_type: ColumnType, n: usize, ra: std.mem.Allocator) !chunk.ColumnData {
    return switch (col_type) {
        .bool_u8       => .{ .bool_u8       = try ra.alloc(u8,           n) },
        .int64         => .{ .int64         = try ra.alloc(i64,          n) },
        .uint64        => .{ .uint64        = try ra.alloc(u64,          n) },
        .float64       => .{ .float64       = try ra.alloc(f64,          n) },
        .date_u16      => .{ .date_u16      = try ra.alloc(u16,          n) },
        .datetime64_ms => .{ .datetime64_ms = try ra.alloc(i64,          n) },
        .string        => .{ .string        = try ra.alloc([]const u8,   n) },
        .array_string  => .{ .array_string  = try ra.alloc([][]const u8, n) },
    };
}

fn setColValue(data: *chunk.ColumnData, r: usize, v: Value, ra: std.mem.Allocator) void {
    switch (data.*) {
        .bool_u8       => |s| s[r] = switch (v) { .bool_u8 => |x| x, else => @intCast(v.toI64() orelse 0) },
        .int64         => |s| s[r] = v.toI64() orelse 0,
        .uint64        => |s| s[r] = v.toU64() orelse 0,
        .float64       => |s| s[r] = v.toF64() orelse 0.0,
        .date_u16      => |s| s[r] = switch (v) { .date_u16 => |x| x, .uint64 => |u| @truncate(u), else => @as(u16, @intCast(@max(0, v.toI64() orelse 0))) },
        .datetime64_ms => |s| s[r] = v.toI64() orelse 0,
        .string        => |s| s[r] = ra.dupe(u8, v.toStr() orelse "") catch (v.toStr() orelse ""),
        .array_string  => |s| s[r] = switch (v) { .array_string => |a| a, else => &.{} },
    }
}

/// Execute a PhysicalNode tree recursively, returning a ResultSet.
/// Handles all node types including pipeline breakers (HashAgg, ScalarAgg,
/// OrderBy, TopK, HashJoin).
pub fn executePlan(
    node: *const plan.PhysicalNode,
    ctx: *QueryContext,
) !ResultSet {
    const alloc = ctx.allocator();

    // ── Scannable path: stream chunks directly into ResultSink ─────────────
    if (isScannable(node)) {
        var sink = ResultSink.init(alloc);
        try executeScannableToSink(node, ctx, &sink);
        return sink.finish();
    }

    // ── Breaker path: existing RowList → ResultSet (single copy) ───────────
    var rl = try executeNode(node, ctx);
    return rl.toResultSet(alloc);
}

/// Stream a scannable node (scan/filter/project/limit) directly to a ResultSink.
/// Avoids building a RowList by operating on DataChunks throughout.
fn executeScannableToSink(
    node: *const plan.PhysicalNode,
    ctx:  *QueryContext,
    sink: *ResultSink,
) !void {
    const alloc = ctx.allocator();
    var filter_state: ?FilterState = null;
    var project_items: ?[]const plan.ProjectItem = null;
    var lim_state: ?LimitState = null;

    var cur = node;
    while (true) {
        switch (cur.*) {
            .limit   => |lim| { if (lim_state == null) lim_state = .{ .limit = lim.limit, .offset = lim.offset }; cur = lim.input; },
            .filter  => |f|   { if (filter_state == null) filter_state = .{ .predicate = f.predicate }; cur = f.input; },
            .project => |p|   {
                if (project_items == null) {
                    project_items = p.items;
                }
                cur = p.input;
            },
            else => break,
        }
    }

    ctx.source.reset();
    var c: DataChunk = undefined;
    // Row buffer for projection; allocated once on first non-empty chunk.
    var row_buf: ?[]?Value = null;
    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state)  |*fs| try fs.apply(&c, ctx);
        if (lim_state)     |*ls| ls.apply(&c);
        if (c.num_rows == 0) {
            if (lim_state) |ls| if (ls.done()) break;
            continue;
        }
        if (project_items) |items| {
            // Lazy-init row_buf once on first non-empty chunk.
            if (row_buf == null) {
                const rb = try alloc.alloc(?Value, c.columns.len);
                @memset(rb, null);
                row_buf = rb;
            }
            const rb = row_buf.?;
            const n = c.num_rows;
            const out_cols = try alloc.alloc(chunk.Column, items.len);
            for (items, 0..) |item, ci| {
                const nw        = chunk.nullMaskWords(n);
                const null_mask = try alloc.alloc(u64, nw);
                @memset(null_mask, 0);
                const data = try allocColumnData(item.out_type, n, alloc);
                out_cols[ci] = .{ .name = item.alias, .data = data, .null_mask = null_mask, .len = n };
            }
            for (0..n) |r| {
                for (c.columns, 0..) |col, j| {
                    rb[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                for (items, 0..) |item, ci| {
                    const v_opt = try kernels.evalExpr(item.expr, rb, null, alloc);
                    if (v_opt) |v| {
                        setColumnValue(&out_cols[ci].data, r, v);
                    } else {
                        chunk.setNull(out_cols[ci].null_mask, r);
                        setColumnZero(&out_cols[ci].data, r);
                    }
                }
            }
            c.columns = out_cols;
        }
        try sink.consume(c);
        if (lim_state) |ls| if (ls.done()) break;
    }
}fn executeNode(node: *const plan.PhysicalNode, ctx: *QueryContext) !RowList {
    const alloc = ctx.allocator();
    switch (node.*) {
        // ── Sources ───────────────────────────────────────────────────────────
        .part_scan, .mem_scan => {
            const schema_metas = ctx.source.schema();
            const metas = try alloc.dupe(result.ColMeta, schema_metas);
            var rl = RowList.init(metas);
            var c: DataChunk = undefined;
            while (try ctx.source.nextChunk(&c, ctx)) {
                // Note: do NOT defer c.deinit() here — row values hold slices
                // into the chunk's arena (e.g. array_string elems). Those
                // slices remain valid until qctx.deinit() frees the parent
                // arena that owns the chunk sub-arenas.
                for (0..c.num_rows) |r| {
                    const row = try c.readRow(r, alloc);
                    try rl.append(alloc, row);
                }
            }
            return rl;
        },

        .chunk_source => |cs| return executeNode(cs.input, ctx),

        // ── Filter ────────────────────────────────────────────────────────────
        .filter => |f| {
            if (isScannable(f.input)) {
                return executeLimitChunked(node, ctx);
            }
            const inner = try executeNode(f.input, ctx);
            var rl = RowList.init(inner.metas);
            for (inner.rows.items) |row| {
                const v_opt = try kernels.evalExpr(f.predicate, row, null, alloc);
                const keep = valueToBool(v_opt);
                if (keep) try rl.append(alloc, row);
            }
            return rl;
        },

        // ── Project ───────────────────────────────────────────────────────────
        .project => |p| {
            if (isScannable(p.input)) {
                return executeLimitChunked(node, ctx);
            }
            // Detect: project → top_k → scannable  (e.g. SELECT * … ORDER BY col LIMIT k)
            // Stream scannable input directly into heap to avoid materialising all rows.
            if (p.input.* == .top_k) {
                const tk = p.input.top_k;
                if (isScannable(tk.input)) {
                    var proj_over_scan = plan.PhysicalNode{ .project = .{ .input = tk.input, .items = p.items } };
                    return executeTopKFromScannable(&proj_over_scan, tk.keys, @intCast(tk.k), ctx);
                }
            }
            const inner = try executeNode(p.input, ctx);
            return projectRowList(inner, p.items, alloc);
        },

        // ── Limit ─────────────────────────────────────────────────────────────
        .limit => |lim| {
            if (isScannable(node)) {
                return executeLimitChunked(node, ctx);
            }
            const inner = try executeNode(lim.input, ctx);
            var rl = RowList.init(inner.metas);
            var skipped: u64 = 0;
            var emitted: u64 = 0;
            for (inner.rows.items) |row| {
                if (skipped < lim.offset) { skipped += 1; continue; }
                if (emitted >= lim.limit) break;
                try rl.append(alloc, row);
                emitted += 1;
            }
            return rl;
        },

        // ── ScalarAgg ─────────────────────────────────────────────────────────
        .scalar_agg => |sa| {
            if (isScannable(sa.input)) {
                // Try parallel path first (requires fetchRange support and no LIMIT).
                if (try executeScalarAggParallel(sa.input, sa.aggs, ctx)) |r| return r;
                return executeScalarAggChunked(sa.input, sa.aggs, ctx);
            }
            const inner = try executeNode(sa.input, ctx);
            return executeScalarAgg(inner, sa.aggs, alloc);
        },

        // ── HashAgg ───────────────────────────────────────────────────────────
        .hash_agg => |ha| {
            if (isScannable(ha.input)) {
                // Try parallel compact int-key hash agg first.
                if (try executeHashAggParallelCompact(ha.input, ha.keys, ha.aggs, ctx)) |rl| return rl;
                // Try parallel string-key hash agg (str_min/str_max support).
                if (try executeHashAggParallelStrKey(ha.input, ha.keys, ha.aggs, &.{}, 0, ctx)) |rl| return rl;
                return executeHashAggChunked(ha.input, ha.keys, ha.aggs, ctx);
            }
            const inner = try executeNode(ha.input, ctx);
            return executeHashAgg(inner, ha.keys, ha.aggs, alloc);
        },

        // ── OrderBy ───────────────────────────────────────────────────────────
        .order_by => |ob| {
            const inner = try executeNode(ob.input, ctx);
            return executeOrderBy(inner, ob.keys, alloc);
        },

         // ── TopK ──────────────────────────────────────────────────────────────
         .top_k => |tk| {
             const k = @as(usize, @intCast(tk.k));
             // Fast path: stream scannable input directly into heap — avoids
             // materialising all rows into a RowList before sorting.
             if (isScannable(tk.input)) {
                 return executeTopKFromScannable(tk.input, tk.keys, k, ctx);
             }
             // Fusion: top_k(hash_agg(scannable)) — avoid building full RowList.
             if (tk.input.* == .hash_agg and isScannable(tk.input.hash_agg.input)) {
                 const ha = tk.input.hash_agg;
                 if (try executeHashAggParallelCompactTopK(ha.input, ha.keys, ha.aggs, tk.keys, k, ctx)) |rl| return rl;
                 if (try executeHashAggParallelStrKey(ha.input, ha.keys, ha.aggs, tk.keys, k, ctx)) |rl| return rl;
             }
             const inner = try executeNode(tk.input, ctx);
             // For small K, use a partial selection (heap-based) instead of full sort.
             if (k <= 1024 and inner.rows.items.len > k * 4) {
                 return executeTopK(inner, tk.keys, k, alloc);
             }
             const sorted = try executeOrderBy(inner, tk.keys, alloc);
             const take = @min(sorted.rows.items.len, k);
             var rl = RowList.init(sorted.metas);
             for (sorted.rows.items[0..take]) |row| try rl.append(alloc, row);
             return rl;
         },

        // ── HashJoin ──────────────────────────────────────────────────────────
        .hash_join => |hj| {
            const left_rl  = try executeNode(hj.left,  ctx);
            const right_rl = try executeNode(hj.right, ctx);
            return executeHashJoin(left_rl, right_rl, hj, alloc);
        },
    }
}

fn valueToBool(v: ?Value) bool {
    return if (v) |val| switch (val) {
        .bool_u8 => |b| b != 0,
        .int64   => |i| i != 0,
        .uint64  => |u| u != 0,
        .float64 => |f| f != 0.0,
        else     => false,
    } else false;
}

// ── Project helper ────────────────────────────────────────────────────────────

fn projectRowList(inner: RowList, items: []const plan.ProjectItem, alloc: std.mem.Allocator) !RowList {
    const new_metas = try alloc.alloc(result.ColMeta, items.len);
    for (items, 0..) |item, ci| {
        new_metas[ci] = .{ .name = item.alias, .col_type = item.out_type, .ch_type = item.ch_type };
    }
    var rl = RowList.init(new_metas);

    // Detect arrayJoin(expr) calls among the projection items.
    // Collect all indices; they will be expanded in lockstep (element i for all).
    // Other columns repeat their value for each element.
    var aj_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer aj_indices.deinit(alloc);
    for (items, 0..) |item, ci| {
        switch (item.expr) {
            .fn_call => |fc| if (std.mem.eql(u8, fc.name, "arrayJoin")) {
                try aj_indices.append(alloc, ci);
            },
            else => {},
        }
    }

    for (inner.rows.items) |row| {
        if (aj_indices.items.len > 0) {
            // Evaluate the first arrayJoin argument to determine the expansion count.
            const first_ai = aj_indices.items[0];
            const first_aj_expr = switch (items[first_ai].expr) {
                .fn_call => |fc| fc.args[0],
                else => unreachable,
            };
            const first_arr_val = try kernels.evalExpr(first_aj_expr, row, null, alloc);
            const first_elements: []const []const u8 = switch (first_arr_val orelse Value{ .array_string = &.{} }) {
                .array_string => |a| a,
                else => &.{},
            };
            // Evaluate all arrayJoin arrays upfront (for lockstep expansion).
            const aj_arrays = try alloc.alloc([]const []const u8, aj_indices.items.len);
            aj_arrays[0] = first_elements;
            for (aj_indices.items[1..], 1..) |ai, k| {
                const aj_expr = switch (items[ai].expr) {
                    .fn_call => |fc| fc.args[0],
                    else => unreachable,
                };
                const arr_val = try kernels.evalExpr(aj_expr, row, null, alloc);
                aj_arrays[k] = switch (arr_val orelse Value{ .array_string = &.{} }) {
                    .array_string => |a| a,
                    else => &.{},
                };
            }
            // Emit one output row per element (or one null row if empty)
            const n = if (first_elements.len > 0) first_elements.len else @as(usize, 1);
            for (0..n) |ei| {
                const new_row = try alloc.alloc(?Value, items.len);
                // Find which aj_index slot this column corresponds to (if any).
                for (items, 0..) |item, ci| {
                    var is_aj = false;
                    for (aj_indices.items, 0..) |ai, k| {
                        if (ci == ai) {
                            const elems = aj_arrays[k];
                            new_row[ci] = if (elems.len > ei)
                                Value{ .string = elems[ei] }
                            else
                                null;
                            is_aj = true;
                            break;
                        }
                    }
                    if (!is_aj) {
                        new_row[ci] = try kernels.evalExpr(item.expr, row, null, alloc);
                    }
                }
                try rl.append(alloc, new_row);
            }
        } else {
            const new_row = try alloc.alloc(?Value, items.len);
            for (items, 0..) |item, ci| {
                const v_opt = try kernels.evalExpr(item.expr, row, null, alloc);
                new_row[ci] = v_opt;
            }
            try rl.append(alloc, new_row);
        }
    }
    return rl;
}

// ── Chunked agg helpers ───────────────────────────────────────────────────────

/// Returns true if node is a direct source (part_scan/mem_scan) or a
/// filter/project/limit over a direct source — i.e. no pipeline breakers.
fn isScannable(node: *const plan.PhysicalNode) bool {
    return switch (node.*) {
        .part_scan, .mem_scan, .chunk_source => true,
        .filter  => |f| isScannable(f.input),
        .project => |p| isScannable(p.input),
        .limit   => |l| isScannable(l.input),
        else => false,
    };
}

/// Drive the source (and optional filter/project/limit pipeline) chunk by
/// chunk and accumulate scalar aggregates without materialising any rows.
fn executeScalarAggChunked(
    input: *const plan.PhysicalNode,
    aggs:  []const plan.ProjectItem,
    ctx:   *QueryContext,
) !RowList {
    const alloc = ctx.allocator();
    const accums = try alloc.alloc(AggAccum, aggs.len);
    for (aggs, 0..) |item, ci| accums[ci] = initAccumForAgg(item.expr);

    var filter_state: ?FilterState = extractFilter(input);
    var lim_state:    ?LimitState  = extractLimit(input);

    // Count-only mode: if all aggs are COUNT(*), skip copyRow in pure-LIKE filter.
    // Downstream only reads c.num_rows for count accumulation.
    if (filter_state != null) {
        const all_count_star = for (aggs) |item| {
            const ok = item.expr == .agg_call and item.expr.agg_call.kind == .count_star;
            if (!ok) break false;
        } else true;
        if (all_count_star) filter_state.?.count_only_mode = true;
    }

    var c: DataChunk = undefined;
    ctx.source.reset();
    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state) |*fs| try fs.apply(&c, ctx);
        if (lim_state)    |*ls| ls.apply(&c);
        if (c.num_rows == 0) {
            if (lim_state) |ls| if (ls.done()) break;
            continue;
        }
        try updateAccumsFromChunk(accums, aggs, &c, alloc);
        if (lim_state) |ls| if (ls.done()) break;
    }

    const metas   = try alloc.alloc(result.ColMeta, aggs.len);
    const out_row = try alloc.alloc(?Value, aggs.len);
    for (aggs, 0..) |item, ci| {
        metas[ci]   = .{ .name = item.alias, .col_type = item.out_type };
        out_row[ci] = try finalizeAccum(accums[ci], item, alloc);
    }
    var rl = RowList.init(metas);
    try rl.append(alloc, out_row);
    return rl;
}

/// Parallel scalar aggregation: split rows into T morsels, merge partial accumulators.
/// Falls back to single-threaded if source does not support fetchRange.
fn executeScalarAggParallel(
    input: *const plan.PhysicalNode,
    aggs:  []const plan.ProjectItem,
    ctx:   *QueryContext,
) !?RowList {
    if (!ctx.source.supportsRange()) return null;
    const total_rows = ctx.source.rowCount();
    if (total_rows == 0) return null;
    // Only run parallel for large datasets (10M+ rows benefit most).
    const MIN_ROWS_FOR_PARALLEL: u64 = 500_000;
    if (total_rows < MIN_ROWS_FOR_PARALLEL) return null;

    const filter_pred: ?plan.Expr = switch (input.*) {
        .filter  => |f| f.predicate,
        .project => |p| switch (p.input.*) { .filter => |f| f.predicate, else => null },
        else => null,
    };
    // For now, only parallelize queries without LIMIT (LIMIT complicates merge).
    const has_limit: bool = switch (input.*) {
        .limit => true,
        .filter => |f| switch (f.input.*) { .limit => true, else => false },
        else => false,
    };
    if (has_limit) return null;

    const alloc = ctx.allocator();
    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;

    const ParCtx = struct {
        source:      SourceIface,
        filter_pred: ?plan.Expr,
        aggs:        []const plan.ProjectItem,
        accums:      []AggAccum,
        morsel_src:  *parallel.MorselSource,
        parent_alloc: std.mem.Allocator,
        err:         ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| { self.err = e; };
        }

        fn runWork(self: *@This()) !void {
            var thread_arena = std.heap.ArenaAllocator.init(self.parent_alloc);
            defer thread_arena.deinit();
            const talloc = thread_arena.allocator();

            while (self.morsel_src.next()) |m| {
                var chunk_arena = std.heap.ArenaAllocator.init(talloc);
                const calloc = chunk_arena.allocator();
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, calloc);

                // Apply filter if any.
                if (self.filter_pred) |pred| {
                    var fs = FilterState{ .predicate = pred };
                    var fake_ctx: QueryContext = undefined;
                    fake_ctx.arena = std.heap.ArenaAllocator.init(calloc);
                    try fs.apply(&c, &fake_ctx);
                }
                if (c.num_rows == 0) continue;
                try updateAccumsFromChunk(self.accums, self.aggs, &c, talloc);
                chunk_arena.deinit();
            }
        }
    };

    // Allocate per-thread accumulators.
    const thread_accums = try alloc.alloc([]AggAccum, n_threads);
    for (thread_accums) |*ta| {
        ta.* = try alloc.alloc(AggAccum, aggs.len);
        for (aggs, 0..) |item, ci| ta.*[ci] = initAccumForAgg(item.expr);
    }

    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    const pctxs = try alloc.alloc(ParCtx, n_threads);

    for (pctxs, 0..) |*pc, ti| {
        pc.* = .{
            .source      = ctx.source,
            .filter_pred = filter_pred,
            .aggs        = aggs,
            .accums      = thread_accums[ti],
            .morsel_src  = &morsel_src,
            .parent_alloc = alloc,
        };
    }

    try parallel.parallelFor(alloc, ParCtx, ParCtx.work, pctxs, &morsel_src);

    // Check errors.
    for (pctxs) |pc| {
        if (pc.err) |e| return e;
    }

    // Merge all thread accumulators into thread_accums[0].
    const merged = thread_accums[0];
    for (thread_accums[1..]) |ta| {
        for (merged, ta, 0..) |*m, t, ci| {
            try mergeAccum(m, t, aggs[ci], alloc);
        }
    }

    const metas   = try alloc.alloc(result.ColMeta, aggs.len);
    const out_row = try alloc.alloc(?Value, aggs.len);
    for (aggs, 0..) |item, ci| {
        metas[ci]   = .{ .name = item.alias, .col_type = item.out_type };
        out_row[ci] = try finalizeAccum(merged[ci], item, alloc);
    }
    var rl = RowList.init(metas);
    try rl.append(alloc, out_row);
    return rl;
}

/// Merge accumulator `src` into `dst` in-place.
fn mergeAccum(dst: *AggAccum, src: AggAccum, item: plan.ProjectItem, alloc: std.mem.Allocator) !void {
    _ = item;
    _ = alloc;
    switch (dst.*) {
        .count     => dst.count     += src.count,
        .i64_sum   => dst.i64_sum   +%= src.i64_sum,
        .u64_sum   => dst.u64_sum   +%= src.u64_sum,
        .f64_sum   => dst.f64_sum   += src.f64_sum,
        .i64_min   => dst.i64_min   = @min(dst.i64_min, src.i64_min),
        .i64_max   => dst.i64_max   = @max(dst.i64_max, src.i64_max),
        .u64_min   => dst.u64_min   = @min(dst.u64_min, src.u64_min),
        .u64_max   => dst.u64_max   = @max(dst.u64_max, src.u64_max),
        .f64_min   => dst.f64_min   = @min(dst.f64_min, src.f64_min),
        .f64_max   => dst.f64_max   = @max(dst.f64_max, src.f64_max),
        .str_min   => {
            if (src.str_min) |sv| {
                if (dst.str_min == null or std.mem.lessThan(u8, sv, dst.str_min.?)) {
                    dst.str_min = sv;
                }
            }
        },
        .str_max   => {
            if (src.str_max) |sv| {
                if (dst.str_max == null or std.mem.lessThan(u8, dst.str_max.?, sv)) {
                    dst.str_max = sv;
                }
            }
        },
        .any_val   => {
            if (dst.any_val == null) dst.any_val = src.any_val;
        },
        // For uniq_strs (count_distinct), parallel merge is complex; skip.
        .uniq_strs => {},
    }
}
/// a necessary (but not sufficient) condition for the int-key fast path.
fn keysAreIntExpr(keys: []const plan.ProjectItem) bool {
    for (keys) |k| {
        switch (k.expr) {
            .col_ref => {},
            .add => |op| { if (op.left != .col_ref or op.right != .lit_i64) return false; },
            .sub => |op| { if (op.left != .col_ref or op.right != .lit_i64) return false; },
            else => return false,
        }
    }
    return true;
}

/// Returns true if all keys are plain col_ref expressions.
fn keysAreColRef(keys: []const plan.ProjectItem) bool {
    for (keys) |k| {
        if (k.expr != .col_ref) return false;
    }
    return true;
}


/// Drive the source chunk by chunk and build a hash aggregate without rows.
/// Convert compact u64 accumulator values to output Values for emit.
/// Shared between CompactIntKeyHashTable and StrAggHashTable emit paths.
fn emitCompactVals(
    vals:  []const u64,
    kinds: []const ht.CompactAggKind,
    aggs:  []const plan.ProjectItem,
    out:   []?Value,
) void {
    for (vals, kinds, aggs, 0..) |v, kind, item, i| {
        out[i] = switch (kind) {
            .count, .u64_sum, .u64_min, .u64_max => Value{ .uint64 = v },
            .i64_sum, .i64_min, .i64_max => Value{ .int64 = @bitCast(v) },
            .f64_sum => blk: {
                const sum: f64 = @bitCast(v);
                if (item.expr == .agg_call and item.expr.agg_call.kind == .avg) {
                    var cnt: u64 = 0;
                    for (vals, kinds) |cv, ck| {
                        if (ck == .count) { cnt = cv; break; }
                    }
                    if (cnt > 0) break :blk Value{ .float64 = sum / @as(f64, @floatFromInt(cnt)) };
                }
                break :blk Value{ .float64 = sum };
            },
            .f64_min, .f64_max => Value{ .float64 = @bitCast(v) },
            // str_min/str_max: emitted via sidecar; return empty string as sentinel.
            .str_min, .str_max => Value{ .string = "" },
        };
    }
}

/// Like emitCompactVals but reads str_min/str_max from StrAggHashTable sidecar.
fn emitCompactValsWithSidecar(
    vals:         []const u64,
    kinds:        []const ht.CompactAggKind,
    aggs:         []const plan.ProjectItem,
    out:          []?Value,
    str_ht:       *const ht.StrAggHashTable,
    slot:         usize,
    sidecar_idx:  []const usize,
) void {
    for (vals, kinds, aggs, 0..) |v, kind, item, i| {
        out[i] = switch (kind) {
            .count, .u64_sum, .u64_min, .u64_max => Value{ .uint64 = v },
            .i64_sum, .i64_min, .i64_max => Value{ .int64 = @bitCast(v) },
            .f64_sum => blk: {
                const sum: f64 = @bitCast(v);
                if (item.expr == .agg_call and item.expr.agg_call.kind == .avg) {
                    var cnt: u64 = 0;
                    for (vals, kinds) |cv, ck| {
                        if (ck == .count) { cnt = cv; break; }
                    }
                    if (cnt > 0) break :blk Value{ .float64 = sum / @as(f64, @floatFromInt(cnt)) };
                }
                break :blk Value{ .float64 = sum };
            },
            .f64_min, .f64_max => Value{ .float64 = @bitCast(v) },
            .str_min, .str_max => blk: {
                const s = str_ht.getStrSidecar(slot, sidecar_idx[i]) orelse "";
                break :blk Value{ .string = s };
            },
        };
    }
}

/// Update compact u64 accumulator slots for a single row.
/// Shared between int-key and str-agg paths to avoid code duplication.
inline fn updateCompactVals(
    slot_vals:       []u64,
    ck:              []const ht.CompactAggKind,
    aggs:            []const plan.ProjectItem,
    c:               *const DataChunk,
    r:               usize,
    str_ht:          ?*ht.StrAggHashTable,
    slot:            usize,
    sidecar_indices: []const usize,
) !void {
    for (aggs, 0..) |item, ci| {
        if (item.expr != .agg_call) continue;
        const ac = item.expr.agg_call;
        switch (ck[ci]) {
            .count => {
                if (ac.kind == .count_star) {
                    slot_vals[ci] += 1;
                } else if (ac.kind == .count) {
                    if (ac.arg) |arg| {
                        if (arg == .col_ref) {
                            if (!c.columns[arg.col_ref.index].isRowNull(r))
                                slot_vals[ci] += 1;
                        } else slot_vals[ci] += 1;
                    }
                }
            },
            .i64_sum => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .int64  => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                        .uint64 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += @as(i64, @bitCast(v[r])); slot_vals[ci] = @bitCast(s); },
                        .bool_u8 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                        else => {},
                    };
                }}
            },
            .f64_sum => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .float64 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                        .int64   => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                        .uint64  => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                        else => {},
                    };
                }}
            },
            .i64_min => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .int64 => |v| { const cur: i64 = @bitCast(slot_vals[ci]); if (v[r] < cur) slot_vals[ci] = @bitCast(v[r]); },
                        else => {},
                    };
                }}
            },
            .i64_max => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .int64 => |v| { const cur: i64 = @bitCast(slot_vals[ci]); if (v[r] > cur) slot_vals[ci] = @bitCast(v[r]); },
                        else => {},
                    };
                }}
            },
            .u64_sum => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .uint64 => |v| slot_vals[ci] += v[r],
                        else => {},
                    };
                }}
            },
            .u64_min => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .uint64 => |v| { if (v[r] < slot_vals[ci]) slot_vals[ci] = v[r]; },
                        else => {},
                    };
                }}
            },
            .u64_max => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .uint64 => |v| { if (v[r] > slot_vals[ci]) slot_vals[ci] = v[r]; },
                        else => {},
                    };
                }}
            },
            .f64_min => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .float64 => |v| { const cur: f64 = @bitCast(slot_vals[ci]); if (v[r] < cur) slot_vals[ci] = @bitCast(v[r]); },
                        else => {},
                    };
                }}
            },
            .f64_max => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .float64 => |v| { const cur: f64 = @bitCast(slot_vals[ci]); if (v[r] > cur) slot_vals[ci] = @bitCast(v[r]); },
                        else => {},
                    };
                }}
            },
            .str_min => {
                if (str_ht) |sht| {
                    if (ac.arg) |arg| { if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .string => |v| sht.updateStrSidecar(slot, sidecar_indices[ci], v[r], true),
                            else => {},
                        };
                    }}
                }
            },
            .str_max => {
                if (str_ht) |sht| {
                    if (ac.arg) |arg| { if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .string => |v| sht.updateStrSidecar(slot, sidecar_indices[ci], v[r], false),
                            else => {},
                        };
                    }}
                }
            },
        }
    }
}

fn executeHashAggChunked(
    input: *const plan.PhysicalNode,
    keys:  []const plan.ProjectItem,
    aggs:  []const plan.ProjectItem,
    ctx:   *QueryContext,
) !RowList {
    const alloc = ctx.allocator();
    // Pre-size hash table when input is a bare scan (no filter reduces cardinality).
    // Use min(row_count, 2M) to avoid excessive memory that degrades subsequent queries.
    const MAX_PRESIZED: u64 = 8_000_000;
    const est_rows: u64 = switch (input.*) {
        .part_scan, .mem_scan, .chunk_source => @min(ctx.source.rowCount(), MAX_PRESIZED),
        else => 0,
    };

    // Fast path: if all keys are integer col_ref / col_ref±lit expressions, try
    // IntKeyHashTable (no []Value boxing, inline key storage).
    const maybe_int_keys = keysAreIntExpr(keys);

    // Compact accum fast path: if all aggs are pure-numeric, use
    // CompactIntKeyHashTable (8B/agg vs 32B/agg), cutting the accum slab 4×.
    // Only active when maybe_int_keys is true.
    const compact_kinds: ?[]ht.CompactAggKind = blk: {
        const kinds = try alloc.alloc(ht.CompactAggKind, aggs.len);
        for (aggs, 0..) |item, ci| {
            if (item.expr != .agg_call) break :blk null;
            kinds[ci] = switch (item.expr.agg_call.kind) {
                .count_star, .count => .count,
                .sum  => .i64_sum,   // type refined at runtime (int64/uint64/f64)
                .avg  => .f64_sum,
                // min/max: string args use str_min/str_max (StrAggHashTable sidecar);
                // numeric args use the appropriate numeric kind (refined at runtime).
                .min  => if (item.out_type == .string) .str_min else .i64_min,
                .max  => if (item.out_type == .string) .str_max else .i64_max,
                .group_uniq_array, .any => break :blk null,
            };
        }
        break :blk kinds;
    };
    // init_vals: u64 encoding of the initial value per compact agg kind.
    const compact_init_vals: []u64 = if (compact_kinds) |ck| blk: {
        const iv = try alloc.alloc(u64, ck.len);
        for (ck, 0..) |kind, ci| {
            iv[ci] = switch (kind) {
                .count, .i64_sum, .u64_sum, .u64_max => 0,
                .f64_sum => @bitCast(@as(f64, 0.0)),
                .i64_min => @bitCast(@as(i64, std.math.maxInt(i64))),
                .i64_max => @bitCast(@as(i64, std.math.minInt(i64))),
                .u64_min => std.math.maxInt(u64),
                .f64_min => @bitCast(std.math.inf(f64)),
                .f64_max => @bitCast(-std.math.inf(f64)),
                // str_min/str_max: vals_flat slot unused; sidecar handles the string.
                .str_min, .str_max => 0,
            };
        }
        break :blk iv;
    } else &.{};

    // Count str_min/str_max aggs for StrAggHashTable sidecar sizing.
    const num_str_aggs: usize = if (compact_kinds) |ck| blk: {
        var n: usize = 0;
        for (ck) |k| { if (k == .str_min or k == .str_max) n += 1; }
        break :blk n;
    } else 0;
    // Map compact_kind index → sidecar index (only valid for str_min/str_max entries).
    const str_agg_sidecar_idx: []usize = if (compact_kinds) |ck| blk: {
        const m = try alloc.alloc(usize, ck.len);
        var si: usize = 0;
        for (ck, 0..) |k, ci| {
            if (k == .str_min or k == .str_max) { m[ci] = si; si += 1; }
            else m[ci] = 0;
        }
        break :blk m;
    } else &.{};

    // Detect Q29-style regexp_replace(col_ref, lit_str_pattern, lit_str_repl) key.
    // Cache col_idx + whether it's the URL-domain pattern to avoid per-row checks.
    const RegexpReplaceKeyDesc = struct {
        col_idx: usize,
        is_url_domain: bool,  // true = Q29 fast path
    };
    var regexp_replace_key_descs: ?[]RegexpReplaceKeyDesc = null;
    check_rr: {
        if (keys.len == 0) break :check_rr;
        // Quick pre-check: first key must be a fn_call named regexp_replace.
        if (keys[0].expr != .fn_call) break :check_rr;
        const fc0 = keys[0].expr.fn_call;
        if (!(std.mem.eql(u8, fc0.name, "regexp_replace") or
              std.mem.eql(u8, fc0.name, "replaceRegexpOne"))) break :check_rr;
        // All keys must be regexp_replace(col_ref, lit_str, lit_str).
        const descs_buf = try alloc.alloc(RegexpReplaceKeyDesc, keys.len);
        for (keys, 0..) |k, ki| {
            if (k.expr != .fn_call) break :check_rr;
            const fc = k.expr.fn_call;
            if (!(std.mem.eql(u8, fc.name, "regexp_replace") or
                  std.mem.eql(u8, fc.name, "replaceRegexpOne")) or
                fc.args.len < 3 or
                fc.args[0] != .col_ref or
                fc.args[1] != .lit_str or
                fc.args[2] != .lit_str)
            {
                break :check_rr;
            }
            const pattern = fc.args[1].lit_str;
            const is_url = std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*$") or
                           std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*");
            descs_buf[ki] = .{ .col_idx = fc.args[0].col_ref.index, .is_url_domain = is_url };
        }
        regexp_replace_key_descs = descs_buf;
    }

    // StrAggHashTable for regexp_replace single-key path (e.g. Q29).
    // Initialized here if rr_descs is single-key and aggs are compact-numeric.
    const rr_can_use_str_agg = compact_kinds != null and
        regexp_replace_key_descs != null and
        regexp_replace_key_descs.?.len == 1;

    var ht_agg = try ht.AggHashTable.initWithCapacity(alloc, keys.len, aggs.len, est_rows);
    // Use CompactIntKeyHashTable when keys are all-int AND aggs are all pure-numeric.
    // Falls back to IntKeyHashTable when compact_kinds is null (e.g. any_val agg).
    var ht_compact: ?ht.CompactIntKeyHashTable = if (compact_kinds != null and maybe_int_keys and num_str_aggs == 0)
        try ht.CompactIntKeyHashTable.initWithCapacity(alloc, keys.len, aggs.len, est_rows)
    else null;
    var ht_int: ?ht.IntKeyHashTable = if (maybe_int_keys and (compact_kinds == null or num_str_aggs > 0))
        try ht.IntKeyHashTable.initWithCapacity(alloc, keys.len, aggs.len, est_rows)
    else null;

    // StrCountHashTable fast path: exactly one col_ref key (others may be constants) + count(*) agg.
    // Handles Q34 (GROUP BY URL) and Q35 (GROUP BY 1, URL).
    const maybe_str_count = blk: {
        if (aggs.len != 1) break :blk false;
        if (aggs[0].expr != .agg_call) break :blk false;
        if (aggs[0].expr.agg_call.kind != .count_star) break :blk false;
        var col_ref_count: usize = 0;
        for (keys) |k| {
            switch (k.expr) {
                .col_ref => col_ref_count += 1,
                .lit_i64, .lit_str => {},
                else => break :blk false,
            }
        }
        break :blk col_ref_count == 1;
    };
    var ht_str_count: ?ht.StrCountHashTable = null;
    var str_count_col_idx: usize = 0;
    var use_str_count_path: bool = false;

    // StrAggHashTable fast path: single string col_ref key + all-compact aggs
    // (including str_min/str_max via sidecar).
    // Handles Q22/Q23 (GROUP BY SearchPhrase + MIN/COUNT) and the Q29 regexp_replace path.
    // Also triggered when maybe_str_count would apply but there are additional aggs beyond COUNT(*).
    const str_agg_col_idx: ?usize = blk: {
        if (maybe_int_keys) break :blk null;      // int key path takes priority
        if (compact_kinds == null) break :blk null; // aggs not all compact
        if (keys.len != 1) break :blk null;        // single key only
        if (keys[0].expr != .col_ref) break :blk null;
        break :blk keys[0].expr.col_ref.index;
    };
    var ht_str_agg: ?ht.StrAggHashTable = if (str_agg_col_idx != null or rr_can_use_str_agg)
        try ht.StrAggHashTable.initWithCapacity(alloc, aggs.len, num_str_aggs, est_rows)
    else null;
    var use_str_agg_path: bool = false;
    // Set to true when regexp_replace key path routes to ht_str_agg (e.g. Q29).
    var rr_used_str_agg: bool = false;

    // PairCountHashTable fast path: exactly two col_ref keys (one i64, one string) + count(*).
    // Handles Q17/Q18 (GROUP BY UserID, SearchPhrase) and Q19 (3 keys — not handled here).
    const maybe_pair_count = blk: {
        if (aggs.len != 1) break :blk false;
        if (aggs[0].expr != .agg_call) break :blk false;
        if (aggs[0].expr.agg_call.kind != .count_star) break :blk false;
        var col_ref_count: usize = 0;
        for (keys) |k| {
            if (k.expr == .col_ref) col_ref_count += 1
            else break :blk false;
        }
        break :blk col_ref_count == 2;
    };
    var ht_pair_count: ?ht.PairCountHashTable = null;
    var pair_i64_col_idx: usize = 0;
    var pair_str_col_idx: usize = 0;
    var use_pair_count_path: bool = false;

    // TripleCountHashTable fast path: (i64_col, date_part(unit, datetime_col), string_col) + count(*).
    // Handles Q19: GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase.
    const DatePartUnit = enum { minute, hour, day };
    const TripleDesc = struct {
        n0_col:   usize,   // first i64 col_ref index
        dp_col:   usize,   // col_ref index inside date_part(...)
        dp_unit:  DatePartUnit,
        str_col:  usize,   // string col_ref index
        // Order of keys in output row: 0=n0, 1=dp, 2=str  or some permutation.
        key_order: [3]u8,  // key_order[i] = which variable fills keys[i]
    };
    const maybe_triple_count: ?TripleDesc = blk: {
        if (aggs.len != 1) break :blk null;
        if (aggs[0].expr != .agg_call) break :blk null;
        if (aggs[0].expr.agg_call.kind != .count_star) break :blk null;
        if (keys.len != 3) break :blk null;
        // Find which key is the date_part fn_call and record the others.
        var dp_idx: ?usize = null;
        var dp_col: usize = 0;
        var dp_unit: DatePartUnit = .minute;
        var col_ref_indices: [2]usize = .{0, 0};
        var cri: usize = 0;
        for (keys, 0..) |k, ki| {
            switch (k.expr) {
                .col_ref => {
                    if (cri >= 2) break :blk null;
                    col_ref_indices[cri] = ki;
                    cri += 1;
                },
                .fn_call => |fc| {
                    if (dp_idx != null) break :blk null; // two fn_calls
                    if (!(std.mem.eql(u8, fc.name, "date_part") or
                          std.mem.eql(u8, fc.name, "extract"))) break :blk null;
                    if (fc.args.len < 2) break :blk null;
                    if (fc.args[0] != .lit_str) break :blk null;
                    if (fc.args[1] != .col_ref) break :blk null;
                    const unit_str = fc.args[0].lit_str;
                    dp_unit = if (std.mem.eql(u8, unit_str, "minute") or std.mem.eql(u8, unit_str, "min"))
                        .minute
                    else if (std.mem.eql(u8, unit_str, "hour"))
                        .hour
                    else if (std.mem.eql(u8, unit_str, "day") or std.mem.eql(u8, unit_str, "dayofmonth"))
                        .day
                    else
                        break :blk null;
                    dp_col = fc.args[1].col_ref.index;
                    dp_idx = ki;
                },
                else => break :blk null,
            }
        }
        if (dp_idx == null or cri != 2) break :blk null;
        const key_order: [3]u8 = blk2: {
            var order: [3]u8 = .{0, 0, 0};
            for (keys, 0..) |k, ki| {
                if (ki == dp_idx.?) order[ki] = 1  // date_part → n1
                else if (k.expr == .col_ref) order[ki] = if (ki == col_ref_indices[0]) 0 else 2;
            }
            break :blk2 order;
        };
        _ = key_order;
        break :blk TripleDesc{
            .n0_col    = keys[col_ref_indices[0]].expr.col_ref.index,
            .dp_col    = dp_col,
            .dp_unit   = dp_unit,
            .str_col   = keys[col_ref_indices[1]].expr.col_ref.index,
            .key_order = blk2: {
                var order: [3]u8 = .{0, 0, 0};
                for (keys, 0..) |_, ki| {
                    if (ki == dp_idx.?) order[ki] = 1
                    else if (ki == col_ref_indices[0]) order[ki] = 0
                    else order[ki] = 2;
                }
                break :blk2 order;
            },
        };
    };
    var ht_triple_count: ?ht.TripleCountHashTable = null;
    var use_triple_count_path: bool = false;
    var triple_desc: TripleDesc = if (maybe_triple_count) |d| d else .{
        .n0_col = 0, .dp_col = 0, .dp_unit = .minute, .str_col = 0, .key_order = .{0,1,2},
    };

    const init_accums = try alloc.alloc(AggAccum, aggs.len);
    for (aggs, 0..) |item, ci| init_accums[ci] = initAccumForAgg(item.expr);
    const key_buf     = try alloc.alloc(Value, keys.len);
    const int_key_buf = try alloc.alloc(i64,   keys.len);

    var filter_state: ?FilterState = extractFilter(input);
    // Once we've verified on the first chunk that all key columns are int64/uint64,
    // this flag is set to true and we use ht_int for all subsequent rows.
    var use_int_path: bool = false;
    var int_path_checked: bool = false;

    // Column descriptors for int key path: per key, col index and addend.
    const IntKeyDesc = struct { col_idx: usize, addend: i64 };
    const int_key_descs = try alloc.alloc(IntKeyDesc, keys.len);

    // Compute which column indices are referenced by keys and aggs.
    // Apply column restriction to avoid decoding unused columns.
    {
        var needed_mask = [_]bool{false} ** 256;
        const ncols = @min(256, ctx.source.schema().len);
        for (keys) |item| collectColRefs(item.expr, needed_mask[0..ncols]);
        for (aggs) |item| collectColRefs(item.expr, needed_mask[0..ncols]);
        if (filter_state) |*fs| collectColRefs(fs.predicate, needed_mask[0..ncols]);
        var needed_count: usize = 0;
        for (needed_mask[0..ncols]) |m| { if (m) needed_count += 1; }
        if (needed_count * 2 < ctx.source.schema().len) {
            var names_buf: [32][]const u8 = undefined;
            var names_len: usize = 0;
            const sm = ctx.source.schema();
            for (needed_mask[0..ncols], 0..) |m, i| {
                if (m and names_len < names_buf.len) {
                    names_buf[names_len] = sm[i].name;
                    names_len += 1;
                }
            }
            ctx.source.setNeededCols(names_buf[0..names_len]);
        }
    }
    defer ctx.source.setNeededCols(null);

    ctx.source.reset();
    var ref_indices: ?[]usize = null;

    var c: DataChunk = undefined;
    var row_buf: []?Value = &.{};
    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state) |*fs| try fs.apply(&c, ctx);
        if (c.num_rows == 0) continue;
        // Build ref_indices once (on first non-empty chunk).
        if (ref_indices == null) {
            row_buf = try alloc.alloc(?Value, c.columns.len);
            @memset(row_buf, null);
            const mask = try alloc.alloc(bool, c.columns.len);
            @memset(mask, false);
            for (keys) |k| collectColRefs(k.expr, mask);
            for (aggs) |a| collectColRefs(a.expr, mask);
            var cnt: usize = 0;
            for (mask) |m| { if (m) cnt += 1; }
            const idxs = try alloc.alloc(usize, cnt);
            var wi: usize = 0;
            for (mask, 0..) |m, j| { if (m) { idxs[wi] = j; wi += 1; } }
            ref_indices = idxs;
        }
        // Verify int-key eligibility on first chunk.
        if (!int_path_checked) {
            int_path_checked = true;
            if (maybe_int_keys) {
                var all_int = true;
                for (keys, 0..) |k, ki| {
                    const col_idx: usize = switch (k.expr) {
                        .col_ref => |cr| cr.index,
                        .add     => |op| op.left.col_ref.index,
                        .sub     => |op| op.left.col_ref.index,
                        else => { all_int = false; break; },
                    };
                    const addend: i64 = switch (k.expr) {
                        .col_ref => 0,
                        .add     => |op| op.right.lit_i64,
                        .sub     => |op| -op.right.lit_i64,
                        else     => 0,
                    };
                    const cd = c.columns[col_idx];
                    switch (cd.data) {
                        .int64, .uint64 => {},
                        else => { all_int = false; break; },
                    }
                    int_key_descs[ki] = .{ .col_idx = col_idx, .addend = addend };
                }
                use_int_path = all_int;
            }
            // Verify str-count eligibility (single string key col).
            if (maybe_str_count and !use_int_path) {
                // Find the single col_ref key (others are literals).
                var found_col_ref: ?usize = null;
                for (keys) |k| {
                    if (k.expr == .col_ref) { found_col_ref = k.expr.col_ref.index; break; }
                }
                if (found_col_ref) |col_idx| {
                    if (col_idx < c.columns.len) {
                        switch (c.columns[col_idx].data) {
                            .string => {
                                str_count_col_idx = col_idx;
                                ht_str_count = try ht.StrCountHashTable.initWithCapacity(alloc, est_rows);
                                use_str_count_path = true;
                            },
                            else => {},
                        }
                    }
                }
            }
            // Verify str-agg eligibility: single string col_ref key + compact numeric aggs.
            if (str_agg_col_idx) |col_idx| {
                if (!use_str_count_path and col_idx < c.columns.len and
                    c.columns[col_idx].data == .string)
                {
                    use_str_agg_path = true;
                }
            }
            // Verify pair-count eligibility: exactly two col_refs, one i64 and one string.
            if (maybe_pair_count and !use_int_path and !use_str_count_path) {
                const c0 = keys[0].expr.col_ref.index;
                const c1 = keys[1].expr.col_ref.index;
                if (c0 < c.columns.len and c1 < c.columns.len) {
                    const d0 = c.columns[c0].data;
                    const d1 = c.columns[c1].data;
                    const ok0_i = (d0 == .int64 or d0 == .uint64) and d1 == .string;
                    const ok1_i = (d1 == .int64 or d1 == .uint64) and d0 == .string;
                    if (ok0_i) {
                        pair_i64_col_idx = c0;
                        pair_str_col_idx = c1;
                        ht_pair_count = try ht.PairCountHashTable.initWithCapacity(alloc, est_rows);
                        use_pair_count_path = true;
                    } else if (ok1_i) {
                        pair_i64_col_idx = c1;
                        pair_str_col_idx = c0;
                        ht_pair_count = try ht.PairCountHashTable.initWithCapacity(alloc, est_rows);
                        use_pair_count_path = true;
                    }
                }
            }
            // Verify triple-count eligibility: (i64, date_part_datetime, string) + count(*).
            if (maybe_triple_count != null and !use_int_path and !use_str_count_path and !use_pair_count_path) {
                const td = maybe_triple_count.?;
                if (td.n0_col < c.columns.len and td.dp_col < c.columns.len and td.str_col < c.columns.len) {
                    const n0_ok = c.columns[td.n0_col].data == .int64 or c.columns[td.n0_col].data == .uint64;
                    const dp_ok = c.columns[td.dp_col].data == .datetime64_ms or c.columns[td.dp_col].data == .int64;
                    const str_ok = c.columns[td.str_col].data == .string;
                    if (n0_ok and dp_ok and str_ok) {
                        triple_desc = td;
                        ht_triple_count = try ht.TripleCountHashTable.initWithCapacity(alloc, est_rows);
                        use_triple_count_path = true;
                    }
                }
            }
        }
        const refs = ref_indices.?;

        if (use_str_count_path) {
            // ── String-key count(*) fast path ─────────────────────────────────
            const col = c.columns[str_count_col_idx];
            const strs = col.data.string;
            for (0..c.num_rows) |r| {
                try ht_str_count.?.increment(strs[r]);
            }
            continue;
        }

        if (use_pair_count_path) {
            // ── (i64, string) pair count(*) fast path ─────────────────────────
            const strs = c.columns[pair_str_col_idx].data.string;
            // Handle both int64 and uint64 key columns.
            switch (c.columns[pair_i64_col_idx].data) {
                .int64  => |ints| { for (0..c.num_rows) |r| try ht_pair_count.?.increment(ints[r], strs[r]); },
                .uint64 => |ints| { for (0..c.num_rows) |r| try ht_pair_count.?.increment(@bitCast(ints[r]), strs[r]); },
                else    => unreachable,
            }
            continue;
        }

        if (use_triple_count_path) {
            // ── (i64, date_part, string) triple count(*) fast path ────────────
            const td = triple_desc;
            const n0_col = c.columns[td.n0_col];
            const dp_col = c.columns[td.dp_col];
            const strs   = c.columns[td.str_col].data.string;
            for (0..c.num_rows) |r| {
                const n0: i64 = switch (n0_col.data) {
                    .int64  => |v| v[r],
                    .uint64 => |v| @bitCast(v[r]),
                    else    => unreachable,
                };
                const ms: i64 = switch (dp_col.data) {
                    .datetime64_ms => |v| v[r],
                    .int64         => |v| v[r] * 1000,
                    else           => unreachable,
                };
                const secs = @divTrunc(ms, 1000);
                const n1: i64 = switch (td.dp_unit) {
                    .minute => @mod(@divTrunc(secs, 60), 60),
                    .hour   => @mod(@divTrunc(secs, 3600), 24),
                    .day    => blk: {
                        const days = @divTrunc(ms, 86400 * 1000);
                        // Simple day-of-month: reuse date math from kernels.
                        const d = if (days >= 0) @as(u64, @intCast(days)) else 0;
                        // Gregorian calendar: days since epoch.
                        const n: u64 = d + 719468;
                        const era: u64 = @divTrunc(n, 146097);
                        const doe: u64 = n - era * 146097;
                        const yoe: u64 = @divTrunc(doe - @divTrunc(doe, 1460) + @divTrunc(doe, 36524) - @divTrunc(doe, 146096), 365);
                        const doy: u64 = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
                        const mp:  u64 = @divTrunc(5 * doy + 2, 153);
                        break :blk @intCast(doy - @divTrunc(153 * mp + 2, 5) + 1);
                    },
                };
                try ht_triple_count.?.increment(n0, n1, strs[r]);
            }
            continue;
        }

        if (use_int_path) {
            // ── Integer-key fast path ──────────────────────────────────────────
            if (ht_compact) |*htc| {
                // ── Compact accum sub-path: 8B/agg instead of 32B/agg ─────────
                const ck = compact_kinds.?;
                for (0..c.num_rows) |r| {
                    var key_valid = true;
                    for (int_key_descs, 0..) |desc, ki| {
                        const col = c.columns[desc.col_idx];
                        if (chunk.isNull(col.null_mask, r)) { key_valid = false; break; }
                        int_key_buf[ki] = switch (col.data) {
                            .int64  => |v| v[r] +% desc.addend,
                            .uint64 => |v| @as(i64, @bitCast(v[r])) +% desc.addend,
                            else    => { key_valid = false; break; },
                        };
                    }
                    if (!key_valid) continue;
                    const slot_vals = try htc.getOrInsert(int_key_buf, compact_init_vals);
                    try updateCompactVals(slot_vals, ck, aggs, &c, r, null, 0, str_agg_sidecar_idx);
                }
            } else {
            // ── Regular AggAccum sub-path ──────────────────────────────────────
            for (0..c.num_rows) |r| {
                // Build int key without Value boxing.
                var key_valid = true;
                for (int_key_descs, 0..) |desc, ki| {
                    const col = c.columns[desc.col_idx];
                    if (chunk.isNull(col.null_mask, r)) { key_valid = false; break; }
                    const raw: i64 = switch (col.data) {
                        .int64  => |v| v[r],
                        .uint64 => |v| @bitCast(v[r]),
                        else    => { key_valid = false; break; },
                    };
                    int_key_buf[ki] = raw +% desc.addend;
                }
                if (!key_valid) continue;
                const bucket = try ht_int.?.getOrInsert(int_key_buf, init_accums);
                // Update accumulators (still uses row_buf for agg args).
                for (refs) |j| {
                    const col = c.columns[j];
                    row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                for (aggs, 0..) |item, ci| {
                    const v_opt = try evalAggArg(item.expr, row_buf, alloc);
                    try kernels.updateAccum(&bucket[ci], v_opt, alloc);
                }
            }
            } // end else (regular path)
    } else if (use_str_agg_path) {
            // ── String-key compact agg path (e.g. Q22/Q23 GROUP BY SearchPhrase) ──
            const col_idx = str_agg_col_idx.?;
            const ck      = compact_kinds.?;
            const strs    = c.columns[col_idx].data.string;
            for (0..c.num_rows) |r| {
                if (c.columns[col_idx].isRowNull(r)) continue;
                const s = strs[r];
                const res = try ht_str_agg.?.getOrInsert(s, compact_init_vals);
                try updateCompactVals(res.vals, ck, aggs, &c, r, &ht_str_agg.?, res.slot, str_agg_sidecar_idx);
            }
        } else if (regexp_replace_key_descs) |rr_descs| {
            // ── regexp_replace key fast path (e.g. Q29) ───────────────────────
            // Avoids per-row pattern string comparison in evalFnCall.
            const use_rr_str_agg = ht_str_agg != null and rr_descs.len == 1;
            if (use_rr_str_agg) rr_used_str_agg = true;
            const ck = compact_kinds;
            for (0..c.num_rows) |r| {
                for (refs) |j| {
                    const col = c.columns[j];
                    row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                var key_valid = true;
                var domain_str: []const u8 = "";
                for (rr_descs, 0..) |desc, ki| {
                    const s_opt = row_buf[desc.col_idx];
                    const s = if (s_opt) |v| (v.toStr() orelse null) else null;
                    const domain: ?Value = if (s) |str| d: {
                        if (desc.is_url_domain) {
                            const after_proto = if (std.mem.startsWith(u8, str, "https://"))
                                str[8..]
                            else if (std.mem.startsWith(u8, str, "http://"))
                                str[7..]
                            else
                                break :d Value{ .string = str };
                            const slash = std.mem.indexOfScalar(u8, after_proto, '/') orelse
                                break :d Value{ .string = str };
                            var host = after_proto[0..slash];
                            if (std.mem.startsWith(u8, host, "www.")) host = host[4..];
                            break :d Value{ .string = host };
                        }
                        break :d Value{ .string = str };
                    } else null;
                    if (domain == null) { key_valid = false; break; }
                    key_buf[ki] = domain.?;
                    if (ki == 0) domain_str = domain.?.string;
                }
                if (!key_valid) continue;
                if (use_rr_str_agg) {
                    const res = try ht_str_agg.?.getOrInsert(domain_str, compact_init_vals);
                    try updateCompactVals(res.vals, ck.?, aggs, &c, r, &ht_str_agg.?, res.slot, str_agg_sidecar_idx);
                } else {
                    const bucket = try ht_agg.getOrInsert(key_buf, init_accums);
                    for (aggs, 0..) |item, ci| {
                        const v_opt = try evalAggArg(item.expr, row_buf, alloc);
                        try kernels.updateAccum(&bucket[ci], v_opt, alloc);
                    }
                }
            }
        } else {
            // ── General path ──────────────────────────────────────────────────
            for (0..c.num_rows) |r| {
                // Fill only referenced columns.
                for (refs) |j| {
                    const col = c.columns[j];
                    row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                for (keys, 0..) |k, ki| {
                    // Inline fast path for common key expressions (avoids evalExpr dispatch).
                    const v: ?Value = switch (k.expr) {
                        .col_ref => |cr| row_buf[cr.index],
                        .add => |op| blk: {
                            if (op.left == .col_ref and op.right == .lit_i64) {
                                if (row_buf[op.left.col_ref.index]) |base| {
                                    if (base.toI64()) |bv| break :blk Value{ .int64 = bv +% op.right.lit_i64 };
                                }
                            }
                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                        },
                        .sub => |op| blk: {
                            if (op.left == .col_ref and op.right == .lit_i64) {
                                if (row_buf[op.left.col_ref.index]) |base| {
                                    if (base.toI64()) |bv| break :blk Value{ .int64 = bv -% op.right.lit_i64 };
                                }
                            }
                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                        },
                        .fn_call => |fc| blk: {
                            // Fast path: date_part('minute'/'hour', col_ref) — avoids arg eval + string dispatch.
                            if (fc.args.len == 2 and
                                fc.args[0] == .lit_str and
                                fc.args[1] == .col_ref)
                            {
                                const unit = fc.args[0].lit_str;
                                const col_idx = fc.args[1].col_ref.index;
                                if (row_buf[col_idx]) |ts_val| {
                                    const ms: i64 = switch (ts_val) {
                                        .datetime64_ms => |m| m,
                                        .int64         => |i| i * 1000,
                                        else           => {
                                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                                        },
                                    };
                                    const secs = @divTrunc(ms, 1000);
                                    if (std.mem.eql(u8, unit, "minute") or std.mem.eql(u8, unit, "min")) {
                                        break :blk Value{ .int64 = @mod(@divTrunc(secs, 60), 60) };
                                    }
                                    if (std.mem.eql(u8, unit, "hour")) {
                                        break :blk Value{ .int64 = @mod(@divTrunc(secs, 3600), 24) };
                                    }
                                }
                            }
                            // Fast path: regexp_replace(col_ref, lit_str_pattern, lit_str_repl)
                            // for the Q29 URL-domain extraction pattern.
                            if (fc.name.len > 0 and
                                (std.mem.eql(u8, fc.name, "regexp_replace") or
                                 std.mem.eql(u8, fc.name, "replaceRegexpOne")) and
                                fc.args.len >= 3 and
                                fc.args[0] == .col_ref and
                                fc.args[1] == .lit_str and
                                fc.args[2] == .lit_str)
                            {
                                const col_idx = fc.args[0].col_ref.index;
                                const pattern = fc.args[1].lit_str;
                                const s = (row_buf[col_idx] orelse break :blk null).toStr() orelse break :blk null;
                                if (std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*$") or
                                    std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*"))
                                {
                                    const after_proto = if (std.mem.startsWith(u8, s, "https://"))
                                        s[8..]
                                    else if (std.mem.startsWith(u8, s, "http://"))
                                        s[7..]
                                    else
                                        break :blk Value{ .string = s };
                                    const slash = std.mem.indexOfScalar(u8, after_proto, '/') orelse
                                        break :blk Value{ .string = s };
                                    var host = after_proto[0..slash];
                                    if (std.mem.startsWith(u8, host, "www.")) host = host[4..];
                                    break :blk Value{ .string = host };
                                }
                            }
                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                        },
                        else => try kernels.evalExpr(k.expr, row_buf, null, alloc),
                    };
                    key_buf[ki] = v orelse Value{ .int64 = 0 };
                }
                const bucket = try ht_agg.getOrInsert(key_buf, init_accums);
                for (aggs, 0..) |item, ci| {
                    const v_opt = try evalAggArg(item.expr, row_buf, alloc);
                    try kernels.updateAccum(&bucket[ci], v_opt, alloc);
                }
            }
        }
    }

    const out_metas = try alloc.alloc(result.ColMeta, keys.len + aggs.len);
    for (keys, 0..) |k, ki| out_metas[ki] = .{ .name = k.alias, .col_type = k.out_type };
    for (aggs, 0..) |a, ai| out_metas[keys.len + ai] = .{ .name = a.alias, .col_type = a.out_type };

    var rl = RowList.init(out_metas);

    if (use_str_count_path) {
        // Emit from StrCountHashTable. Keys may include literals (e.g. Q35: GROUP BY 1, URL).
        const EmitCtxS = struct {
            rl: *RowList, alloc: std.mem.Allocator,
            keys: []const plan.ProjectItem,
        };
        var emit_ctx_s = EmitCtxS{ .rl = &rl, .alloc = alloc, .keys = keys };
        ht_str_count.?.iterate(&emit_ctx_s, struct {
            fn cb(ec: *EmitCtxS, s: []const u8, count: u64) void {
                const row = ec.alloc.alloc(?Value, ec.keys.len + 1) catch return;
                for (ec.keys, 0..) |k, i| {
                    row[i] = switch (k.expr) {
                        .col_ref => Value{ .string = s },
                        .lit_i64 => |v| Value{ .int64 = v },
                        .lit_str => |v| Value{ .string = v },
                        else => Value{ .int64 = 0 },
                    };
                }
                row[ec.keys.len] = Value{ .uint64 = count };
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_pair_count_path) {
        // Emit from PairCountHashTable: restore key order (i64, str or str, i64).
        const k0_is_i64 = keys[0].expr.col_ref.index == pair_i64_col_idx;
        const EmitCtxP = struct {
            rl: *RowList, alloc: std.mem.Allocator, k0_is_i64: bool,
        };
        var emit_ctx_p = EmitCtxP{ .rl = &rl, .alloc = alloc, .k0_is_i64 = k0_is_i64 };
        ht_pair_count.?.iterate(&emit_ctx_p, struct {
            fn cb(ec: *EmitCtxP, n: i64, s: []const u8, count: u64) void {
                const row = ec.alloc.alloc(?Value, 3) catch return;
                if (ec.k0_is_i64) {
                    row[0] = Value{ .int64 = n };
                    row[1] = Value{ .string = s };
                } else {
                    row[0] = Value{ .string = s };
                    row[1] = Value{ .int64 = n };
                }
                row[2] = Value{ .uint64 = count };
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_triple_count_path) {
        // Emit from TripleCountHashTable: restore key order per triple_desc.key_order.
        const td = triple_desc;
        const EmitCtxT = struct {
            rl: *RowList, alloc: std.mem.Allocator, key_order: [3]u8,
        };
        var emit_ctx_t = EmitCtxT{ .rl = &rl, .alloc = alloc, .key_order = td.key_order };
        ht_triple_count.?.iterate(&emit_ctx_t, struct {
            fn cb(ec: *EmitCtxT, n0: i64, n1: i64, s: []const u8, count: u64) void {
                const row = ec.alloc.alloc(?Value, 4) catch return;
                for (ec.key_order, 0..) |kind, i| {
                    row[i] = switch (kind) {
                        0 => Value{ .int64 = n0 },
                        1 => Value{ .int64 = n1 },
                        else => Value{ .string = s },
                    };
                }
                row[3] = Value{ .uint64 = count };
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_str_agg_path or rr_used_str_agg) {
        // Emit from StrAggHashTable: string key + compact aggs → Values.
        // Uses sidecar for str_min/str_max aggs.
        // Also used when regexp_replace key path routed to ht_str_agg (Q29).
        const EmitCtxSA = struct {
            rl:           *RowList,
            alloc:        std.mem.Allocator,
            aggs:         []const plan.ProjectItem,
            kinds:        []const ht.CompactAggKind,
            str_ht:       *ht.StrAggHashTable,
            sidecar_idx:  []const usize,
        };
        var emit_ctx_sa = EmitCtxSA{
            .rl          = &rl,
            .alloc       = alloc,
            .aggs        = aggs,
            .kinds       = compact_kinds.?,
            .str_ht      = &ht_str_agg.?,
            .sidecar_idx = str_agg_sidecar_idx,
        };
        ht_str_agg.?.iterateWithSlot(&emit_ctx_sa, struct {
            fn cb(ec: *EmitCtxSA, s: []const u8, vals: []const u64, slot: usize) void {
                const row = ec.alloc.alloc(?Value, 1 + vals.len) catch return;
                row[0] = Value{ .string = s };
                emitCompactValsWithSidecar(vals, ec.kinds, ec.aggs, row[1..], ec.str_ht, slot, ec.sidecar_idx);
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_int_path) {
        if (ht_compact) |*htc| {
             // Emit from CompactIntKeyHashTable: u64 vals → Values.
             const EmitCtxC = struct {
                 rl:    *RowList,
                 alloc: std.mem.Allocator,
                 keys:  []const plan.ProjectItem,
                 aggs:  []const plan.ProjectItem,
                 kinds: []const ht.CompactAggKind,
                 descs: []const IntKeyDesc,
             };
             var emit_ctx_c = EmitCtxC{
                 .rl    = &rl,
                 .alloc = alloc,
                 .keys  = keys,
                 .aggs  = aggs,
                 .kinds = compact_kinds.?,
                 .descs = int_key_descs,
             };
             htc.iterate(&emit_ctx_c, struct {
                 fn cb(ec: *EmitCtxC, k: []const i64, vals: []const u64) void {
                     const row = ec.alloc.alloc(?Value, ec.keys.len + vals.len) catch return;
                     for (k, 0..) |raw_val, i| {
                         _ = ec.descs[i];
                         row[i] = Value{ .int64 = raw_val };
                     }
                     emitCompactVals(vals, ec.kinds, ec.aggs, row[ec.keys.len..]);
                     ec.rl.append(ec.alloc, row) catch {};
                 }
             }.cb);
        } else {
        // Emit from IntKeyHashTable: convert i64 keys back to Values.
        const EmitCtxI = struct {
            rl: *RowList, alloc: std.mem.Allocator,
            keys: []const plan.ProjectItem,
            aggs: []const plan.ProjectItem,
            descs: []const IntKeyDesc,
        };
        var emit_ctx_i = EmitCtxI{
            .rl = &rl, .alloc = alloc, .keys = keys, .aggs = aggs, .descs = int_key_descs,
        };
        ht_int.?.iterate(&emit_ctx_i, struct {
            fn cb(ec: *EmitCtxI, k: []const i64, bucket: []const AggAccum) void {
                const row = ec.alloc.alloc(?Value, ec.keys.len + bucket.len) catch return;
                for (k, ec.descs, 0..) |raw_val, desc, i| {
                    // Convert back: if column was uint64, re-interpret; otherwise int64.
                    _ = desc;
                    row[i] = Value{ .int64 = raw_val };
                }
                for (bucket, ec.aggs, 0..) |acc, item, i| {
                    row[ec.keys.len + i] = finalizeAccum(acc, item, ec.alloc) catch null;
                }
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
        } // end else (regular IntKeyHashTable emit)
    } else {
        const CtxT = struct {
            rl: *RowList, alloc: std.mem.Allocator, keys_len: usize, aggs: []const plan.ProjectItem,
        };
        var emit_ctx = CtxT{ .rl = &rl, .alloc = alloc, .keys_len = keys.len, .aggs = aggs };
        ht_agg.iterate(&emit_ctx, struct {
            fn cb(ec: *CtxT, k: []const Value, bucket: []const AggAccum) void {
                const row = ec.alloc.alloc(?Value, ec.keys_len + bucket.len) catch return;
                for (k, 0..) |kv, i| row[i] = kv;
                for (bucket, ec.aggs, 0..) |acc, item, i| {
                    row[ec.keys_len + i] = finalizeAccum(acc, item, ec.alloc) catch null;
                }
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    }
    return rl;
}

/// Accumulate aggregate state from one DataChunk without building a row slice.
/// Fast-path: count_star and sum(col_ref) work vectorially on column slices.
/// Fallback: all other aggs are handled in a single per-row pass at the end.
fn updateAccumsFromChunk(
    accums: []AggAccum,
    aggs:   []const plan.ProjectItem,
    c:      *const DataChunk,
    alloc:  std.mem.Allocator,
) !void {
    // Track which aggs need a per-row fallback pass (one pass covers all of them).
    var needs_fallback = false;
    // Temp boolean array to mark which indices need fallback.
    const fb_mask = try alloc.alloc(bool, aggs.len);
    @memset(fb_mask, false);

    // Fast path: all aggs are SUM(same_col) or SUM(same_col + k) for int64 column —
    // compute SUM(col) + count*k in a single pass instead of one pass per agg.
    // Saves O(90) passes for Q30 (90× SUM(ResolutionWidth + k)).
    if (aggs.len > 1) blk: {
        var base_col_idx: ?usize = null;
        for (aggs) |item| {
            const ac = switch (item.expr) { .agg_call => |a| a, else => break :blk };
            if (ac.kind != .sum) break :blk;
            const arg = ac.arg orelse break :blk;
            switch (arg) {
                .col_ref => |cr| {
                    if (base_col_idx == null) base_col_idx = cr.index
                    else if (base_col_idx.? != cr.index) break :blk;
                },
                .add => |bo| {
                    const cr = switch (bo.left) { .col_ref => |c2| c2, else => break :blk };
                    _ = switch (bo.right) { .lit_i64 => {}, else => break :blk };
                    if (base_col_idx == null) base_col_idx = cr.index
                    else if (base_col_idx.? != cr.index) break :blk;
                },
                else => break :blk,
            }
        }
        const col_idx = base_col_idx orelse break :blk;
        const col = c.columns[col_idx];
        const vals = switch (col.data) { .int64 => |v| v, else => break :blk };
        // Verify all accumulators are i64_sum.
        for (0..aggs.len) |ci| {
            if (accums[ci] != .i64_sum) break :blk;
        }
        // Single pass: accumulate col_sum and non_null_count.
        var col_sum: i64 = 0;
        var non_null_count: i64 = 0;
        if (chunk.allNonNull(col.null_mask)) {
            // Fast path: no nulls — use SIMD sum.
            col_sum = simd.sumI64(vals[0..c.num_rows]);
            non_null_count = @intCast(c.num_rows);
        } else {
            for (0..c.num_rows) |r| {
                if (!chunk.isNull(col.null_mask, r)) {
                    col_sum +%= vals[r];
                    non_null_count += 1;
                }
            }
        }
        // Update each accumulator analytically: SUM(col+k) = SUM(col) + count*k.
        for (aggs, 0..) |item, ci| {
            const k: i64 = switch (item.expr.agg_call.arg.?) {
                .col_ref => 0,
                .add => |bo| bo.right.lit_i64,
                else => 0,
            };
            accums[ci].i64_sum +%= col_sum + non_null_count * k;
        }
        return;
    }

    for (aggs, 0..) |item, ci| {
        const acc_ptr = &accums[ci];
        var handled = false;
        switch (item.expr) {
            .agg_call => |ac| {
                switch (ac.kind) {
                    .count_star => {
                        acc_ptr.count += c.num_rows;
                        handled = true;
                    },
                    .count => {
                        if (ac.arg) |arg| {
                            switch (arg) {
                                .col_ref => |cr| {
                                    const col = c.columns[cr.index];
                                    for (0..c.num_rows) |r| {
                                        if (!chunk.isNull(col.null_mask, r)) acc_ptr.count += 1;
                                    }
                                    handled = true;
                                },
                                else => {},
                            }
                        } else {
                            acc_ptr.count += c.num_rows;
                            handled = true;
                        }
                    },
                    .sum => {
                        if (ac.arg) |arg| {
                            switch (arg) {
                                .col_ref => |cr| {
                                    const col = c.columns[cr.index];
                                    switch (col.data) {
                                        .int64 => |vals| {
                                            if (acc_ptr.* == .i64_sum) {
                                                acc_ptr.i64_sum +%= simd.sumI64(vals[0..c.num_rows]);
                                                handled = true;
                                            }
                                        },
                                        .uint64 => |vals| {
                                            if (acc_ptr.* == .u64_sum) {
                                                acc_ptr.u64_sum +%= @bitCast(simd.sumU64(vals[0..c.num_rows]));
                                                handled = true;
                                            } else if (acc_ptr.* == .i64_sum) {
                                                acc_ptr.i64_sum +%= simd.sumU64(vals[0..c.num_rows]);
                                                handled = true;
                                            }
                                        },
                                        .float64 => |vals| {
                                            if (acc_ptr.* == .f64_sum) {
                                                acc_ptr.f64_sum += simd.sumF64(vals[0..c.num_rows]);
                                                handled = true;
                                            }
                                        },
                                        else => {},
                                    }
                                },
                                // SUM(col + int_literal): vectorized sum of (val + k)
                                .add => |bo| {
                                    const cr_opt: ?plan.ColRef = switch (bo.left) { .col_ref => |c2| c2, else => null };
                                    const k_opt: ?i64 = switch (bo.right) { .lit_i64 => |v| v, else => null };
                                    if (cr_opt != null and k_opt != null) {
                                        const cr = cr_opt.?;
                                        const k = k_opt.?;
                                        const col = c.columns[cr.index];
                                        switch (col.data) {
                                            .int64 => |vals| {
                                                if (acc_ptr.* == .i64_sum) {
                                                    if (chunk.allNonNull(col.null_mask)) {
                                                        acc_ptr.i64_sum +%= simd.sumI64(vals[0..c.num_rows]) +% (@as(i64, @intCast(c.num_rows)) *% k);
                                                    } else {
                                                        for (0..c.num_rows) |r| {
                                                            if (!chunk.isNull(col.null_mask, r)) acc_ptr.i64_sum +%= vals[r] + k;
                                                        }
                                                    }
                                                    handled = true;
                                                }
                                             },
                                             else => {},
                                         }
                                     }
                                 },
                                 else => {},
                             }
                         }
                    },
                    .avg => {
                        // AVG accumulates into f64_sum (finalization divides by count elsewhere).
                        if (ac.arg) |arg| {
                            if (arg == .col_ref) {
                                const col = c.columns[arg.col_ref.index];
                                switch (col.data) {
                                    .int64 => |vals| {
                                        if (acc_ptr.* == .f64_sum) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                // Fast path: no nulls — sum as i64 then cast once.
                                                acc_ptr.f64_sum += @floatFromInt(simd.sumI64(vals[0..c.num_rows]));
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r))
                                                        acc_ptr.f64_sum += @floatFromInt(vals[r]);
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .uint64 => |vals| {
                                        if (acc_ptr.* == .f64_sum) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                acc_ptr.f64_sum += @floatFromInt(@as(u64, @bitCast(simd.sumU64(vals[0..c.num_rows]))));
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r))
                                                        acc_ptr.f64_sum += @floatFromInt(vals[r]);
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .float64 => |vals| {
                                        if (acc_ptr.* == .f64_sum) {
                                            acc_ptr.f64_sum += simd.sumF64(vals[0..c.num_rows]);
                                            handled = true;
                                        }
                                    },
                                    else => {},
                                }
                            }
                        }
                    },
                    .min => {
                        if (ac.arg) |arg| {
                            if (arg == .col_ref) {
                                const col = c.columns[arg.col_ref.index];
                                switch (col.data) {
                                    .int64 => |vals| {
                                        if (acc_ptr.* == .i64_min) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.minI64(vals[0..c.num_rows]);
                                                if (v < acc_ptr.i64_min) acc_ptr.i64_min = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] < acc_ptr.i64_min)
                                                        acc_ptr.i64_min = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                     },
                                    .uint64 => |vals| {
                                        if (acc_ptr.* == .u64_min) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r) and vals[r] < acc_ptr.u64_min)
                                                    acc_ptr.u64_min = vals[r];
                                            }
                                            handled = true;
                                        }
                                    },
                                    .float64 => |vals| {
                                        if (acc_ptr.* == .f64_min) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.minF64(vals[0..c.num_rows]);
                                                if (v < acc_ptr.f64_min) acc_ptr.f64_min = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] < acc_ptr.f64_min)
                                                        acc_ptr.f64_min = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .date_u16 => |vals| {
                                        if (acc_ptr.* == .i64_min) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r)) {
                                                    const v: i64 = vals[r];
                                                    if (v < acc_ptr.i64_min) acc_ptr.i64_min = v;
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    else => {},
                                }
                            }
                        }
                    },
                    .max => {
                        if (ac.arg) |arg| {
                            if (arg == .col_ref) {
                                const col = c.columns[arg.col_ref.index];
                                switch (col.data) {
                                    .int64 => |vals| {
                                        if (acc_ptr.* == .i64_max) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.maxI64(vals[0..c.num_rows]);
                                                if (v > acc_ptr.i64_max) acc_ptr.i64_max = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] > acc_ptr.i64_max)
                                                        acc_ptr.i64_max = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .uint64 => |vals| {
                                        if (acc_ptr.* == .u64_max) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r) and vals[r] > acc_ptr.u64_max)
                                                    acc_ptr.u64_max = vals[r];
                                            }
                                            handled = true;
                                        }
                                    },
                                    .float64 => |vals| {
                                        if (acc_ptr.* == .f64_max) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.maxF64(vals[0..c.num_rows]);
                                                if (v > acc_ptr.f64_max) acc_ptr.f64_max = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] > acc_ptr.f64_max)
                                                        acc_ptr.f64_max = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .date_u16 => |vals| {
                                        if (acc_ptr.* == .i64_max) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r)) {
                                                    const v: i64 = vals[r];
                                                    if (v > acc_ptr.i64_max) acc_ptr.i64_max = v;
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    else => {},
                                }
                            }
                        }
                    },
                    else => {},
                }
            },
            else => {},
        }
        if (!handled) {
            fb_mask[ci] = true;
            needs_fallback = true;
        }
    }

    if (!needs_fallback) return;

    // Collect referenced columns for all fallback aggs.
    const ref_mask2 = try alloc.alloc(bool, c.columns.len);
    @memset(ref_mask2, false);
    for (aggs, 0..) |item, ci| { if (fb_mask[ci]) collectColRefs(item.expr, ref_mask2); }

    // Single per-row pass for all fallback aggs.
    const row = try alloc.alloc(?Value, c.columns.len);
    @memset(row, null);
    for (0..c.num_rows) |r| {
        for (ref_mask2, 0..) |m, j| if (m) {
            const col = c.columns[j];
            row[j] = if (col.isRowNull(r)) null else col.data.get(r);
        };
        for (aggs, 0..) |item, ci| {
            if (!fb_mask[ci]) continue;
            const v_opt = try evalAggArg(item.expr, row, alloc);
            try kernels.updateAccum(&accums[ci], v_opt, alloc);
        }
    }
}

/// Extract the filter predicate from the outermost filter/limit/project wrapping a scan.
fn extractFilter(node: *const plan.PhysicalNode) ?FilterState {
    return switch (node.*) {
        .filter  => |f| .{ .predicate = f.predicate },
        .limit   => |l| extractFilter(l.input),
        .project => |p| extractFilter(p.input),
        else => null,
    };
}

/// Extract the limit state from the outermost limit wrapping a scan.
fn extractLimit(node: *const plan.PhysicalNode) ?LimitState {
    return switch (node.*) {
        .limit   => |l| .{ .limit = l.limit, .offset = l.offset, .emitted = 0 },
        .filter  => |f| extractLimit(f.input),
        .project => |p| extractLimit(p.input),
        else => null,
    };
}

// ── Chunked limit helper ──────────────────────────────────────────────────────

/// Chunked streaming execution for limit/project/filter/scan patterns.
fn executeLimitChunked(node: *const plan.PhysicalNode, ctx: *QueryContext) !RowList {
    const alloc = ctx.allocator();

    var filter_state: ?FilterState = null;
    var project_items: ?[]const plan.ProjectItem = null;
    var lim_state: LimitState = .{ .limit = std.math.maxInt(u64), .offset = 0 };

    var cur = node;
    while (true) {
        switch (cur.*) {
            .limit => |lim| { lim_state = .{ .limit = lim.limit, .offset = lim.offset }; cur = lim.input; },
            .filter => |f| { if (filter_state == null) filter_state = .{ .predicate = f.predicate }; cur = f.input; },
            .project => |p| { if (project_items == null) project_items = p.items; cur = p.input; },
            else => break,
        }
    }

    const schema_metas = ctx.source.schema();
    const out_metas: []result.ColMeta = if (project_items) |items| blk: {
        const m = try alloc.alloc(result.ColMeta, items.len);
        for (items, 0..) |item, i| m[i] = .{ .name = item.alias, .col_type = item.out_type };
        break :blk m;
    } else try alloc.dupe(result.ColMeta, schema_metas);
    var rl = RowList.init(out_metas);

    ctx.source.reset();
    var c: DataChunk = undefined;
    var skipped: u64 = 0;
    var emitted: u64 = 0;
    var row_ref_indices: ?[]usize = null;
    var row_buf: []?Value = &.{}; // allocated once on first chunk

    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state) |*fs| try fs.apply(&c, ctx);
        if (c.num_rows == 0) continue;

        if (row_ref_indices == null and c.columns.len > 0) {
            const mask = try alloc.alloc(bool, c.columns.len);
            @memset(mask, false);
            if (project_items) |items| { for (items) |item| collectColRefs(item.expr, mask); }
            else @memset(mask, true);
            var cnt: usize = 0;
            for (mask) |m| { if (m) cnt += 1; }
            const idxs = try alloc.alloc(usize, cnt);
            var wi: usize = 0;
            for (mask, 0..) |m, j| { if (m) { idxs[wi] = j; wi += 1; } }
            row_ref_indices = idxs;
            // Allocate row_buf once (reused across all chunks).
            row_buf = try alloc.alloc(?Value, c.columns.len);
            @memset(row_buf, null);
        }
        const refs = row_ref_indices orelse &[_]usize{};

        for (0..c.num_rows) |r| {
            if (skipped < lim_state.offset) { skipped += 1; continue; }
            if (emitted >= lim_state.limit) break;
            for (refs) |j| {
                const col = c.columns[j];
                row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
            }
            const out_row: []?Value = if (project_items) |items| blk: {
                const out = try alloc.alloc(?Value, items.len);
                for (items, 0..) |item, i| out[i] = try kernels.evalExpr(item.expr, row_buf, null, alloc);
                break :blk out;
            } else blk: {
                const out = try alloc.alloc(?Value, c.columns.len);
                for (c.columns, 0..) |col, j| out[j] = if (col.isRowNull(r)) null else col.data.get(r);
                break :blk out;
            };
            try rl.append(alloc, out_row);
            emitted += 1;
        }
        if (emitted >= lim_state.limit) break;
    }
    return rl;
}

// ── ScalarAgg helper ──────────────────────────────────────────────────────────

fn executeScalarAgg(inner: RowList, aggs: []const plan.ProjectItem, alloc: std.mem.Allocator) !RowList {
    const accums = try alloc.alloc(AggAccum, aggs.len);
    for (aggs, 0..) |item, ci| accums[ci] = initAccumForAgg(item.expr);

    for (inner.rows.items) |row| {
        for (aggs, 0..) |item, ci| {
            const v_opt = try evalAggArg(item.expr, row, alloc);
            try kernels.updateAccum(&accums[ci], v_opt, alloc);
        }
    }

    const metas = try alloc.alloc(result.ColMeta, aggs.len);
    const out_row = try alloc.alloc(?Value, aggs.len);
    for (aggs, 0..) |item, ci| {
        metas[ci] = .{ .name = item.alias, .col_type = item.out_type };
        out_row[ci] = try finalizeAccum(accums[ci], item, alloc);
    }
    var rl = RowList.init(metas);
    try rl.append(alloc, out_row);
    return rl;
}

// ── HashAgg helper ────────────────────────────────────────────────────────────

fn executeHashAgg(
    inner: RowList,
    keys:  []const plan.ProjectItem,
    aggs:  []const plan.ProjectItem,
    alloc: std.mem.Allocator,
) !RowList {
    var ht_agg = try ht.AggHashTable.init(alloc, keys.len, aggs.len);

    const init_accums = try alloc.alloc(AggAccum, aggs.len);
    for (aggs, 0..) |item, ci| init_accums[ci] = initAccumForAgg(item.expr);

    const key_buf = try alloc.alloc(Value, keys.len);
    for (inner.rows.items) |row| {
        for (keys, 0..) |k, ki| {
            const v = try kernels.evalExpr(k.expr, row, null, alloc);
            key_buf[ki] = v orelse Value{ .int64 = 0 }; // NULL → zero sentinel for hashing
        }
        const bucket = try ht_agg.getOrInsert(key_buf, init_accums);
        for (aggs, 0..) |item, ci| {
            const v_opt = try evalAggArg(item.expr, row, alloc);
            try kernels.updateAccum(&bucket[ci], v_opt, alloc);
        }
    }

    const out_metas = try alloc.alloc(result.ColMeta, keys.len + aggs.len);
    for (keys, 0..) |k, ki| out_metas[ki] = .{ .name = k.alias, .col_type = k.out_type };
    for (aggs, 0..) |a, ai| out_metas[keys.len + ai] = .{ .name = a.alias, .col_type = a.out_type };

    var rl = RowList.init(out_metas);

    const CtxT = struct {
        rl:       *RowList,
        alloc:    std.mem.Allocator,
        keys_len: usize,
        aggs:     []const plan.ProjectItem,
    };
    var emit_ctx = CtxT{ .rl = &rl, .alloc = alloc, .keys_len = keys.len, .aggs = aggs };
    ht_agg.iterate(&emit_ctx, struct {
        fn cb(c: *CtxT, k: []const Value, bucket: []const AggAccum) void {
            const row = c.alloc.alloc(?Value, c.keys_len + bucket.len) catch return;
            for (k, 0..) |kv, i| row[i] = kv;
            for (bucket, c.aggs, 0..) |acc, item, i| {
                row[c.keys_len + i] = finalizeAccum(acc, item, c.alloc) catch null;
            }
            c.rl.append(c.alloc, row) catch {};
        }
    }.cb);

    return rl;
}

// ── OrderBy helper ────────────────────────────────────────────────────────────

/// Heap-based top-K selection: O(n log k) time, O(k) extra memory.
/// Returns exactly min(k, n) rows in sorted order.
fn executeTopK(inner: RowList, keys: []const plan.SortKey, k: usize, alloc: std.mem.Allocator) !RowList {
    const rows = inner.rows.items;
    if (rows.len == 0 or k == 0) return RowList.init(inner.metas);

    // SortCtx: lessThan(a, b) = true means a should appear before b in the output.
    const SortCtx = struct {
        keys: []const plan.SortKey,
        fn lessThan(self: @This(), a: []?Value, b: []?Value) bool {
            for (self.keys) |key| {
                const av: ?Value = if (key.col_idx < a.len) a[key.col_idx] else null;
                const bv: ?Value = if (key.col_idx < b.len) b[key.col_idx] else null;
                const ord: std.math.Order = if (av != null and bv != null)
                    Value.order(av.?, bv.?)
                else if (av == null and bv == null)
                    .eq
                else if (av == null)
                    .lt
                else
                    .gt;
                if (ord == .eq) continue;
                return if (key.desc) ord == .gt else ord == .lt;
            }
            return false;
        }
        // heapLess(a,b): for a min-heap of the BEST k rows, the "min" is the
        // worst element (the one we'd evict). So heapLess = !lessThan.
        fn heapLess(self: @This(), a: []?Value, b: []?Value) std.math.Order {
            if (self.lessThan(a, b)) return .lt;
            if (self.lessThan(b, a)) return .gt;
            return .eq;
        }
    };
    const ctx = SortCtx{ .keys = keys };

    // Build a min-heap (worst-of-best k) to track the top-k rows.
    const heap_buf = try alloc.alloc([]?Value, k);
    var heap_len: usize = 0;

    for (rows) |row| {
        if (heap_len < k) {
            heap_buf[heap_len] = row;
            heap_len += 1;
            // Sift up.
            var i = heap_len - 1;
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (ctx.lessThan(heap_buf[i], heap_buf[parent])) {
                    const tmp = heap_buf[i]; heap_buf[i] = heap_buf[parent]; heap_buf[parent] = tmp;
                    i = parent;
                } else break;
            }
        } else {
            // If this row is better than the heap root (worst of current best), replace root.
            if (ctx.lessThan(row, heap_buf[0])) {
                heap_buf[0] = row;
                // Sift down.
                var i: usize = 0;
                while (true) {
                    const l = 2 * i + 1;
                    const r = 2 * i + 2;
                    var smallest = i;
                    if (l < heap_len and ctx.lessThan(heap_buf[l], heap_buf[smallest])) smallest = l;
                    if (r < heap_len and ctx.lessThan(heap_buf[r], heap_buf[smallest])) smallest = r;
                    if (smallest == i) break;
                    const tmp = heap_buf[i]; heap_buf[i] = heap_buf[smallest]; heap_buf[smallest] = tmp;
                    i = smallest;
                }
            }
        }
    }

    // Sort the heap to get the final ordered result.
    std.sort.pdq([]?Value, heap_buf[0..heap_len], ctx, SortCtx.lessThan);

    var rl = RowList.init(inner.metas);
    for (heap_buf[0..heap_len]) |row| try rl.append(alloc, row);
    return rl;
}

/// Stream a scannable node (scan/filter/project/limit) directly into a min-heap
/// of at most K rows, avoiding materialisation of all rows into a RowList.
/// Only rows that actually enter the heap (≤ K rows) are fully materialised via readRow.
/// Read a single row from a slice of columns (without a DataChunk wrapper).
fn readRowFromCols(cols: []const chunk.Column, row: usize, a: std.mem.Allocator) ![]?Value {
    const vals = try a.alloc(?Value, cols.len);
    for (cols, 0..) |col, ci| vals[ci] = if (col.isRowNull(row)) null else col.data.get(row);
    return vals;
}

/// Late-materialization top-K: phase 1 scans only filter+sort columns using fetchRange
/// (so global row indices are stable), phase 2 fetches all columns for the K winners.
/// Returns null if unable to proceed (falls back to standard path).
fn executeTopKLateMat(
    schema_metas: []const result.ColMeta,
    out_metas:    []const result.ColMeta,
    filter_pred:  plan.Expr,
    keys:         []const plan.SortKey,
    k:            usize,
    ctx:          *QueryContext,
    alloc:        std.mem.Allocator,
) !?RowList {
    const total_rows = ctx.source.rowCount();
    if (total_rows == 0) return RowList.init(@constCast(out_metas));

    // Collect filter column names via col_ref traversal.
    var col_mask = [_]bool{false} ** 256;
    collectColRefs(filter_pred, col_mask[0..@min(256, schema_metas.len)]);

    // Build scan column names: filter cols + sort key cols.
    var scan_names_buf: [32][]const u8 = undefined;
    var scan_names_len: usize = 0;
    for (col_mask[0..@min(256, schema_metas.len)], 0..) |needed, idx| {
        if (needed and scan_names_len < scan_names_buf.len) {
            scan_names_buf[scan_names_len] = schema_metas[idx].name;
            scan_names_len += 1;
        }
    }
    for (keys) |key| {
        if (key.col_idx >= schema_metas.len or scan_names_len >= scan_names_buf.len) continue;
        var dup = false;
        for (scan_names_buf[0..scan_names_len]) |n| {
            if (std.mem.eql(u8, n, schema_metas[key.col_idx].name)) { dup = true; break; }
        }
        if (!dup) { scan_names_buf[scan_names_len] = schema_metas[key.col_idx].name; scan_names_len += 1; }
    }

    // Phase 1: restrict source to scan cols, iterate fetchRange morsels.
    ctx.source.setNeededCols(scan_names_buf[0..scan_names_len]);
    defer ctx.source.setNeededCols(null);

    const HeapEntry = struct { global_row: u64, key_vals: []?Value };
    const heap_e = try alloc.alloc(HeapEntry, k);
    var heap_e_len: usize = 0;
    const key_scratch = try alloc.alloc(?Value, schema_metas.len);
    @memset(key_scratch, null);

    const SortCtxE = struct {
        keys: []const plan.SortKey,
        fn lessThan(self: @This(), a: HeapEntry, b: HeapEntry) bool {
            for (self.keys) |key| {
                const av = if (key.col_idx < a.key_vals.len) a.key_vals[key.col_idx] else null;
                const bv = if (key.col_idx < b.key_vals.len) b.key_vals[key.col_idx] else null;
                const ord: std.math.Order = if (av != null and bv != null)
                    Value.order(av.?, bv.?)
                else if (av == null and bv == null) .eq
                else if (av == null) .lt else .gt;
                if (ord == .eq) continue;
                return if (key.desc) ord == .gt else ord == .lt;
            }
            return false;
        }
    };
    const sctx_e = SortCtxE{ .keys = keys };

    const morsel_size: usize = 65536;
    var pos: u64 = 0;
    var phase1_fs = FilterState{ .predicate = filter_pred };
    var chunk_arena = std.heap.ArenaAllocator.init(alloc);
    defer chunk_arena.deinit();
    var fake_ctx: QueryContext = .{
        .arena  = std.heap.ArenaAllocator.init(chunk_arena.allocator()),
        .source = ctx.source,
    };

    // Pre-compile LikeGuards from filter predicate (once per query).
    // Used in phase 1 to fast-reject rows before evalExpr.
    var phase1_like_guards_list = std.ArrayListUnmanaged(LikeGuard){ .items = &.{}, .capacity = 0 };
    collectLikeGuards(filter_pred, &phase1_like_guards_list, alloc);
    const phase1_like_guards = phase1_like_guards_list.items;

    // True when filter predicate is purely a single LIKE/NOT_LIKE col_ref — then
    // we can skip evalExpr entirely and use LikeMatcher.match directly.
    const phase1_pure_like: bool = switch (filter_pred) {
        .like, .not_like => phase1_like_guards.len == 1,
        else => false,
    };

    // ── Parallel Phase 1 ─────────────────────────────────────────────────────
    // For large datasets with a pure LIKE filter (e.g. Q24), parallelize Phase 1
    // across N threads each maintaining a local heap of size k.
    // Merge after: collect all N local heaps, sort, keep top-k.
    const n_par_threads = parallel.defaultThreads();
    const use_parallel_phase1 = phase1_pure_like and
        total_rows >= 200_000 and
        n_par_threads > 1 and
        ctx.source.supportsRange();

    if (use_parallel_phase1) {
        const lg0 = phase1_like_guards[0];
        const negate0 = lg0.negate;
        const matcher0 = lg0.matcher;

        const ParPhase1Ctx = struct {
            source:       SourceIface,
            keys:         []const plan.SortKey,
            like_col_idx: usize,
            matcher:      kernels.LikeMatcher,
            negate:       bool,
            k:            usize,
            schema_len:   usize,
            morsel_src:   *parallel.MorselSource,
            parent_alloc: std.mem.Allocator,
            // Output (allocated per-ctx from parent_alloc after run).
            local_heap: []HeapEntry = &.{},
            local_len:  usize = 0,
            err:        ?anyerror = null,

            const LHeapEntry = struct { global_row: u64, key_vals: []?Value };

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                self.run() catch |e| { self.err = e; };
            }

            fn run(self: *@This()) !void {
                var arena = std.heap.ArenaAllocator.init(self.parent_alloc);
                defer arena.deinit();
                const talloc = arena.allocator();
                const heap_buf = try talloc.alloc(HeapEntry, self.k);
                var heap_len: usize = 0;
                const local_key_scratch = try talloc.alloc(?Value, self.schema_len);
                @memset(local_key_scratch, null);

                const SCtx = struct {
                    keys: []const plan.SortKey,
                    fn lt(sc: @This(), a: HeapEntry, b: HeapEntry) bool {
                        for (sc.keys) |key| {
                            const av = if (key.col_idx < a.key_vals.len) a.key_vals[key.col_idx] else null;
                            const bv = if (key.col_idx < b.key_vals.len) b.key_vals[key.col_idx] else null;
                            const ord: std.math.Order = if (av != null and bv != null)
                                Value.order(av.?, bv.?)
                            else if (av == null and bv == null) .eq
                            else if (av == null) .lt else .gt;
                            if (ord == .eq) continue;
                            return if (key.desc) ord == .gt else ord == .lt;
                        }
                        return false;
                    }
                };
                const sctx2 = SCtx{ .keys = self.keys };

                while (self.morsel_src.next()) |m| {
                    var morsel_chunk_arena = std.heap.ArenaAllocator.init(talloc);
                    defer morsel_chunk_arena.deinit();
                    var c: DataChunk = undefined;
                    try self.source.fetchRange(@intCast(m.start), m.end - m.start, &c, morsel_chunk_arena.allocator());

                    if (self.like_col_idx >= c.columns.len or c.columns[self.like_col_idx].data != .string) continue;
                    const str_col = c.columns[self.like_col_idx];

                    for (0..c.num_rows) |r| {
                        const s = if (str_col.isRowNull(r)) "" else str_col.data.string[r];
                        if (self.matcher.match(s) == self.negate) continue;
                        const global_row = m.start + r;

                        // Read sort key values.
                        const key_vals = try talloc.alloc(?Value, self.schema_len);
                        @memset(key_vals, null);
                        for (self.keys) |key| {
                            if (key.col_idx < c.columns.len) {
                                const col2 = &c.columns[key.col_idx];
                                key_vals[key.col_idx] = if (col2.isRowNull(r)) null else col2.data.get(r);
                            }
                        }

                        if (heap_len < self.k) {
                            heap_buf[heap_len] = .{ .global_row = @intCast(global_row), .key_vals = key_vals };
                            heap_len += 1;
                            var i = heap_len - 1;
                            while (i > 0) {
                                const parent = (i - 1) / 2;
                                if (sctx2.lt(heap_buf[i], heap_buf[parent])) {
                                    const tmp = heap_buf[i]; heap_buf[i] = heap_buf[parent]; heap_buf[parent] = tmp;
                                    i = parent;
                                } else break;
                            }
                        } else {
                            for (self.keys) |key| {
                                if (key.col_idx < c.columns.len) {
                                    const col2 = &c.columns[key.col_idx];
                                    local_key_scratch[key.col_idx] = if (col2.isRowNull(r)) null else col2.data.get(r);
                                }
                            }
                            const candidate = HeapEntry{ .global_row = @intCast(global_row), .key_vals = local_key_scratch };
                            if (sctx2.lt(candidate, heap_buf[0])) {
                                heap_buf[0] = .{ .global_row = @intCast(global_row), .key_vals = key_vals };
                                var i: usize = 0;
                                while (true) {
                                    const l = 2 * i + 1; const r2 = 2 * i + 2;
                                    var sm = i;
                                    if (l < heap_len and sctx2.lt(heap_buf[l], heap_buf[sm])) sm = l;
                                    if (r2 < heap_len and sctx2.lt(heap_buf[r2], heap_buf[sm])) sm = r2;
                                    if (sm == i) break;
                                    const tmp = heap_buf[i]; heap_buf[i] = heap_buf[sm]; heap_buf[sm] = tmp;
                                    i = sm;
                                }
                            }
                        }
                    }
                }

                // Copy heap to parent-alloc'd memory (talloc will be freed after this fn).
                const out_heap = try self.parent_alloc.alloc(HeapEntry, heap_len);
                for (out_heap, 0..) |*e, i| {
                    e.global_row = heap_buf[i].global_row;
                    // Copy key_vals to parent alloc.
                    const kv = try self.parent_alloc.alloc(?Value, heap_buf[i].key_vals.len);
                    @memcpy(kv, heap_buf[i].key_vals);
                    e.key_vals = kv;
                }
                self.local_heap = out_heap;
                self.local_len = heap_len;
            }
        };

        var morsel_src = parallel.MorselSource.init(@intCast(total_rows), 65536);
        const pctxs = try alloc.alloc(ParPhase1Ctx, n_par_threads);
        for (pctxs) |*pc| {
            pc.* = .{
                .source       = ctx.source,
                .keys         = keys,
                .like_col_idx = lg0.col_idx,
                .matcher      = matcher0,
                .negate       = negate0,
                .k            = k,
                .schema_len   = schema_metas.len,
                .morsel_src   = &morsel_src,
                .parent_alloc = alloc,
            };
        }
        try parallel.parallelFor(alloc, ParPhase1Ctx, ParPhase1Ctx.work, pctxs, &morsel_src);
        for (pctxs) |pc| { if (pc.err) |e| return e; }

        // Merge: collect all local heaps into one buffer, sort, take top-k.
        var total_candidates: usize = 0;
        for (pctxs) |pc| total_candidates += pc.local_len;
        const merged = try alloc.alloc(HeapEntry, total_candidates);
        var mi: usize = 0;
        for (pctxs) |pc| {
            @memcpy(merged[mi .. mi + pc.local_len], pc.local_heap[0..pc.local_len]);
            mi += pc.local_len;
        }
        std.sort.pdq(HeapEntry, merged, sctx_e, SortCtxE.lessThan);
        const take = @min(k, merged.len);

        // Phase 2: load full columns for top-k rows.
        ctx.source.setNeededCols(null);
        var rl2 = RowList.init(@constCast(out_metas));
        for (merged[0..take]) |entry| {
            var full_chunk: DataChunk = undefined;
            try ctx.source.fetchRange(entry.global_row, 1, &full_chunk, alloc);
            const row = try full_chunk.readRow(0, alloc);
            try rl2.append(alloc, row);
        }
        return rl2;
    }

    while (pos < total_rows) {
        const n = @min(morsel_size, total_rows - pos);
        var c: DataChunk = undefined;
        try ctx.source.fetchRange(pos, n, &c, chunk_arena.allocator());

        // Apply filter; rows that pass keep their in-chunk index.
        // We need to know the original global index of each passing row.
        // Since we use fetchRange (not nextChunk), row i in chunk = global row pos+i.
        // BUT: the filter compacts rows in-place, losing original indices.
        // Solution: apply filter manually without compaction — just compute a pass mask.
        const pass_mask = try chunk_arena.allocator().alloc(bool, c.num_rows);
        @memset(pass_mask, true);

        // Evaluate filter per-row using a non-compacting approach.
        // We need ref_indices and row_buf from FilterState.
        if (phase1_fs.ref_indices == null) try phase1_fs.apply(&c, &fake_ctx);
        // Use int_conds if available for fast path.
        if (phase1_fs.int_conds) |conds| {
            if (conds.len > 0 and phase1_fs.int_conds_complete) {
                for (0..c.num_rows) |r| {
                    for (conds) |cond| {
                        if (cond.col_idx >= c.columns.len) { pass_mask[r] = false; break; }
                        const col = c.columns[cond.col_idx];
                        if (col.isRowNull(r)) { pass_mask[r] = false; break; }
                        const v: i64 = switch (col.data) {
                            .int64 => |a| a[r],
                            .uint64 => |a| @bitCast(a[r]),
                            .date_u16 => |a| @as(i64, a[r]),
                            .datetime64_ms => |a| a[r],
                            .bool_u8 => |a| @as(i64, a[r]),
                            else => { pass_mask[r] = false; break; },
                        };
                        const pass = switch (cond.op) {
                            .eq => v == cond.val, .neq => v != cond.val,
                            .lt => v < cond.val, .lte => v <= cond.val,
                            .gt => v > cond.val, .gte => v >= cond.val,
                            .in2 => v == cond.val or v == cond.val2,
                        };
                        if (!pass) { pass_mask[r] = false; break; }
                    }
                }
            }
        } else {
            // General path: evalExpr per row.
            const ref = phase1_fs.ref_indices orelse &.{};
            const row = phase1_fs.row_buf orelse try chunk_arena.allocator().alloc(?Value, c.columns.len);
            for (0..c.num_rows) |r| {
                for (ref) |j| {
                    if (j < c.columns.len) {
                        const col = c.columns[j];
                        row[j] = if (col.isRowNull(r)) null else col.data.get(r);
                    }
                }
                const v = try kernels.evalExpr(filter_pred, row, null, chunk_arena.allocator());
                pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
            }
        }

        // For LIKE-based filters, re-run with like_guards or general evalExpr.
        // Actually for Q24 the int_conds path won't fire (LIKE is not int).
        // We need the general evalExpr path to handle LIKE.
        // Let's use a combined approach: check int_conds (if any) then evalExpr for LIKE.
        // The above logic handles int-only conds. For LIKE we need the general path.
        // Redo with general evalExpr for non-int-complete predicates:
        if (phase1_fs.int_conds == null or !phase1_fs.int_conds_complete) {
            if (phase1_pure_like and phase1_like_guards.len == 1) {
                // Fast path: pure col_ref LIKE/NOT_LIKE lit_str — use pre-compiled LikeMatcher.
                const lg = phase1_like_guards[0];
                if (lg.col_idx < c.columns.len and c.columns[lg.col_idx].data == .string) {
                    const col = c.columns[lg.col_idx];
                    for (0..c.num_rows) |r| {
                        if (!pass_mask[r]) continue;
                        const s = if (col.isRowNull(r)) "" else col.data.string[r];
                        pass_mask[r] = lg.matcher.match(s) != lg.negate;
                    }
                } else {
                    // Column not string or out of range — fall back to evalExpr.
                    const ref = phase1_fs.ref_indices orelse &.{};
                    const row = try chunk_arena.allocator().alloc(?Value, c.columns.len);
                    @memset(row, null);
                    for (0..c.num_rows) |r| {
                        if (!pass_mask[r]) continue;
                        for (ref) |j| {
                            if (j < c.columns.len) {
                                const col2 = c.columns[j];
                                row[j] = if (col2.isRowNull(r)) null else col2.data.get(r);
                            }
                        }
                        const v = try kernels.evalExpr(filter_pred, row, null, chunk_arena.allocator());
                        pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
                    }
                }
            } else if (phase1_like_guards.len > 0) {
                // Multi-guard path: pre-filter with like_guards, then evalExpr only for survivors.
                const ref = phase1_fs.ref_indices orelse &.{};
                const row = try chunk_arena.allocator().alloc(?Value, c.columns.len);
                @memset(row, null);
                row_loop: for (0..c.num_rows) |r| {
                    if (!pass_mask[r]) continue;
                    // Check each LIKE guard with LikeMatcher (fast-reject).
                    for (phase1_like_guards) |lg| {
                        if (lg.col_idx >= c.columns.len) { pass_mask[r] = false; continue :row_loop; }
                        const col = c.columns[lg.col_idx];
                        if (col.data != .string) continue;
                        const s = if (col.isRowNull(r)) "" else col.data.string[r];
                        if (lg.matcher.match(s) == lg.negate) { pass_mask[r] = false; continue :row_loop; }
                    }
                    // Guards passed — evalExpr for full predicate.
                    for (ref) |j| {
                        if (j < c.columns.len) {
                            const col2 = c.columns[j];
                            row[j] = if (col2.isRowNull(r)) null else col2.data.get(r);
                        }
                    }
                    const v = try kernels.evalExpr(filter_pred, row, null, chunk_arena.allocator());
                    pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
                }
            } else {
                // General path: no LIKE guards — evalExpr per row.
                const ref = phase1_fs.ref_indices orelse &.{};
                const row = try chunk_arena.allocator().alloc(?Value, c.columns.len);
                @memset(row, null);
                for (0..c.num_rows) |r| {
                    if (!pass_mask[r]) continue;
                    for (ref) |j| {
                        if (j < c.columns.len) {
                            const col2 = c.columns[j];
                            row[j] = if (col2.isRowNull(r)) null else col2.data.get(r);
                        }
                    }
                    const v = try kernels.evalExpr(filter_pred, row, null, chunk_arena.allocator());
                    pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
                }
            }
        }

        for (0..c.num_rows) |r| {
            if (!pass_mask[r]) continue;
            const global_row = pos + r;

            // Read sort key values.
            const key_vals = try alloc.alloc(?Value, schema_metas.len);
            @memset(key_vals, null);
            for (keys) |key| {
                if (key.col_idx < c.columns.len) {
                    const col = &c.columns[key.col_idx];
                    key_vals[key.col_idx] = if (col.isRowNull(r)) null else col.data.get(r);
                }
            }

            if (heap_e_len < k) {
                heap_e[heap_e_len] = .{ .global_row = global_row, .key_vals = key_vals };
                heap_e_len += 1;
                var i = heap_e_len - 1;
                while (i > 0) {
                    const parent = (i - 1) / 2;
                    if (sctx_e.lessThan(heap_e[i], heap_e[parent])) {
                        const tmp = heap_e[i]; heap_e[i] = heap_e[parent]; heap_e[parent] = tmp;
                        i = parent;
                    } else break;
                }
            } else {
                for (keys) |key| {
                    if (key.col_idx < c.columns.len) {
                        const col = &c.columns[key.col_idx];
                        key_scratch[key.col_idx] = if (col.isRowNull(r)) null else col.data.get(r);
                    }
                }
                const candidate = HeapEntry{ .global_row = global_row, .key_vals = key_scratch };
                if (sctx_e.lessThan(candidate, heap_e[0])) {
                    heap_e[0] = .{ .global_row = global_row, .key_vals = key_vals };
                    var i: usize = 0;
                    while (true) {
                        const l = 2 * i + 1; const r2 = 2 * i + 2;
                        var sm = i;
                        if (l < heap_e_len and sctx_e.lessThan(heap_e[l], heap_e[sm])) sm = l;
                        if (r2 < heap_e_len and sctx_e.lessThan(heap_e[r2], heap_e[sm])) sm = r2;
                        if (sm == i) break;
                        const tmp = heap_e[i]; heap_e[i] = heap_e[sm]; heap_e[sm] = tmp;
                        i = sm;
                    }
                }
            }
        }

        pos += n;
        _ = chunk_arena.reset(.retain_capacity);
        fake_ctx.arena = std.heap.ArenaAllocator.init(chunk_arena.allocator());
    }

    // Sort K winners.
    std.sort.pdq(HeapEntry, heap_e[0..heap_e_len], sctx_e, SortCtxE.lessThan);

    // Phase 2: fetch all columns for the K winner rows.
    ctx.source.setNeededCols(null); // restore full decode
    var rl = RowList.init(@constCast(out_metas));
    for (heap_e[0..heap_e_len]) |entry| {
        var full_chunk: DataChunk = undefined;
        try ctx.source.fetchRange(entry.global_row, 1, &full_chunk, alloc);
        const row = try full_chunk.readRow(0, alloc);
        try rl.append(alloc, row);
    }
    return rl;
}

fn executeTopKFromScannable(
    node: *const plan.PhysicalNode,
    keys: []const plan.SortKey,
    k:    usize,
    ctx:  *QueryContext,
) !RowList {
    const alloc = ctx.allocator();

    // Traverse to extract filter / project / limit wrappers.
    var filter_state:  ?FilterState               = null;
    var project_items: ?[]const plan.ProjectItem  = null;
    var lim_state:     ?LimitState                = null;
    var cur = node;
    while (true) {
        switch (cur.*) {
            .limit   => |lim| { if (lim_state == null) lim_state = .{ .limit = lim.limit, .offset = lim.offset }; cur = lim.input; },
            .filter  => |f|   { if (filter_state == null) filter_state = .{ .predicate = f.predicate }; cur = f.input; },
            .project => |p|   { if (project_items == null) project_items = p.items; cur = p.input; },
            else => break,
        }
    }

    const schema_metas = ctx.source.schema();
    const out_metas: []result.ColMeta = if (project_items) |items| blk: {
        const m = try alloc.alloc(result.ColMeta, items.len);
        for (items, 0..) |item, i| m[i] = .{ .name = item.alias, .col_type = item.out_type };
        break :blk m;
    } else try alloc.dupe(result.ColMeta, schema_metas);

    if (k == 0) return RowList.init(out_metas);

    // ── Late-materialization path ─────────────────────────────────────────────
    // For SELECT * with a filter, scan with only filter+sort columns (phase 1)
    // to avoid decoding all 100+ columns per row. Track the global row indices
    // of the top-K winners. Then fetch only those K rows with all columns (phase 2).
    // Detect if this is effectively SELECT * (all project items are identity col_refs).
    const is_select_star = if (project_items) |items| blk: {
        if (items.len != schema_metas.len) break :blk false;
        var all_ident = true;
        for (items, 0..) |item, i| {
            if (item.expr != .col_ref or item.expr.col_ref.index != i) { all_ident = false; break; }
        }
        break :blk all_ident;
    } else true;

    const use_late_mat = is_select_star and
        filter_state != null and
        ctx.source.vtable.setNeededCols != null and
        ctx.source.supportsRange() and
        schema_metas.len > 8; // only worth it for wide schemas

    // For non-SELECT* with few needed cols, also apply column restriction during scan.
    // This covers Q25/Q26/Q27 style: SELECT col1 WHERE col2 <> '' ORDER BY col3.
    const narrow_scan_possible = !is_select_star and
        ctx.source.vtable.setNeededCols != null and
        filter_state != null;
    if (narrow_scan_possible) {
        // Collect all needed cols: project cols + filter cols + sort key cols.
        var needed_mask = [_]bool{false} ** 256;
        if (filter_state) |*fs| {
            const pred = fs.predicate;
            collectColRefs(pred, needed_mask[0..@min(256, schema_metas.len)]);
        }
        for (keys) |key| { if (key.col_idx < 256) needed_mask[key.col_idx] = true; }
        if (project_items) |items| {
            for (items) |item| {
                collectColRefs(item.expr, needed_mask[0..@min(256, schema_metas.len)]);
            }
        }
        var needed_count: usize = 0;
        for (needed_mask[0..@min(256, schema_metas.len)]) |m| { if (m) needed_count += 1; }
        // Only worth restricting if we skip at least half the columns.
        if (needed_count * 2 < schema_metas.len) {
            var names_buf: [32][]const u8 = undefined;
            var names_len: usize = 0;
            for (needed_mask[0..@min(256, schema_metas.len)], 0..) |m, i| {
                if (m and names_len < names_buf.len) {
                    names_buf[names_len] = schema_metas[i].name;
                    names_len += 1;
                }
            }
            ctx.source.setNeededCols(names_buf[0..names_len]);
            // Will be reset at the end of executeTopKFromScannable via defer below.
        }
    }

    if (use_late_mat) {
        const result_opt = try executeTopKLateMat(
            schema_metas, out_metas, filter_state.?.predicate, keys, k, ctx, alloc,
        );
        if (result_opt) |rl| return rl;
        // If late_mat failed for any reason, fall through to standard path.
    }

    const SortCtx = struct {
        keys: []const plan.SortKey,
        fn lessThan(self: @This(), a: []?Value, b: []?Value) bool {
            for (self.keys) |key| {
                const av: ?Value = if (key.col_idx < a.len) a[key.col_idx] else null;
                const bv: ?Value = if (key.col_idx < b.len) b[key.col_idx] else null;
                const ord: std.math.Order = if (av != null and bv != null)
                    Value.order(av.?, bv.?)
                else if (av == null and bv == null) .eq
                else if (av == null) .lt
                else .gt;
                if (ord == .eq) continue;
                return if (key.desc) ord == .gt else ord == .lt;
            }
            return false;
        }
    };
    const sctx = SortCtx{ .keys = keys };

    // If narrow_scan was applied, restore all cols after the scan is done.
    defer if (narrow_scan_possible) ctx.source.setNeededCols(null);

    // Strategy: accumulate up to K raw (pre-projection) schema rows in the heap.
    // Project only the final K winners — avoids projecting all 300K+ matching rows.
    // sort key col_idx = schema column index (same in pre- and post-projection).
    const heap_buf = try alloc.alloc([]?Value, k);
    var heap_len: usize = 0;

    // Scratch: only key columns need to be read for heap-root comparison.
    const num_schema_cols = schema_metas.len;
    const key_scratch = try alloc.alloc(?Value, num_schema_cols);
    @memset(key_scratch, null);

    // When narrow_scan is active and source supports range queries, use large fetchRange
    // morsels (65536 rows) to reduce per-chunk overhead vs nextChunk (CHUNK_SIZE=2048).
    const use_fetchrange = narrow_scan_possible and ctx.source.supportsRange();
    const morsel_sz: usize = 65536;

    var chunk_arena = std.heap.ArenaAllocator.init(alloc);
    defer chunk_arena.deinit();

    const HeapChunkLoop = struct {
        fn process(
            c: *DataChunk,
            heap_b: [][]?Value,
            heap_l: *usize,
            k2: usize,
            ks: []const plan.SortKey,
            kscratch: []?Value,
            sctx2: SortCtx,
            lim: ?*LimitState,
            fs: ?*FilterState,
            qctx: *QueryContext,
        ) !bool {
            if (fs) |f| try f.apply(c, qctx);
            if (lim) |ls| ls.apply(c);
            if (c.num_rows == 0) {
                if (lim) |ls| if (ls.done()) return true;
                return false;
            }
            const a = qctx.allocator();
            for (0..c.num_rows) |r| {
                if (heap_l.* < k2) {
                    const row = try c.readRow(r, a);
                    heap_b[heap_l.*] = row;
                    heap_l.* += 1;
                    var i = heap_l.* - 1;
                    while (i > 0) {
                        const parent = (i - 1) / 2;
                        if (sctx2.lessThan(heap_b[i], heap_b[parent])) {
                            const tmp = heap_b[i]; heap_b[i] = heap_b[parent]; heap_b[parent] = tmp;
                            i = parent;
                        } else break;
                    }
                } else {
                    for (ks) |key| {
                        if (key.col_idx < c.columns.len) {
                            const col = &c.columns[key.col_idx];
                            kscratch[key.col_idx] = if (col.isRowNull(r)) null else col.data.get(r);
                        }
                    }
                    if (sctx2.lessThan(kscratch, heap_b[0])) {
                        const row = try c.readRow(r, a);
                        heap_b[0] = row;
                        var i: usize = 0;
                        while (true) {
                            const l = 2 * i + 1;
                            const r2 = 2 * i + 2;
                            var smallest = i;
                            if (l < heap_b.len and sctx2.lessThan(heap_b[l], heap_b[smallest])) smallest = l;
                            if (r2 < heap_b.len and sctx2.lessThan(heap_b[r2], heap_b[smallest])) smallest = r2;
                            if (smallest == i) break;
                            const tmp = heap_b[i]; heap_b[i] = heap_b[smallest]; heap_b[smallest] = tmp;
                            i = smallest;
                        }
                    }
                }
            }
            if (lim) |ls| if (ls.done()) return true;
            return false;
        }
    };

    var fs_mut = filter_state;
    var ls_mut = lim_state;
    const fs_ptr: ?*FilterState = if (fs_mut != null) &fs_mut.? else null;
    const ls_ptr: ?*LimitState  = if (ls_mut != null) &ls_mut.? else null;

    if (use_fetchrange) {
        const total_rows = ctx.source.rowCount();
        var pos: u64 = 0;
        while (pos < total_rows) {
            const n = @min(morsel_sz, total_rows - pos);
            var c: DataChunk = undefined;
            _ = chunk_arena.reset(.retain_capacity);
            try ctx.source.fetchRange(pos, n, &c, chunk_arena.allocator());
            pos += n;
            const done = try HeapChunkLoop.process(&c, heap_buf, &heap_len, k, keys, key_scratch, sctx, ls_ptr, fs_ptr, ctx);
            if (done) break;
        }
    } else {
        ctx.source.reset();
        var c: DataChunk = undefined;
        while (try ctx.source.nextChunk(&c, ctx)) {
            const done = try HeapChunkLoop.process(&c, heap_buf, &heap_len, k, keys, key_scratch, sctx, ls_ptr, fs_ptr, ctx);
            if (done) break;
        }
    }

    std.sort.pdq([]?Value, heap_buf[0..heap_len], sctx, SortCtx.lessThan);

    // Project the K winners (only K rows — negligible cost).
    var rl = RowList.init(out_metas);
    if (project_items) |items| {
        const row_buf = try alloc.alloc(?Value, schema_metas.len);
        for (heap_buf[0..heap_len]) |raw_row| {
            const proj_row = try alloc.alloc(?Value, items.len);
            @memcpy(row_buf[0..raw_row.len], raw_row);
            for (items, 0..) |item, ci| {
                proj_row[ci] = try kernels.evalExpr(item.expr, row_buf, null, alloc);
            }
            try rl.append(alloc, proj_row);
        }
    } else {
        for (heap_buf[0..heap_len]) |row| try rl.append(alloc, row);
    }
    return rl;
}


fn executeOrderBy(inner: RowList, keys: []const plan.SortKey, alloc: std.mem.Allocator) !RowList {
    const rows_copy = try alloc.dupe([]?Value, inner.rows.items);

    const SortCtx = struct {
        keys: []const plan.SortKey,
        fn lessThan(self: @This(), a: []?Value, b: []?Value) bool {
            for (self.keys) |key| {
                const av: ?Value = if (key.col_idx < a.len) a[key.col_idx] else null;
                const bv: ?Value = if (key.col_idx < b.len) b[key.col_idx] else null;
                const ord: std.math.Order = if (av != null and bv != null)
                    Value.order(av.?, bv.?)
                else if (av == null and bv == null)
                    .eq
                else if (av == null)
                    .lt  // NULL sorts first
                else
                    .gt;
                if (ord == .eq) continue;
                return if (key.desc) ord == .gt else ord == .lt;
            }
            return false;
        }
    };
    std.sort.pdq([]?Value, rows_copy, SortCtx{ .keys = keys }, SortCtx.lessThan);

    var rl = RowList.init(inner.metas);
    for (rows_copy) |row| try rl.append(alloc, row);
    return rl;
}

// ── HashJoin helper ───────────────────────────────────────────────────────────

fn executeHashJoin(
    left_rl:  RowList,
    right_rl: RowList,
    hj: plan.HashJoinNode,
    alloc: std.mem.Allocator,
) !RowList {
    var jht = try ht.JoinHashTable.init(alloc);
    const key_buf = try alloc.alloc(Value, hj.equi_keys.len);

    for (right_rl.rows.items, 0..) |row, ri| {
        for (hj.equi_keys, 0..) |ek, ki| {
            key_buf[ki] = (if (ek.right_col_idx < row.len) row[ek.right_col_idx] else null) orelse Value{ .int64 = 0 };
        }
        try jht.insert(key_buf, @intCast(ri));
    }

    const combined_metas = try alloc.alloc(result.ColMeta, left_rl.metas.len + right_rl.metas.len);
    @memcpy(combined_metas[0..left_rl.metas.len], left_rl.metas);
    @memcpy(combined_metas[left_rl.metas.len..], right_rl.metas);

    var rl = RowList.init(combined_metas);

    for (left_rl.rows.items) |lrow| {
        for (hj.equi_keys, 0..) |ek, ki| {
            key_buf[ki] = ((if (ek.left_col_idx < lrow.len) lrow[ek.left_col_idx] else null)) orelse Value{ .int64 = 0 };
        }
        const matches = jht.probe(key_buf);

        if (matches.len == 0 and hj.join_type == .left) {
            const combined = try alloc.alloc(?Value, combined_metas.len);
            @memcpy(combined[0..lrow.len], lrow);
            for (combined[lrow.len..]) |*vv| vv.* = null;
            try rl.append(alloc, combined);
            continue;
        }

        for (matches) |ri| {
            const rrow = right_rl.rows.items[ri];
            const combined = try alloc.alloc(?Value, combined_metas.len);
            @memcpy(combined[0..lrow.len], lrow);
            @memcpy(combined[lrow.len..lrow.len + rrow.len], rrow);
            if (hj.filter) |filt| {
                const keep_v = try kernels.evalExpr(filt, combined, null, alloc);
                if (!valueToBool(keep_v)) continue;
            }
            try rl.append(alloc, combined);
        }
    }

    return rl;
}

// ── Aggregate helpers ─────────────────────────────────────────────────────────

/// Finalize one accumulator for a ProjectItem.
/// When the agg is group_uniq_array with a sep, joins the array into a string.
fn finalizeAccum(acc: AggAccum, item: plan.ProjectItem, alloc: std.mem.Allocator) !?Value {
    const sep: ?[]const u8 = switch (item.expr) {
        .agg_call => |ac| ac.sep,
        else => null,
    };
    if (sep) |s| {
        const arr_val = try acc.toArrayValue(alloc);
        const elems = arr_val.array_string;
        if (elems.len == 0) return Value{ .string = "" };
        // Calculate total length.
        var total: usize = 0;
        for (elems) |e| total += e.len;
        total += s.len * (elems.len - 1);
        const buf = try alloc.alloc(u8, total);
        var pos: usize = 0;
        for (elems, 0..) |e, idx| {
            if (idx > 0) {
                @memcpy(buf[pos..pos + s.len], s);
                pos += s.len;
            }
            @memcpy(buf[pos..pos + e.len], e);
            pos += e.len;
        }
        return Value{ .string = buf };
    }
    return acc.toValue() catch (try acc.toArrayValue(alloc));
}

fn initAccumForAgg(expr: plan.Expr) AggAccum {
    return switch (expr) {
        .agg_call => |ac| switch (ac.kind) {
            .count_star, .count => .{ .count = 0 },
            .sum  => .{ .i64_sum = 0 },
            .avg  => .{ .f64_sum = 0.0 },
            .min  => .{ .i64_min = std.math.maxInt(i64) },
            .max  => .{ .i64_max = std.math.minInt(i64) },
            .group_uniq_array => .{ .uniq_strs = .{} },
            .any  => .{ .any_val = null },
        },
        else => .{ .count = 0 },
    };
}

fn evalAggArg(expr: plan.Expr, row: []const ?Value, alloc: std.mem.Allocator) !?Value {
    return switch (expr) {
        .agg_call => |ac| if (ac.arg) |arg| blk: {
            // Inline fast paths for common single-arg function calls to avoid dispatch overhead.
            if (arg == .fn_call) {
                const fc = arg.fn_call;
                if (fc.args.len == 1 and fc.args[0] == .col_ref) {
                    const col_val = row[fc.args[0].col_ref.index] orelse break :blk null;
                    if (std.mem.eql(u8, fc.name, "length") or
                        std.mem.eql(u8, fc.name, "char_length") or
                        std.mem.eql(u8, fc.name, "len"))
                    {
                        const s = col_val.toStr() orelse break :blk null;
                        break :blk Value{ .int64 = @intCast(s.len) };
                    }
                }
            }
            break :blk kernels.evalExpr(arg, row, null, alloc);
        } else null,
        else => kernels.evalExpr(expr, row, null, alloc),
    };
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "LimitState basic" {
    var b = chunk.ChunkBuilder.init(std.testing.allocator, 5);
    defer b.chunk.deinit();
    const col = try b.addColumn("n", .int64);
    for (0..5) |i| col.data.int64[i] = @intCast(i);
    var c = b.finish();

    var lim = LimitState{ .limit = 3, .offset = 0 };
    lim.apply(&c);
    try std.testing.expectEqual(@as(usize, 3), c.num_rows);
}

// ── Mock source for executePlan tests ─────────────────────────────────────────

const MockSource = struct {
    chunk: DataChunk,
    done: bool = false,

    const metas_storage = [_]result.ColMeta{
        .{ .name = "n", .col_type = .int64 },
    };

    fn nextChunk(ptr: *anyopaque, out: *DataChunk, _ctx: *QueryContext) !bool {
        _ = _ctx;
        const self: *MockSource = @ptrCast(@alignCast(ptr));
        if (self.done) return false;
        self.done = true;
        out.* = self.chunk;
        return true;
    }
    fn reset(ptr: *anyopaque) void {
        const self: *MockSource = @ptrCast(@alignCast(ptr));
        self.done = false;
    }
    fn schema(_ptr: *anyopaque) []const result.ColMeta {
        _ = _ptr;
        return &metas_storage;
    }

    const vtable = SourceIface.VTable{
        .nextChunk = nextChunk,
        .reset     = reset,
        .schema    = schema,
        .rowCount  = struct { fn f(_: *anyopaque) u64 { return 0; } }.f,
    };

    fn iface(self: *MockSource) SourceIface {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

test "executePlan: scalar_agg count(*)" {
    const alloc = std.testing.allocator;

    // Build a chunk with 4 rows, column "n" Int64: [1, 2, 3, 4]
    var b = chunk.ChunkBuilder.init(alloc, 4);
    const ci = try b.addColumn("n", .int64);
    for (0..4) |i| b.chunk.columns[ci].data.int64[i] = @intCast(i + 1);
    const mock_chunk = b.finish();

    var src = MockSource{ .chunk = mock_chunk };

    // Plan: scalar_agg [ count(*) ]
    var agg_call = plan.AggCall{ .kind = .count_star, .arg = null, .distinct = false };
    const agg_items = [_]plan.ProjectItem{.{
        .expr     = .{ .agg_call = &agg_call },
        .alias    = "count()",
        .out_type = .uint64,
    }};
    const scan_node = plan.PhysicalNode{ .part_scan = .{ .db = "db", .table = "t", .columns = &.{}, .filter = null } };
    const agg_node  = plan.PhysicalNode{ .scalar_agg = .{ .input = @constCast(&scan_node), .aggs = &agg_items } };

    var ctx = QueryContext.init(alloc, src.iface());
    defer ctx.deinit();

    var rs = try executePlan(&agg_node, &ctx);
    defer rs.deinit();

    try std.testing.expectEqual(@as(usize, 1), rs.num_rows);
    try std.testing.expectEqual(@as(usize, 1), rs.metas.len);
    const v = rs.get(0, 0).?;
    try std.testing.expectEqual(Value{ .uint64 = 4 }, v);
}

test "executePlan: filter + limit" {
    const alloc = std.testing.allocator;

    // 5 rows: [1..5], keep n > 2, limit 2 → [3, 4]
    var b = chunk.ChunkBuilder.init(alloc, 5);
    const ci = try b.addColumn("n", .int64);
    for (0..5) |i| b.chunk.columns[ci].data.int64[i] = @intCast(i + 1);
    const mock_chunk = b.finish();

    var src = MockSource{ .chunk = mock_chunk };

    // Filter: n > 2   (col_ref index=0 > lit_i64 2)
    var gt_binop = plan.BinOp{
        .left  = .{ .col_ref = .{ .index = 0, .name = "n" } },
        .right = .{ .lit_i64 = 2 },
    };
    const scan_node   = plan.PhysicalNode{ .part_scan = .{ .db = "db", .table = "t", .columns = &.{}, .filter = null } };
    const filter_node = plan.PhysicalNode{ .filter = .{ .input = @constCast(&scan_node), .predicate = .{ .gt = &gt_binop } } };
    const limit_node  = plan.PhysicalNode{ .limit  = .{ .input = @constCast(&filter_node), .limit = 2, .offset = 0 } };

    var ctx = QueryContext.init(alloc, src.iface());
    defer ctx.deinit();

    var rs = try executePlan(&limit_node, &ctx);
    defer rs.deinit();

    try std.testing.expectEqual(@as(usize, 2), rs.num_rows);
    try std.testing.expectEqual(Value{ .int64 = 3 }, rs.get(0, 0).?);
    try std.testing.expectEqual(Value{ .int64 = 4 }, rs.get(0, 1).?);
}

/// Parallel hash aggregation for integer-keyed queries with compact accumulators.
/// Returns null if unable to handle (falls back to sequential executeHashAggChunked).
fn executeHashAggParallelCompact(
    input: *const plan.PhysicalNode,
    keys:  []const plan.ProjectItem,
    aggs:  []const plan.ProjectItem,
    ctx:   *QueryContext,
) !?RowList {
    return executeHashAggParallelCompactTopK(input, keys, aggs, &.{}, 0, ctx);
}

/// Parallel hash_agg for single string col_ref key + compact aggs (incl. str_min/str_max).
/// Handles Q22/Q23-style: GROUP BY string_col + MIN(string_col) + COUNT(*).
/// Each thread builds a local StrAggHashTable; then tables are merged serially.
/// Hash-sidecar fast path for string GROUP BY with a parallel int64 hash column.
/// Groups by the int64 hash (fast), then late-materializes the string for top-K output.
/// Only called when str key column has hash_col_name set, no str aggs, no CASE WHEN.
fn executeHashAggParallelStrKeyViaHash(
    input:        *const plan.PhysicalNode,
    keys:         []const plan.ProjectItem,
    aggs:         []const plan.ProjectItem,
    sort_keys:    []const plan.SortKey,
    top_k:        usize,
    ctx:          *QueryContext,
    str_col_idx:  usize,  // original string column index
    str_key_pos:  usize,  // position in keys[] of the string key
    hash_col_idx: usize,  // int64 hash column index
) !?RowList {
    const alloc = ctx.allocator();
    const total_rows = ctx.source.rowCount();
    if (total_rows < 500_000) return null;
    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;
    const sm = ctx.source.schema();

    // Build a synthetic keys array: replace the string col_ref with the hash col_ref.
    const syn_keys = try alloc.alloc(plan.ProjectItem, keys.len);
    defer alloc.free(syn_keys);
    @memcpy(syn_keys, keys);
    syn_keys[str_key_pos] = .{
        .alias    = keys[str_key_pos].alias,
        .expr     = .{ .col_ref = .{ .index = hash_col_idx, .name = sm[hash_col_idx].name } },
        .out_type = .int64,
    };

    // Run compact int aggregation with hash keys.
    const compact_rl = (try executeHashAggParallelCompactTopK(
        input, syn_keys, aggs, sort_keys, top_k, ctx,
    )) orelse return null;

    // Late-materialize: replace int64 hash values with actual strings.
    const out_rows = compact_rl.rows.items.len;
    if (out_rows == 0) return compact_rl;

    // Collect result hashes.
    const result_hashes = try alloc.alloc(i64, out_rows);
    defer alloc.free(result_hashes);
    const result_strs  = try alloc.alloc([]const u8, out_rows);
    defer alloc.free(result_strs);

    for (compact_rl.rows.items, 0..) |row, r| {
        result_hashes[r] = if (row[str_key_pos]) |v| v.toI64() orelse 0 else 0;
        result_strs[r]   = "";
    }

    // Scan str + hash columns to reverse-map hash → string.
    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    var remaining: usize = out_rows;
    scan_loop: while (morsel_src.next()) |m| {
        var chunk_arena = std.heap.ArenaAllocator.init(alloc);
        defer chunk_arena.deinit();
        var c: DataChunk = undefined;
        ctx.source.fetchRange(m.start, m.end - m.start, &c, chunk_arena.allocator()) catch continue;
        if (str_col_idx >= c.columns.len or hash_col_idx >= c.columns.len) continue;
        const scol = c.columns[str_col_idx];
        const hcol = c.columns[hash_col_idx];
        if (scol.data != .string) continue;

        for (0..c.num_rows) |r| {
            const h: i64 = switch (hcol.data) {
                .int64  => |a| a[r],
                .uint64 => |a| @bitCast(a[r]),
                else    => continue,
            };
            for (result_hashes, 0..) |rh, ri| {
                if (rh == h and result_strs[ri].len == 0) {
                    result_strs[ri] = try alloc.dupe(u8, scol.data.string[r]);
                    remaining -= 1;
                    if (remaining == 0) break :scan_loop;
                    break;
                }
            }
        }
    }

    // Build final RowList replacing the hash column values with string values.
    const out_metas = try alloc.alloc(result.ColMeta, compact_rl.metas.len);
    @memcpy(out_metas, compact_rl.metas);
    out_metas[str_key_pos] = .{
        .name     = keys[str_key_pos].alias,
        .col_type = .string,
    };

    var final_rl = RowList.init(out_metas);

    for (compact_rl.rows.items, 0..) |old_row, r| {
        const new_row = try alloc.alloc(?Value, compact_rl.metas.len);
        @memcpy(new_row, old_row);
        new_row[str_key_pos] = .{ .string = result_strs[r] };
        try final_rl.append(alloc, new_row);
    }

    return final_rl;
}

fn executeHashAggParallelStrKey(
    input:     *const plan.PhysicalNode,
    keys:      []const plan.ProjectItem,
    aggs:      []const plan.ProjectItem,
    sort_keys: []const plan.SortKey,
    top_k:     usize,
    ctx:       *QueryContext,
) !?RowList {
    if (!ctx.source.supportsRange()) return null;
    const total_rows = ctx.source.rowCount();
    if (total_rows < 500_000) return null;
    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;
    const alloc = ctx.allocator();

    // Require: all keys are plain col_ref, lit_i64 (constant), OR exactly one simple CASE WHEN str key;
    // exactly one col_ref must be a string column (all others int).
    // (Multi-key int+string queries like Q15/Q17/Q18 are handled via composite key.)
    if (keys.len == 0) return null;

    // Guard: for large unfiltered scans with multi-key (int+str), the parallel merge cost
    // dominates and causes high variance (millions of unique pairs exhaust L3).
    // Fall back to sequential executeHashAggChunked for better consistency.
    // Exception: if all "extra" keys are lit_i64 constants (e.g. Q35: GROUP BY 1, URL),
    // the hash-sidecar path reduces it to a single int key — no variance issue.
    const has_filter_str: bool = switch (input.*) {
        .filter  => true,
        .project => |p| p.input.* == .filter,
        else => false,
    };
    {
        if (!has_filter_str and total_rows >= 3_000_000 and keys.len >= 2) {
            // Count non-constant keys. If only 1 non-constant key exists, hash-sidecar
            // can reduce it to a single int aggregation — allow it through.
            var non_const_count: usize = 0;
            for (keys) |k| { if (k.expr != .lit_i64) non_const_count += 1; }
            if (non_const_count >= 2) return null;
        }
    }
    var cw_key: ?CaseWhenStrKey = null;
    var cw_key_pos: usize = 0;
    for (keys, 0..) |k, ki| {
        if (k.expr == .col_ref) continue;
        if (k.expr == .lit_i64) continue; // constant key (e.g. GROUP BY 1)
        if (k.expr == .case_when) {
            if (cw_key != null) return null; // at most one CASE WHEN
            const cw = extractCaseWhenStrKey(k.expr) orelse return null;
            cw_key = cw;
            cw_key_pos = ki;
            continue;
        }
        return null;
    }
    const sm_pre = ctx.source.schema();
    var str_key_count: usize = 0;
    var str_key_col_idx: usize = 0;
    var str_key_pos: usize = 0; // position among keys array for the string key
    for (keys, 0..) |k, ki| {
        if (k.expr != .col_ref) continue; // skip CASE WHEN keys
        const ci = k.expr.col_ref.index;
        const is_str = ci < sm_pre.len and (sm_pre[ci].col_type == .string or sm_pre[ci].col_type == .array_string);
        if (is_str) { str_key_count += 1; str_key_col_idx = ci; str_key_pos = ki; }
    }
    // Must have exactly one col_ref string key (the primary string key).
    // If there's also a CASE WHEN, it becomes the secondary string component.
    if (str_key_count != 1) return null;
    const key_col_idx = str_key_col_idx;

    // ── Hash-sidecar fast path ───────────────────────────────────────────────
    // If the string key has a parallel int64 hash column (e.g. URL → URLHash),
    // aggregate by the int64 hash (much faster: no string hashing/comparison),
    // then late-materialize the actual string for the top-K output rows.
    // Only applies when there are no string aggs, no CASE WHEN key, and no filter
    // (with filters, the StrCountHashTable path via executeHashAggChunked is better
    // because the filter reduces cardinality and the existing path handles it efficiently).
    if (cw_key == null and str_key_count == 1 and !has_filter_str) {
        const str_meta = sm_pre[key_col_idx];
        if (str_meta.hash_col_name) |hcn| {
            // Find the hash column index.
            var hash_ci: ?usize = null;
            for (sm_pre, 0..) |m, i| {
                if (std.mem.eql(u8, m.name, hcn)) { hash_ci = i; break; }
            }
            if (hash_ci) |hci| {
                // Check no string aggs (str_min/str_max require actual string).
                var has_str_agg = false;
                for (aggs) |item| {
                    if (item.expr == .agg_call) {
                        const k = item.expr.agg_call.kind;
                        if (k == .min or k == .max) {
                            if (item.out_type == .string) { has_str_agg = true; break; }
                        }
                    }
                }
                if (!has_str_agg) {
                    if (try executeHashAggParallelStrKeyViaHash(
                        input, keys, aggs, sort_keys, top_k, ctx,
                        key_col_idx, str_key_pos, hci,
                    )) |rl| return rl;
                }
            }
        }
    }

    // Build compact_kinds; allow str_min/str_max.
    const compact_kinds = try alloc.alloc(ht.CompactAggKind, aggs.len);
    for (aggs, 0..) |item, ci| {
        if (item.expr != .agg_call) return null;
        compact_kinds[ci] = switch (item.expr.agg_call.kind) {
            .count_star, .count => .count,
            .sum  => .i64_sum,
            .avg  => .f64_sum,
            .min  => if (item.out_type == .string) .str_min else .i64_min,
            .max  => if (item.out_type == .string) .str_max else .i64_max,
            else  => return null,
        };
    }

    // Count str aggs and build sidecar_idx map.
    var num_str_aggs: usize = 0;
    const sidecar_idx = try alloc.alloc(usize, aggs.len);
    for (compact_kinds, 0..) |kind, ci| {
        if (kind == .str_min or kind == .str_max) {
            sidecar_idx[ci] = num_str_aggs;
            num_str_aggs += 1;
        } else {
            sidecar_idx[ci] = 0; // unused
        }
    }

    // Build init_vals.
    const compact_init_vals = try alloc.alloc(u64, aggs.len);
    for (compact_kinds, 0..) |kind, ci| {
        compact_init_vals[ci] = switch (kind) {
            .count, .i64_sum, .u64_sum, .u64_max, .str_min, .str_max => 0,
            .f64_sum => @bitCast(@as(f64, 0.0)),
            .i64_min => @bitCast(@as(i64, std.math.maxInt(i64))),
            .i64_max => @bitCast(@as(i64, std.math.minInt(i64))),
            .u64_min => std.math.maxInt(u64),
            .f64_min => @bitCast(std.math.inf(f64)),
            .f64_max => @bitCast(-std.math.inf(f64)),
        };
    }

    // Extract filter predicate.
    const filter_pred: ?plan.Expr = switch (input.*) {
        .filter  => |f| f.predicate,
        .project => |p| switch (p.input.*) { .filter => |f| f.predicate, else => null },
        else => null,
    };

    // Apply column restriction.
    {
        const sm = ctx.source.schema();
        var needed_mask = [_]bool{false} ** 256;
        const ncols = @min(256, sm.len);
        for (keys) |item| collectColRefs(item.expr, needed_mask[0..ncols]);
        for (aggs) |item| collectColRefs(item.expr, needed_mask[0..ncols]);
        if (filter_pred) |fp| collectColRefs(fp, needed_mask[0..ncols]);
        var needed_count: usize = 0;
        for (needed_mask[0..ncols]) |m| { if (m) needed_count += 1; }
        if (needed_count * 2 < sm.len) {
            var names_buf: [32][]const u8 = undefined;
            var names_len: usize = 0;
            for (needed_mask[0..ncols], 0..) |m, i| {
                if (m and names_len < names_buf.len) { names_buf[names_len] = sm[i].name; names_len += 1; }
            }
            ctx.source.setNeededCols(names_buf[0..names_len]);
        }
    }
    defer ctx.source.setNeededCols(null);

    // Preload columns.
    {
        var dummy: DataChunk = undefined;
        ctx.source.fetchRange(0, 0, &dummy, alloc) catch {};
    }

    const ParStrCtx = struct {
        source:       SourceIface,
        filter_pred:  ?plan.Expr,
        key_col_idx:  usize,
        keys:         []const plan.ProjectItem,
        str_key_pos:  usize,
        aggs:         []const plan.ProjectItem,
        compact_kinds: []const ht.CompactAggKind,
        compact_init_vals: []const u64,
        sidecar_idx:  []const usize,
        morsel_src:   *parallel.MorselSource,
        parent_alloc: std.mem.Allocator,
        local_ht:     ht.StrAggHashTable,
        err:          ?anyerror = null,
        // Preextracted fast-path conditions (avoids pass_mask allocation + two-pass scan).
        inline_ic:    [16]IntCmpCond = undefined,
        inline_ic_n:  usize = 0,
        inline_sc:    [8]StrCmpCond  = undefined,
        inline_sc_n:  usize = 0,
        use_inline_filter: bool = false,
        // Optional secondary CASE WHEN string key (e.g. Q40).
        cw_key: ?CaseWhenStrKey = null,
        cw_key_pos: usize = 0,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| { self.err = e; };
        }

        fn runWork(self: *@This()) !void {
            var thread_arena = std.heap.ArenaAllocator.init(self.parent_alloc);
            defer thread_arena.deinit();
             const talloc = thread_arena.allocator();

             // Build int_key_specs: each non-string, non-CASE-WHEN key is either a column ref or a constant.
             const IntKeySpec = struct {
                 is_col: bool,
                 col_idx: usize = 0,    // valid if is_col
                 const_val: u64 = 0,   // valid if !is_col
             };
             var int_key_specs_buf: [16]IntKeySpec = undefined;
             var int_key_n: usize = 0;
             for (self.keys) |k| {
                 switch (k.expr) {
                     .col_ref => |cr| {
                         if (cr.index == self.key_col_idx) continue; // skip the string key
                         if (int_key_n < 16) { int_key_specs_buf[int_key_n] = .{ .is_col = true, .col_idx = cr.index }; int_key_n += 1; }
                     },
                     .lit_i64 => |v| {
                         if (int_key_n < 16) { int_key_specs_buf[int_key_n] = .{ .is_col = false, .const_val = @bitCast(v) }; int_key_n += 1; }
                     },
                     else => {}, // skip CASE WHEN and other handled keys
                 }
             }
             const int_key_specs = int_key_specs_buf[0..int_key_n];
             // If all int keys are constants (lit_i64), skip int prefix in the composite key.
             // They don't affect grouping cardinality; we reconstruct them at emit time.
             var all_const_int_keys = true;
             for (int_key_specs) |spec| { if (spec.is_col) { all_const_int_keys = false; break; } }
             const int_prefix_len: usize = if (all_const_int_keys) 0 else int_key_n * 8;

            while (self.morsel_src.next()) |m| {
                var chunk_arena = std.heap.ArenaAllocator.init(talloc);
                defer chunk_arena.deinit();
                const calloc = chunk_arena.allocator();
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, calloc);

                 // Build pass_mask from filter.
                 var pass_mask: ?[]bool = null;
                 if (self.filter_pred) |fp| {
                     const pm = try calloc.alloc(bool, c.num_rows);
                     @memset(pm, true);
                     // Fast int-cond path.
                     var ic_buf: [16]IntCmpCond = undefined;
                     var ic_n: usize = 0;
                     const ic_complete = extractAndIntConds(fp, &ic_buf, &ic_n, false);
                     if (ic_complete and ic_n > 0) {
                         for (0..c.num_rows) |r| {
                             for (ic_buf[0..ic_n]) |cond| {
                                 if (cond.col_idx >= c.columns.len) { pm[r] = false; break; }
                                 const col = c.columns[cond.col_idx];
                                 if (col.isRowNull(r)) { pm[r] = false; break; }
                                 const v: i64 = switch (col.data) {
                                     .int64 => |a| a[r], .uint64 => |a| @bitCast(a[r]),
                                     .bool_u8 => |a| @as(i64, a[r]), .date_u16 => |a| @as(i64, a[r]),
                                     .datetime64_ms => |a| a[r],
                                     else => { pm[r] = false; break; },
                                 };
                                 const pass = switch (cond.op) {
                                     .eq => v == cond.val, .neq => v != cond.val,
                                     .lt => v < cond.val, .lte => v <= cond.val,
                                     .gt => v > cond.val, .gte => v >= cond.val,
                                     .in2 => v == cond.val or v == cond.val2,
                                 };
                                 if (!pass) { pm[r] = false; break; }
                             }
                         }
                      } else {
                          // Fast str-cond path: covers str_col != 'literal' (e.g. Q11 MobilePhoneModel <> '').
                          var sc_buf: [8]StrCmpCond = undefined;
                          var sc_n: usize = 0;
                          const sc_complete = extractAndStrConds(fp, &sc_buf, &sc_n, false);
                          if (sc_complete and sc_n > 0) {
                              for (0..c.num_rows) |r| {
                                  for (sc_buf[0..sc_n]) |cond| {
                                      if (cond.col_idx >= c.columns.len) { pm[r] = false; break; }
                                      const col = c.columns[cond.col_idx];
                                      const s: []const u8 = if (col.isRowNull(r)) "" else switch (col.data) {
                                          .string => |a| a[r],
                                          else => { pm[r] = false; break; },
                                      };
                                      const pass = switch (cond.op) {
                                          .eq  => std.mem.eql(u8, s, cond.val),
                                          .neq => !std.mem.eql(u8, s, cond.val),
                                      };
                                      if (!pass) { pm[r] = false; break; }
                                  }
                              }
                          } else {
                          // Mixed fast-path: handles AND of int + str comparisons without evalExpr.
                          // Covers e.g. Q37: CounterID=62 AND EventDate range AND URL<>'' etc.
                          var mic_buf: [16]IntCmpCond = undefined;
                          var mic_n: usize = 0;
                          var msc_buf: [8]StrCmpCond = undefined;
                          var msc_n: usize = 0;
                          const mixed_complete = extractMixedAndConds(fp, &mic_buf, &mic_n, &msc_buf, &msc_n);
                          if (mixed_complete and (mic_n > 0 or msc_n > 0)) {
                              mixed_loop: for (0..c.num_rows) |r| {
                                  for (mic_buf[0..mic_n]) |cond| {
                                      if (cond.col_idx >= c.columns.len) { pm[r] = false; continue :mixed_loop; }
                                      const col = c.columns[cond.col_idx];
                                      if (col.isRowNull(r)) { pm[r] = false; continue :mixed_loop; }
                                      const v: i64 = switch (col.data) {
                                          .int64 => |a| a[r], .uint64 => |a| @bitCast(a[r]),
                                          .bool_u8 => |a| @as(i64, a[r]), .date_u16 => |a| @as(i64, a[r]),
                                          .datetime64_ms => |a| a[r],
                                          else => { pm[r] = false; continue :mixed_loop; },
                                      };
                                      const pass = switch (cond.op) {
                                          .eq => v == cond.val, .neq => v != cond.val,
                                          .lt => v < cond.val, .lte => v <= cond.val,
                                          .gt => v > cond.val, .gte => v >= cond.val,
                                          .in2 => v == cond.val or v == cond.val2,
                                      };
                                      if (!pass) { pm[r] = false; continue :mixed_loop; }
                                  }
                                  for (msc_buf[0..msc_n]) |cond| {
                                      if (cond.col_idx >= c.columns.len) { pm[r] = false; continue :mixed_loop; }
                                      const col = c.columns[cond.col_idx];
                                      const s: []const u8 = if (col.isRowNull(r)) "" else switch (col.data) {
                                          .string => |a| a[r],
                                          else => { pm[r] = false; continue :mixed_loop; },
                                      };
                                      const pass = switch (cond.op) {
                                          .eq  => std.mem.eql(u8, s, cond.val),
                                          .neq => !std.mem.eql(u8, s, cond.val),
                                      };
                                      if (!pass) { pm[r] = false; continue :mixed_loop; }
                                  }
                              }
                          } else {
                          // Partial int pre-filter: apply any int conditions fast before evalExpr.
                          // This short-circuits most rows (e.g. CounterID=62 filters out 99%).
                          var pic_buf: [16]IntCmpCond = undefined;
                          var pic_n: usize = 0;
                          _ = extractAndIntConds(fp, &pic_buf, &pic_n, true);

                          // Best-effort str cond pre-filter (applied before LIKE guard).
                          // For Q22/Q23: extracts SearchPhrase <> '' fast, reducing LIKE calls.
                          var psc_buf: [8]StrCmpCond = undefined;
                          var psc_n: usize = 0;
                          _ = extractAndStrConds(fp, &psc_buf, &psc_n, true);

                         // Build like guards for fast pre-filtering.
                         var guards_list = std.ArrayListUnmanaged(LikeGuard){ .items = &.{}, .capacity = 0 };
                         collectLikeGuards(fp, &guards_list, calloc);
                         const guards = guards_list.items;

                         const ref_mask = try calloc.alloc(bool, @min(256, c.columns.len));
                         @memset(ref_mask, false);
                         collectColRefs(fp, ref_mask);
                         var ref_buf = try calloc.alloc(usize, c.columns.len);
                         var ref_n: usize = 0;
                         for (ref_mask, 0..) |m2, idx| { if (m2 and idx < c.columns.len) { ref_buf[ref_n] = idx; ref_n += 1; } }
                         const refs = ref_buf[0..ref_n];
                         const row_v = try calloc.alloc(?Value, c.columns.len);
                         @memset(row_v, null);

                         row_loop: for (0..c.num_rows) |r| {
                             // Partial int conditions pre-check (fast path).
                             if (pic_n > 0) {
                                 for (pic_buf[0..pic_n]) |cond| {
                                     if (cond.col_idx >= c.columns.len) { pm[r] = false; continue :row_loop; }
                                     const col = c.columns[cond.col_idx];
                                     if (col.isRowNull(r)) { pm[r] = false; continue :row_loop; }
                                     const v: i64 = switch (col.data) {
                                         .int64 => |a| a[r], .uint64 => |a| @bitCast(a[r]),
                                         .bool_u8 => |a| @as(i64, a[r]), .date_u16 => |a| @as(i64, a[r]),
                                         .datetime64_ms => |a| a[r],
                                         else => { pm[r] = false; continue :row_loop; },
                                     };
                                     const pass = switch (cond.op) {
                                         .eq => v == cond.val, .neq => v != cond.val,
                                         .lt => v < cond.val, .lte => v <= cond.val,
                                         .gt => v > cond.val, .gte => v >= cond.val,
                                         .in2 => v == cond.val or v == cond.val2,
                                     };
                                     if (!pass) { pm[r] = false; continue :row_loop; }
                                 }
                             }
                             // Partial str cond pre-check (applied before LIKE to reduce expensive scan).
                             if (psc_n > 0) {
                                 for (psc_buf[0..psc_n]) |cond| {
                                     if (cond.col_idx >= c.columns.len) { pm[r] = false; continue :row_loop; }
                                     const col = c.columns[cond.col_idx];
                                     const s: []const u8 = if (col.isRowNull(r)) "" else switch (col.data) {
                                         .string => |a| a[r],
                                         else => continue,
                                     };
                                     const pass = switch (cond.op) {
                                         .eq  => std.mem.eql(u8, s, cond.val),
                                         .neq => !std.mem.eql(u8, s, cond.val),
                                     };
                                     if (!pass) { pm[r] = false; continue :row_loop; }
                                 }
                             }
                             // LIKE guard pre-filter.
                             for (guards) |lg| {
                                 if (lg.col_idx >= c.columns.len) { pm[r] = false; continue :row_loop; }
                                 const col = c.columns[lg.col_idx];
                                 const s = if (col.isRowNull(r)) "" else col.data.string[r];
                                 if (lg.matcher.match(s) == lg.negate) { pm[r] = false; continue :row_loop; }
                             }
                             for (refs) |j| {
                                 const col = c.columns[j];
                                 row_v[j] = if (col.isRowNull(r)) null else col.data.get(r);
                             }
                             const v = try kernels.evalExpr(fp, row_v, null, calloc);
                             pm[r] = if (v) |val| val.bool_u8 != 0 else false;
                         }
                         } // end mixed_complete else
                         } // end sc_complete else
                     }
                     pass_mask = pm;
                 }

                 // Aggregate into local hash table.
                 if (self.key_col_idx >= c.columns.len) continue;
                 const key_col = c.columns[self.key_col_idx];
                 if (key_col.data != .string) continue;
                 const strs = key_col.data.string;

                 agg_loop: for (0..c.num_rows) |r| {
                     // Inline fast filter: avoids separate pass_mask allocation+scan.
                     if (self.use_inline_filter) {
                         for (self.inline_ic[0..self.inline_ic_n]) |cond| {
                             if (cond.col_idx >= c.columns.len) continue :agg_loop;
                             const col = c.columns[cond.col_idx];
                             if (col.isRowNull(r)) continue :agg_loop;
                             const v: i64 = switch (col.data) {
                                 .int64 => |a| a[r], .uint64 => |a| @bitCast(a[r]),
                                 .bool_u8 => |a| @as(i64, a[r]), .date_u16 => |a| @as(i64, a[r]),
                                 .datetime64_ms => |a| a[r],
                                 else => continue :agg_loop,
                             };
                             const pass = switch (cond.op) {
                                 .eq => v == cond.val, .neq => v != cond.val,
                                 .lt => v < cond.val, .lte => v <= cond.val,
                                 .gt => v > cond.val, .gte => v >= cond.val,
                                 .in2 => v == cond.val or v == cond.val2,
                             };
                             if (!pass) continue :agg_loop;
                         }
                         for (self.inline_sc[0..self.inline_sc_n]) |cond| {
                             if (cond.col_idx >= c.columns.len) continue :agg_loop;
                             const col = c.columns[cond.col_idx];
                             const s: []const u8 = if (col.isRowNull(r)) "" else switch (col.data) {
                                 .string => |a| a[r],
                                 else => continue :agg_loop,
                             };
                             const pass = switch (cond.op) {
                                 .eq  => std.mem.eql(u8, s, cond.val),
                                 .neq => !std.mem.eql(u8, s, cond.val),
                             };
                             if (!pass) continue :agg_loop;
                         }
                     } else if (pass_mask) |pm| { if (!pm[r]) continue; }
                     if (key_col.isRowNull(r)) continue;
                    const str_val = strs[r];

                     // Evaluate optional CASE WHEN secondary string key.
                     const cw_str: []const u8 = if (self.cw_key) |*cwk| cwk.eval(&c, r) else "";

                     // Build composite key: [int_key_0:u64LE]...[cw_len:u16LE][cw_bytes][str_val_bytes]
                     // When no int keys and no CASE WHEN: use str_val directly (no alloc).
                     // When all int keys are constants: also use str_val directly (no prefix needed).
                     // When CASE WHEN present: encode as [int_prefix][cw_len:2B][cw_bytes][url_bytes].
                     const composite_key: []const u8 = if (self.cw_key != null) blk: {
                         const total_len = int_prefix_len + 2 + cw_str.len + str_val.len;
                         const kbuf = try talloc.alloc(u8, total_len);
                         if (!all_const_int_keys) {
                             for (int_key_specs, 0..) |spec, ki| {
                                 const ival: u64 = if (!spec.is_col) spec.const_val else blk2: {
                                     const col = c.columns[spec.col_idx];
                                     break :blk2 if (col.isRowNull(r)) 0 else switch (col.data) {
                                         .int64 => |a| @bitCast(a[r]),
                                         .uint64 => |a| a[r],
                                         .bool_u8 => |a| @as(u64, a[r]),
                                         .date_u16 => |a| @as(u64, a[r]),
                                         .datetime64_ms => |a| @bitCast(a[r]),
                                         else => 0,
                                     };
                                 };
                                 std.mem.writeInt(u64, kbuf[ki*8..ki*8+8][0..8], ival, .little);
                             }
                         }
                         std.mem.writeInt(u16, kbuf[int_prefix_len..int_prefix_len+2][0..2], @intCast(@min(cw_str.len, 65535)), .little);
                         @memcpy(kbuf[int_prefix_len+2..int_prefix_len+2+cw_str.len], cw_str);
                         @memcpy(kbuf[int_prefix_len+2+cw_str.len..], str_val);
                         break :blk kbuf;
                     } else if (int_key_n == 0 or all_const_int_keys) str_val else blk: {
                         const total_len = int_prefix_len + str_val.len;
                         const kbuf = try talloc.alloc(u8, total_len);
                         for (int_key_specs, 0..) |spec, ki| {
                             const ival: u64 = if (!spec.is_col) spec.const_val else blk2: {
                                 const col = c.columns[spec.col_idx];
                                 break :blk2 if (col.isRowNull(r)) 0 else switch (col.data) {
                                     .int64 => |a| @bitCast(a[r]),
                                     .uint64 => |a| a[r],
                                     .bool_u8 => |a| @as(u64, a[r]),
                                     .date_u16 => |a| @as(u64, a[r]),
                                     .datetime64_ms => |a| @bitCast(a[r]),
                                     else => 0,
                                 };
                             };
                             std.mem.writeInt(u64, kbuf[ki*8..ki*8+8][0..8], ival, .little);
                         }
                         @memcpy(kbuf[int_prefix_len..], str_val);
                         break :blk kbuf;
                     };

                    const res = try self.local_ht.getOrInsert(composite_key, self.compact_init_vals);
                    try updateCompactVals(res.vals, self.compact_kinds, self.aggs, &c, r,
                        &self.local_ht, res.slot, self.sidecar_idx);
                }
            }
        }
    };

    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    const pctxs = try alloc.alloc(ParStrCtx, n_threads);

    // Pre-extract inline filter conditions (mixed int+str AND predicates).
    // When complete, threads skip pass_mask allocation and check inline in the agg loop.
    var pre_inline_ic: [16]IntCmpCond = undefined;
    var pre_inline_ic_n: usize = 0;
    var pre_inline_sc: [8]StrCmpCond = undefined;
    var pre_inline_sc_n: usize = 0;
     var use_inline_filter = false;
     if (filter_pred) |fp| {
         if (extractMixedAndConds(fp, &pre_inline_ic, &pre_inline_ic_n, &pre_inline_sc, &pre_inline_sc_n)) {
             use_inline_filter = true;
         }
     }


    for (pctxs) |*pc| {
        pc.* = .{
            .source            = ctx.source,
            .filter_pred       = if (use_inline_filter) null else filter_pred,
            .key_col_idx       = key_col_idx,
            .keys              = keys,
            .str_key_pos       = str_key_pos,
            .aggs              = aggs,
            .compact_kinds     = compact_kinds,
            .compact_init_vals = compact_init_vals,
            .sidecar_idx       = sidecar_idx,
            .morsel_src        = &morsel_src,
            .parent_alloc      = alloc,
            .local_ht          = try ht.StrAggHashTable.initWithCapacity(alloc, aggs.len, num_str_aggs, 256),
            .use_inline_filter = use_inline_filter,
            .cw_key            = cw_key,
            .cw_key_pos        = cw_key_pos,
        };
        if (use_inline_filter) {
            @memcpy(pc.inline_ic[0..pre_inline_ic_n], pre_inline_ic[0..pre_inline_ic_n]);
            pc.inline_ic_n = pre_inline_ic_n;
            @memcpy(pc.inline_sc[0..pre_inline_sc_n], pre_inline_sc[0..pre_inline_sc_n]);
            pc.inline_sc_n = pre_inline_sc_n;
        }
    }

    try parallel.parallelFor(alloc, ParStrCtx, ParStrCtx.work, pctxs, &morsel_src);
    for (pctxs) |*pc| { if (pc.err) |e| return e; }

    // Merge all local tables into pctxs[0].
    for (pctxs[1..]) |*pc| {
        try pctxs[0].local_ht.mergeFrom(&pc.local_ht, compact_kinds, compact_init_vals);
    }

    // Emit result.
    const out_metas = try alloc.alloc(result.ColMeta, keys.len + aggs.len);
    for (keys, 0..) |k, i| out_metas[i] = .{ .name = k.alias, .col_type = k.out_type };
    for (aggs, 0..) |a, i| out_metas[keys.len + i] = .{ .name = a.alias, .col_type = a.out_type };

    var rl = RowList.init(out_metas);

    // Build emit_int_key_n for emit decoding (same order as in runWork: col_ref non-str + lit_i64).
    // When all int keys are constants, int_prefix_len2 = 0 (not encoded in composite key).
    var emit_int_key_n: usize = 0;
    var emit_all_const = true;
    for (keys) |k| {
        switch (k.expr) {
            .col_ref => |cr| { if (cr.index != key_col_idx) { emit_int_key_n += 1; emit_all_const = false; } },
            .lit_i64 => emit_int_key_n += 1,
            else => {},
        }
    }
    if (emit_int_key_n == 0) emit_all_const = true;
    const int_prefix_len2: usize = if (emit_all_const) 0 else emit_int_key_n * 8;

    const EmitCtx = struct {
        rl:            *RowList,
        alloc:         std.mem.Allocator,
        aggs:          []const plan.ProjectItem,
        kinds:         []const ht.CompactAggKind,
        str_ht:        *ht.StrAggHashTable,
        sidecar_idx:   []const usize,
        keys:          []const plan.ProjectItem,
        str_key_pos:   usize,
        cw_key_pos:    usize,
        has_cw:        bool,
        int_prefix:    usize, // bytes
        all_const_ints: bool, // true when all int keys are lit_i64 constants (not in composite key)
        sm:            []const result.ColMeta,
    };
    const sm_emit = ctx.source.schema();
    var emit_ctx = EmitCtx{
        .rl             = &rl,
        .alloc          = alloc,
        .aggs           = aggs,
        .kinds          = compact_kinds,
        .str_ht         = &pctxs[0].local_ht,
        .sidecar_idx    = sidecar_idx,
        .keys           = keys,
        .str_key_pos    = str_key_pos,
        .cw_key_pos     = cw_key_pos,
        .has_cw         = cw_key != null,
        .int_prefix     = int_prefix_len2,
        .all_const_ints = emit_all_const,
        .sm             = sm_emit,
    };
    pctxs[0].local_ht.iterateWithSlot(&emit_ctx, struct {
        fn cb(ec: *EmitCtx, composite: []const u8, vals: []const u64, slot: usize) void {
            const row = ec.alloc.alloc(?Value, ec.keys.len + vals.len) catch return;
            // Decode composite key into row slots.
            // Format (with CASE WHEN):   [int_prefix][cw_len:u16LE][cw_bytes][str_bytes]
            // Format (without CASE WHEN): [int_prefix][str_bytes]
            const cw_len: usize = if (ec.has_cw and composite.len >= ec.int_prefix + 2)
                @as(usize, std.mem.readInt(u16, composite[ec.int_prefix..ec.int_prefix+2][0..2], .little))
            else 0;
            const cw_start: usize = ec.int_prefix + (if (ec.has_cw) @as(usize, 2) else 0);
            const str_start: usize = cw_start + cw_len;
            const cw_str: []const u8 = if (ec.has_cw and cw_start + cw_len <= composite.len)
                composite[cw_start..cw_start+cw_len] else "";
            const str_val: []const u8 = if (str_start <= composite.len)
                composite[str_start..] else "";
            var int_ki: usize = 0;
            for (ec.keys, 0..) |k, ki| {
                if (ec.has_cw and ki == ec.cw_key_pos) {
                    row[ki] = Value{ .string = cw_str };
                    continue;
                }
                if (k.expr != .col_ref and k.expr != .lit_i64) { row[ki] = Value{ .int64 = 0 }; continue; }
                if (k.expr == .lit_i64) {
                    if (ec.all_const_ints) {
                        // Constant key — value is just the literal (not stored in composite key).
                        row[ki] = Value{ .int64 = k.expr.lit_i64 };
                    } else {
                        // Constant stored in int prefix of composite key.
                        const ival = std.mem.readInt(u64, composite[int_ki*8..int_ki*8+8][0..8], .little);
                        row[ki] = Value{ .int64 = @bitCast(ival) };
                        int_ki += 1;
                    }
                    continue;
                }
                const ci = k.expr.col_ref.index;
                if (ki == ec.str_key_pos) {
                    row[ki] = Value{ .string = str_val };
                } else {
                    const ival = std.mem.readInt(u64, composite[int_ki*8..int_ki*8+8][0..8], .little);
                    row[ki] = if (ci < ec.sm.len) switch (ec.sm[ci].col_type) {
                        .int64 => Value{ .int64 = @bitCast(ival) },
                        .uint64 => Value{ .uint64 = ival },
                        .date_u16 => Value{ .date_u16 = @truncate(ival) },
                        .bool_u8 => Value{ .bool_u8 = @truncate(ival) },
                        .datetime64_ms => Value{ .datetime64_ms = @bitCast(ival) },
                        else => Value{ .int64 = @bitCast(ival) },
                    } else Value{ .int64 = @bitCast(ival) };
                    int_ki += 1;
                }
            }
            emitCompactValsWithSidecar(vals, ec.kinds, ec.aggs, row[ec.keys.len..], ec.str_ht, slot, ec.sidecar_idx);
            ec.rl.append(ec.alloc, row) catch {};
        }
    }.cb);

    // Apply top-K sort if requested.
    if (top_k > 0 and sort_keys.len > 0 and rl.rows.items.len > top_k) {
        const sorted = try executeTopK(rl, sort_keys, top_k, alloc);
        return sorted;
    }
    if (sort_keys.len > 0) {
        return try executeOrderBy(rl, sort_keys, alloc);
    }
    return rl;
}

/// Same as executeHashAggParallelCompact but with optional top-K emit.
/// When top_k > 0 and sort_keys is non-empty, emits into a min-heap instead of a full RowList.
fn executeHashAggParallelCompactTopK(
    input:     *const plan.PhysicalNode,
    keys:      []const plan.ProjectItem,
    aggs:      []const plan.ProjectItem,
    sort_keys: []const plan.SortKey,
    top_k:     usize,
    ctx:       *QueryContext,
) !?RowList {
    if (!ctx.source.supportsRange()) return null;
    const total_rows = ctx.source.rowCount();
    if (total_rows < 500_000) return null; // not worth parallelizing small inputs

    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;

    const alloc = ctx.allocator();

    // Check: all keys must be int col_ref, lit_i64 constant, col ± lit_i64, or date_trunc fn_call.
    for (keys) |k| {
        switch (k.expr) {
            .col_ref => {},
            .lit_i64 => {}, // constant key (e.g. GROUP BY 1)
            .add => |op| { if (op.left != .col_ref or op.right != .lit_i64) return null; },
            .sub => |op| { if (op.left != .col_ref or op.right != .lit_i64) return null; },
            .fn_call => |fc| {
                // Allow toStartOfMinute/toStartOfHour/toStartOfDay(col_ref) — date_trunc variants.
                const ok = (std.mem.eql(u8, fc.name, "toStartOfMinute") or
                            std.mem.eql(u8, fc.name, "toStartOfHour")   or
                            std.mem.eql(u8, fc.name, "toStartOfDay"))   and
                           fc.args.len == 1 and fc.args[0] == .col_ref;
                if (!ok) return null;
            },
            else => return null,
        }
    }

    // Check: all key columns must be integer (not string) type.
    // String keys must be handled by executeHashAggParallelStrKey instead.
    {
        const sm = ctx.source.schema();
        for (keys) |k| {
            const col_idx: usize = switch (k.expr) {
                .col_ref => |cr| cr.index,
                .add     => |op| op.left.col_ref.index,
                .sub     => |op| op.left.col_ref.index,
                .fn_call => |fc| fc.args[0].col_ref.index,
                else     => continue,
            };
            if (col_idx < sm.len) {
                switch (sm[col_idx].col_type) {
                    .string, .array_string => return null,
                    else => {},
                }
            }
        }
    }

    // Check: all aggs must be compact (no str_min/str_max).
    const compact_kinds = try alloc.alloc(ht.CompactAggKind, aggs.len);
    for (aggs, 0..) |item, ci| {
        if (item.expr != .agg_call) return null;
        compact_kinds[ci] = switch (item.expr.agg_call.kind) {
            .count_star, .count => .count,
            .sum  => .i64_sum,
            .avg  => .f64_sum,
            .min  => if (item.out_type == .string) return null else .i64_min,
            .max  => if (item.out_type == .string) return null else .i64_max,
            else  => return null,
        };
    }

    // Extract filter predicate from input node.
    const filter_pred: ?plan.Expr = switch (input.*) {
        .filter  => |f| f.predicate,
        .project => |p| switch (p.input.*) { .filter => |f| f.predicate, else => null },
        else => null,
    };

    // No-filter path now supports COUNT, i64_sum, and f64_sum (AVG numerator).
    // Only fall back for unusual agg kinds (i64_min/max, u64_*) without filter.
    if (filter_pred == null) {
        for (compact_kinds) |kind| {
            switch (kind) {
                .count, .i64_sum, .f64_sum => {},
                else => return null,
            }
        }
    }

    // Apply column restriction for scan.
    {
        const sm = ctx.source.schema();
        var needed_mask = [_]bool{false} ** 256;
        const ncols = @min(256, sm.len);
        for (keys) |item| collectColRefs(item.expr, needed_mask[0..ncols]);
        for (aggs) |item| collectColRefs(item.expr, needed_mask[0..ncols]);
        if (filter_pred) |fp| collectColRefs(fp, needed_mask[0..ncols]);
        var needed_count: usize = 0;
        for (needed_mask[0..ncols]) |m| { if (m) needed_count += 1; }
        if (needed_count * 2 < sm.len) {
            var names_buf: [32][]const u8 = undefined;
            var names_len: usize = 0;
            for (needed_mask[0..ncols], 0..) |m, i| {
                if (m and names_len < names_buf.len) { names_buf[names_len] = sm[i].name; names_len += 1; }
            }
            ctx.source.setNeededCols(names_buf[0..names_len]);
        }
    }
    defer ctx.source.setNeededCols(null);

    // If the filter is a simple `col_ref != ''` (string non-empty check), tell the
    // source to decode that column as bool_u8 instead of fat string pointers.
    // This reduces write bandwidth by ~16× for the filter column (1B vs 16B per row).
    const str_nonempty_col_name: ?[]const u8 = if (filter_pred) |fp| blk: {
        if (fp == .neq) {
            const op = fp.neq;
            if (op.left == .col_ref and op.right == .lit_str and op.right.lit_str.len == 0) {
                const ci = op.left.col_ref.index;
                const sm2 = ctx.source.schema();
                if (ci < sm2.len) break :blk sm2[ci].name;
            }
        }
        break :blk null;
    } else null;
    if (str_nonempty_col_name) |col_name| ctx.source.setStringNonEmptyBool(col_name);
    defer ctx.source.setStringNonEmptyBool(null);

    // Load columns before parallel scan.
    {
        var dummy: DataChunk = undefined;
        ctx.source.fetchRange(0, 0, &dummy, alloc) catch {};
    }

    const compact_init_vals = try alloc.alloc(u64, aggs.len);
    for (compact_kinds, 0..) |kind, i| {
        compact_init_vals[i] = switch (kind) {
            .count => 0,
            .i64_sum, .u64_sum, .f64_sum => 0,
            .i64_min => @as(u64, @bitCast(@as(i64, std.math.maxInt(i64)))),
            .i64_max => @as(u64, @bitCast(@as(i64, std.math.minInt(i64)))),
            .u64_min => std.math.maxInt(u64),
            .u64_max => 0,
            .f64_min => @bitCast(std.math.inf(f64)),
            .f64_max => @bitCast(-std.math.inf(f64)),
            .str_min, .str_max => 0,
        };
    }

    // ── Two-phase partitioned aggregation (scatter + small-HT aggregate) ──────────
    // For large plain-col-ref-key queries (Q33, Q32), the per-thread HT
    // exceeds L3, causing ~15ms of DRAM stalls per scan.  Two-phase avoids this:
    // Phase1: scatter rows to 64 partition buffers (no HT → no random misses).
    // Phase2: aggregate each partition with a small (~15K entries) L2-fitting HT.
    // Also supports queries with a single col_ref eq/neq lit_str filter (e.g. Q32:
    // SearchPhrase <> '') — the filter is inlined in the scatter phase with no
    // per-row arena allocation.
    two_phase: {
        // Guard: only handles exactly 2 plain col_ref integer keys.
        // 1-key queries (Q16) have smaller HTs that fit in L3 — two-phase adds scatter
        // overhead without the DRAM-stall benefit, so we keep the prefetch path for them.
        if (keys.len != 2) break :two_phase;
        for (keys) |k| { if (k.expr != .col_ref) break :two_phase; }
        // Guard: skip two-phase for low-cardinality keys (int16/bool etc.) — their HTs
        // are L1-fitting already; scatter overhead exceeds the DRAM-stall benefit.
        {
            const src_schema = ctx.source.schema();
            for (keys) |k| {
                const ci = k.expr.col_ref.index;
                if (ci < src_schema.len and src_schema[ci].is_narrow_int) break :two_phase;
            }
        }
        // If there's a filter, try:
        //   (a) simple single-term col_ref eq/neq lit_str (e.g. Q32: SearchPhrase <> '')
        //   (b) pure int AND conditions (e.g. Q41: CounterID=62 AND TraficSourceID IN (-1,6) AND ...)
        // Fall back to regular parallel compact if neither applies.
        var str_filt: ?SimpleStrFilter = null;
        var int_filt: ?[]const IntCmpCond = null;
        if (filter_pred) |fp| {
            if (tryExtractSimpleStrFilter(fp)) |sf| {
                str_filt = sf;
            } else {
                var ic_buf: [16]IntCmpCond = undefined;
                var ic_n: usize = 0;
                const ic_complete = extractAndIntConds(fp, &ic_buf, &ic_n, false);
                if (ic_complete and ic_n > 0) {
                    int_filt = try alloc.dupe(IntCmpCond, ic_buf[0..ic_n]);
                } else {
                    break :two_phase;
                }
            }
        }
        if (try executeTwoPhaseHashAgg(
            keys, aggs, compact_kinds, compact_init_vals,
            total_rows, n_threads, alloc, ctx, sort_keys, top_k, str_filt, int_filt,
        )) |two_phase_rl| return two_phase_rl;
    }

    const ParHashCtx = struct {
        source:      SourceIface,
        filter_pred: ?plan.Expr,
        keys:        []const plan.ProjectItem,
        aggs:        []const plan.ProjectItem,
        compact_kinds: []const ht.CompactAggKind,
        compact_init_vals: []const u64,
        morsel_src:  *parallel.MorselSource,
        parent_alloc: std.mem.Allocator,
        local_ht:    ht.CompactIntKeyHashTable,
        err:         ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| { self.err = e; };
        }

        fn runWork(self: *@This()) !void {
            var thread_arena = std.heap.ArenaAllocator.init(self.parent_alloc);
            defer thread_arena.deinit();
            const talloc = thread_arena.allocator();
            const key_buf = try talloc.alloc(i64, self.keys.len);

            while (self.morsel_src.next()) |m| {
                var chunk_arena = std.heap.ArenaAllocator.init(talloc);
                defer chunk_arena.deinit();
                const calloc = chunk_arena.allocator();
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, calloc);

                // Apply filter (non-compacting for parallel).
                if (self.filter_pred) |fp| {
                    var pass_mask = try calloc.alloc(bool, c.num_rows);
                    @memset(pass_mask, true);
                    // Apply int conds directly.
                    var ic_buf: [16]IntCmpCond = undefined;
                    var ic_n: usize = 0;
                    const ic_complete = extractAndIntConds(fp, &ic_buf, &ic_n, false);
                    if (ic_complete and ic_n > 0) {
                        const conds = ic_buf[0..ic_n];
                        for (0..c.num_rows) |r| {
                            for (conds) |cond| {
                                if (cond.col_idx >= c.columns.len) { pass_mask[r] = false; break; }
                                const col = c.columns[cond.col_idx];
                                if (col.isRowNull(r)) { pass_mask[r] = false; break; }
                                const v: i64 = switch (col.data) {
                                    .int64 => |a| a[r],
                                    .uint64 => |a| @bitCast(a[r]),
                                    .bool_u8 => |a| @as(i64, a[r]),
                                    .date_u16 => |a| @as(i64, a[r]),
                                    .datetime64_ms => |a| a[r],
                                    else => { pass_mask[r] = false; break; },
                                };
                                const pass = switch (cond.op) {
                                    .eq => v == cond.val, .neq => v != cond.val,
                                    .lt => v < cond.val, .lte => v <= cond.val,
                                    .gt => v > cond.val, .gte => v >= cond.val,
                                    .in2 => v == cond.val or v == cond.val2,
                                };
                                if (!pass) { pass_mask[r] = false; break; }
                            }
                        }
                    } else like_str_path: {
                        // Fast path: pure LIKE / NOT_LIKE col_ref lit_str.
                        switch (fp) {
                            .like, .not_like => |op| if (op.left == .col_ref and op.right == .lit_str) {
                                const col_idx2 = op.left.col_ref.index;
                                if (col_idx2 < c.columns.len and c.columns[col_idx2].data == .string) {
                                    const matcher = kernels.LikeMatcher.compile(op.right.lit_str);
                                    const negate = (fp == .not_like);
                                    const col2 = c.columns[col_idx2];
                                    for (0..c.num_rows) |r| {
                                        const s = if (col2.isRowNull(r)) "" else col2.data.string[r];
                                        pass_mask[r] = (matcher.match(s) != negate);
                                    }
                                    break :like_str_path;
                                }
                            },
                            // Fast path: col_ref != lit_str  (e.g. SearchPhrase <> '').
                            .neq => |op| if (op.left == .col_ref and op.right == .lit_str) {
                                const col_idx2 = op.left.col_ref.index;
                                const lit2 = op.right.lit_str;
                                if (col_idx2 < c.columns.len) {
                                    if (c.columns[col_idx2].data == .string) {
                                        const col2 = c.columns[col_idx2];
                                        for (0..c.num_rows) |r| {
                                            const s = if (col2.isRowNull(r)) "" else col2.data.string[r];
                                            pass_mask[r] = !std.mem.eql(u8, s, lit2);
                                        }
                                        break :like_str_path;
                                    } else if (c.columns[col_idx2].data == .bool_u8 and lit2.len == 0) {
                                        // Column decoded as bool_u8 (1=non-empty) via setStringNonEmptyBool.
                                        const col2 = c.columns[col_idx2];
                                        for (0..c.num_rows) |r| {
                                            pass_mask[r] = col2.data.bool_u8[r] != 0;
                                        }
                                        break :like_str_path;
                                    }
                                }
                            },
                            else => {},
                        }
                        // General evalExpr path.
                        const ref_mask = try calloc.alloc(bool, @min(256, c.columns.len));
                        @memset(ref_mask, false);
                        collectColRefs(fp, ref_mask);
                        var ref_buf = try calloc.alloc(usize, c.columns.len);
                        var ref_n: usize = 0;
                        for (ref_mask, 0..) |m2, i| { if (m2 and i < c.columns.len) { ref_buf[ref_n] = i; ref_n += 1; } }
                        const refs = ref_buf[0..ref_n];
                        const row = try calloc.alloc(?Value, c.columns.len);
                        @memset(row, null);
                        for (0..c.num_rows) |r| {
                            for (refs) |j| {
                                const col = c.columns[j];
                                row[j] = if (col.isRowNull(r)) null else col.data.get(r);
                            }
                            const v = try kernels.evalExpr(fp, row, null, calloc);
                            pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
                        }
                    }

                    // Process passing rows.
                    for (0..c.num_rows) |r| {
                        if (!pass_mask[r]) continue;
                        var key_valid = true;
                        for (self.keys, 0..) |k, ki| {
                            // Special case: fn_call date_trunc variants.
                            if (k.expr == .fn_call) {
                                const fc = k.expr.fn_call;
                                const col_idx2 = fc.args[0].col_ref.index;
                                if (col_idx2 >= c.columns.len) { key_valid = false; break; }
                                const col = c.columns[col_idx2];
                                if (col.isRowNull(r)) { key_valid = false; break; }
                                const secs: i64 = switch (col.data) {
                                    .int64  => |a| a[r],
                                    .uint64 => |a| @as(i64, @bitCast(a[r])),
                                    else => { key_valid = false; break; },
                                };
                                const divisor_ms: i64 = if (std.mem.eql(u8, fc.name, "toStartOfMinute")) 60_000
                                    else if (std.mem.eql(u8, fc.name, "toStartOfHour")) 3_600_000
                                    else 86_400_000;
                                const ms = secs * 1000;
                                key_buf[ki] = @divTrunc(ms, divisor_ms) * divisor_ms;
                                continue;
                            }
                            // Constant key (lit_i64): value does not depend on row.
                            if (k.expr == .lit_i64) {
                                key_buf[ki] = k.expr.lit_i64;
                                continue;
                            }
                            const col_idx2: usize = switch (k.expr) {
                                .col_ref => |cr| cr.index,
                                .add     => |op| op.left.col_ref.index,
                                .sub     => |op| op.left.col_ref.index,
                                else     => { key_valid = false; break; },
                            };
                            const addend2: i64 = switch (k.expr) {
                                .col_ref => 0,
                                .add     => |op| op.right.lit_i64,
                                .sub     => |op| -op.right.lit_i64,
                                else     => 0,
                            };
                            if (col_idx2 >= c.columns.len) { key_valid = false; break; }
                            const col = c.columns[col_idx2];
                            if (col.isRowNull(r)) { key_valid = false; break; }
                            const raw2: i64 = switch (col.data) {
                                .int64  => |a| a[r],
                                .uint64 => |a| @as(i64, @bitCast(a[r])),
                                .bool_u8 => |a| @as(i64, a[r]),
                                .date_u16 => |a| @as(i64, a[r]),
                                else => { key_valid = false; break; },
                            };
                            key_buf[ki] = raw2 +% addend2;
                        }
                        if (!key_valid) continue;
                        const slot_vals = try self.local_ht.getOrInsert(key_buf, self.compact_init_vals);
                        for (self.aggs, 0..) |item, ci| {
                            if (item.expr != .agg_call) continue;
                            const ac = item.expr.agg_call;
                            switch (self.compact_kinds[ci]) {
                                .count => slot_vals[ci] += 1,
                                .i64_sum => if (ac.arg) |arg| { if (arg == .col_ref) {
                                    const col = c.columns[arg.col_ref.index];
                                    if (!col.isRowNull(r)) switch (col.data) {
                                        .int64  => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                                        .uint64 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += @as(i64, @bitCast(v[r])); slot_vals[ci] = @bitCast(s); },
                                        else => {},
                                    };
                                }},
                                else => slot_vals[ci] += 1, // fallback: count
                            }
                        }
                    }
                } else {
                    // No filter.
                    // ── Fast path with ahead-prefetch for simple int col_ref keys ──────────
                    // Covers Q16 (1 key: UserID), Q33 (2 keys: WatchID+ClientIP), etc.
                    // For each row r, prefetch the HT cache line that row r+PDIST will need
                    // so that the L3/DRAM latency is hidden behind useful computation.
                    // PDIST=64: HT exceeds L3 on most queries → DRAM latency ~100ns,
                    // loop body ~1-2ns/row, so 64 rows gives ~64-128ns prefetch lead.
                     // PDIST: prefetch distance in rows.  DRAM latency ~100ns, loop body
                     // ~1-2ns/row, so 64 rows gives ~64-128ns prefetch lead.
                     const PDIST: usize = 64;
                     const fast_handled: bool = fast_nofilter: {
                         // All keys must be plain col_ref (no fn_call, lit_i64, arithmetic).
                         for (self.keys) |k| { if (k.expr != .col_ref) break :fast_nofilter false; }
                         if (self.keys.len == 1) {
                            const ci0 = self.keys[0].expr.col_ref.index;
                            if (ci0 >= c.columns.len) break :fast_nofilter false;
                            const col0 = c.columns[ci0];
                            for (0..c.num_rows) |r| {
                                // Prefetch HT entry for row r+PDIST.
                                if (r + PDIST < c.num_rows) {
                                    const fv: i64 = switch (col0.data) {
                                        .int64    => |a| a[r + PDIST],
                                        .uint64   => |a| @bitCast(a[r + PDIST]),
                                        .date_u16 => |a| @as(i64, a[r + PDIST]),
                                        .bool_u8  => |a| @as(i64, a[r + PDIST]),
                                        else => 0,
                                    };
                                    self.local_ht.prefetchForKey1(fv);
                                }
                                if (col0.isRowNull(r)) continue;
                                key_buf[0] = switch (col0.data) {
                                    .int64    => |a| a[r],
                                    .uint64   => |a| @bitCast(a[r]),
                                    .date_u16 => |a| @as(i64, a[r]),
                                    .bool_u8  => |a| @as(i64, a[r]),
                                    else => continue,
                                };
                                const slot_vals = try self.local_ht.getOrInsert(key_buf[0..1], self.compact_init_vals);
                                for (self.aggs, 0..) |item, ci| {
                                    if (item.expr != .agg_call) continue;
                                    const ac = item.expr.agg_call;
                                    switch (self.compact_kinds[ci]) {
                                        .count => slot_vals[ci] += 1,
                                        .i64_sum => if (ac.arg) |arg| { if (arg == .col_ref) {
                                            const acol = c.columns[arg.col_ref.index];
                                            if (!acol.isRowNull(r)) switch (acol.data) {
                                                .int64  => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                                                .uint64 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += @as(i64, @bitCast(v[r])); slot_vals[ci] = @bitCast(s); },
                                                .bool_u8 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += @as(i64, v[r]); slot_vals[ci] = @bitCast(s); },
                                                else => {},
                                            };
                                        }},
                                        .f64_sum => if (ac.arg) |arg| { if (arg == .col_ref) {
                                            const acol = c.columns[arg.col_ref.index];
                                            if (!acol.isRowNull(r)) switch (acol.data) {
                                                .int64  => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                                                .uint64 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                                                .bool_u8 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                                                .float64 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                                                else => {},
                                            };
                                        }},
                                        else => slot_vals[ci] += 1,
                                    }
                                }
                            }
                            break :fast_nofilter true; // all rows processed
                        } else if (self.keys.len == 2) {
                            const ci0 = self.keys[0].expr.col_ref.index;
                            const ci1 = self.keys[1].expr.col_ref.index;
                            if (ci0 >= c.columns.len or ci1 >= c.columns.len) break :fast_nofilter false;
                            const col0 = c.columns[ci0];
                            const col1 = c.columns[ci1];
                             for (0..c.num_rows) |r| {
                                 if (r + PDIST < c.num_rows) {
                                     const fv0: i64 = switch (col0.data) {
                                         .int64    => |a| a[r + PDIST],
                                         .uint64   => |a| @bitCast(a[r + PDIST]),
                                         .date_u16 => |a| @as(i64, a[r + PDIST]),
                                         .bool_u8  => |a| @as(i64, a[r + PDIST]),
                                         else => 0,
                                     };
                                     const fv1: i64 = switch (col1.data) {
                                         .int64    => |a| a[r + PDIST],
                                         .uint64   => |a| @bitCast(a[r + PDIST]),
                                         .date_u16 => |a| @as(i64, a[r + PDIST]),
                                         .bool_u8  => |a| @as(i64, a[r + PDIST]),
                                         else => 0,
                                     };
                                     self.local_ht.prefetchForKeys(fv0, fv1);
                                 }
                                if (col0.isRowNull(r) or col1.isRowNull(r)) continue;
                                key_buf[0] = switch (col0.data) {
                                    .int64    => |a| a[r],
                                    .uint64   => |a| @bitCast(a[r]),
                                    .date_u16 => |a| @as(i64, a[r]),
                                    .bool_u8  => |a| @as(i64, a[r]),
                                    else => continue,
                                };
                                key_buf[1] = switch (col1.data) {
                                    .int64    => |a| a[r],
                                    .uint64   => |a| @bitCast(a[r]),
                                    .date_u16 => |a| @as(i64, a[r]),
                                    .bool_u8  => |a| @as(i64, a[r]),
                                    else => continue,
                                };
                                const slot_vals = try self.local_ht.getOrInsert(key_buf[0..2], self.compact_init_vals);
                                for (self.aggs, 0..) |item, ci| {
                                    if (item.expr != .agg_call) continue;
                                    const ac = item.expr.agg_call;
                                    switch (self.compact_kinds[ci]) {
                                        .count => slot_vals[ci] += 1,
                                        .i64_sum => if (ac.arg) |arg| { if (arg == .col_ref) {
                                            const acol = c.columns[arg.col_ref.index];
                                            if (!acol.isRowNull(r)) switch (acol.data) {
                                                .int64  => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                                                .uint64 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += @as(i64, @bitCast(v[r])); slot_vals[ci] = @bitCast(s); },
                                                .bool_u8 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += @as(i64, v[r]); slot_vals[ci] = @bitCast(s); },
                                                else => {},
                                            };
                                        }},
                                        .f64_sum => if (ac.arg) |arg| { if (arg == .col_ref) {
                                            const acol = c.columns[arg.col_ref.index];
                                            if (!acol.isRowNull(r)) switch (acol.data) {
                                                .int64  => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                                                .uint64 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                                                .bool_u8 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                                                .float64 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                                                else => {},
                                            };
                                        }},
                                        else => slot_vals[ci] += 1,
                                    }
                                }
                            }
                            break :fast_nofilter true; // all rows processed
                        }
                        break :fast_nofilter false; // keys.len > 2 or unhandled
                    };
                    // General no-filter loop (handles fn_call keys, lit_i64, arithmetic, 3+ keys).
                    if (!fast_handled) {
                    for (0..c.num_rows) |r| {
                        var key_valid = true;
                        for (self.keys, 0..) |k, ki| {
                            // Special case: fn_call date_trunc variants.
                            if (k.expr == .fn_call) {
                                const fc = k.expr.fn_call;
                                const col_idx3 = fc.args[0].col_ref.index;
                                if (col_idx3 >= c.columns.len) { key_valid = false; break; }
                                const col = c.columns[col_idx3];
                                if (col.isRowNull(r)) { key_valid = false; break; }
                                const secs: i64 = switch (col.data) {
                                    .int64  => |a| a[r],
                                    .uint64 => |a| @as(i64, @bitCast(a[r])),
                                    else => { key_valid = false; break; },
                                };
                                const divisor_ms: i64 = if (std.mem.eql(u8, fc.name, "toStartOfMinute")) 60_000
                                    else if (std.mem.eql(u8, fc.name, "toStartOfHour")) 3_600_000
                                    else 86_400_000;
                                const ms = secs * 1000;
                                key_buf[ki] = @divTrunc(ms, divisor_ms) * divisor_ms;
                                continue;
                            }
                            // Constant key (lit_i64): value does not depend on row.
                            if (k.expr == .lit_i64) {
                                key_buf[ki] = k.expr.lit_i64;
                                continue;
                            }
                            const col_idx3: usize = switch (k.expr) {
                                .col_ref => |cr| cr.index,
                                .add     => |op| op.left.col_ref.index,
                                .sub     => |op| op.left.col_ref.index,
                                else     => { key_valid = false; break; },
                            };
                            const addend3: i64 = switch (k.expr) {
                                .col_ref => 0,
                                .add     => |op| op.right.lit_i64,
                                .sub     => |op| -op.right.lit_i64,
                                else     => 0,
                            };
                            if (col_idx3 >= c.columns.len) { key_valid = false; break; }
                            const col = c.columns[col_idx3];
                            if (col.isRowNull(r)) { key_valid = false; break; }
                            const raw3: i64 = switch (col.data) {
                                .int64  => |a| a[r],
                                .uint64 => |a| @as(i64, @bitCast(a[r])),
                                .bool_u8 => |a| @as(i64, a[r]),
                                .date_u16 => |a| @as(i64, a[r]),
                                else => { key_valid = false; break; },
                            };
                            key_buf[ki] = raw3 +% addend3;
                        }
                        if (!key_valid) continue;
                        const slot_vals = try self.local_ht.getOrInsert(key_buf, self.compact_init_vals);
                        for (self.aggs, 0..) |item, ci| {
                            if (item.expr != .agg_call) continue;
                            const ac = item.expr.agg_call;
                            switch (self.compact_kinds[ci]) {
                                .count => slot_vals[ci] += 1,
                                .i64_sum => if (ac.arg) |arg| { if (arg == .col_ref) {
                                    const col = c.columns[arg.col_ref.index];
                                    if (!col.isRowNull(r)) switch (col.data) {
                                        .int64  => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                                        .uint64 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += @as(i64, @bitCast(v[r])); slot_vals[ci] = @bitCast(s); },
                                        .bool_u8 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += @as(i64, v[r]); slot_vals[ci] = @bitCast(s); },
                                        else => {},
                                    };
                                }},
                                .f64_sum => if (ac.arg) |arg| { if (arg == .col_ref) {
                                    const col = c.columns[arg.col_ref.index];
                                    if (!col.isRowNull(r)) switch (col.data) {
                                        .int64  => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                                        .uint64 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                                        .bool_u8 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                                         .float64 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                                         else => {},
                                     };
                                 }},
                                 else => slot_vals[ci] += 1, // fallback: count
                             }
                         }
                     }
                    } // if (!fast_handled)
                }
            }
        }
    };

    // Allocate per-thread contexts.
    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);

    // For narrow int key columns (int8/int16) with a filter, cardinality is likely very low.
    // Start with a tiny HT (grows naturally to fit actual unique count) so it stays L1-resident.
    const all_narrow_keys: bool = blk: {
        if (filter_pred == null) break :blk false;
        const sm_narrow = ctx.source.schema();
        for (keys) |k| {
            if (k.expr != .col_ref) break :blk false;
            const ci = k.expr.col_ref.index;
            if (ci >= sm_narrow.len or !sm_narrow[ci].is_narrow_int) break :blk false;
        }
        break :blk true;
    };

    const pctxs = try alloc.alloc(ParHashCtx, n_threads);
    for (pctxs) |*pc| {
        pc.* = .{
            .source           = ctx.source,
            .filter_pred      = filter_pred,
            .keys             = keys,
            .aggs             = aggs,
            .compact_kinds    = compact_kinds,
            .compact_init_vals = compact_init_vals,
            .morsel_src       = &morsel_src,
            .parent_alloc     = alloc,
            .local_ht         = try ht.CompactIntKeyHashTable.initWithCapacity(alloc, keys.len, aggs.len,
                // For narrow int keys (int8/int16) with a filter, actual cardinality is
                // typically very low (e.g. Q42: ~200 unique WindowClientWidth×Height pairs).
                // Start tiny (est_rows=0 → INITIAL_CAP=64); grows naturally to fit the actual
                // unique count and stays L1-resident. Avoids allocating a 524KB L3-resident HT.
                // For wide keys with filter: use conservative pre-size to avoid memset overhead.
                // Without filter: pre-size to avoid scan-phase doubling (Q33-style full scans).
                if (all_narrow_keys) 0
                else if (filter_pred != null) @max(256, total_rows / n_threads / 32)
                else @max(256, total_rows / n_threads + 1)),
        };
    }

    try parallel.parallelFor(alloc, ParHashCtx, ParHashCtx.work, pctxs, &morsel_src);

    for (pctxs) |*pc| {
        if (pc.err) |e| return e;
    }

    // Compute total unique entries across all local HTs.
    var total_count: usize = 0;
    for (pctxs) |*pc| total_count += pc.local_ht.count;

    // Parallel partitioned merge: split the key space into `part_T` partitions.
    // Each thread handles one partition → its output HT is ~total/part_T entries.
    // part_T = n_threads: each thread handles exactly 1 partition, maximizing
    // parallelism for the merge phase (4 threads × 1 partition each).
    // Each partition master is ~total/4 entries; for Q33 (1M groups) this is
    // ~250K entries = 18MB per partition.  All 4 threads scan all local HTs
    // simultaneously (reading the same cache lines = shared DRAM bandwidth).
    const part_T: usize = blk: {
        var p: usize = 1;
        while (p * 2 <= n_threads) p <<= 1;
        break :blk p;
    };
    const part_mask: u64 = @as(u64, @intCast(part_T)) - 1;

    const part_masters = try alloc.alloc(ht.CompactIntKeyHashTable, part_T);
    {
        const cap_per_part: u64 = @max(64, @as(u64, @intCast(total_count)) / @as(u64, @intCast(part_T)) * 100 / 65 + 16);
        for (0..part_T) |t| {
            part_masters[t] = try ht.CompactIntKeyHashTable.initWithCapacity(
                alloc, keys.len, aggs.len, cap_per_part);
        }
    }

    const PMCtx = struct {
        pctxs:             []ParHashCtx,
        part_masters:      []ht.CompactIntKeyHashTable,
        compact_kinds:     []const ht.CompactAggKind,
        compact_init_vals: []const u64,
        part_mask:         u64,
        err:               ?anyerror = null,

        fn work(self: *@This(), src: *parallel.MorselSource) void {
            while (src.next()) |m| {
                for (m.start..m.end) |t| {
                    const part_id = @as(u64, @intCast(t));
                    for (self.pctxs) |*pc| {
                        pc.local_ht.mergeIntoPartitioned(
                            &self.part_masters[t],
                            self.compact_kinds, self.compact_init_vals,
                            part_id, self.part_mask,
                        ) catch |e| { self.err = e; return; };
                    }
                }
            }
        }
    };

    const pm_ctxs = try alloc.alloc(PMCtx, n_threads);
    for (pm_ctxs) |*pm| pm.* = .{
        .pctxs             = pctxs,
        .part_masters      = part_masters,
        .compact_kinds     = compact_kinds,
        .compact_init_vals = compact_init_vals,
        .part_mask         = part_mask,
    };
    var pm_src = parallel.MorselSource.init(part_T, 1);
    try parallel.parallelFor(alloc, PMCtx, PMCtx.work, pm_ctxs, &pm_src);
    for (pm_ctxs) |*pm| { if (pm.err) |e| return e; }

    const use_part_merge = true;
    const part_hts = part_masters;

    // Emit result — same logic as in executeHashAggChunked compact emit path.
    const schema_metas = ctx.source.schema();
    _ = schema_metas;

    // Precompute key output types so makeRow emits the correct Value union variant.
    const key_out_types_buf = try alloc.alloc(ColumnType, keys.len);
    for (keys, 0..) |k, i| key_out_types_buf[i] = k.out_type;
    const out_metas = try alloc.alloc(result.ColMeta, keys.len + aggs.len);
    for (keys, 0..) |k, i| out_metas[i] = .{ .name = k.alias, .col_type = k.out_type };
    for (aggs, 0..) |a, i| out_metas[keys.len + i] = .{ .name = a.alias, .col_type = a.out_type };
    _ = schema_metas;

    var rl = RowList.init(out_metas);
    const MCtx = struct {
        keys_n: usize,
        aggs_n: usize,
        compact_kinds: []const ht.CompactAggKind,
        rl: *RowList,
        alloc: std.mem.Allocator,
        err: ?anyerror = null,
        // Optional top-K heap (non-null → emit into heap instead of rl).
        heap:     ?[][]?Value = null,
        heap_len: usize = 0,
        heap_k:   usize = 0,
        sort_keys: []const plan.SortKey = &.{},
        key_out_types: []const ColumnType = &.{},
        // Cached raw sort-key value of heap[0] (the heap minimum for DESC, maximum for ASC).
        // Updated whenever the heap changes. Avoids double pointer dereference in hot path.
        heap_min_cached: i64 = std.math.minInt(i64),

        fn rowLessThan(sk: []const plan.SortKey, a: []?Value, b: []?Value) bool {
            for (sk) |key| {
                const av: ?Value = if (key.col_idx < a.len) a[key.col_idx] else null;
                const bv: ?Value = if (key.col_idx < b.len) b[key.col_idx] else null;
                const ord: std.math.Order = if (av != null and bv != null)
                    Value.order(av.?, bv.?)
                else if (av == null and bv == null) .eq
                else if (av == null) .lt else .gt;
                if (ord == .eq) continue;
                return if (key.desc) ord == .gt else ord == .lt;
            }
            return false;
        }

        fn heapSiftDown(self: *@This(), i: usize) void {
            var cur = i;
            while (true) {
                var worst = cur;
                const l = cur * 2 + 1;
                const r = cur * 2 + 2;
                if (l < self.heap_len and @This().rowLessThan(self.sort_keys, self.heap.?[worst], self.heap.?[l])) worst = l;
                if (r < self.heap_len and @This().rowLessThan(self.sort_keys, self.heap.?[worst], self.heap.?[r])) worst = r;
                if (worst == cur) break;
                const tmp = self.heap.?[cur]; self.heap.?[cur] = self.heap.?[worst]; self.heap.?[worst] = tmp;
                cur = worst;
            }
        }

        fn heapSiftUp(self: *@This(), i: usize) void {
            var cur = i;
            while (cur > 0) {
                const parent = (cur - 1) / 2;
                if (@This().rowLessThan(self.sort_keys, self.heap.?[parent], self.heap.?[cur])) {
                    const tmp = self.heap.?[cur]; self.heap.?[cur] = self.heap.?[parent]; self.heap.?[parent] = tmp;
                    cur = parent;
                } else break;
            }
        }

        // Update `heap_min_cached` from heap[0] after any heap structural change.
        fn updateHeapMinCache(self: *@This()) void {
            if (self.heap_len == 0 or self.sort_keys.len == 0) return;
            const heap = self.heap.?;
            const ci = self.sort_keys[0].col_idx;
            if (ci >= heap[0].len) return;
            self.heap_min_cached = if (heap[0][ci]) |v| switch (v) {
                .int64   => |x| x,
                .uint64  => |x| @bitCast(x),
                .float64 => |x| @as(i64, @bitCast(x)),
                else     => std.math.minInt(i64),
            } else std.math.minInt(i64);
        }

        fn makeRow(self: *@This(), key_vals: []const i64, acc_vals: []const u64) ?[]?Value {
            const row = self.alloc.alloc(?Value, self.keys_n + self.aggs_n) catch return null;
            for (key_vals, 0..) |kv, i| {
                const out_type: ColumnType = if (i < self.key_out_types.len) self.key_out_types[i] else .int64;
                row[i] = switch (out_type) {
                    .datetime64_ms => .{ .datetime64_ms = kv },
                    .date_u16 => .{ .date_u16 = @intCast(kv) },
                    else => .{ .int64 = kv },
                };
            }
            for (self.compact_kinds, 0..) |kind, i| {
                row[self.keys_n + i] = switch (kind) {
                    .count => .{ .int64 = @intCast(acc_vals[i]) },
                    .i64_sum => .{ .int64 = @bitCast(acc_vals[i]) },
                    .u64_sum => .{ .uint64 = acc_vals[i] },
                    .f64_sum => .{ .float64 = @bitCast(acc_vals[i]) },
                    .i64_min, .i64_max => .{ .int64 = @bitCast(acc_vals[i]) },
                    .u64_min, .u64_max => .{ .int64 = @bitCast(acc_vals[i]) },
                    .f64_min, .f64_max => .{ .float64 = @bitCast(acc_vals[i]) },
                    .str_min, .str_max => .{ .int64 = 0 },
                };
            }
            return row;
        }

        fn cb(self: *@This(), key_vals: []const i64, acc_vals: []const u64) void {
            if (self.heap) |heap| {
                // Top-K heap path.
                // Ultra-fast pre-check using cached heap minimum: avoids 2 pointer
                // dereferences for the common case (1M entries, only 10 qualify).
                if (self.heap_len >= self.heap_k and self.sort_keys.len > 0) {
                    const sk = self.sort_keys[0];
                    const ci = sk.col_idx;
                    const new_raw: i64 = blk: {
                        if (ci < self.keys_n) break :blk key_vals[ci];
                        const ai = ci - self.keys_n;
                        if (ai < self.compact_kinds.len) {
                            break :blk switch (self.compact_kinds[ai]) {
                                .count        => @intCast(acc_vals[ai]),
                                .i64_sum,
                                .i64_min,
                                .i64_max,
                                .u64_min,
                                .u64_max      => @bitCast(acc_vals[ai]),
                                else          => std.math.maxInt(i64),
                            };
                        }
                        break :blk std.math.maxInt(i64);
                    };
                    // Use cached heap_min (L1-resident scalar, no pointer chase).
                    const qualifies = if (sk.desc)
                        new_raw > self.heap_min_cached
                    else
                        new_raw < self.heap_min_cached;
                    if (!qualifies) return;
                }
                const row = self.makeRow(key_vals, acc_vals) orelse { self.err = error.OutOfMemory; return; };
                if (self.heap_len < self.heap_k) {
                    heap[self.heap_len] = row;
                    self.heap_len += 1;
                    self.heapSiftUp(self.heap_len - 1);
                    self.updateHeapMinCache();
                } else if (@This().rowLessThan(self.sort_keys, heap[0], row)) {
                    heap[0] = row;
                    self.heapSiftDown(0);
                    self.updateHeapMinCache();
                }
            } else {
                const row = self.makeRow(key_vals, acc_vals) orelse { self.err = error.OutOfMemory; return; };
                self.rl.append(self.alloc, row) catch |e| { self.err = e; };
            }
        }
    };
    const use_heap = top_k > 0 and sort_keys.len > 0;
    const heap_buf: ?[][]?Value = if (use_heap) try alloc.alloc([]?Value, top_k) else null;
    var emit_ctx = MCtx{
        .keys_n = keys.len, .aggs_n = aggs.len,
        .compact_kinds = compact_kinds, .rl = &rl, .alloc = alloc,
        .heap = heap_buf, .heap_len = 0, .heap_k = top_k, .sort_keys = sort_keys,
        .key_out_types = key_out_types_buf,
    };
    if (use_part_merge) {
        // Emit from all partition HTs (each small → L3-friendly).
        for (part_hts) |*ph| {
            ph.iterate(&emit_ctx, MCtx.cb);
            if (emit_ctx.err) |e| return e;
        }
    } else {
        pctxs[0].local_ht.iterate(&emit_ctx, MCtx.cb);
        if (emit_ctx.err) |e| return e;
    }

    if (use_heap) {
        // Sort the heap buffer and emit top-k rows in order.
        const heap_rows = emit_ctx.heap.?[0..emit_ctx.heap_len];
        // Sort descending (reverse of heap order = best first).
        const SortCtx2 = struct {
            sort_keys: []const plan.SortKey,
            fn lessThan(self2: @This(), a: []?Value, b: []?Value) bool {
                for (self2.sort_keys) |key| {
                    const av = if (key.col_idx < a.len) a[key.col_idx] else null;
                    const bv = if (key.col_idx < b.len) b[key.col_idx] else null;
                    const ord: std.math.Order = if (av != null and bv != null) Value.order(av.?, bv.?)
                        else if (av == null and bv == null) .eq
                        else if (av == null) .lt else .gt;
                    if (ord == .eq) continue;
                    return if (key.desc) ord == .gt else ord == .lt;
                }
                return false;
            }
        };
        std.sort.pdq([]?Value, heap_rows, SortCtx2{ .sort_keys = sort_keys }, SortCtx2.lessThan);
        var result_rl = RowList.init(out_metas);
        for (heap_rows) |row| try result_rl.append(alloc, row);
        return result_rl;
    }

    return rl;
}


/// Called after parallel workers finish to combine their per-thread results.
fn mergeCompactIntoMaster(
    master: *ht.CompactIntKeyHashTable,
    local:  *const ht.CompactIntKeyHashTable,
    kinds:  []const ht.CompactAggKind,
    init_vals: []const u64,
) !void {
    // Use mergeInto which reuses precomputed hashes from tags — avoids rehashing.
    return local.mergeInto(master, kinds, init_vals);
}

// ── Two-phase scatter → aggregate ────────────────────────────────────────────
//
// Avoids per-thread HT exceeding L3 cache for high-cardinality GROUP BY.
//
// Phase 1 (parallel scatter):
//   Each thread scans its morsels and scatters (hash, k0[, k1], agg_partial...)
//   into N_PARTS=64 per-thread partition ArrayLists.  No HT touched — pure
//   sequential writes to hot (cache-resident) partition buffers.
//
// Phase 2 (parallel aggregate):
//   Thread t owns partitions [t*16 .. (t+1)*16).  For each partition p, it
//   collects all thread scatter bufs for p and aggregates with a small HT
//   (~1M/64 = 15K expected entries → 1MB → fits in L2 cache).
//
// Conditions: 1-2 plain col_ref integer keys, count/i64_sum/f64_sum aggs.
// str_filter: optional single col_ref eq/neq lit_str pre-filter applied inline in scatter phase.
fn executeTwoPhaseHashAgg(
    keys:              []const plan.ProjectItem,
    aggs:              []const plan.ProjectItem,
    compact_kinds:     []const ht.CompactAggKind,
    compact_init_vals: []const u64,
    total_rows:        u64,
    n_threads:         usize,
    alloc:             std.mem.Allocator,
    ctx:               *QueryContext,
    sort_keys:         []const plan.SortKey,
    top_k:             usize,
    str_filter:        ?SimpleStrFilter,
    int_filter:        ?[]const IntCmpCond,
) !?RowList {
    const N_PARTS: usize = 64;
    // row_stride = 1 (stored hash) + n_keys + n_aggs
    const n_keys = keys.len;
    const n_aggs = aggs.len;
    const row_stride = 1 + n_keys + n_aggs;

    // Pre-extract key column indices (all must be col_ref, checked by caller).
    const key_ci = [2]usize{
        keys[0].expr.col_ref.index,
        if (n_keys >= 2) keys[1].expr.col_ref.index else 0,
    };

    // Pre-extract agg info: (col_idx or ~0 for no-arg, kind).
    const AggInfo = struct { col_idx: usize, kind: ht.CompactAggKind };
    const agg_infos = try alloc.alloc(AggInfo, n_aggs);
    for (aggs, compact_kinds, agg_infos) |ag, kind, *info| {
        const ac = ag.expr.agg_call;
        info.* = .{
            .col_idx = if (ac.arg != null and ac.arg.? == .col_ref) ac.arg.?.col_ref.index else ~@as(usize, 0),
            .kind    = kind,
        };
    }

    // ── Phase 1: parallel scatter ─────────────────────────────────────────────

    const ScatterCtx = struct {
        // Flat u64 scatter buffers per partition (stride=row_stride per record).
        // Each entry: [hash, k0, k1?, agg0, agg1, ...]
        bufs:         [N_PARTS]std.ArrayListUnmanaged(u64),
        // Per-ctx arena backed by page_allocator (thread-safe: no sharing with other ctxs).
        buf_arena:    std.heap.ArenaAllocator,
        source:       SourceIface,
        morsel_src:   *parallel.MorselSource,
        parent_alloc: std.mem.Allocator,
        n_keys:       usize,
        n_aggs:       usize,
        row_stride:   usize,
        key_ci:       [2]usize,
        agg_infos:    []const AggInfo,
        compact_kinds: []const ht.CompactAggKind,
        str_filter:   ?SimpleStrFilter,
        int_filter:   ?[]const IntCmpCond,
        err:          ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| { self.err = e; };
        }

        fn runWork(self: *@This()) !void {
            const buf_alloc = self.buf_arena.allocator();

            var thread_arena = std.heap.ArenaAllocator.init(self.parent_alloc);
            defer thread_arena.deinit();
            const talloc = thread_arena.allocator();

            while (self.morsel_src.next()) |m| {
                var chunk_arena = std.heap.ArenaAllocator.init(talloc);
                defer chunk_arena.deinit();
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, chunk_arena.allocator());

                const ci0 = self.key_ci[0];
                const ci1 = self.key_ci[1];
                if (ci0 >= c.columns.len) continue;
                if (self.n_keys >= 2 and ci1 >= c.columns.len) continue;
                const col0 = c.columns[ci0];
                const col1 = if (self.n_keys >= 2) c.columns[ci1] else c.columns[ci0]; // unused if n_keys==1

                // Resolve str_filter column once per chunk (null if col not present or wrong type).
                // Accept both .string (normal) and .bool_u8 (set by setStringNonEmptyBool).
                const sf_col: ?chunk.Column = if (self.str_filter) |sf|
                    if (sf.col_idx < c.columns.len and
                       (c.columns[sf.col_idx].data == .string or c.columns[sf.col_idx].data == .bool_u8))
                        c.columns[sf.col_idx]
                    else
                        null
                else null;

                var rec: [18]u64 = undefined; // max row_stride = 18

                for (0..c.num_rows) |r| {
                    // Inline string pre-filter (e.g. SearchPhrase <> '').
                    if (self.str_filter) |sf| {
                        if (sf_col) |sfc| {
                            if (!sf.passes(sfc, r)) continue;
                        }
                    }
                    // Inline int pre-filter (e.g. Q41: CounterID=62 AND TraficSourceID IN (-1,6) AND ...).
                    if (self.int_filter) |ics| {
                        var pass = true;
                        for (ics) |cond| {
                            if (cond.col_idx >= c.columns.len) { pass = false; break; }
                            const col = c.columns[cond.col_idx];
                            if (col.isRowNull(r)) { pass = false; break; }
                            const v: i64 = switch (col.data) {
                                .int64 => |a| a[r], .uint64 => |a| @bitCast(a[r]),
                                .bool_u8 => |a| @as(i64, a[r]), .date_u16 => |a| @as(i64, a[r]),
                                .datetime64_ms => |a| a[r],
                                else => { pass = false; break; },
                            };
                            const ok = switch (cond.op) {
                                .eq  => v == cond.val, .neq => v != cond.val,
                                .lt  => v <  cond.val, .lte => v <= cond.val,
                                .gt  => v >  cond.val, .gte => v >= cond.val,
                                .in2 => v == cond.val or v == cond.val2,
                            };
                            if (!ok) { pass = false; break; }
                        }
                        if (!pass) continue;
                    }
                    if (col0.isRowNull(r)) continue;
                    const k0: i64 = switch (col0.data) {
                        .int64    => |a| a[r],
                        .uint64   => |a| @bitCast(a[r]),
                        .date_u16 => |a| @as(i64, a[r]),
                        .bool_u8  => |a| @as(i64, a[r]),
                        else => continue,
                    };
                    const k1: i64 = if (self.n_keys >= 2) blk: {
                        if (col1.isRowNull(r)) continue;
                        break :blk switch (col1.data) {
                            .int64    => |a| a[r],
                            .uint64   => |a| @bitCast(a[r]),
                            .date_u16 => |a| @as(i64, a[r]),
                            .bool_u8  => |a| @as(i64, a[r]),
                            else => continue,
                        };
                    } else 0;

                    // Compute hash (same formula as CompactIntKeyHashTable.hashI64s).
                    const h: u64 = if (self.n_keys == 1) blk: {
                        var hh: u64 = @bitCast(k0);
                        hh ^= hh >> 33; hh *%= 0xff51afd7ed558ccd;
                        hh ^= hh >> 33; hh *%= 0xc4ceb9fe1a85ec53;
                        hh ^= hh >> 33;
                        break :blk hh | (1 << 63);
                    } else blk: {
                        const hk0: u64 = @bitCast(k0);
                        const hk1: u64 = @bitCast(k1);
                        var hh = hk0 *% 0x9e3779b97f4a7c15 ^ hk1 *% 0x6c62272e07bb0142;
                        hh ^= hh >> 30; hh *%= 0xbf58476d1ce4e5b9;
                        hh ^= hh >> 27; hh *%= 0x94d049bb133111eb;
                        hh ^= hh >> 31;
                        break :blk hh | (1 << 63);
                    };

                    const part_id = h & (N_PARTS - 1);
                    rec[0] = h;
                    rec[1] = @bitCast(k0);
                    if (self.n_keys >= 2) rec[2] = @bitCast(k1);
                    // Agg partial contributions.
                    for (self.agg_infos, 0..) |info, ai| {
                        const base_off = 1 + self.n_keys + ai;
                        switch (info.kind) {
                            .count => { rec[base_off] = 1; },
                            .i64_sum => {
                                if (info.col_idx == ~@as(usize, 0) or info.col_idx >= c.columns.len) {
                                    rec[base_off] = 0;
                                } else {
                                    const ac = c.columns[info.col_idx];
                                    if (ac.isRowNull(r)) { rec[base_off] = 0; continue; }
                                    rec[base_off] = switch (ac.data) {
                                        .int64  => |v| @bitCast(v[r]),
                                        .uint64 => |v| v[r],
                                        .bool_u8 => |v| @as(u64, v[r]),
                                        else => 0,
                                    };
                                }
                            },
                            .f64_sum => {
                                if (info.col_idx == ~@as(usize, 0) or info.col_idx >= c.columns.len) {
                                    rec[base_off] = 0;
                                } else {
                                    const ac = c.columns[info.col_idx];
                                    if (ac.isRowNull(r)) { rec[base_off] = 0; continue; }
                                    const fv: f64 = switch (ac.data) {
                                        .int64   => |v| @floatFromInt(v[r]),
                                        .uint64  => |v| @floatFromInt(v[r]),
                                        .bool_u8 => |v| @floatFromInt(v[r]),
                                        .float64 => |v| v[r],
                                        else => 0.0,
                                    };
                                    rec[base_off] = @bitCast(fv);
                                }
                            },
                            else => { rec[base_off] = 0; },
                        }
                    }

                    try self.bufs[part_id].appendSlice(buf_alloc, rec[0..self.row_stride]);
                }
            }
        }
    };

    var morsel_src1 = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    const scatter_ctxs = try alloc.alloc(ScatterCtx, n_threads);
    for (scatter_ctxs) |*sc| {
        sc.* = .{
            .bufs         = [_]std.ArrayListUnmanaged(u64){.{ .items = &.{}, .capacity = 0 }} ** N_PARTS,
            .buf_arena    = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .source       = ctx.source,
            .morsel_src   = &morsel_src1,
            .parent_alloc = alloc,
            .n_keys       = n_keys,
            .n_aggs       = n_aggs,
            .row_stride   = row_stride,
            .key_ci       = key_ci,
            .agg_infos    = agg_infos,
            .compact_kinds = compact_kinds,
            .str_filter   = str_filter,
            .int_filter   = int_filter,
        };
    }
    try parallel.parallelFor(alloc, ScatterCtx, ScatterCtx.work, scatter_ctxs, &morsel_src1);
    for (scatter_ctxs) |*sc| { if (sc.err) |e| return e; }
    // Note: scatter_ctxs[*].buf_arena must NOT be freed yet — Phase 2 reads from the bufs.

    // ── Phase 2: parallel aggregate per partition ─────────────────────────────

    // Allocate output partition HTs (filled by Phase 2 workers).
    const part_hts = try alloc.alloc(ht.CompactIntKeyHashTable, N_PARTS);
    // Initialize all with minimal capacity (Phase 2 will resize as needed).
    for (part_hts) |*ph| {
        ph.* = try ht.CompactIntKeyHashTable.initWithCapacity(alloc, n_keys, n_aggs, 0);
    }

    const AggCtx = struct {
        scatter_ctxs:      []ScatterCtx,
        part_hts:          []ht.CompactIntKeyHashTable,
        compact_kinds:     []const ht.CompactAggKind,
        compact_init_vals: []const u64,
        row_stride:        usize,
        n_keys:            usize,
        n_aggs:            usize,
        morsel_src:        *parallel.MorselSource,
        alloc:             std.mem.Allocator,
        err:               ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| { self.err = e; };
        }

        fn runWork(self: *@This()) !void {
            while (self.morsel_src.next()) |m| {
                for (m.start..m.end) |p| {
                    // Count total rows for this partition across all scatter threads.
                    var total_p: usize = 0;
                    for (self.scatter_ctxs) |*sc| total_p += sc.bufs[p].items.len / self.row_stride;
                    if (total_p == 0) continue;

                    // Size small HT to fit this partition (should fit in L2 cache).
                    const ht_cap = @max(64, total_p * 100 / 65 + 16);
                    try self.part_hts[p].growTo(ht_cap);

                    // Aggregate all scatter records for partition p.
                    var key_buf: [4]i64 = undefined;
                    for (self.scatter_ctxs) |*sc| {
                        const buf   = sc.bufs[p].items;
                        const rs    = self.row_stride;
                        var   i: usize = 0;
                        while (i < buf.len) : (i += rs) {
                            const h = buf[i];
                            for (0..self.n_keys) |ki| key_buf[ki] = @bitCast(buf[i + 1 + ki]);
                            const partial = buf[i + 1 + self.n_keys .. i + rs];
                            const slot_vals = try self.part_hts[p].getOrInsertH(
                                key_buf[0..self.n_keys], h, self.compact_init_vals);
                            for (self.compact_kinds, 0..) |kind, ci| {
                                const src = partial[ci];
                                switch (kind) {
                                    .count, .u64_sum => slot_vals[ci] += src,
                                    .i64_sum => {
                                        const a: i64 = @bitCast(slot_vals[ci]);
                                        const b: i64 = @bitCast(src);
                                        slot_vals[ci] = @bitCast(a + b);
                                    },
                                    .f64_sum => {
                                        const a: f64 = @bitCast(slot_vals[ci]);
                                        const b: f64 = @bitCast(src);
                                        slot_vals[ci] = @bitCast(a + b);
                                    },
                                    else => slot_vals[ci] += src,
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    var morsel_src2 = parallel.MorselSource.init(N_PARTS, 1);
    const agg_ctxs = try alloc.alloc(AggCtx, n_threads);
    for (agg_ctxs) |*ac| {
        ac.* = .{
            .scatter_ctxs      = scatter_ctxs,
            .part_hts          = part_hts,
            .compact_kinds     = compact_kinds,
            .compact_init_vals = compact_init_vals,
            .row_stride        = row_stride,
            .n_keys            = n_keys,
            .n_aggs            = n_aggs,
            .morsel_src        = &morsel_src2,
            .alloc             = alloc,
        };
    }
    try parallel.parallelFor(alloc, AggCtx, AggCtx.work, agg_ctxs, &morsel_src2);
    for (agg_ctxs) |*ac| { if (ac.err) |e| return e; }
    // Phase 2 done — release scatter buf memory (page_allocator-backed).
    for (scatter_ctxs) |*sc| sc.buf_arena.deinit();

    // ── Emit from partition HTs ────────────────────────────────────────────────

    const key_out_types_buf = try alloc.alloc(ColumnType, n_keys);
    for (keys, 0..) |k, i| key_out_types_buf[i] = k.out_type;
    const out_metas = try alloc.alloc(result.ColMeta, n_keys + n_aggs);
    for (keys, 0..) |k, i| out_metas[i] = .{ .name = k.alias, .col_type = k.out_type };
    for (aggs, 0..) |a, i| out_metas[n_keys + i] = .{ .name = a.alias, .col_type = a.out_type };

    var rl = RowList.init(out_metas);
    const EmitCtx = struct {
        keys_n:         usize,
        aggs_n:         usize,
        compact_kinds:  []const ht.CompactAggKind,
        rl:             *RowList,
        alloc:          std.mem.Allocator,
        key_out_types:  []const ColumnType,
        heap:           ?[][]?Value = null,
        heap_len:       usize = 0,
        heap_k:         usize = 0,
        sort_keys:      []const plan.SortKey = &.{},
        heap_min_cached: i64 = std.math.minInt(i64),
        err:            ?anyerror = null,

        fn rowLessThan(sk: []const plan.SortKey, a: []?Value, b: []?Value) bool {
            for (sk) |key| {
                const av: ?Value = if (key.col_idx < a.len) a[key.col_idx] else null;
                const bv: ?Value = if (key.col_idx < b.len) b[key.col_idx] else null;
                const ord: std.math.Order = if (av != null and bv != null) Value.order(av.?, bv.?)
                    else if (av == null and bv == null) .eq
                    else if (av == null) .lt else .gt;
                if (ord == .eq) continue;
                return if (key.desc) ord == .gt else ord == .lt;
            }
            return false;
        }
        fn heapSiftDown(self: *@This(), i: usize) void {
            var cur = i;
            while (true) {
                var worst = cur;
                const l = cur * 2 + 1;
                const r = cur * 2 + 2;
                if (l < self.heap_len and @This().rowLessThan(self.sort_keys, self.heap.?[worst], self.heap.?[l])) worst = l;
                if (r < self.heap_len and @This().rowLessThan(self.sort_keys, self.heap.?[worst], self.heap.?[r])) worst = r;
                if (worst == cur) break;
                const tmp = self.heap.?[cur]; self.heap.?[cur] = self.heap.?[worst]; self.heap.?[worst] = tmp;
                cur = worst;
            }
        }
        fn heapSiftUp(self: *@This(), i: usize) void {
            var cur = i;
            while (cur > 0) {
                const parent = (cur - 1) / 2;
                if (@This().rowLessThan(self.sort_keys, self.heap.?[parent], self.heap.?[cur])) {
                    const tmp = self.heap.?[cur]; self.heap.?[cur] = self.heap.?[parent]; self.heap.?[parent] = tmp;
                    cur = parent;
                } else break;
            }
        }
        fn updateHeapMinCache(self: *@This()) void {
            if (self.heap_len == 0 or self.sort_keys.len == 0) return;
            const heap = self.heap.?;
            const ci = self.sort_keys[0].col_idx;
            if (ci >= heap[0].len) return;
            self.heap_min_cached = if (heap[0][ci]) |v| switch (v) {
                .int64   => |x| x,
                .uint64  => |x| @bitCast(x),
                .float64 => |x| @as(i64, @bitCast(x)),
                else     => std.math.minInt(i64),
            } else std.math.minInt(i64);
        }
        fn makeRow(self: *@This(), key_vals: []const i64, acc_vals: []const u64) ?[]?Value {
            const row = self.alloc.alloc(?Value, self.keys_n + self.aggs_n) catch return null;
            for (key_vals, 0..) |kv, i| {
                const out_type: ColumnType = if (i < self.key_out_types.len) self.key_out_types[i] else .int64;
                row[i] = switch (out_type) {
                    .datetime64_ms => .{ .datetime64_ms = kv },
                    .date_u16 => .{ .date_u16 = @intCast(kv) },
                    else => .{ .int64 = kv },
                };
            }
            for (self.compact_kinds, 0..) |kind, i| {
                row[self.keys_n + i] = switch (kind) {
                    .count   => .{ .int64 = @intCast(acc_vals[i]) },
                    .i64_sum => .{ .int64 = @bitCast(acc_vals[i]) },
                    .u64_sum => .{ .uint64 = acc_vals[i] },
                    .f64_sum => .{ .float64 = @bitCast(acc_vals[i]) },
                    .i64_min, .i64_max => .{ .int64 = @bitCast(acc_vals[i]) },
                    .u64_min, .u64_max => .{ .int64 = @bitCast(acc_vals[i]) },
                    .f64_min, .f64_max => .{ .float64 = @bitCast(acc_vals[i]) },
                    .str_min, .str_max => .{ .int64 = 0 },
                };
            }
            return row;
        }
        fn cb(self: *@This(), key_vals: []const i64, acc_vals: []const u64) void {
            if (self.heap) |heap| {
                if (self.heap_len >= self.heap_k and self.sort_keys.len > 0) {
                    const sk = self.sort_keys[0];
                    const ci = sk.col_idx;
                    const new_raw: i64 = blk: {
                        if (ci < self.keys_n) break :blk key_vals[ci];
                        const ai = ci - self.keys_n;
                        if (ai < self.compact_kinds.len) {
                            break :blk switch (self.compact_kinds[ai]) {
                                .count        => @intCast(acc_vals[ai]),
                                .i64_sum, .i64_min, .i64_max, .u64_min, .u64_max => @bitCast(acc_vals[ai]),
                                else          => std.math.maxInt(i64),
                            };
                        }
                        break :blk std.math.maxInt(i64);
                    };
                    const qualifies = if (sk.desc) new_raw > self.heap_min_cached else new_raw < self.heap_min_cached;
                    if (!qualifies) return;
                }
                const row = self.makeRow(key_vals, acc_vals) orelse { self.err = error.OutOfMemory; return; };
                if (self.heap_len < self.heap_k) {
                    heap[self.heap_len] = row;
                    self.heap_len += 1;
                    self.heapSiftUp(self.heap_len - 1);
                    self.updateHeapMinCache();
                } else if (@This().rowLessThan(self.sort_keys, heap[0], row)) {
                    heap[0] = row;
                    self.heapSiftDown(0);
                    self.updateHeapMinCache();
                }
            } else {
                const row = self.makeRow(key_vals, acc_vals) orelse { self.err = error.OutOfMemory; return; };
                self.rl.append(self.alloc, row) catch |e| { self.err = e; };
            }
        }
    };

    const use_heap = top_k > 0 and sort_keys.len > 0;
    const heap_buf: ?[][]?Value = if (use_heap) try alloc.alloc([]?Value, top_k) else null;
    var emit_ctx = EmitCtx{
        .keys_n = n_keys, .aggs_n = n_aggs,
        .compact_kinds = compact_kinds, .rl = &rl, .alloc = alloc,
        .key_out_types = key_out_types_buf,
        .heap = heap_buf, .heap_len = 0, .heap_k = top_k, .sort_keys = sort_keys,
    };
    for (part_hts) |*ph| {
        ph.iterate(&emit_ctx, EmitCtx.cb);
        if (emit_ctx.err) |e| return e;
    }

    if (use_heap) {
        const heap_rows = emit_ctx.heap.?[0..emit_ctx.heap_len];
        const SortCtx2 = struct {
            sort_keys: []const plan.SortKey,
            fn lessThan(self2: @This(), a: []?Value, b: []?Value) bool {
                for (self2.sort_keys) |key| {
                    const av = if (key.col_idx < a.len) a[key.col_idx] else null;
                    const bv = if (key.col_idx < b.len) b[key.col_idx] else null;
                    const ord: std.math.Order = if (av != null and bv != null) Value.order(av.?, bv.?)
                        else if (av == null and bv == null) .eq
                        else if (av == null) .lt else .gt;
                    if (ord == .eq) continue;
                    return if (key.desc) ord == .gt else ord == .lt;
                }
                return false;
            }
        };
        std.sort.pdq([]?Value, heap_rows, SortCtx2{ .sort_keys = sort_keys }, SortCtx2.lessThan);
        var result_rl = RowList.init(out_metas);
        for (heap_rows) |row| try result_rl.append(alloc, row);
        return result_rl;
    }

    return rl;
}
