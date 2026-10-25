const std = @import("std");
const types = @import("../types.zig");
const chunk = @import("../chunk.zig");
const result = @import("../result.zig");
const plan = @import("plan.zig");
const ht = @import("hash_table.zig");
const topn = @import("topn.zig");
const row_buffer = @import("row_buffer.zig");

pub const Value = types.Value;
pub const AggAccum = types.AggAccum;
pub const ColumnType = types.ColumnType;
pub const RowBuffer = row_buffer.RowBuffer;
pub const DataChunk = chunk.DataChunk;
pub const executeTopK = topn.executeTopK;
pub const TopKSortCtx = topn.TopKSortCtx;

pub fn finalizeAggRows(
    rows: RowBuffer,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    alloc: std.mem.Allocator,
) !RowBuffer {
    if (sort_keys.len == 0) return rows;
    if (top_k > 0 and rows.rows.items.len > top_k) return executeTopK(rows, sort_keys, top_k, alloc);
    return executeOrderBy(rows, sort_keys, alloc);
}

pub fn emitCompactVals(
    vals: []const u64,
    kinds: []const ht.CompactAggKind,
    aggs: []const plan.ProjectItem,
    out: []?Value,
) void {
    for (vals, kinds, aggs, 0..) |v, kind, item, i| {
        out[i] = switch (kind) {
            .count, .u64_sum, .u64_min, .u64_max, .count_distinct_u64 => Value{ .uint64 = v },
            .i64_sum, .i64_min, .i64_max => Value{ .int64 = @bitCast(v) },
            .f64_sum, .f64_str_len_sum => blk: {
                const sum: f64 = @bitCast(v);
                if (item.expr == .agg_call and item.expr.agg_call.kind == .avg) {
                    var cnt: u64 = 0;
                    for (vals, kinds) |cv, ck| {
                        if (ck == .count) {
                            cnt = cv;
                            break;
                        }
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

pub fn emitCompactValsWithSidecar(
    vals: []const u64,
    kinds: []const ht.CompactAggKind,
    aggs: []const plan.ProjectItem,
    out: []?Value,
    str_ht: *const ht.StrAggHashTable,
    slot: usize,
    sidecar_idx: []const usize,
) void {
    for (vals, kinds, aggs, 0..) |v, kind, item, i| {
        out[i] = switch (kind) {
            .count, .u64_sum, .u64_min, .u64_max, .count_distinct_u64 => Value{ .uint64 = v },
            .i64_sum, .i64_min, .i64_max => Value{ .int64 = @bitCast(v) },
            .f64_sum, .f64_str_len_sum => blk: {
                const sum: f64 = @bitCast(v);
                if (item.expr == .agg_call and item.expr.agg_call.kind == .avg) {
                    var cnt: u64 = 0;
                    for (vals, kinds) |cv, ck| {
                        if (ck == .count) {
                            cnt = cv;
                            break;
                        }
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

pub fn updateCompactVals(
    slot_vals: []u64,
    ck: []const ht.CompactAggKind,
    aggs: []const plan.ProjectItem,
    c: *const DataChunk,
    r: usize,
    str_ht: ?*ht.StrAggHashTable,
    slot: usize,
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
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .int64 => |v| {
                                var s: i64 = @bitCast(slot_vals[ci]);
                                s += v[r];
                                slot_vals[ci] = @bitCast(s);
                            },
                            .uint64 => |v| {
                                var s: i64 = @bitCast(slot_vals[ci]);
                                s += @as(i64, @bitCast(v[r]));
                                slot_vals[ci] = @bitCast(s);
                            },
                            .bool_u8 => |v| {
                                var s: i64 = @bitCast(slot_vals[ci]);
                                s += v[r];
                                slot_vals[ci] = @bitCast(s);
                            },
                            else => {},
                        };
                    }
                }
            },
            .f64_sum => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .float64 => |v| {
                                var s: f64 = @bitCast(slot_vals[ci]);
                                s += v[r];
                                slot_vals[ci] = @bitCast(s);
                            },
                            .int64 => |v| {
                                var s: f64 = @bitCast(slot_vals[ci]);
                                s += @floatFromInt(v[r]);
                                slot_vals[ci] = @bitCast(s);
                            },
                            .uint64 => |v| {
                                var s: f64 = @bitCast(slot_vals[ci]);
                                s += @floatFromInt(v[r]);
                                slot_vals[ci] = @bitCast(s);
                            },
                            else => {},
                        };
                    }
                }
            },
            .i64_min => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .int64 => |v| {
                                const cur: i64 = @bitCast(slot_vals[ci]);
                                if (v[r] < cur) slot_vals[ci] = @bitCast(v[r]);
                            },
                            else => {},
                        };
                    }
                }
            },
            .i64_max => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .int64 => |v| {
                                const cur: i64 = @bitCast(slot_vals[ci]);
                                if (v[r] > cur) slot_vals[ci] = @bitCast(v[r]);
                            },
                            else => {},
                        };
                    }
                }
            },
            .u64_sum => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .uint64 => |v| slot_vals[ci] += v[r],
                            else => {},
                        };
                    }
                }
            },
            .u64_min => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .uint64 => |v| {
                                if (v[r] < slot_vals[ci]) slot_vals[ci] = v[r];
                            },
                            else => {},
                        };
                    }
                }
            },
            .u64_max => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .uint64 => |v| {
                                if (v[r] > slot_vals[ci]) slot_vals[ci] = v[r];
                            },
                            else => {},
                        };
                    }
                }
            },
            .f64_min => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .float64 => |v| {
                                const cur: f64 = @bitCast(slot_vals[ci]);
                                if (v[r] < cur) slot_vals[ci] = @bitCast(v[r]);
                            },
                            else => {},
                        };
                    }
                }
            },
            .f64_max => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .float64 => |v| {
                                const cur: f64 = @bitCast(slot_vals[ci]);
                                if (v[r] > cur) slot_vals[ci] = @bitCast(v[r]);
                            },
                            else => {},
                        };
                    }
                }
            },
            .str_min => {
                if (str_ht) |sht| {
                    if (ac.arg) |arg| {
                        if (arg == .col_ref) {
                            const col = c.columns[arg.col_ref.index];
                            if (!col.isRowNull(r)) switch (col.data) {
                                .string => |v| sht.updateStrSidecar(slot, sidecar_indices[ci], v[r], true),
                                else => {},
                            };
                        }
                    }
                }
            },
            .str_max => {
                if (str_ht) |sht| {
                    if (ac.arg) |arg| {
                        if (arg == .col_ref) {
                            const col = c.columns[arg.col_ref.index];
                            if (!col.isRowNull(r)) switch (col.data) {
                                .string => |v| sht.updateStrSidecar(slot, sidecar_indices[ci], v[r], false),
                                else => {},
                            };
                        }
                    }
                }
            },
            .f64_str_len_sum => {
                if (ac.arg) |arg| {
                    // Resolve the string column index: either a direct col_ref (rare)
                    // or fn_call("length", col_ref) — the common AVG(length(col)) pattern.
                    const str_ci: usize = blk2: {
                        if (arg == .col_ref) break :blk2 arg.col_ref.index;
                        if (arg == .fn_call and
                            std.mem.eql(u8, arg.fn_call.name, "length") and
                            arg.fn_call.args.len == 1 and
                            arg.fn_call.args[0] == .col_ref)
                            break :blk2 arg.fn_call.args[0].col_ref.index;
                        break; // unsupported arg pattern — skip
                    };
                    const col = c.columns[str_ci];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .string => |v| {
                            const cur: f64 = @bitCast(slot_vals[ci]);
                            slot_vals[ci] = @bitCast(cur + @as(f64, @floatFromInt(v[r].len)));
                        },
                        else => {},
                    };
                }
            },
            // count_distinct_u64: deduplication is handled by the global pair-set
            // in the caller (executeHashAggChunked compact path); no-op here.
            .count_distinct_u64 => {},
        }
    }
}

pub fn executeOrderBy(inner: RowBuffer, keys: []const plan.SortKey, alloc: std.mem.Allocator) !RowBuffer {
    const rows_copy = try alloc.dupe([]?Value, inner.rows.items);
    std.sort.pdq([]?Value, rows_copy, TopKSortCtx{ .keys = keys }, TopKSortCtx.lessThan);
    var rl = RowBuffer.init(inner.metas);
    for (rows_copy) |row| try rl.append(alloc, row);
    return rl;
}
