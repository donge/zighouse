/// Hash aggregation physical strategy selection.
///
/// This module is intentionally small: it keeps physical strategy choice out of
/// the SQL planner so planner code can focus on binding/rewrite, while the
/// executor still receives the same `HashAggNode.Strategy` enum as before.
const std = @import("std");
const core = @import("core");
const plan = core.exec.plan;

pub const ProjectItem = plan.ProjectItem;
pub const Strategy = plan.HashAggNode.Strategy;

pub fn select(keys: []const ProjectItem, aggs: []const ProjectItem) Strategy {
    var str_keys: usize = 0;
    var has_case_str_key = false;
    for (keys) |k| {
        if (k.out_type == .string) str_keys += 1;
        if (k.expr == .case_when and k.out_type == .string) has_case_str_key = true;
    }

    var count_only = aggs.len > 0;
    var has_distinct = false;
    for (aggs) |a| {
        if (a.expr != .agg_call) {
            count_only = false;
            continue;
        }
        const ac = a.expr.agg_call;
        if (ac.distinct) has_distinct = true;
        if (ac.kind != .count_star and ac.kind != .count) count_only = false;
    }

    if (has_distinct) {
        if (keys.len == 1 and str_keys == 0) return .single_int_distinct_topk;
        if (str_keys == 1) return .string_distinct_topk;
        return .grouped_distinct;
    }
    if (count_only and keys.len == 1 and str_keys == 0) return .single_int_count_topk;
    if (count_only and has_case_str_key and str_keys >= 1) return .case_string_key_topk;
    if (str_keys == 0) return .compact_int;
    if (count_only and str_keys == 1 and keys.len == 2) return .pair_count;
    if (count_only and str_keys == 1 and keys.len == 3) return .triple_count;
    return .string_key;
}

test "agg strategy recognizes core physical agg shapes" {
    var count_call = plan.AggCall{ .kind = .count_star, .arg = null, .distinct = false };
    const distinct_arg = plan.Expr{ .lit_i64 = 1 };
    var distinct_call = plan.AggCall{ .kind = .count, .arg = distinct_arg, .distinct = true };

    const int_key = ProjectItem{ .expr = .{ .lit_i64 = 1 }, .alias = "k", .out_type = .int64 };
    const str_key = ProjectItem{ .expr = .{ .lit_str = "s" }, .alias = "s", .out_type = .string };
    const count_agg = ProjectItem{ .expr = .{ .agg_call = &count_call }, .alias = "c", .out_type = .uint64 };
    const distinct_agg = ProjectItem{ .expr = .{ .agg_call = &distinct_call }, .alias = "u", .out_type = .uint64 };

    try std.testing.expectEqual(Strategy.single_int_count_topk, select(&.{int_key}, &.{count_agg}));
    try std.testing.expectEqual(Strategy.single_int_distinct_topk, select(&.{int_key}, &.{distinct_agg}));
    try std.testing.expectEqual(Strategy.string_distinct_topk, select(&.{str_key}, &.{distinct_agg}));
    try std.testing.expectEqual(Strategy.pair_count, select(&.{ int_key, str_key }, &.{count_agg}));
}
