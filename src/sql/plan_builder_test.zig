/// Tests for plan_builder.zig — AST → generic_sql.Plan translation.
/// Pulled in via `test { _ = @import("plan_builder_test.zig"); }` at the bottom of plan_builder.zig.

const std         = @import("std");
const sql_parser  = @import("sql_parser");
const generic_sql = @import("../generic_sql.zig");
const pb          = @import("plan_builder.zig");

const buildPlan   = pb.buildPlan;
const AggregateFn = generic_sql.AggregateFn;

// ── Existing tests (moved from plan_builder.zig) ──────────────────────────────

test "plan_builder: simple SELECT column_ref" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = sql_parser.parse(alloc, "SELECT a, b FROM t WHERE x = 1");
    try std.testing.expect(stmt != null);
    const plan = try buildPlan(alloc, stmt.?, &.{});
    try std.testing.expectEqualStrings("t", plan.table);
    try std.testing.expectEqual(@as(usize, 2), plan.projections.len);
    try std.testing.expect(plan.projections[0].func == .column_ref);
    try std.testing.expectEqualStrings("a", plan.projections[0].column.?);
    try std.testing.expect(plan.where_text != null);
    try std.testing.expectEqualStrings("x = 1", plan.where_text.?);
}

test "plan_builder: COUNT(*) aggregate" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = sql_parser.parse(alloc, "SELECT COUNT(*) as n FROM t GROUP BY x");
    try std.testing.expect(stmt != null);
    const plan = try buildPlan(alloc, stmt.?, &.{});
    try std.testing.expectEqual(AggregateFn.count_star, plan.projections[0].func);
    try std.testing.expectEqualStrings("n", plan.projections[0].alias.?);
    try std.testing.expect(plan.group_by != null);
    try std.testing.expectEqualStrings("x", plan.group_by.?);
}

test "plan_builder: function call as column_ref" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = sql_parser.parse(alloc, "SELECT has(['a','b'], 'b') as r");
    try std.testing.expect(stmt != null);
    const plan = try buildPlan(alloc, stmt.?, &.{});
    try std.testing.expectEqual(AggregateFn.column_ref, plan.projections[0].func);
    try std.testing.expect(plan.projections[0].column != null);
    // The text should contain "has"
    try std.testing.expect(std.mem.indexOf(u8, plan.projections[0].column.?, "has") != null);
}

test "plan_builder: NOT IN subquery sets where_text" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const sql = "SELECT 'x' as v WHERE 'x' not in (select content from rule_filter_dict where content!='' and length(host)=0 and risk='test')";
    const stmt = sql_parser.parse(alloc, sql);
    try std.testing.expect(stmt != null);
    const plan = try buildPlan(alloc, stmt.?, &.{});
    try std.testing.expect(plan.where_text != null);
    try std.testing.expect(std.mem.indexOf(u8, plan.where_text.?, "NOT IN") != null);
}

test "plan_builder: UNION ALL" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = sql_parser.parse(alloc, "SELECT a FROM t1 UNION ALL SELECT b FROM t2");
    try std.testing.expect(stmt != null);
    const plan = try buildPlan(alloc, stmt.?, &.{});
    try std.testing.expect(plan.union_other != null);
    try std.testing.expectEqualStrings("t1", plan.table);
    try std.testing.expectEqualStrings("t2", plan.union_other.?.table);
}

test "plan_builder: INNER JOIN equi-key extraction" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = sql_parser.parse(alloc,
        "SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id LIMIT 10");
    try std.testing.expect(stmt != null);
    const plan = try buildPlan(alloc, stmt.?, &.{});
    try std.testing.expect(plan.join != null);
    const j = plan.join.?;
    try std.testing.expectEqual(generic_sql.JoinKind.inner, j.kind);
    try std.testing.expectEqualStrings("t1", j.left.table);
    try std.testing.expectEqualStrings("t2", j.right.table);
    try std.testing.expectEqual(@as(usize, 1), j.on_left.len);
    try std.testing.expectEqualStrings("t1.id", j.on_left[0]);
    try std.testing.expectEqualStrings("t2.id", j.on_right[0]);
    try std.testing.expectEqual(@as(?usize, 10), plan.limit);
}

test "plan_builder: LEFT JOIN multi-key" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = sql_parser.parse(alloc,
        "SELECT a FROM t1 LEFT JOIN t2 ON t1.x = t2.x AND t1.y = t2.y");
    try std.testing.expect(stmt != null);
    const plan = try buildPlan(alloc, stmt.?, &.{});
    try std.testing.expect(plan.join != null);
    const j = plan.join.?;
    try std.testing.expectEqual(generic_sql.JoinKind.left, j.kind);
    try std.testing.expectEqual(@as(usize, 2), j.on_left.len);
}

// ── P4.3: uniqHLL12 / uniqHLL12If translations ────────────────────────────────

test "plan_builder: uniqHLL12 single-arg → uniq_exact" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = sql_parser.parse(alloc, "SELECT uniqHLL12(col) as u FROM t GROUP BY x");
    try std.testing.expect(stmt != null);
    const plan = try buildPlan(alloc, stmt.?, &.{});
    try std.testing.expectEqual(AggregateFn.uniq_exact, plan.projections[0].func);
    try std.testing.expectEqualStrings("u", plan.projections[0].alias.?);
}

test "plan_builder: uniqHLL12 multi-arg → uniq_exact with concat column" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = sql_parser.parse(alloc, "SELECT uniqHLL12(a, b) as u FROM t GROUP BY x");
    try std.testing.expect(stmt != null);
    const plan = try buildPlan(alloc, stmt.?, &.{});
    try std.testing.expectEqual(AggregateFn.uniq_exact, plan.projections[0].func);
    try std.testing.expect(plan.projections[0].column != null);
    try std.testing.expect(std.mem.indexOf(u8, plan.projections[0].column.?, "concat") != null);
}

test "plan_builder: uniqHLL12If → uniq_exact_if" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = sql_parser.parse(alloc, "SELECT uniqHLL12If(col, cond) as u FROM t GROUP BY x");
    try std.testing.expect(stmt != null);
    const plan = try buildPlan(alloc, stmt.?, &.{});
    try std.testing.expectEqual(AggregateFn.uniq_exact_if, plan.projections[0].func);
    try std.testing.expectEqualStrings("u", plan.projections[0].alias.?);
}
