/// Tests for planner.zig — SQL plan → PhysicalNode IR translation.
/// Pulled in via `test { _ = @import("planner_test.zig"); }` at the bottom of planner.zig.

const std         = @import("std");
const generic_sql = @import("generic_sql");
const schema_mod  = @import("schema");
const planner_mod = @import("planner.zig");

const PlannerCtx  = planner_mod.PlannerCtx;
const plan_query  = planner_mod.plan_query;

// ── Existing tests (moved from planner.zig) ───────────────────────────────────

test "planner: simple scan" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    // Build a minimal schema: table "t" with column "n" Int64
    const cols = [_]schema_mod.Column{
        .{ .name = "n", .ty = .int64 },
    };
    const tbl = schema_mod.Table{ .name = "t", .columns = &cols };
    var ctx = PlannerCtx.init(alloc, tbl);

    const proj_expr = generic_sql.Expr{
        .func   = .column_ref,
        .column = "n",
        .alias  = "n",
    };
    const projs = [_]generic_sql.Expr{proj_expr};
    const gplan = generic_sql.Plan{
        .table       = "t",
        .projections = &projs,
    };

    const node = try plan_query(&ctx, gplan);
    try std.testing.expect(node != null);
    try std.testing.expect(node.?.* == .project or node.?.* == .part_scan);
}

test "planner: count(*) scalar agg" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const cols = [_]schema_mod.Column{ .{ .name = "n", .ty = .int64 } };
    const tbl = schema_mod.Table{ .name = "t", .columns = &cols };
    var ctx = PlannerCtx.init(alloc, tbl);

    const proj_expr = generic_sql.Expr{ .func = .count_star, .alias = "cnt" };
    const projs = [_]generic_sql.Expr{proj_expr};
    const gplan = generic_sql.Plan{ .table = "t", .projections = &projs };

    const node = try plan_query(&ctx, gplan);
    try std.testing.expect(node != null);
    // Root should be scalar_agg
    var n = node.?;
    while (true) {
        switch (n.*) {
            .scalar_agg => break,
            .limit => |lm| n = lm.input,
            else => { try std.testing.expect(false); break; },
        }
    }
}

// ── P4.1: 2-param lambda expression parses without error ──────────────────────

test "planner: 2-param lambda parse" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    // Two text columns referenced inside a 2-param arrayMap lambda.
    const cols = [_]schema_mod.Column{
        .{ .name = "col1", .ty = .text },
        .{ .name = "col2", .ty = .text },
    };
    const tbl = schema_mod.Table{ .name = "t", .columns = &cols };
    var ctx = PlannerCtx.init(alloc, tbl);

    // The expression "(x,y)->x" as a lambda — plan_builder emits this as a
    // column_ref whose .column string is the full arrayMap call text.
    const proj_expr = generic_sql.Expr{
        .func   = .column_ref,
        .column = "arrayMap((x,y)->x, col1, col2)",
        .alias  = "r",
    };
    const projs = [_]generic_sql.Expr{proj_expr};
    const gplan = generic_sql.Plan{ .table = "t", .projections = &projs };

    // plan_query must succeed (non-null) — confirms the pratt parser handled
    // the "(x,y)->" 2-param lambda syntax without returning null or erroring.
    const node = try plan_query(&ctx, gplan);
    try std.testing.expect(node != null);
}
