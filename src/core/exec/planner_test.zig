/// Tests for planner.zig — SQL plan → PhysicalNode IR translation.
/// Pulled in via `test { _ = @import("planner_test.zig"); }` at the bottom of planner.zig.
const std = @import("std");
const generic_sql = @import("generic_sql");
const schema_mod = @import("schema");
const planner_mod = @import("planner.zig");

const PlannerCtx = planner_mod.PlannerCtx;
const plan_query = planner_mod.plan_query;

fn findHashAggNode(node: *const planner_mod.PhysicalNode) ?*const planner_mod.PhysicalNode {
    return switch (node.*) {
        .hash_agg => node,
        .project => |p| findHashAggNode(p.input),
        .filter => |f| findHashAggNode(f.input),
        .order_by => |o| findHashAggNode(o.input),
        .top_k => |t| findHashAggNode(t.input),
        .limit => |l| findHashAggNode(l.input),
        else => null,
    };
}

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
        .func = .column_ref,
        .column = "n",
        .alias = "n",
    };
    const projs = [_]generic_sql.Expr{proj_expr};
    const gplan = generic_sql.Plan{
        .table = "t",
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

    const cols = [_]schema_mod.Column{.{ .name = "n", .ty = .int64 }};
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
            else => {
                try std.testing.expect(false);
                break;
            },
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
        .func = .column_ref,
        .column = "arrayMap((x,y)->x, col1, col2)",
        .alias = "r",
    };
    const projs = [_]generic_sql.Expr{proj_expr};
    const gplan = generic_sql.Plan{ .table = "t", .projections = &projs };

    // plan_query must succeed (non-null) — confirms the pratt parser handled
    // the "(x,y)->" 2-param lambda syntax without returning null or erroring.
    const node = try plan_query(&ctx, gplan);
    try std.testing.expect(node != null);
}

test "planner: Array(String) cast forms plan as array_string scalar" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const cols = [_]schema_mod.Column{.{ .name = "n", .ty = .int64 }};
    const tbl = schema_mod.Table{ .name = "t", .columns = &cols };

    const cases = [_][]const u8{
        "CAST([], 'Array(String)')",
        "CAST([] AS Array(String))",
    };
    for (cases) |expr_text| {
        var ctx = PlannerCtx.init(alloc, tbl);
        const proj_expr = generic_sql.Expr{
            .func = .column_ref,
            .column = expr_text,
            .alias = "a",
        };
        const projs = [_]generic_sql.Expr{proj_expr};
        const gplan = generic_sql.Plan{ .table = "t", .projections = &projs };
        const node = try plan_query(&ctx, gplan);
        try std.testing.expect(node != null);
        try std.testing.expect(node.?.* == .project);
        const item = node.?.project.items[0];
        try std.testing.expectEqual(planner_mod.ColumnType.array_string, item.out_type);
        try std.testing.expect(item.expr == .fn_call);
        try std.testing.expectEqualStrings("CAST_array_string", item.expr.fn_call.name);
    }
}

test "planner: dict score conditional infers Float64" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const cols = [_]schema_mod.Column{
        .{ .name = "dst_ip", .ty = .text, .ch_type = "IPv6" },
    };
    const tbl = schema_mod.Table{ .name = "detect_events", .columns = &cols };
    var ctx = PlannerCtx.init(alloc, tbl);

    const ip = "if(startsWith(toString(dst_ip), '::ffff:'), substring(toString(dst_ip), 8), toString(dst_ip))";
    const expr = try std.fmt.allocPrint(alloc,
        "if(isIPv4String({0s}), dictGetOrDefault('vprobe.dict_ip_reputation_trie', 'score', tuple(IPv4StringToNumOrDefault({0s})), 0.0), if(isIPv6String({0s}), dictGetOrDefault('vprobe.dict_ip_reputation_trie', 'score', tuple(IPv6StringToNumOrDefault({0s})), 0.0), 0.0))",
        .{ip},
    );
    const projs = [_]generic_sql.Expr{.{
        .func = .column_ref,
        .column = expr,
        .alias = "intel_score",
    }};
    const gplan = generic_sql.Plan{ .table = "detect_events", .projections = &projs };

    const node = try plan_query(&ctx, gplan) orelse return error.NullPlan;
    try std.testing.expect(node.* == .project);
    try std.testing.expectEqual(planner_mod.ColumnType.float64, node.project.items[0].out_type);
}

test "planner: parsed dict score conditional infers Float64" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const cols = [_]schema_mod.Column{
        .{ .name = "dst_ip", .ty = .text, .ch_type = "IPv6" },
    };
    const tbl = schema_mod.Table{ .name = "detect_events", .columns = &cols };
    var ctx = PlannerCtx.init(alloc, tbl);

    const sql =
        "SELECT if(isIPv4String(if(startsWith(toString(dst_ip), '::ffff:'), substring(toString(dst_ip), 8), toString(dst_ip))), " ++
        "dictGetOrDefault('vprobe.dict_ip_reputation_trie', 'score', tuple(IPv4StringToNumOrDefault(if(startsWith(toString(dst_ip), '::ffff:'), substring(toString(dst_ip), 8), toString(dst_ip)))), 0.0), " ++
        "if(isIPv6String(if(startsWith(toString(dst_ip), '::ffff:'), substring(toString(dst_ip), 8), toString(dst_ip))), " ++
        "dictGetOrDefault('vprobe.dict_ip_reputation_trie', 'score', tuple(IPv6StringToNumOrDefault(if(startsWith(toString(dst_ip), '::ffff:'), substring(toString(dst_ip), 8), toString(dst_ip)))), 0.0), 0.0)) AS intel_score " ++
        "FROM detect_events";
    const gplan = (try generic_sql.parse(alloc, sql)) orelse return error.ParseFailed;
    defer generic_sql.deinit(alloc, gplan);

    const node = try plan_query(&ctx, gplan) orelse return error.NullPlan;
    try std.testing.expect(node.* == .project);
    try std.testing.expectEqual(planner_mod.ColumnType.float64, node.project.items[0].out_type);
}

test "planner: trie tags any aggregate becomes empty Array(String)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const cols = [_]schema_mod.Column{
        .{ .name = "dst_ip", .ty = .text, .ch_type = "IPv6" },
    };
    const tbl = schema_mod.Table{ .name = "detect_events", .columns = &cols };
    var ctx = PlannerCtx.init(alloc, tbl);

    const sql =
        "SELECT any(if(isIPv4String(toString(dst_ip)), " ++
        "dictGetOrDefault('vprobe.dict_ip_reputation_trie', 'tags', tuple(IPv4StringToNumOrDefault(toString(dst_ip))), CAST([], 'Array(String)')), " ++
        "CAST([], 'Array(String)'))) AS intel_tags FROM detect_events GROUP BY toString(dst_ip)";
    const gplan = (try generic_sql.parse(alloc, sql)) orelse return error.ParseFailed;
    defer generic_sql.deinit(alloc, gplan);

    const node = try plan_query(&ctx, gplan) orelse return error.NullPlan;
    const ha = findHashAggNode(node) orelse return error.HashAggNotFound;
    try std.testing.expectEqual(planner_mod.ColumnType.array_string, ha.hash_agg.aggs[0].out_type);
    const ac = ha.hash_agg.aggs[0].expr.agg_call;
    try std.testing.expect(ac.arg.? == .lit_array);
    try std.testing.expectEqual(@as(usize, 0), ac.arg.?.lit_array.len);
}

test "planner: user function substitution respects identifier boundaries" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const cols = [_]schema_mod.Column{
        .{ .name = "n", .ty = .int64 },
        .{ .name = "x1", .ty = .int64 },
    };
    const tbl = schema_mod.Table{ .name = "t", .columns = &cols };
    var ctx = PlannerCtx.init(alloc, tbl);
    var functions = std.StringHashMap([]const u8).init(alloc);
    try functions.put("bump", "(x) -> x + x1");
    ctx.user_functions = &functions;

    const proj_expr = generic_sql.Expr{
        .func = .column_ref,
        .column = "bump(n)",
        .alias = "r",
    };
    const projs = [_]generic_sql.Expr{proj_expr};
    const gplan = generic_sql.Plan{ .table = "t", .projections = &projs };

    const node = try plan_query(&ctx, gplan);
    try std.testing.expect(node != null);
}
