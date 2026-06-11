/// Tests for kernels.zig — scalar expression evaluation and helper functions.
/// Pulled in via `test { _ = @import("kernels_test.zig"); }` at the bottom of kernels.zig.
const std = @import("std");
const plan = @import("plan.zig");
const k = @import("kernels.zig");
const chunk = @import("../chunk.zig");

const Value = k.Value;
const evalExpr = k.evalExpr;
const likeMatch = k.likeMatch;
const daysToYMD = k.daysToYMD;

// ── Existing tests (moved from kernels.zig) ────────────────────────────────────

test "evalExpr literal" {
    const row: []const ?Value = &.{};
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const v = try evalExpr(.{ .lit_i64 = 42 }, row, null, arena.allocator());
    try std.testing.expectEqual(Value{ .int64 = 42 }, v.?);
}

test "evalExpr comparison" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const row = [_]Value{ .{ .int64 = 5 }, .{ .int64 = 3 } };
    var gt_expr = plan.BinOp{
        .left = .{ .col_ref = .{ .index = 0, .name = "a" } },
        .right = .{ .col_ref = .{ .index = 1, .name = "b" } },
    };
    const v = try evalExpr(.{ .gt = &gt_expr }, &row, null, arena.allocator());
    try std.testing.expectEqual(Value{ .bool_u8 = 1 }, v.?);
}

test "evalExprSelection returns passing row ids" {
    var b = chunk.ChunkBuilder.init(std.testing.allocator, 5);
    defer b.chunk.deinit();

    const ci = try b.addColumn("x", .int64);
    const vals = [_]i64{ 1, 5, 9, 2, 7 };
    @memcpy(b.chunk.columns[ci].data.int64, &vals);

    var gt = plan.BinOp{
        .left = .{ .col_ref = .{ .index = 0, .name = "x" } },
        .right = .{ .lit_i64 = 4 },
    };
    var sel_buf: [5]u32 = undefined;
    var mask_buf: [5]i16 = undefined;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const sel = try k.evalExprSelection(.{ .gt = &gt }, b.chunk, &sel_buf, &mask_buf, arena.allocator());
    try std.testing.expectEqual(@as(usize, 3), sel.len);
    try std.testing.expectEqual(@as(u32, 1), sel.slice()[0]);
    try std.testing.expectEqual(@as(u32, 2), sel.slice()[1]);
    try std.testing.expectEqual(@as(u32, 4), sel.slice()[2]);
}

test "likeMatch" {
    try std.testing.expect(likeMatch("hello world", "hello%"));
    try std.testing.expect(likeMatch("abc", "a_c"));
    try std.testing.expect(!likeMatch("abc", "a_d"));
    try std.testing.expect(likeMatch("", "%"));
}

test "evalFnCall: substring" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const row = [_]Value{.{ .string = "hello world" }};
    var fc = plan.FnCall{
        .name = "substring",
        .args = @constCast(&[_]plan.Expr{
            .{ .col_ref = .{ .index = 0, .name = "s" } },
            .{ .lit_i64 = 7 },
            .{ .lit_i64 = 5 },
        }),
    };
    const v = try evalExpr(.{ .fn_call = &fc }, &row, null, arena.allocator());
    try std.testing.expectEqualStrings("world", v.?.string);
}

test "evalFnCall: startsWith" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const row = [_]Value{.{ .string = "::ffff:1.2.3.4" }};
    var fc = plan.FnCall{
        .name = "startsWith",
        .args = @constCast(&[_]plan.Expr{
            .{ .col_ref = .{ .index = 0, .name = "s" } },
            .{ .lit_str = "::ffff:" },
        }),
    };
    const v = try evalExpr(.{ .fn_call = &fc }, &row, null, arena.allocator());
    try std.testing.expectEqual(Value{ .bool_u8 = 1 }, v.?);
}

test "evalFnCall: floor and round" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const row = [_]Value{.{ .float64 = 3.7 }};
    var fc_floor = plan.FnCall{ .name = "floor", .args = @constCast(&[_]plan.Expr{.{ .col_ref = .{ .index = 0, .name = "x" } }}) };
    var fc_round = plan.FnCall{ .name = "round", .args = @constCast(&[_]plan.Expr{.{ .col_ref = .{ .index = 0, .name = "x" } }}) };
    const vf = try evalExpr(.{ .fn_call = &fc_floor }, &row, null, arena.allocator());
    const vr = try evalExpr(.{ .fn_call = &fc_round }, &row, null, arena.allocator());
    try std.testing.expectEqual(Value{ .float64 = 3.0 }, vf.?);
    try std.testing.expectEqual(Value{ .float64 = 4.0 }, vr.?);
}

test "evalFnCall: multiIf" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const row = [_]Value{.{ .int64 = 75 }};
    var fc = plan.FnCall{
        .name = "multiIf",
        .args = @constCast(&[_]plan.Expr{
            .{ .lit_bool = false },
            .{ .lit_str = "low" },
            .{ .lit_bool = true },
            .{ .lit_str = "mid" },
            .{ .lit_str = "high" },
        }),
    };
    _ = row;
    const v = try evalExpr(.{ .fn_call = &fc }, &.{}, null, arena.allocator());
    try std.testing.expectEqualStrings("mid", v.?.string);
}

test "evalFnCall: CAST_array_string" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var empty_fc = plan.FnCall{
        .name = "CAST_array_string",
        .args = @constCast(&[_]plan.Expr{.{ .lit_str = "" }}),
    };
    const empty = try evalExpr(.{ .fn_call = &empty_fc }, &.{}, null, arena.allocator());
    try std.testing.expectEqual(@as(usize, 0), empty.?.array_string.len);

    var one_fc = plan.FnCall{
        .name = "CAST_array_string",
        .args = @constCast(&[_]plan.Expr{.{ .lit_str = "x" }}),
    };
    const one = try evalExpr(.{ .fn_call = &one_fc }, &.{}, null, arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), one.?.array_string.len);
    try std.testing.expectEqualStrings("x", one.?.array_string[0]);
}

test "evalFnCall: arrayMax over arrayMap numeric strings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var body = plan.Expr.lambda_param;
    const lam = plan.Lambda{ .param = "x", .body = &body };
    var map_fc = plan.FnCall{
        .name = "arrayMap",
        .args = @constCast(&[_]plan.Expr{
            .{ .lambda = lam },
            .{ .lit_array = @constCast(&[_][]const u8{ "1", "20", "3" }) },
        }),
    };
    var max_fc = plan.FnCall{
        .name = "arrayMax",
        .args = @constCast(&[_]plan.Expr{.{ .fn_call = &map_fc }}),
    };
    const v = try evalExpr(.{ .fn_call = &max_fc }, &.{}, null, arena.allocator());
    try std.testing.expectEqualStrings("20", v.?.string);
}

test "evalFnCall: IPv4StringToNumOrDefault" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var fc = plan.FnCall{
        .name = "IPv4StringToNumOrDefault",
        .args = @constCast(&[_]plan.Expr{.{ .lit_str = "1.2.3.4" }}),
    };
    const v = try evalExpr(.{ .fn_call = &fc }, &.{}, null, arena.allocator());
    // 1*16777216 + 2*65536 + 3*256 + 4 = 16909060
    try std.testing.expectEqual(Value{ .uint64 = 16909060 }, v.?);
}

test "evalFnCall: toYYYYMMDD" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // 1970-01-01 = day 0
    var fc = plan.FnCall{ .name = "toYYYYMMDD", .args = @constCast(&[_]plan.Expr{.{ .lit_i64 = 0 }}) };
    const v = try evalExpr(.{ .fn_call = &fc }, &.{}, null, arena.allocator());
    try std.testing.expectEqual(Value{ .uint64 = 19700101 }, v.?);
}

test "daysToYMD" {
    // 2024-05-19: compute expected days
    const ymd = daysToYMD(19862); // pre-computed days for 2024-05-19
    try std.testing.expectEqual(@as(u32, 2024), ymd[0]);
    try std.testing.expectEqual(@as(u32, 5), ymd[1]);
    try std.testing.expectEqual(@as(u32, 19), ymd[2]);
}

// ── P4.1: 2-param arrayMap lambda ─────────────────────────────────────────────

test "evalFnCall: arrayMap 2-param lambda" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // (x, y) -> x - y  applied to ["3.0","1.0"] and ["1.0","2.0"] → ["2","−1"]
    var binop = plan.BinOp{ .left = .lambda_param, .right = .lambda_param2 };
    var body = plan.Expr{ .sub = &binop };
    const lam = plan.Lambda{ .param = "x", .param2 = "y", .body = &body };
    var fc = plan.FnCall{
        .name = "arrayMap",
        .args = @constCast(&[_]plan.Expr{
            .{ .lambda = lam },
            .{ .lit_array = @constCast(&[_][]const u8{ "3.0", "1.0" }) },
            .{ .lit_array = @constCast(&[_][]const u8{ "1.0", "2.0" }) },
        }),
    };
    const v = try evalExpr(.{ .fn_call = &fc }, &.{}, null, arena.allocator());
    try std.testing.expect(v != null);
    const arr = v.?.array_string;
    try std.testing.expectEqual(@as(usize, 2), arr.len);
    const f0 = try std.fmt.parseFloat(f64, arr[0]);
    const f1 = try std.fmt.parseFloat(f64, arr[1]);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), f0, 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, -1.0), f1, 1e-9);
}

// ── P4.2: format() placeholder substitution ───────────────────────────────────

test "evalFnCall: format placeholder" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var fc = plan.FnCall{
        .name = "format",
        .args = @constCast(&[_]plan.Expr{
            .{ .lit_str = "hello {} world {}" },
            .{ .lit_str = "cruel" },
            .{ .lit_i64 = 42 },
        }),
    };
    const v = try evalExpr(.{ .fn_call = &fc }, &.{}, null, arena.allocator());
    try std.testing.expect(v != null);
    try std.testing.expectEqualStrings("hello cruel world 42", v.?.string);
}
