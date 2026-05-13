const std = @import("std");
const generic_sql = @import("../generic_sql.zig");

pub const Fallback = union(enum) {
    url_count_top: struct { include_constant: bool },
};

pub fn matchGenericFallback(plan: generic_sql.Plan) ?Fallback {
    if (matchUrlCountTop(plan)) |fallback| return fallback;
    return null;
}

fn matchUrlCountTop(plan: generic_sql.Plan) ?Fallback {
    if (plan.filter != null or plan.limit != 10 or !orderByAlias(plan, "c")) return null;
    if (asciiEqlIgnoreCase(plan.group_by orelse return null, "URL")) {
        if (plan.projections.len != 2) return null;
        if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return null, "URL")) return null;
        if (plan.projections[1].func != .count_star or !asciiEqlIgnoreCase(plan.projections[1].alias orelse return null, "c")) return null;
        return .{ .url_count_top = .{ .include_constant = false } };
    }
    if (asciiEqlIgnoreCase(plan.group_by orelse return null, "1, URL")) {
        if (plan.projections.len != 3) return null;
        if (plan.projections[0].func != .int_literal or plan.projections[0].int_offset != 1) return null;
        if (plan.projections[1].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[1].column orelse return null, "URL")) return null;
        if (plan.projections[2].func != .count_star or !asciiEqlIgnoreCase(plan.projections[2].alias orelse return null, "c")) return null;
        return .{ .url_count_top = .{ .include_constant = true } };
    }
    return null;
}

fn orderByAlias(plan: generic_sql.Plan, alias: []const u8) bool {
    return if (plan.order_by_alias) |got| asciiEqlIgnoreCase(got, alias) else false;
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ac, bc| if (std.ascii.toLower(ac) != std.ascii.toLower(bc)) return false;
    return true;
}
