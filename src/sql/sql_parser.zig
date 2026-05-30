/// Entry point for the native SQL parser.
/// Re-exports the public API from parser.zig (AST only, no Plan types).
/// plan_builder.zig is NOT part of this module — it lives in the generic_sql
/// module to avoid a circular dependency (plan_builder imports generic_sql).

const std = @import("std");
pub const ast = @import("ast.zig");
pub const tokenizer = @import("tokenizer.zig");
pub const parser = @import("parser.zig");

/// Parse a SQL string and return an AST statement, or null on failure.
/// Caller must free with an arena allocator.
pub fn parse(allocator: std.mem.Allocator, sql: []const u8) ?*ast.Stmt {
    return parser.parse(allocator, sql);
}
