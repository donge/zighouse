//! BoundColumn -> physical column adapter (PR-B2b).
//!
//! `BoundColumn` (see bind.zig) is the schema-driven runtime description of
//! a column. Algorithm modules (exec/group.zig, exec/reduce.zig) operate on
//! their own physical Column unions optimized for tight inner loops.
//!
//! These adapters bridge the two layers: a single schema-driven binder
//! (e.g. `Native.bindColumn`) feeds both group and reduce code paths via
//! `asGroupColumn` / `asReduceColumn`, eliminating the duplicated lookup
//! tables that previously lived in five sibling functions.
//!
//! Adapters are *projections*: they discard schema metadata that the
//! physical layer doesn't need. A BoundColumn that carries no equivalent
//! physical representation (e.g. a derived expression with no materialized
//! slice) returns `error.UnsupportedGenericColumn` -- callers fall back to
//! their original error path.

const bind = @import("bind.zig");
const native_group = @import("group.zig");
const native_reduce = @import("reduce.zig");

/// Project a BoundColumn into a `native_group.BoundColumn` consumable by
/// the group-by algorithms. Lossy: only int / lowcard_text variants map.
///
/// For `lowcard_text` columns, only the `empty_text_id` shape is produced
/// (used by GROUP BY on an interned string column). The caller must
/// pre-decide whether to consume the column as group key or as filter
/// predicate; this adapter doesn't know that context.
pub fn asGroupColumn(bc: bind.BoundColumn) !native_group.BoundColumn {
    return switch (bc) {
        .fixed_i16 => |c| .{ .name = c.name, .column = .{ .i16 = c.values } },
        .fixed_i32 => |c| .{ .name = c.name, .column = .{ .i32 = c.values } },
        .fixed_date => |c| .{ .name = c.name, .column = .{ .i32 = c.values } },
        .fixed_i64 => |c| .{ .name = c.name, .column = .{ .i64 = c.values } },
        .fixed_timestamp => |c| .{ .name = c.name, .column = .{ .i64 = c.values } },
        .lowcard_text => |c| blk: {
            const empty_id = c.empty_id orelse return error.UnsupportedGenericColumn;
            break :blk .{
                .name = c.name,
                .column = .{ .empty_text_id = .{ .ids = c.column.ids.values, .empty_id = empty_id } },
            };
        },
        else => error.UnsupportedGenericColumn,
    };
}

/// Project a BoundColumn into a `native_reduce.Column` consumable by the
/// scalar reduce algorithms. Lossy: only int variants map.
pub fn asReduceColumn(bc: bind.BoundColumn) !native_reduce.Column {
    return switch (bc) {
        .fixed_i16 => |c| .{ .i16 = c.values },
        .fixed_i32 => |c| .{ .i32 = c.values },
        .fixed_date => |c| .{ .date = c.values },
        .fixed_i64 => |c| .{ .i64 = c.values },
        .fixed_timestamp => |c| .{ .i64 = c.values },
        else => error.UnsupportedGenericColumn,
    };
}
