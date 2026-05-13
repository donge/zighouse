//! Plan shape classification (PR-A2).
//!
//! `PlanShape` is a coarse structural classification of a `generic_sql.Plan`
//! used as the first half of the (PlanShape, CapabilityTag) executor
//! dispatch table. `inferShape(plan, table)` is a pure function: it inspects
//! group-by, projection list, order-by, limit, offset, and the schema's
//! capability tags for the relevant columns to choose a shape.
//!
//! Design notes (PR-A2):
//!   - Shapes are intentionally coarse. A single shape may cover multiple
//!     ClickBench queries; per-query specialization happens in PR-A3+ via
//!     the (PlanShape, CapabilityTag) lookup.
//!   - When in doubt, return `.unknown`. Dispatch will then fall back to
//!     the existing specialized matchers (clickbench/dispatch.zig).
//!   - This file performs no allocation, no I/O, and never reads runtime
//!     data — only schema metadata.

const std = @import("std");
const schema = @import("../schema.zig");
const generic_sql = @import("../generic_sql.zig");
const bind = @import("bind.zig");

/// Coarse physical execution shape. See docs/architecture.md §2.3.
pub const PlanShape = enum {
    /// No GROUP BY; pure scalar aggregate(s) over (optionally) a filtered
    /// scan. Examples: q1 COUNT(*), q3 SUM/COUNT/AVG, q4 AVG(UserID),
    /// q5 COUNT(DISTINCT UserID), q7 MIN/MAX(EventDate), q33 wide sums.
    scalar_aggregate,

    /// Scalar aggregate with a non-trivial filter (LIKE, multi-predicate,
    /// or anything beyond a simple integer comparison). Examples:
    /// q23 COUNT(*) WHERE URL LIKE '%google%'.
    filtered_scalar,

    /// GROUP BY single lowcard_text column, projection = (col, COUNT(*)),
    /// ORDER BY count DESC, LIMIT N. Example: q13 SearchPhrase top.
    lowcard_count_top,

    /// GROUP BY single lowcard_text column, projection = (col,
    /// COUNT(DISTINCT user_col)), ORDER BY distinct DESC, LIMIT N.
    /// Examples: q11 MobilePhoneModel, q14 SearchPhrase.
    lowcard_distinct_top,

    /// GROUP BY single fixed_iN column, projection = (col, COUNT(*)),
    /// ORDER BY count DESC, LIMIT N. (Not yet observed in ClickBench;
    /// reserved for future generic dispatch.)
    fixed_count_top,

    /// GROUP BY single fixed_iN column, projection = (col,
    /// COUNT(DISTINCT user_col)), ORDER BY distinct DESC, LIMIT N.
    /// Example: q9 RegionID.
    fixed_distinct_top,

    /// GROUP BY single dense small-integer column, no LIMIT, ORDER BY
    /// COUNT(*) DESC. Example: q8 AdvEngineID.
    dense_count_group,

    /// GROUP BY single column with mixed aggregates (sum/avg/count/
    /// count_distinct), ORDER BY one alias DESC, LIMIT N. Examples:
    /// q10 RegionID stats, q34 url_length_by_counter.
    dense_avg_count_top,

    /// GROUP BY with OFFSET clause present. Examples: q36 window_size,
    /// q37/q40-43 dashboards.
    offset_count_top,

    /// GROUP BY multiple columns (composite key). Examples: q12
    /// MobilePhone+MobilePhoneModel, q15 SearchEngineID+SearchPhrase,
    /// q16-18 UserID+SearchPhrase, q19-20 client_ip_agg_top, q22 1+URL,
    /// q35 ClientIP+offsets.
    tuple_agg_top,

    /// GROUP BY a hash_text column (URL/Title) with hash sidecar. Example:
    /// q21 URL count top.
    hashed_late_materialize_top,

    /// Plan does not match any known shape. Dispatch falls back to
    /// specialized matchers.
    unknown,
};

/// Infer the PlanShape from a parsed plan and a table schema.
///
/// `table` is the schema describing `plan.table`. Caller is responsible
/// for table lookup; this function does not inspect plan.table.
pub fn inferShape(plan: generic_sql.Plan, table: *const schema.Table) PlanShape {
    // No GROUP BY → scalar variants.
    if (plan.group_by == null) {
        if (plan.order_by_text != null or plan.limit != null or plan.offset != null) {
            // Projection-only top-N (e.g. q24, q30-q32) — not yet handled.
            return .unknown;
        }
        if (plan.where_text) |w| {
            // Distinguish simple int filter (already captured by plan.filter)
            // from text/LIKE filters.
            if (plan.filter == null and w.len > 0) return .filtered_scalar;
            // ASCII-uppercase scan for "LIKE" inside where_text.
            if (containsKeyword(w, "like")) return .filtered_scalar;
        }
        return .scalar_aggregate;
    }

    // GROUP BY present.
    const gb = plan.group_by.?;

    // Composite key → tuple_agg_top.
    if (std.mem.indexOfScalar(u8, gb, ',') != null) {
        return .tuple_agg_top;
    }

    // OFFSET present on a single-key group → offset_count_top.
    if (plan.offset != null) {
        return .offset_count_top;
    }

    // Single-column group: classify by capability of the group column.
    const tag = bind.lookupCapability(table, gb) orelse return .unknown;

    // Detect projection signature: count_star vs count_distinct vs mixed.
    const proj_kind = classifyProjections(plan.projections);

    return switch (tag) {
        .lowcard_text => switch (proj_kind) {
            .single_count_star => .lowcard_count_top,
            .single_count_distinct => .lowcard_distinct_top,
            .mixed_aggregates => .dense_avg_count_top,
            .unknown => .unknown,
        },
        .fixed_i16, .fixed_i32, .fixed_i64, .fixed_date, .fixed_timestamp => switch (proj_kind) {
            .single_count_star => blk: {
                // No LIMIT + COUNT(*) DESC ordering → dense_count_group.
                if (plan.limit == null) break :blk .dense_count_group;
                break :blk .fixed_count_top;
            },
            .single_count_distinct => .fixed_distinct_top,
            .mixed_aggregates => .dense_avg_count_top,
            .unknown => .unknown,
        },
        .hash_text => switch (proj_kind) {
            .single_count_star => .hashed_late_materialize_top,
            else => .unknown,
        },
        .lazy_text, .fixed_char, .derived => .unknown,
    };
}

const ProjectionKind = enum {
    /// (col, COUNT(*))
    single_count_star,
    /// (col, COUNT(DISTINCT other))
    single_count_distinct,
    /// Anything with sum/avg/min/max combined with count.
    mixed_aggregates,
    unknown,
};

fn classifyProjections(projections: []const generic_sql.Expr) ProjectionKind {
    if (projections.len < 2) return .unknown;

    // First projection should be a column reference (the group key).
    if (projections[0].func != .column_ref) return .unknown;

    if (projections.len == 2) {
        return switch (projections[1].func) {
            .count_star => .single_count_star,
            .count_distinct => .single_count_distinct,
            else => .unknown,
        };
    }

    // 3+ projections: look for any sum/avg/min/max → mixed.
    var has_aggregate = false;
    for (projections[1..]) |p| {
        switch (p.func) {
            .sum, .avg, .min, .max => has_aggregate = true,
            .count_star, .count_distinct, .column_ref, .int_literal => {},
        }
    }
    return if (has_aggregate) .mixed_aggregates else .unknown;
}

/// Case-insensitive scan for `keyword` as a sub-string. Adequate for
/// detecting SQL keywords inside `where_text` (the parser preserves the
/// original case).
fn containsKeyword(haystack: []const u8, keyword: []const u8) bool {
    if (haystack.len < keyword.len) return false;
    var i: usize = 0;
    while (i + keyword.len <= haystack.len) : (i += 1) {
        var matches = true;
        for (keyword, 0..) |kc, j| {
            const hc = haystack[i + j];
            const lo = if (hc >= 'A' and hc <= 'Z') hc + 32 else hc;
            if (lo != kc) {
                matches = false;
                break;
            }
        }
        if (matches) return true;
    }
    return false;
}

// Tests live in src/main.zig (see PR-A1 rationale for why exec/*.zig
// cannot serve as independent test roots).
