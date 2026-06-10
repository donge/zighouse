/// Physical query plan IR — the output of the Planner, input to the Pipeline.
///
/// A PhysicalPlan is a tree of PhysicalNode values. Each node describes one
/// operation; leaf nodes are scans, internal nodes are operators.
///
/// This module defines only the plan IR (data structures). Execution is in
/// src/core/exec/pipeline.zig and src/core/exec/operators.zig.
const std = @import("std");
const types = @import("../types.zig");

pub const ColumnType = types.ColumnType;
pub const Value      = types.Value;

// ── Expression IR ─────────────────────────────────────────────────────────────

/// A scalar expression that can be evaluated against a DataChunk row.
/// Produced by the Planner from the DuckDB AST column list / WHERE clause.
pub const Expr = union(enum) {
    // Literals
    lit_i64:    i64,
    lit_u64:    u64,
    lit_f64:    f64,
    lit_str:    []const u8,
    lit_bool:   bool,
    lit_null,

    // Column reference by index in the input schema
    col_ref:    ColRef,

    // Arithmetic
    add:        *BinOp,
    sub:        *BinOp,
    mul:        *BinOp,
    div:        *BinOp,
    mod:        *BinOp,

    // Comparison — always yields bool_u8
    eq:         *BinOp,
    neq:        *BinOp,
    lt:         *BinOp,
    lte:        *BinOp,
    gt:         *BinOp,
    gte:        *BinOp,

    // Logical
    @"and":     *BinOp,
    @"or":      *BinOp,
    not:        *UnOp,

    // IS NULL / IS NOT NULL
    is_null:    *UnOp,
    is_not_null: *UnOp,

    // String functions
    like:       *BinOp,
    not_like:   *BinOp,
    concat:     *BinOp,

    // CASE WHEN ... THEN ... ELSE ... END
    case_when:  *CaseWhen,

    // Aggregate function call — appears in project nodes only
    agg_call:   *AggCall,

    // Scalar function call
    fn_call:    *FnCall,

    // Cast
    cast:       *Cast,

    // Dictionary function call
    dict_call:  *DictCall,

    // Array literal: ['a', 'b', ...]  (string elements only)
    lit_array:  [][]const u8,

    // Lambda expression: x -> body  (used as first arg to arrayMap/Filter/Exists)
    lambda:      Lambda,

    // Reference to the current lambda-bound element (replaces the param name inside body)
    lambda_param,

    // Reference to the second lambda-bound element for 2-param lambdas: (x,y)->body
    lambda_param2,
};

pub const Lambda = struct {
    /// Parameter name (e.g. "x") — used only during planning, not at eval time.
    param: []const u8,
    /// Optional second parameter name for (x,y)->body lambdas.
    param2: ?[]const u8 = null,
    /// The body expression; references to `param` are compiled as `lambda_param`.
    body:  *Expr,
};

pub const ColRef = struct {
    /// Index into the input DataChunk's columns slice.
    index: usize,
    /// Original column name (for error messages).
    name:  []const u8,
};

pub const BinOp = struct {
    left:  Expr,
    right: Expr,
};

pub const UnOp = struct {
    operand: Expr,
};

pub const CaseWhen = struct {
    /// Parallel slices: when[i] is the condition, then[i] is the value.
    when: []Expr,
    then: []Expr,
    /// else_expr is null if there is no ELSE clause.
    else_expr: ?Expr,
};

pub const AggCall = struct {
    pub const Kind = enum {
        count_star,
        count,
        sum,
        avg,
        min,
        max,
        group_uniq_array,
        any,
    };

    kind:     Kind,
    arg:      ?Expr,    // null for count(*)
    distinct: bool,
    /// Optional separator for group_uniq_array — if set, output is a string (joined).
    sep:      ?[]const u8 = null,
};

pub const FnCall = struct {
    name: []const u8,
    args: []Expr,
};

/// Dictionary function call — dictHas / dictGet / dictGetOrDefault / dictGetOrNull.
/// Keys are the tuple arguments (after dict name and optional attr name).
pub const DictCall = struct {
    fn_name:   []const u8,   // "dictHas", "dictGet", "dictGetOrDefault", "dictGetOrNull"
    dict_name: []const u8,   // e.g. "vprobe.dict_intel"
    attr_name: ?[]const u8,  // null for dictHas; attribute name for dictGet*
    keys:      []Expr,       // key expressions
    default_expr: ?Expr,     // for dictGetOrDefault; null otherwise
};

pub const Cast = struct {
    expr:    Expr,
    to_type: ColumnType,
};

// ── Projection item ───────────────────────────────────────────────────────────

/// One item in a SELECT list.
pub const ProjectItem = struct {
    /// The expression to evaluate.
    expr:  Expr,
    /// Output column name (alias or derived name).
    alias: []const u8,
    /// The output type inferred by the Planner.
    out_type: ColumnType,
    /// Optional wire type override forwarded to ColMeta.ch_type (e.g. "UInt16", "UInt32").
    /// Null means no override (use out_type's default wire name).
    ch_type: ?[]const u8 = null,
};

// ── Order / limit ─────────────────────────────────────────────────────────────

pub const SortKey = struct {
    /// Index into the projection output (after SELECT list is evaluated).
    col_idx: usize,
    desc:    bool,
    nulls_first: bool,
};

// ── Scan predicate ────────────────────────────────────────────────────────────

/// Optional predicate pushed down into the scan.
/// If null, the scan returns all rows.
pub const ScanFilter = struct {
    expr: Expr,
};

// ── Physical nodes ────────────────────────────────────────────────────────────

/// A node in the physical plan tree.
pub const PhysicalNode = union(enum) {
    // ── Sources ──────────────────────────────────────────────────────────────

    /// Full table scan over persisted ClickHouse parts (ZigHouse).
    part_scan: PartScanNode,

    /// Scan over an in-memory MemTable (ZigDB).
    mem_scan: MemScanNode,

    /// Use the output of a subquery as a source.
    chunk_source: ChunkSourceNode,

    // ── Transforms ───────────────────────────────────────────────────────────

    /// Filter rows by a predicate.
    filter: FilterNode,

    /// Evaluate the SELECT list, possibly including aggregates.
    project: ProjectNode,

    /// Limit the number of output rows.
    limit: LimitNode,

    // ── Pipeline breakers ─────────────────────────────────────────────────────

    /// GROUP BY + aggregate functions → hash aggregation.
    hash_agg: HashAggNode,

    /// Aggregate functions with no GROUP BY → scalar aggregation.
    scalar_agg: ScalarAggNode,

    /// INNER / LEFT JOIN two sub-plans → hash join.
    hash_join: HashJoinNode,

    /// ORDER BY.
    order_by: OrderByNode,

    /// ORDER BY ... LIMIT N (merge-sort top-K, can be streamed).
    top_k: TopKNode,
};

// ── Source nodes ──────────────────────────────────────────────────────────────

pub const PartScanNode = struct {
    db:    []const u8,
    table: []const u8,
    /// Column names to read (projection pushdown). Empty = read all.
    columns: [][]const u8,
    /// Optional predicate pushed into the scan.
    filter: ?ScanFilter,
};

pub const MemScanNode = struct {
    /// Logical table name for error messages.
    table: []const u8,
    /// The actual data — provided at execution time via QueryContext.
    /// (The plan is reusable; the data pointer is bound at run time.)
    table_ref: usize,  // index into QueryContext.mem_tables
};

pub const ChunkSourceNode = struct {
    /// The sub-plan whose output is used as this node's source.
    input: *PhysicalNode,
};

// ── Transform nodes ───────────────────────────────────────────────────────────

pub const FilterNode = struct {
    input:     *PhysicalNode,
    predicate: Expr,
};

pub const ProjectNode = struct {
    input: *PhysicalNode,
    items: []ProjectItem,
};

pub const LimitNode = struct {
    input:  *PhysicalNode,
    limit:  u64,
    offset: u64,
};

// ── Pipeline breaker nodes ────────────────────────────────────────────────────

pub const HashAggNode = struct {
    pub const Strategy = enum {
        auto,
        compact_int,
        single_int_count_topk,
        single_int_distinct_topk,
        pair_count,
        triple_count,
        string_key,
        string_distinct_topk,
        case_string_key_topk,
        grouped_distinct,
    };

    input:   *PhysicalNode,
    /// GROUP BY key expressions.
    keys:    []ProjectItem,
    /// Aggregate function expressions (must be agg_call Expr variants).
    aggs:    []ProjectItem,
    /// Planner-selected physical strategy. Executors still fall back to `auto`
    /// probing if a strategy's preconditions fail at runtime.
    strategy: Strategy = .auto,
};

pub const ScalarAggNode = struct {
    input: *PhysicalNode,
    /// Aggregate function expressions.
    aggs:  []ProjectItem,
};

pub const HashJoinNode = struct {
    pub const JoinType = enum { inner, left, right, full };

    left:      *PhysicalNode,
    right:     *PhysicalNode,
    join_type: JoinType,
    /// Equi-join conditions: left_col_idx == right_col_idx pairs.
    /// Non-equi conditions are handled as a post-join filter.
    equi_keys: []EquiKey,
    /// Optional post-join filter.
    filter:    ?Expr,
};

pub const EquiKey = struct {
    left_col_idx:  usize,
    right_col_idx: usize,
};

pub const OrderByNode = struct {
    input: *PhysicalNode,
    keys:  []SortKey,
};

pub const TopKNode = struct {
    input: *PhysicalNode,
    keys:  []SortKey,
    k:     u64,
    offset: u64 = 0,
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "PhysicalNode tagged union size" {
    // Ensure the union is well-formed (compilation check).
    _ = @sizeOf(PhysicalNode);
    _ = @sizeOf(Expr);
}
