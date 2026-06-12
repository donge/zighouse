/// SQL AST for the zighouse native parser.
///
/// This AST is intentionally minimal: it captures the structural parts of a
/// SELECT query (projection list, FROM clause, WHERE, GROUP BY, ORDER BY,
/// LIMIT/OFFSET, UNION ALL, subqueries) and represents individual expressions
/// as a recursive Expr tree.
///
/// The plan_builder.zig then translates this AST into generic_sql.Plan.
const std = @import("std");

// ── Top-level statement ───────────────────────────────────────────────────────

pub const Stmt = union(enum) {
    select: SelectStmt,
    union_all: UnionStmt,
};

pub const UnionStmt = struct {
    left: *Stmt,
    right: *Stmt,
    all: bool, // UNION ALL vs UNION DISTINCT
};

pub const SelectStmt = struct {
    distinct: bool = false,
    projections: []Projection,
    from: ?FromClause = null,
    array_join: []ArrayJoinItem = &.{},
    where: ?*Expr = null,
    group_by: []Expr,
    having: ?*Expr = null,
    order_by: []OrderItem,
    limit: ?i64 = null,
    offset: ?i64 = null,
    ctes: []Cte = &.{},
};

pub const ArrayJoinItem = struct {
    expr: Expr,
    alias: ?[]const u8,
};

pub const Projection = struct {
    expr: Expr,
    alias: ?[]const u8, // may be null if no AS
};

pub const JoinKind = enum { inner, left, right, full };

pub const JoinClause = struct {
    kind: JoinKind,
    left: *FromClause,
    right: *FromClause,
    on: *Expr,
};

pub const FromClause = union(enum) {
    table: TableRef, // FROM table_name or db.table_name
    subquery: SubqueryFrom, // FROM (SELECT ...) AS alias
    cte_ref: []const u8, // FROM cte_name (resolved during plan build)
    numbers: i64, // FROM numbers(N) or system.numbers LIMIT N
    table_func: TableFunc, // FROM func(args) AS alias
    join: JoinClause, // FROM t1 [INNER|LEFT|RIGHT|FULL] JOIN t2 ON ...
};

pub const TableRef = struct {
    db: ?[]const u8,
    name: []const u8,
    alias: ?[]const u8,
};

pub const SubqueryFrom = struct {
    stmt: *Stmt,
    alias: ?[]const u8,
};

pub const TableFunc = struct {
    name: []const u8,
    args: []Expr,
    alias: ?[]const u8,
};

pub const Cte = struct {
    name: []const u8,
    stmt: *Stmt,
};

pub const OrderItem = struct {
    expr: Expr,
    desc: bool,
};

// ── Expressions ───────────────────────────────────────────────────────────────

pub const Expr = union(enum) {
    /// Column reference (possibly qualified: db.table.col or table.col or col).
    /// Stored as a single string with dots preserved.
    col: []const u8,
    /// Integer literal.
    int: i64,
    /// Unsigned integer literal (for values that don't fit i64).
    uint: u64,
    /// Float literal.
    float: f64,
    /// String literal — value with quotes stripped ('' escapes already decoded).
    str: []const u8,
    /// NULL literal.
    null_lit,
    /// TRUE / FALSE
    bool_lit: bool,
    /// Array literal: [e1, e2, ...]
    array: []Expr,
    /// Function call (also covers aggregate functions).
    func: FuncExpr,
    /// Binary operation (arithmetic, comparison, AND/OR, LIKE, etc.)
    binop: *BinopExpr,
    /// Unary NOT
    not: *Expr,
    /// Unary minus
    neg: *Expr,
    /// col IN (expr, expr, ...) / col NOT IN (expr, ...)
    in_list: *InListExpr,
    /// col IN (SELECT ...) / col NOT IN (SELECT ...)
    in_subq: *InSubqExpr,
    /// col BETWEEN lo AND hi
    between: *BetweenExpr,
    /// col IS NULL / col IS NOT NULL
    is_null: *IsNullExpr,
    /// CASE WHEN ... THEN ... ELSE ... END
    case_when: *CaseExpr,
    /// expr[subscript]
    subscript: *SubscriptExpr,
    /// Subquery as expression (used in IN subq and scalar subqueries)
    subquery: *Stmt,
    /// CAST(expr AS type)
    cast: *CastExpr,
    /// lambda: x -> expr or (x, y) -> expr
    lambda: *LambdaExpr,
    /// SELECT * wildcard
    star,
    /// Raw text blob — used for expressions we don't parse structurally.
    /// plan_builder emits this as a column_ref text expression.
    raw_text: []const u8,
};

pub const FuncExpr = struct {
    name: []const u8, // lowercase function name
    args: []Expr,
    distinct: bool = false,
};

pub const BinopExpr = struct {
    op: BinOp,
    left: Expr,
    right: Expr,
    /// ESCAPE character for LIKE/NOT LIKE (nil if no ESCAPE clause).
    escape: ?[]const u8 = null,
};

pub const BinOp = enum {
    // Comparisons
    eq,
    neq,
    lt,
    lte,
    gt,
    gte,
    // Logical
    and_,
    or_,
    // Arithmetic
    add,
    sub,
    mul,
    div,
    mod,
    // String
    concat,
    // Other
    like,
    not_like,
};

pub const InListExpr = struct {
    lhs: Expr,
    items: []Expr,
    negated: bool,
};

pub const InSubqExpr = struct {
    lhs: Expr,
    query: *Stmt,
    negated: bool,
};

pub const BetweenExpr = struct {
    val: Expr,
    lo: Expr,
    hi: Expr,
    negated: bool,
};

pub const IsNullExpr = struct {
    val: Expr,
    is_not: bool, // IS NOT NULL if true
};

pub const CaseExpr = struct {
    input: ?*Expr, // CASE expr WHEN ... (null = searched CASE)
    whens: []WhenClause,
    else_: ?*Expr,
};

pub const WhenClause = struct {
    cond: Expr,
    then: Expr,
};

pub const SubscriptExpr = struct {
    base: Expr,
    index: Expr,
};

pub const CastExpr = struct {
    val: Expr,
    type_name: []const u8,
};

pub const LambdaExpr = struct {
    params: [][]const u8,
    body: Expr,
};
