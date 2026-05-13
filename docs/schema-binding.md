# Schema Binding And String Hints

This document describes the schema and hint model for replacing ClickBench-specific string paths with shape-driven executors. The goal is to let the planner match a query shape, bind logical columns through schema metadata, and then call a reusable executor for the bound physical representation.

The schema is split into two files:

- `src/schema.zig`: generic schema model and capability types. It contains no ClickBench column names.
- `src/clickbench/schema.zig`: ClickBench `hits` schema instance, including physical artifacts and execution hints.

Numeric columns can stay mostly type-bound for now. The main value is in strings and string-derived columns, where storage, dictionaries, hashes, late materialization, empty-string handling, and sidecars differ by column.

## Goals

- Keep execution functions generic over physical capabilities, not ClickBench column names.
- Move field-to-storage decisions into schema binding.
- Make query dispatch depend on plan shape plus schema capabilities.
- Preserve specialized paths where the operation is genuinely benchmark-specific, especially LIKE sidecars and dashboard late materialization.
- Allow additional schemas to reuse the same executors by defining equivalent hints.

## Non-Goals

- Full SQL planning.
- Replacing all exact ClickBench query recognition in one step.
- Turning every numeric column into a schema-driven abstraction immediately.
- General regex, LIKE, CASE, and arbitrary expression execution.

## Schema File Format

The schema format is data-oriented and explicit about physical bindings. It is represented as Zig data first, with a direct path to a TOML/YAML/JSON external file later if needed.

### Zig Shape

```zig
pub const ColumnType = enum {
    int16,
    int32,
    int64,
    date,
    timestamp,
    text,
};

pub const StringEncoding = enum {
    none,
    lowcard_dict,
    medium_dict,
    highcard_dict,
    hash_late_materialized,
    lazy_source,
};

pub const EmptySemantics = enum {
    none,
    stored_empty_string, // "" or zero-length dictionary value
    id_zero,
};

pub const PhysicalColumn = union(enum) {
    none,
    fixed: struct {
        path_name: []const u8,
        ty: ColumnType,
    },
    lowcard_text: struct {
        id_path_name: []const u8,
        offsets_path_name: []const u8,
        bytes_path_name: []const u8,
        id_type: ColumnType = .int32,
        empty: EmptySemantics = .stored_empty_string,
    },
    hash_text: struct {
        hash_column: []const u8,
        dict_path_name: ?[]const u8 = null,
        id_path_name: ?[]const u8 = null,
        offsets_path_name: ?[]const u8 = null,
        bytes_path_name: ?[]const u8 = null,
        empty: EmptySemantics = .stored_empty_string,
    },
    lazy_text: struct {
        source_column: []const u8,
        hash_column: ?[]const u8 = null,
        sidecar_path_name: ?[]const u8 = null,
        empty: EmptySemantics = .stored_empty_string,
    },
    derived: struct {
        from: []const u8,
        expr: DerivedExpr,
        path_name: ?[]const u8 = null,
    },
};

pub const DerivedExpr = enum {
    length,
    hash,
    event_minute,
    domain_from_url,
    date_trunc_minute,
};

pub const MaterializationHint = enum {
    fixed_hot_column,
    lowcard_dictionary,
    hash_column,
    hash_to_string_dict,
    contains_index,
    length_column,
    lazy_source_sidecar,
    domain_dictionary,
    result_sidecar,
};

pub const StringCapabilities = struct {
    count_distinct: bool = false,
    group_count_top: bool = false,
    group_distinct_user_top: bool = false,
    group_with_fixed_key: bool = false,
    order_by_value: bool = false,
    order_by_time: bool = false,
    contains_index: bool = false,
    min_value: bool = false,
    length: bool = false,
    late_materialize: bool = false,
    domain_extract: bool = false,
    conditional_materialize: bool = false,
};

pub const Column = struct {
    name: []const u8,
    ty: ColumnType,
    cardinality: CardinalityHint = .none,
    storage: StorageHint = .auto,
    string_encoding: StringEncoding = .none,
    physical: PhysicalColumn = .none,
    materialize: []const MaterializationHint = &.{},
    capabilities: StringCapabilities = .{},
};

pub const Table = struct {
    name: []const u8,
    columns: []const Column,
};
```

### ClickBench Schema Instance

The ClickBench-specific instance lives in `src/clickbench/schema.zig`. It maps logical fields to hot columns, dictionaries, hashes, and execution hints.

```zig
pub const hits_columns = [_]Column{
    .{
        .name = "SearchPhrase",
        .ty = .text,
        .string_encoding = .medium_dict,
        .physical = .{ .lowcard_text = .{
            .id_path_name = "hot_SearchPhrase.id",
            .offsets_path_name = "SearchPhrase.id_offsets.bin",
            .bytes_path_name = "SearchPhrase.id_phrases.bin",
            .empty = .stored_empty_string,
        } },
        .capabilities = .{
            .count_distinct = true,
            .group_count_top = true,
            .group_distinct_user_top = true,
            .group_with_fixed_key = true,
            .order_by_value = true,
            .order_by_time = true,
        },
    },
    .{
        .name = "MobilePhoneModel",
        .ty = .text,
        .string_encoding = .lowcard_dict,
        .physical = .{ .lowcard_text = .{
            .id_path_name = "hot_MobilePhoneModel.id",
            .offsets_path_name = "MobilePhoneModel.dict.offsets",
            .bytes_path_name = "MobilePhoneModel.dict.bytes",
            .empty = .stored_empty_string,
        } },
        .capabilities = .{
            .group_distinct_user_top = true,
            .group_with_fixed_key = true,
        },
    },
    .{
        .name = "URL",
        .ty = .text,
        .string_encoding = .hash_late_materialized,
        .physical = .{ .hash_text = .{
            .hash_column = "URLHash",
            .dict_path_name = "URL.dict.tsv",
            .id_path_name = "hot_URL.id",
            .offsets_path_name = "URL.id_offsets.bin",
            .bytes_path_name = "URL.id_strings.bin",
            .empty = .stored_empty_string,
        } },
        .capabilities = .{
            .group_count_top = true,
            .contains_index = true,
            .min_value = true,
            .length = true,
            .late_materialize = true,
        },
    },
    .{
        .name = "Title",
        .ty = .text,
        .string_encoding = .hash_late_materialized,
        .physical = .{ .hash_text = .{
            .hash_column = "TitleHash",
            .dict_path_name = "Title.dict.tsv",
            .id_path_name = "hot_Title.id",
            .offsets_path_name = "Title.id_offsets.bin",
            .bytes_path_name = "Title.id_strings.bin",
            .empty = .stored_empty_string,
        } },
        .capabilities = .{
            .contains_index = true,
            .min_value = true,
            .late_materialize = true,
        },
    },
    .{
        .name = "Referer",
        .ty = .text,
        .string_encoding = .lazy_source,
        .physical = .{ .lazy_text = .{
            .source_column = "Referer",
            .hash_column = "RefererHash",
            .sidecar_path_name = "Referer.sidecar",
            .empty = .stored_empty_string,
        } },
        .capabilities = .{
            .min_value = true,
            .length = true,
            .late_materialize = true,
            .domain_extract = true,
            .conditional_materialize = true,
        },
    },
    .{
        .name = "URLLength",
        .ty = .int32,
        .physical = .{ .derived = .{ .from = "URL", .expr = .length } },
    },
    .{
        .name = "EventMinute",
        .ty = .int32,
        .physical = .{ .derived = .{ .from = "EventTime", .expr = .event_minute } },
    },
};
```

## Binder Output Types

The binder converts logical names and expressions into physical capabilities. Executors should accept these bound types instead of raw names.

```zig
pub const BoundString = union(enum) {
    lowcard_text: struct {
        name: []const u8,
        ids: []const u32,
        dict: *const lowcard.StringColumn,
        empty: EmptySemantics,
        capabilities: StringCapabilities,
    },
    hash_text: struct {
        name: []const u8,
        hash_column: []const i64,
        resolver: HashTextResolver,
        empty: EmptySemantics,
        capabilities: StringCapabilities,
    },
    lazy_text: struct {
        name: []const u8,
        resolver: LazyTextResolver,
        empty: EmptySemantics,
        capabilities: StringCapabilities,
    },
};
```

The first implemented slice is q13: `SearchPhrase` binds as `.lowcard_text` through `src/clickbench/schema.zig`, checks `.group_count_top`, then `formatLowCardTextCountTop` executes the generic top count.

## Query Matrix From The 43 ClickBench Queries

| Queries | String Inputs | Shape | Required Binding |
|---|---|---|---|
| q6 | SearchPhrase | `COUNT(DISTINCT text)` | `lowcard_text.count_distinct` |
| q11 | MobilePhoneModel | `GROUP BY text COUNT(DISTINCT UserID)` | `lowcard_text.group_distinct_user_top` |
| q12 | MobilePhoneModel | `GROUP BY fixed, text COUNT(DISTINCT UserID)` | `lowcard_text.group_with_fixed_key`, fixed companion key |
| q13 | SearchPhrase | `GROUP BY text COUNT(*) TOP K` | `lowcard_text.group_count_top` |
| q14 | SearchPhrase | `GROUP BY text COUNT(DISTINCT UserID)` | `lowcard_text.group_distinct_user_top` |
| q15 | SearchPhrase | `GROUP BY fixed, text COUNT(*) TOP K` | `lowcard_text.group_with_fixed_key` |
| q17-q19 | SearchPhrase | `GROUP BY UserID, text` and minute variant | `lowcard_text` plus UserID encoding |
| q21 | URL | `text LIKE contains` count | `hash_text.contains_index` or string sidecar |
| q22 | URL, SearchPhrase | URL contains filter, SearchPhrase group, `MIN(URL)` | `hash_text.contains_index`, `lowcard_text`, `hash_text.min_value` |
| q23 | Title, URL, SearchPhrase | Title contains + URL not contains, `MIN(URL)`, `MIN(Title)` | contains sidecars, late materialized min |
| q24 | URL | URL contains filter, ordered rows | contains sidecar, row materialization |
| q25-q27 | SearchPhrase | non-empty filter, order by time/value | `lowcard_text.order_by_time`, `lowcard_text.order_by_value` |
| q28 | URL | `length(URL)` aggregate | derived `URLLength` |
| q29 | Referer | domain extraction, length, min | `lazy_text.domain_extract`, `length`, `min_value` |
| q31-q32 | SearchPhrase | non-empty filter for numeric tuple groups | string non-empty predicate from binding |
| q34-q35 | URL | `GROUP BY URL COUNT(*) TOP K` | `hash_text.group_count_top`, late materialize |
| q37/q39 | URL | filtered URL dashboard top/offset | `hash_text.group_count_top`, filtered executor, late materialize |
| q38 | Title | filtered title dashboard top | `hash_text.group_count_top`, filtered executor, late materialize |
| q40 | Referer, URL | conditional source and destination materialization | `lazy_text.conditional_materialize`, URL late materialize |
| q41 | URLHash | hash proxy output | fixed hash column, no text binding required |
| q42-q43 | none | numeric/derived time dashboards | no string binding |

## Executor Shape Mapping

String-related executors should be named by bound representation and operation:

- `lowcard_text_count_top`: q13 first, later any lowcard text count top.
- `lowcard_text_distinct_user_top`: q11, q14.
- `fixed_lowcard_text_count_top`: q15.
- `fixed_lowcard_text_distinct_user_top`: q12.
- `user_lowcard_text_count_top`: q17, q18.
- `user_minute_lowcard_text_count_top`: q19.
- `hash_text_count_top`: q34, q35.
- `filtered_hash_text_count_top`: q37, q38, q39.
- `text_contains_filter`: q21-q24.
- `lowcard_text_order_limit`: q25-q27.
- `lazy_text_domain_stats`: q29.
- `conditional_text_materialize`: q40.

## Binding Rules

- A query shape must not bind by hard-coded field name inside the executor.
- A dispatch matcher may still identify a ClickBench query family, but it should ask the binder for required capabilities.
- A binder failure means `UnsupportedGenericQuery`, not fallback to an unrelated executor.
- Empty-string filters should bind through `EmptySemantics`; executors should not hard-code `SearchPhrase <> ''` or `MobilePhoneModel <> ''`.
- Late materialization should be represented as a resolver capability, not embedded in aggregate kernels.

## Migration Plan

1. Keep the current q13 slice: `SearchPhrase` binds through ClickBench schema and runs `lowcard_text_count_top`.
2. Move `bindLowCardTextColumn` from `native.zig` into a dedicated `exec/bind.zig` module.
3. Use schema physical names to drive import/build artifacts instead of scattered storage constants.
4. Add `MobilePhoneModel` lowcard binding and migrate q11/q12 candidates incrementally.
5. Add `lowcard_text_distinct_user_top` for q11/q14.
6. Add `fixed_lowcard_text_count_top` for q15.
7. Add `hash_text` binding for URL/Title and route q34/q35 through it.
8. Extend `hash_text` to filtered dashboard tops q37-q39.
9. Keep q21-q24/q29/q40 as specialized string operators until basic bindings are stable.

## Open Questions

- Whether `TitleHash` should become a first-class schema column. The import path already has title hash support, but the ClickBench 105-column source schema lists `URLHash` and `RefererHash`, not `TitleHash`.
- Whether path names should stay as logical storage names resolved by `storage.zig`, or literal file names in schema.
- Whether external schema files are needed now. Zig constants are safer while the physical layout is still moving.
