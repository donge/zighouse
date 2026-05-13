const schema = @import("../schema.zig");

pub const Column = schema.Column;
pub const Table = schema.Table;

fn fixed(name: []const u8, ty: schema.ColumnType) Column {
    return .{ .name = name, .ty = ty, .storage = .fixed_eager, .physical = .{ .fixed = .{ .path_name = name, .ty = ty } }, .materialize = &.{.fixed_hot_column} };
}

fn derived(name: []const u8, ty: schema.ColumnType, from: []const u8, expr: schema.DerivedExpr) Column {
    return .{ .name = name, .ty = ty, .storage = .derived, .physical = .{ .derived = .{ .from = from, .expr = expr, .path_name = name } } };
}

fn text(name: []const u8, cardinality: schema.CardinalityHint, storage: schema.StorageHint) Column {
    return .{ .name = name, .ty = .text, .cardinality = cardinality, .storage = storage };
}

fn lowcardText(
    name: []const u8,
    cardinality: schema.CardinalityHint,
    storage: schema.StorageHint,
    encoding: schema.StringEncoding,
    id_path_name: []const u8,
    offsets_path_name: []const u8,
    bytes_path_name: []const u8,
    capabilities: schema.StringCapabilities,
) Column {
    return .{
        .name = name,
        .ty = .text,
        .cardinality = cardinality,
        .storage = storage,
        .string_encoding = encoding,
        .physical = .{ .lowcard_text = .{
            .id_path_name = id_path_name,
            .offsets_path_name = offsets_path_name,
            .bytes_path_name = bytes_path_name,
            .empty = .stored_empty_string,
        } },
        .materialize = &.{.lowcard_dictionary},
        .capabilities = capabilities,
    };
}

/// Highcard dictionary-encoded text with parallel hash sidecar
/// (URL/Title). Physical layout matches `lowcardText` but dispatch
/// prefers hashed_late_materialize_top via `capabilities.hash_sidecar`.
fn lowcardTextWithHash(
    name: []const u8,
    cardinality: schema.CardinalityHint,
    storage: schema.StorageHint,
    hash_column: []const u8,
    id_path_name: []const u8,
    offsets_path_name: []const u8,
    bytes_path_name: []const u8,
    capabilities: schema.StringCapabilities,
) Column {
    var caps = capabilities;
    caps.hash_sidecar = true;
    return .{
        .name = name,
        .ty = .text,
        .cardinality = cardinality,
        .storage = storage,
        .string_encoding = .highcard_dict,
        .physical = .{ .lowcard_text = .{
            .id_path_name = id_path_name,
            .offsets_path_name = offsets_path_name,
            .bytes_path_name = bytes_path_name,
            .empty = .stored_empty_string,
            .hash_column = hash_column,
        } },
        .materialize = &.{ .lowcard_dictionary, .hash_column },
        .capabilities = caps,
    };
}

fn hashText(
    name: []const u8,
    cardinality: schema.CardinalityHint,
    storage: schema.StorageHint,
    hash_column: []const u8,
    dict_path_name: ?[]const u8,
    id_path_name: ?[]const u8,
    offsets_path_name: ?[]const u8,
    bytes_path_name: ?[]const u8,
    capabilities: schema.StringCapabilities,
) Column {
    return .{
        .name = name,
        .ty = .text,
        .cardinality = cardinality,
        .storage = storage,
        .string_encoding = .hash_late_materialized,
        .physical = .{ .hash_text = .{
            .hash_column = hash_column,
            .dict_path_name = dict_path_name,
            .id_path_name = id_path_name,
            .offsets_path_name = offsets_path_name,
            .bytes_path_name = bytes_path_name,
            .empty = .stored_empty_string,
        } },
        .materialize = &.{ .hash_column, .hash_to_string_dict },
        .capabilities = capabilities,
    };
}

fn lazyText(name: []const u8, cardinality: schema.CardinalityHint, hash_column: ?[]const u8, sidecar_path_name: ?[]const u8, capabilities: schema.StringCapabilities) Column {
    return .{
        .name = name,
        .ty = .text,
        .cardinality = cardinality,
        .storage = .lazy_source,
        .string_encoding = .lazy_source,
        .physical = .{ .lazy_text = .{
            .source_column = name,
            .hash_column = hash_column,
            .sidecar_path_name = sidecar_path_name,
            .empty = .stored_empty_string,
        } },
        .materialize = &.{.lazy_source_sidecar},
        .capabilities = capabilities,
    };
}

pub const hits_columns = [_]Column{
    fixed("WatchID", .int64),
    fixed("JavaEnable", .int16),
    lowcardTextWithHash("Title", .high, .highcard_dict, "TitleHash", "hot_Title.id", "Title.id_offsets.bin", "Title.id_strings.bin", .{ .contains_index = true, .min_value = true, .late_materialize = true }),
    fixed("GoodEvent", .int16),
    fixed("EventTime", .timestamp),
    fixed("EventDate", .date),
    fixed("CounterID", .int32),
    fixed("ClientIP", .int32),
    fixed("RegionID", .int32),
    fixed("UserID", .int64),
    fixed("CounterClass", .int16),
    fixed("OS", .int16),
    fixed("UserAgent", .int16),
    lowcardTextWithHash("URL", .high, .highcard_dict, "URLHash", "hot_URL.id", "URL.id_offsets.bin", "URL.id_strings.bin", .{ .group_count_top = true, .contains_index = true, .min_value = true, .length = true, .late_materialize = true }),
    lazyText("Referer", .mostly_unique, "RefererHash", "Referer.sidecar", .{ .min_value = true, .length = true, .late_materialize = true, .domain_extract = true, .conditional_materialize = true }),
    fixed("IsRefresh", .int16),
    fixed("RefererCategoryID", .int16),
    fixed("RefererRegionID", .int32),
    fixed("URLCategoryID", .int16),
    fixed("URLRegionID", .int32),
    fixed("ResolutionWidth", .int16),
    fixed("ResolutionHeight", .int16),
    fixed("ResolutionDepth", .int16),
    fixed("FlashMajor", .int16),
    fixed("FlashMinor", .int16),
    text("FlashMinor2", .low, .lazy_source),
    fixed("NetMajor", .int16),
    fixed("NetMinor", .int16),
    fixed("UserAgentMajor", .int16),
    text("UserAgentMinor", .low, .lazy_source),
    fixed("CookieEnable", .int16),
    fixed("JavascriptEnable", .int16),
    fixed("IsMobile", .int16),
    fixed("MobilePhone", .int16),
    lowcardText("MobilePhoneModel", .low, .lowcard_dict, .lowcard_dict, "hot_MobilePhoneModel.id", "MobilePhoneModel.dict.offsets", "MobilePhoneModel.dict.bytes", .{ .group_distinct_user_top = true, .group_with_fixed_key = true }),
    text("Params", .mostly_unique, .lazy_source),
    fixed("IPNetworkID", .int32),
    fixed("TraficSourceID", .int16),
    fixed("SearchEngineID", .int16),
    lowcardText("SearchPhrase", .medium, .medium_dict, .medium_dict, "hot_SearchPhrase.id", "SearchPhrase.id_offsets.bin", "SearchPhrase.id_phrases.bin", .{ .count_distinct = true, .group_count_top = true, .group_distinct_user_top = true, .group_with_fixed_key = true, .order_by_value = true, .order_by_time = true }),
    fixed("AdvEngineID", .int16),
    fixed("IsArtifical", .int16),
    fixed("WindowClientWidth", .int16),
    fixed("WindowClientHeight", .int16),
    fixed("ClientTimeZone", .int16),
    fixed("ClientEventTime", .timestamp),
    fixed("SilverlightVersion1", .int16),
    fixed("SilverlightVersion2", .int16),
    fixed("SilverlightVersion3", .int32),
    fixed("SilverlightVersion4", .int16),
    text("PageCharset", .low, .lazy_source),
    fixed("CodeVersion", .int32),
    fixed("IsLink", .int16),
    fixed("IsDownload", .int16),
    fixed("IsNotBounce", .int16),
    fixed("FUniqID", .int64),
    text("OriginalURL", .mostly_unique, .lazy_source),
    fixed("HID", .int32),
    fixed("IsOldCounter", .int16),
    fixed("IsEvent", .int16),
    fixed("IsParameter", .int16),
    fixed("DontCountHits", .int16),
    fixed("WithHash", .int16),
    fixed("HitColor", .char),
    fixed("LocalEventTime", .timestamp),
    fixed("Age", .int16),
    fixed("Sex", .int16),
    fixed("Income", .int16),
    fixed("Interests", .int16),
    fixed("Robotness", .int16),
    fixed("RemoteIP", .int32),
    fixed("WindowName", .int32),
    fixed("OpenerName", .int32),
    fixed("HistoryLength", .int16),
    text("BrowserLanguage", .low, .lazy_source),
    text("BrowserCountry", .low, .lazy_source),
    text("SocialNetwork", .low, .lazy_source),
    text("SocialAction", .low, .lazy_source),
    fixed("HTTPError", .int16),
    fixed("SendTiming", .int32),
    fixed("DNSTiming", .int32),
    fixed("ConnectTiming", .int32),
    fixed("ResponseStartTiming", .int32),
    fixed("ResponseEndTiming", .int32),
    fixed("FetchTiming", .int32),
    fixed("SocialSourceNetworkID", .int16),
    text("SocialSourcePage", .high, .lazy_source),
    fixed("ParamPrice", .int64),
    text("ParamOrderID", .high, .lazy_source),
    text("ParamCurrency", .low, .lazy_source),
    fixed("ParamCurrencyID", .int16),
    text("OpenstatServiceName", .medium, .lazy_source),
    text("OpenstatCampaignID", .high, .lazy_source),
    text("OpenstatAdID", .high, .lazy_source),
    text("OpenstatSourceID", .high, .lazy_source),
    text("UTMSource", .medium, .lazy_source),
    text("UTMMedium", .medium, .lazy_source),
    text("UTMCampaign", .high, .lazy_source),
    text("UTMContent", .high, .lazy_source),
    text("UTMTerm", .high, .lazy_source),
    text("FromTag", .medium, .lazy_source),
    fixed("HasGCLID", .int16),
    fixed("RefererHash", .int64),
    fixed("URLHash", .int64),
    fixed("CLID", .int32),
};

pub const hits = Table{
    .name = "hits",
    .columns = &hits_columns,
};

pub fn findColumn(name: []const u8) ?usize {
    return hits.findColumn(name);
}

test "finds ClickBench columns case insensitively" {
    const std = @import("std");
    try std.testing.expectEqual(@as(?usize, 0), findColumn("watchid"));
    try std.testing.expectEqual(@as(?usize, 0), hits.findColumn("watchid"));
    try std.testing.expectEqual(@as(?usize, 39), findColumn("SearchPhrase"));
    try std.testing.expectEqual(@as(?usize, null), findColumn("missing"));
}

test "ClickBench column count" {
    const std = @import("std");
    try std.testing.expectEqual(@as(usize, 105), hits.columns.len);
}
