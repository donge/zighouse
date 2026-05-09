pub const ColumnType = enum {
    int16,
    int32,
    int64,
    date,
    timestamp,
    text,
    char,

    pub fn fixedWidth(self: ColumnType) ?usize {
        return switch (self) {
            .int16 => 2,
            .int32, .date => 4,
            .int64, .timestamp => 8,
            .char => 1,
            .text => null,
        };
    }

    pub fn isString(self: ColumnType) bool {
        return switch (self) {
            .text => true,
            else => false,
        };
    }
};

pub const CardinalityHint = enum {
    none,
    low,
    medium,
    high,
    mostly_unique,
};

pub const StorageHint = enum {
    auto,
    fixed_eager,
    lowcard_dict,
    medium_dict,
    highcard_dict,
    hash_only,
    lazy_source,
    derived,
};

pub const Column = struct {
    name: []const u8,
    ty: ColumnType,
    cardinality: CardinalityHint = .none,
    storage: StorageHint = .auto,
};

fn fixed(name: []const u8, ty: ColumnType) Column {
    return .{ .name = name, .ty = ty, .storage = .fixed_eager };
}

fn derived(name: []const u8, ty: ColumnType) Column {
    return .{ .name = name, .ty = ty, .storage = .derived };
}

fn text(name: []const u8, cardinality: CardinalityHint, storage: StorageHint) Column {
    return .{ .name = name, .ty = .text, .cardinality = cardinality, .storage = storage };
}

pub const hits_columns = [_]Column{
    fixed("WatchID", .int64),
    fixed("JavaEnable", .int16),
    text("Title", .high, .highcard_dict),
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
    text("URL", .high, .highcard_dict),
    text("Referer", .mostly_unique, .lazy_source),
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
    text("MobilePhoneModel", .low, .lowcard_dict),
    text("Params", .mostly_unique, .lazy_source),
    fixed("IPNetworkID", .int32),
    fixed("TraficSourceID", .int16),
    fixed("SearchEngineID", .int16),
    text("SearchPhrase", .medium, .medium_dict),
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

pub fn findColumn(name: []const u8) ?usize {
    for (hits_columns, 0..) |column, i| {
        if (asciiEqlIgnoreCase(column.name, name)) return i;
    }
    return null;
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (asciiLower(ca) != asciiLower(cb)) return false;
    }
    return true;
}

fn asciiLower(c: u8) u8 {
    if (c >= 'A' and c <= 'Z') return c + 32;
    return c;
}

test "finds columns case insensitively" {
    const std = @import("std");
    try std.testing.expectEqual(@as(?usize, 0), findColumn("watchid"));
    try std.testing.expectEqual(@as(?usize, 39), findColumn("SearchPhrase"));
    try std.testing.expectEqual(@as(?usize, null), findColumn("missing"));
}
