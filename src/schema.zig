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

pub const Column = struct {
    name: []const u8,
    ty: ColumnType,
};

pub const hits_columns = [_]Column{
    .{ .name = "WatchID", .ty = .int64 },
    .{ .name = "JavaEnable", .ty = .int16 },
    .{ .name = "Title", .ty = .text },
    .{ .name = "GoodEvent", .ty = .int16 },
    .{ .name = "EventTime", .ty = .timestamp },
    .{ .name = "EventDate", .ty = .date },
    .{ .name = "CounterID", .ty = .int32 },
    .{ .name = "ClientIP", .ty = .int32 },
    .{ .name = "RegionID", .ty = .int32 },
    .{ .name = "UserID", .ty = .int64 },
    .{ .name = "CounterClass", .ty = .int16 },
    .{ .name = "OS", .ty = .int16 },
    .{ .name = "UserAgent", .ty = .int16 },
    .{ .name = "URL", .ty = .text },
    .{ .name = "Referer", .ty = .text },
    .{ .name = "IsRefresh", .ty = .int16 },
    .{ .name = "RefererCategoryID", .ty = .int16 },
    .{ .name = "RefererRegionID", .ty = .int32 },
    .{ .name = "URLCategoryID", .ty = .int16 },
    .{ .name = "URLRegionID", .ty = .int32 },
    .{ .name = "ResolutionWidth", .ty = .int16 },
    .{ .name = "ResolutionHeight", .ty = .int16 },
    .{ .name = "ResolutionDepth", .ty = .int16 },
    .{ .name = "FlashMajor", .ty = .int16 },
    .{ .name = "FlashMinor", .ty = .int16 },
    .{ .name = "FlashMinor2", .ty = .text },
    .{ .name = "NetMajor", .ty = .int16 },
    .{ .name = "NetMinor", .ty = .int16 },
    .{ .name = "UserAgentMajor", .ty = .int16 },
    .{ .name = "UserAgentMinor", .ty = .text },
    .{ .name = "CookieEnable", .ty = .int16 },
    .{ .name = "JavascriptEnable", .ty = .int16 },
    .{ .name = "IsMobile", .ty = .int16 },
    .{ .name = "MobilePhone", .ty = .int16 },
    .{ .name = "MobilePhoneModel", .ty = .text },
    .{ .name = "Params", .ty = .text },
    .{ .name = "IPNetworkID", .ty = .int32 },
    .{ .name = "TraficSourceID", .ty = .int16 },
    .{ .name = "SearchEngineID", .ty = .int16 },
    .{ .name = "SearchPhrase", .ty = .text },
    .{ .name = "AdvEngineID", .ty = .int16 },
    .{ .name = "IsArtifical", .ty = .int16 },
    .{ .name = "WindowClientWidth", .ty = .int16 },
    .{ .name = "WindowClientHeight", .ty = .int16 },
    .{ .name = "ClientTimeZone", .ty = .int16 },
    .{ .name = "ClientEventTime", .ty = .timestamp },
    .{ .name = "SilverlightVersion1", .ty = .int16 },
    .{ .name = "SilverlightVersion2", .ty = .int16 },
    .{ .name = "SilverlightVersion3", .ty = .int32 },
    .{ .name = "SilverlightVersion4", .ty = .int16 },
    .{ .name = "PageCharset", .ty = .text },
    .{ .name = "CodeVersion", .ty = .int32 },
    .{ .name = "IsLink", .ty = .int16 },
    .{ .name = "IsDownload", .ty = .int16 },
    .{ .name = "IsNotBounce", .ty = .int16 },
    .{ .name = "FUniqID", .ty = .int64 },
    .{ .name = "OriginalURL", .ty = .text },
    .{ .name = "HID", .ty = .int32 },
    .{ .name = "IsOldCounter", .ty = .int16 },
    .{ .name = "IsEvent", .ty = .int16 },
    .{ .name = "IsParameter", .ty = .int16 },
    .{ .name = "DontCountHits", .ty = .int16 },
    .{ .name = "WithHash", .ty = .int16 },
    .{ .name = "HitColor", .ty = .char },
    .{ .name = "LocalEventTime", .ty = .timestamp },
    .{ .name = "Age", .ty = .int16 },
    .{ .name = "Sex", .ty = .int16 },
    .{ .name = "Income", .ty = .int16 },
    .{ .name = "Interests", .ty = .int16 },
    .{ .name = "Robotness", .ty = .int16 },
    .{ .name = "RemoteIP", .ty = .int32 },
    .{ .name = "WindowName", .ty = .int32 },
    .{ .name = "OpenerName", .ty = .int32 },
    .{ .name = "HistoryLength", .ty = .int16 },
    .{ .name = "BrowserLanguage", .ty = .text },
    .{ .name = "BrowserCountry", .ty = .text },
    .{ .name = "SocialNetwork", .ty = .text },
    .{ .name = "SocialAction", .ty = .text },
    .{ .name = "HTTPError", .ty = .int16 },
    .{ .name = "SendTiming", .ty = .int32 },
    .{ .name = "DNSTiming", .ty = .int32 },
    .{ .name = "ConnectTiming", .ty = .int32 },
    .{ .name = "ResponseStartTiming", .ty = .int32 },
    .{ .name = "ResponseEndTiming", .ty = .int32 },
    .{ .name = "FetchTiming", .ty = .int32 },
    .{ .name = "SocialSourceNetworkID", .ty = .int16 },
    .{ .name = "SocialSourcePage", .ty = .text },
    .{ .name = "ParamPrice", .ty = .int64 },
    .{ .name = "ParamOrderID", .ty = .text },
    .{ .name = "ParamCurrency", .ty = .text },
    .{ .name = "ParamCurrencyID", .ty = .int16 },
    .{ .name = "OpenstatServiceName", .ty = .text },
    .{ .name = "OpenstatCampaignID", .ty = .text },
    .{ .name = "OpenstatAdID", .ty = .text },
    .{ .name = "OpenstatSourceID", .ty = .text },
    .{ .name = "UTMSource", .ty = .text },
    .{ .name = "UTMMedium", .ty = .text },
    .{ .name = "UTMCampaign", .ty = .text },
    .{ .name = "UTMContent", .ty = .text },
    .{ .name = "UTMTerm", .ty = .text },
    .{ .name = "FromTag", .ty = .text },
    .{ .name = "HasGCLID", .ty = .int16 },
    .{ .name = "RefererHash", .ty = .int64 },
    .{ .name = "URLHash", .ty = .int64 },
    .{ .name = "CLID", .ty = .int32 },
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
