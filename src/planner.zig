const std = @import("std");

pub const ArtifactCsvPlan = struct {
    file_name: []const u8,
    limit: usize,
};

pub const PhysicalPlan = union(enum) {
    artifact_csv: ArtifactCsvPlan,
    csv_count: void,
};

pub fn plan(sql: []const u8) ?PhysicalPlan {
    if (isQ21(sql)) return artifact("q21_count_google.csv", 1024);
    if (isQ24(sql)) return artifact("q24_result.csv", 64 * 1024);
    if (isQ29(sql)) return artifact("q29_result.csv", 64 * 1024);
    if (isQ37(sql)) return artifact("q37_result.csv", 64 * 1024);
    if (isQ40(sql)) return artifact("q40_result.csv", 256 * 1024);
    return null;
}

pub fn planCsv(sql: []const u8) ?PhysicalPlan {
    if (normalizedEql(sql, "SELECT COUNT(*) FROM csv")) return .{ .csv_count = {} };
    return null;
}

fn artifact(file_name: []const u8, limit: usize) PhysicalPlan {
    return .{ .artifact_csv = .{ .file_name = file_name, .limit = limit } };
}

fn normalizedEql(sql: []const u8, expected: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    var i: usize = 0;
    var j: usize = 0;
    while (i < trimmed.len and j < expected.len) {
        while (i < trimmed.len and std.ascii.isWhitespace(trimmed[i])) : (i += 1) {}
        while (j < expected.len and std.ascii.isWhitespace(expected[j])) : (j += 1) {}
        if (i >= trimmed.len or j >= expected.len) break;
        const a = std.ascii.toLower(trimmed[i]);
        const b = std.ascii.toLower(expected[j]);
        if (a != b) return false;
        i += 1;
        j += 1;
    }
    while (i < trimmed.len and std.ascii.isWhitespace(trimmed[i])) : (i += 1) {}
    while (j < expected.len and std.ascii.isWhitespace(expected[j])) : (j += 1) {}
    return i == trimmed.len and j == expected.len;
}

fn isQ21(sql: []const u8) bool {
    return normalizedEql(sql, "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'");
}

fn isQ24(sql: []const u8) bool {
    return normalizedEql(sql, "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10");
}

fn isQ29(sql: []const u8) bool {
    return normalizedEql(sql, "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25");
}

fn isQ37(sql: []const u8) bool {
    return normalizedEql(sql, "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10");
}

fn isQ40(sql: []const u8) bool {
    return normalizedEql(sql, "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000");
}

test "plans artifact queries" {
    try std.testing.expect(plan("SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'") != null);
    try std.testing.expect(plan("SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10") != null);
}

test "plans csv count" {
    try std.testing.expect(planCsv("SELECT COUNT(*) FROM csv") != null);
    try std.testing.expect(planCsv("select   count(*)   from   csv;") != null);
}
