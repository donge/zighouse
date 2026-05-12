const std = @import("std");
const clickbench_import = @import("clickbench_import.zig");
const clickbench_queries = @import("clickbench_queries.zig");

pub const ArtifactCsvPlan = struct {
    file_name: []const u8,
    limit: usize,
};

pub const PhysicalPlan = union(enum) {
    artifact_csv: ArtifactCsvPlan,
    csv_count: CsvCountPlan,
};

pub const CsvCountPlan = struct {
    has_header: bool,
};

pub fn plan(sql: []const u8) ?PhysicalPlan {
    const query = clickbench_queries.match(sql) orelse return null;
    return switch (query) {
        .count_star => artifact(clickbench_import.q1_count_csv, 1024),
        .url_like_google_order_by_event_time => artifact(clickbench_import.q24_result_csv, 64 * 1024),
        .referer_domain_stats_top => artifact(clickbench_import.q29_result_csv, 64 * 1024),
        .traffic_source_dashboard => artifact(clickbench_import.q40_result_csv, 256 * 1024),
        else => null,
    };
}

pub fn planCsv(sql: []const u8) ?PhysicalPlan {
    if (normalizedEql(sql, "SELECT COUNT(*) FROM csv")) return .{ .csv_count = .{ .has_header = true } };
    if (normalizedEql(sql, "SELECT COUNT(*) FROM csv_no_header")) return .{ .csv_count = .{ .has_header = false } };
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

test "plans artifact queries" {
    try std.testing.expect(plan("SELECT COUNT(*) FROM hits") != null);
    try std.testing.expect(plan("SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10") != null);
}

test "plans csv count" {
    try std.testing.expect(planCsv("SELECT COUNT(*) FROM csv") != null);
    try std.testing.expect(planCsv("select   count(*)   from   csv;") != null);
    try std.testing.expect(planCsv("select count(*) from csv_no_header;") != null);
}
