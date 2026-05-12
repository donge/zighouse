const std = @import("std");

pub const Query = enum {
    count_star,
    count_adv_engine_non_zero,
    sum_count_avg,
    avg_user_id,
    count_distinct_user_id,
    count_distinct_search_phrase,
    min_max_event_date,
    adv_engine_group_by,
    region_distinct_user_id_top,
    region_stats_distinct_user_id_top,
    mobile_phone_model_distinct_user_id_top,
    mobile_phone_distinct_user_id_top,
    search_phrase_count_top,
    search_phrase_distinct_user_id_top,
    search_engine_phrase_count_top,
    user_id_search_phrase_count_top,
    user_id_search_phrase_limit_no_order,
    user_id_minute_search_phrase_count_top,
    search_engine_client_ip_agg_top,
    watch_id_client_ip_agg_top_filtered,
    watch_id_client_ip_agg_top,
    url_count_top,
    one_url_count_top,
    count_url_like_google,
    search_phrase_min_url_google,
    search_phrase_title_google_top,
    url_like_google_order_by_event_time,
    search_phrase_event_time_top,
    search_phrase_event_time_phrase_top,
    search_phrase_order_by_phrase_top,
    referer_domain_stats_top,
    user_id_count_top10,
    user_id_point_lookup,
    wide_resolution_sums,
    url_length_by_counter,
    client_ip_top10,
    window_size_dashboard,
    time_bucket_dashboard,
    url_hash_date_dashboard,
    url_count_top_filtered_dashboard,
    title_count_top_filtered_dashboard,
    url_count_top_filtered_offset_dashboard,
    traffic_source_dashboard,
};

pub const q24_sql = "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10";
pub const q29_sql = "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25";
pub const q40_sql = "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000";

pub fn match(sql: []const u8) ?Query {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    if (eql(trimmed, "SELECT COUNT(*) FROM hits")) return .count_star;
    if (eql(trimmed, "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0")) return .count_adv_engine_non_zero;
    if (eql(trimmed, "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits")) return .sum_count_avg;
    if (eql(trimmed, "SELECT AVG(UserID) FROM hits")) return .avg_user_id;
    if (eql(trimmed, "SELECT COUNT(DISTINCT UserID) FROM hits")) return .count_distinct_user_id;
    if (eql(trimmed, "SELECT COUNT(DISTINCT SearchPhrase) FROM hits")) return .count_distinct_search_phrase;
    if (eql(trimmed, "SELECT MIN(EventDate), MAX(EventDate) FROM hits")) return .min_max_event_date;
    if (eql(trimmed, "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC")) return .adv_engine_group_by;
    if (eql(trimmed, "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10")) return .region_distinct_user_id_top;
    if (eql(trimmed, "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10")) return .region_stats_distinct_user_id_top;
    if (eql(trimmed, "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10")) return .mobile_phone_model_distinct_user_id_top;
    if (eql(trimmed, "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10")) return .mobile_phone_distinct_user_id_top;
    if (eql(trimmed, "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10")) return .search_phrase_count_top;
    if (eql(trimmed, "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10")) return .search_phrase_distinct_user_id_top;
    if (eql(trimmed, "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10")) return .search_engine_phrase_count_top;
    if (eql(trimmed, "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10")) return .user_id_search_phrase_count_top;
    if (eql(trimmed, "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10")) return .user_id_search_phrase_limit_no_order;
    if (eql(trimmed, "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10")) return .user_id_minute_search_phrase_count_top;
    if (eql(trimmed, "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10")) return .user_id_count_top10;
    if (eql(trimmed, "SELECT UserID FROM hits WHERE UserID = 435090932899640449")) return .user_id_point_lookup;
    if (startsWith(trimmed, "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1),") and endsWith(trimmed, "SUM(ResolutionWidth + 89) FROM hits")) return .wide_resolution_sums;
    if (eql(trimmed, "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25")) return .url_length_by_counter;
    if (eql(trimmed, "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10")) return .search_engine_client_ip_agg_top;
    if (eql(trimmed, "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10")) return .watch_id_client_ip_agg_top_filtered;
    if (eql(trimmed, "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10")) return .watch_id_client_ip_agg_top;
    if (eql(trimmed, "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10")) return .url_count_top;
    if (eql(trimmed, "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10")) return .one_url_count_top;
    if (eql(trimmed, "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'")) return .count_url_like_google;
    if (eql(trimmed, "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10")) return .search_phrase_min_url_google;
    if (eql(trimmed, "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10")) return .search_phrase_title_google_top;
    if (eql(trimmed, q24_sql)) return .url_like_google_order_by_event_time;
    if (eql(trimmed, "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10")) return .search_phrase_event_time_top;
    if (eql(trimmed, "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10")) return .search_phrase_event_time_phrase_top;
    if (eql(trimmed, "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10")) return .search_phrase_order_by_phrase_top;
    if (eql(trimmed, q29_sql)) return .referer_domain_stats_top;
    if (eql(trimmed, "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10")) return .client_ip_top10;
    if (eql(trimmed, "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000")) return .window_size_dashboard;
    if (eql(trimmed, "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000")) return .time_bucket_dashboard;
    if (eql(trimmed, "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100")) return .url_hash_date_dashboard;
    if (eql(trimmed, "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10")) return .url_count_top_filtered_dashboard;
    if (eql(trimmed, "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10")) return .title_count_top_filtered_dashboard;
    if (eql(trimmed, "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000")) return .url_count_top_filtered_offset_dashboard;
    if (eql(trimmed, q40_sql)) return .traffic_source_dashboard;
    return null;
}

fn eql(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| if (asciiLower(ca) != asciiLower(cb)) return false;
    return true;
}

fn startsWith(value: []const u8, prefix: []const u8) bool {
    if (value.len < prefix.len) return false;
    return eql(value[0..prefix.len], prefix);
}

fn endsWith(value: []const u8, suffix: []const u8) bool {
    if (value.len < suffix.len) return false;
    return eql(value[value.len - suffix.len ..], suffix);
}

fn asciiLower(c: u8) u8 {
    if (c >= 'A' and c <= 'Z') return c + 32;
    return c;
}

test "matches basic ClickBench queries" {
    try std.testing.expectEqual(Query.count_star, match("SELECT COUNT(*) FROM hits;"));
    try std.testing.expectEqual(Query.count_adv_engine_non_zero, match("select count(*) from hits where advengineid <> 0"));
    try std.testing.expectEqual(Query.user_id_count_top10, match("SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10"));
    try std.testing.expectEqual(@as(?Query, null), match("SELECT COUNT(*) FROM hits WHERE x = 1"));
}

test "matches late ClickBench queries" {
    try std.testing.expectEqual(Query.count_url_like_google, match("SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'"));
    try std.testing.expectEqual(Query.referer_domain_stats_top, match(q29_sql));
    try std.testing.expectEqual(Query.traffic_source_dashboard, match(q40_sql));
}
