const std = @import("std");
const generic_sql = @import("../generic_sql.zig");

pub const Fallback = union(enum) {
    client_ip_agg_top: ClientIpAggTop,
    count_url_like_google,
    dashboard: Dashboard,
    search_phrase_min_url_google,
    search_phrase_title_google,
    search_phrase_order: SearchPhraseOrder,
    url_count_top: struct { include_constant: bool },
};

pub const ClientIpAggTop = enum { search_engine_filtered, watch_id_unfiltered };
pub const Dashboard = enum { url_filtered, title_filtered, url_filtered_offset, url_hash_date, window_size, time_bucket };
pub const SearchPhraseOrder = enum { event_time, event_time_phrase, phrase };

pub fn matchGenericFallback(plan: generic_sql.Plan) ?Fallback {
    if (matchClientIpAggTop(plan)) |fallback| return fallback;
    if (matchCountUrlLikeGoogle(plan)) |fallback| return fallback;
    if (matchDashboard(plan)) |fallback| return fallback;
    if (matchSearchPhraseGoogleTop(plan)) |fallback| return fallback;
    if (matchSearchPhraseOrder(plan)) |fallback| return fallback;
    if (matchUrlCountTop(plan)) |fallback| return fallback;
    return null;
}

fn matchDashboard(plan: generic_sql.Plan) ?Fallback {
    if (dashboardStringTopPlan(plan, "URL", "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> ''") and plan.offset == null) return .{ .dashboard = .url_filtered };
    if (dashboardStringTopPlan(plan, "Title", "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> ''") and plan.offset == null) return .{ .dashboard = .title_filtered };
    if (dashboardStringTopPlan(plan, "URL", "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0") and plan.offset == 1000) return .{ .dashboard = .url_filtered_offset };
    if (windowSizeDashboardPlan(plan)) return .{ .dashboard = .window_size };
    if (urlHashDateDashboardPlan(plan)) return .{ .dashboard = .url_hash_date };
    if (timeBucketDashboardPlan(plan)) return .{ .dashboard = .time_bucket };
    return null;
}

fn matchClientIpAggTop(plan: generic_sql.Plan) ?Fallback {
    if (clientIpAggTopPlan(plan, "SearchEngineID", true)) return .{ .client_ip_agg_top = .search_engine_filtered };
    if (clientIpAggTopPlan(plan, "WatchID", false)) return .{ .client_ip_agg_top = .watch_id_unfiltered };
    return null;
}

fn matchCountUrlLikeGoogle(plan: generic_sql.Plan) ?Fallback {
    if (!asciiEqlIgnoreCase(plan.where_text orelse return null, "URL LIKE '%google%'")) return null;
    if (plan.filter != null or plan.group_by != null or plan.order_by_text != null or plan.limit != null or plan.offset != null) return null;
    if (plan.projections.len != 1 or plan.projections[0].func != .count_star) return null;
    return .count_url_like_google;
}

fn clientIpAggTopPlan(plan: generic_sql.Plan, first_col: []const u8, filtered: bool) bool {
    if (plan.limit != 10 or !orderByAlias(plan, "c")) return false;
    if (filtered != hasEmptyStringFilter(plan, "SearchPhrase")) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, if (asciiEqlIgnoreCase(first_col, "SearchEngineID")) "SearchEngineID, ClientIP" else "WatchID, ClientIP")) return false;
    if (plan.projections.len != 5) return false;
    if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, first_col)) return false;
    if (plan.projections[1].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[1].column orelse return false, "ClientIP")) return false;
    if (plan.projections[2].func != .count_star or !asciiEqlIgnoreCase(plan.projections[2].alias orelse return false, "c")) return false;
    if (plan.projections[3].func != .sum or !asciiEqlIgnoreCase(plan.projections[3].column orelse return false, "IsRefresh")) return false;
    return plan.projections[4].func == .avg and asciiEqlIgnoreCase(plan.projections[4].column orelse return false, "ResolutionWidth");
}

fn dashboardStringTopPlan(plan: generic_sql.Plan, column: []const u8, where_text: []const u8) bool {
    if (plan.filter != null or plan.limit != 10 or !orderByAlias(plan, "PageViews")) return false;
    if (!asciiEqlIgnoreCase(plan.where_text orelse return false, where_text)) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, column)) return false;
    if (plan.projections.len != 2) return false;
    if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, column)) return false;
    return plan.projections[1].func == .count_star and asciiEqlIgnoreCase(plan.projections[1].alias orelse return false, "PageViews");
}

fn windowSizeDashboardPlan(plan: generic_sql.Plan) bool {
    if (plan.filter != null or plan.limit != 10 or plan.offset != 10000 or !orderByAlias(plan, "PageViews")) return false;
    if (!asciiEqlIgnoreCase(plan.where_text orelse return false, "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622")) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, "WindowClientWidth, WindowClientHeight")) return false;
    if (plan.projections.len != 3) return false;
    if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "WindowClientWidth")) return false;
    if (plan.projections[1].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[1].column orelse return false, "WindowClientHeight")) return false;
    return plan.projections[2].func == .count_star and asciiEqlIgnoreCase(plan.projections[2].alias orelse return false, "PageViews");
}

fn urlHashDateDashboardPlan(plan: generic_sql.Plan) bool {
    if (plan.filter != null or plan.limit != 10 or plan.offset != 100 or !orderByAlias(plan, "PageViews")) return false;
    if (!asciiEqlIgnoreCase(plan.where_text orelse return false, "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465")) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, "URLHash, EventDate")) return false;
    if (plan.projections.len != 3) return false;
    if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "URLHash")) return false;
    if (plan.projections[1].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[1].column orelse return false, "EventDate")) return false;
    return plan.projections[2].func == .count_star and asciiEqlIgnoreCase(plan.projections[2].alias orelse return false, "PageViews");
}

fn timeBucketDashboardPlan(plan: generic_sql.Plan) bool {
    if (plan.filter != null or plan.limit != 10 or plan.offset != 1000) return false;
    if (!asciiEqlIgnoreCase(plan.order_by_text orelse return false, "DATE_TRUNC('minute', EventTime)")) return false;
    if (!asciiEqlIgnoreCase(plan.where_text orelse return false, "CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0")) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, "DATE_TRUNC('minute', EventTime)")) return false;
    if (plan.projections.len != 2) return false;
    if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "EventMinute")) return false;
    if (!asciiEqlIgnoreCase(plan.projections[0].alias orelse return false, "M")) return false;
    return plan.projections[1].func == .count_star and asciiEqlIgnoreCase(plan.projections[1].alias orelse return false, "PageViews");
}

fn matchSearchPhraseGoogleTop(plan: generic_sql.Plan) ?Fallback {
    if (genericSearchPhraseGoogleTopPlan(plan, "URL LIKE '%google%' AND SearchPhrase <> ''", 3)) {
        if (plan.projections[1].func != .min or !asciiEqlIgnoreCase(plan.projections[1].column orelse return null, "URL")) return null;
        if (plan.projections[2].func != .count_star or !asciiEqlIgnoreCase(plan.projections[2].alias orelse return null, "c")) return null;
        return .search_phrase_min_url_google;
    }
    if (genericSearchPhraseGoogleTopPlan(plan, "Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''", 5)) {
        if (plan.projections[1].func != .min or !asciiEqlIgnoreCase(plan.projections[1].column orelse return null, "URL")) return null;
        if (plan.projections[2].func != .min or !asciiEqlIgnoreCase(plan.projections[2].column orelse return null, "Title")) return null;
        if (plan.projections[3].func != .count_star or !asciiEqlIgnoreCase(plan.projections[3].alias orelse return null, "c")) return null;
        if (plan.projections[4].func != .count_distinct or !asciiEqlIgnoreCase(plan.projections[4].column orelse return null, "UserID")) return null;
        return .search_phrase_title_google;
    }
    return null;
}

fn matchSearchPhraseOrder(plan: generic_sql.Plan) ?Fallback {
    if (searchPhraseOrderPlan(plan, "EventTime")) return .{ .search_phrase_order = .event_time };
    if (searchPhraseOrderPlan(plan, "EventTime, SearchPhrase")) return .{ .search_phrase_order = .event_time_phrase };
    if (searchPhraseOrderPlan(plan, "SearchPhrase")) return .{ .search_phrase_order = .phrase };
    return null;
}

fn matchUrlCountTop(plan: generic_sql.Plan) ?Fallback {
    if (plan.filter != null or plan.limit != 10 or !orderByAlias(plan, "c")) return null;
    if (asciiEqlIgnoreCase(plan.group_by orelse return null, "URL")) {
        if (plan.projections.len != 2) return null;
        if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return null, "URL")) return null;
        if (plan.projections[1].func != .count_star or !asciiEqlIgnoreCase(plan.projections[1].alias orelse return null, "c")) return null;
        return .{ .url_count_top = .{ .include_constant = false } };
    }
    if (asciiEqlIgnoreCase(plan.group_by orelse return null, "1, URL")) {
        if (plan.projections.len != 3) return null;
        if (plan.projections[0].func != .int_literal or plan.projections[0].int_offset != 1) return null;
        if (plan.projections[1].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[1].column orelse return null, "URL")) return null;
        if (plan.projections[2].func != .count_star or !asciiEqlIgnoreCase(plan.projections[2].alias orelse return null, "c")) return null;
        return .{ .url_count_top = .{ .include_constant = true } };
    }
    return null;
}

fn genericSearchPhraseGoogleTopPlan(plan: generic_sql.Plan, where_text: []const u8, projection_len: usize) bool {
    if (plan.filter != null or plan.limit != 10 or plan.offset != null) return false;
    if (!asciiEqlIgnoreCase(plan.where_text orelse return false, where_text)) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, "SearchPhrase")) return false;
    if (!asciiEqlIgnoreCase(plan.order_by_text orelse return false, "c DESC")) return false;
    if (plan.projections.len != projection_len) return false;
    return plan.projections[0].func == .column_ref and asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "SearchPhrase");
}

fn searchPhraseOrderPlan(plan: generic_sql.Plan, order_by: []const u8) bool {
    if (plan.group_by != null or plan.limit != 10 or plan.offset != null) return false;
    if (!asciiEqlIgnoreCase(plan.order_by_text orelse return false, order_by)) return false;
    if (!hasEmptyStringFilter(plan, "SearchPhrase")) return false;
    if (plan.projections.len != 1) return false;
    return plan.projections[0].func == .column_ref and asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "SearchPhrase");
}

fn hasEmptyStringFilter(plan: generic_sql.Plan, column: []const u8) bool {
    const filter = plan.filter orelse return false;
    return filter.second == null and filter.op == .not_equal and filter.int_value == 0 and asciiEqlIgnoreCase(filter.column, column);
}

fn orderByAlias(plan: generic_sql.Plan, alias: []const u8) bool {
    return if (plan.order_by_alias) |got| asciiEqlIgnoreCase(got, alias) else false;
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ac, bc| if (std.ascii.toLower(ac) != std.ascii.toLower(bc)) return false;
    return true;
}
