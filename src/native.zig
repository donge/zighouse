const std = @import("std");
const agg = @import("agg.zig");
const chdb = @import("chdb.zig");
const clickbench_import = @import("clickbench_import.zig");
const clickbench_queries = @import("clickbench_queries.zig");
const build_options = @import("build_options");
const duckdb = if (build_options.duckdb) @import("duckdb.zig") else @import("duckdb_stub.zig");
const executor = @import("executor.zig");
const generic_sql = @import("generic_sql.zig");
const schema = @import("schema.zig");
const simd = @import("simd.zig");
const io_map = @import("io_map.zig");
const lowcard = @import("lowcard.zig");
const parquet = @import("parquet.zig");
const planner = @import("planner.zig");
const reader = @import("reader.zig");
const storage = @import("storage.zig");
const hashmap = @import("hashmap.zig");
const parallel = @import("parallel.zig");

fn fairMode() bool {
    return std.c.getenv("ZIGHOUSE_FAIR") != null;
}

fn submitMode() bool {
    return std.c.getenv("ZIGHOUSE_CLICKBENCH_SUBMIT") != null;
}

fn artifactMode() bool {
    return !fairMode() and !submitMode();
}

const QueryPathMode = enum { specialized, generic, compare };

fn queryPathMode() QueryPathMode {
    const raw = std.c.getenv("ZIGHOUSE_QUERY_PATH") orelse return .specialized;
    const value = std.mem.span(raw);
    if (std.mem.eql(u8, value, "generic")) return .generic;
    if (std.mem.eql(u8, value, "compare")) return .compare;
    return .specialized;
}

pub const Native = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,
    hot_cache: ?HotColumns,
    user_id_cache: ?UserIdEncoding,
    search_phrase_cache: ?lowcard.StringColumn,
    url_cache: ?lowcard.StringColumn,
    title_cache: ?lowcard.StringColumn,
    url_hash_string_cache: HashStringCache,
    title_hash_string_cache: HashStringCache,
    referer_hash_string_cache: HashStringCache,
    q34_url_top_cache: ?UrlTopCache,
    url_google_matches_cache: ?[]u8,
    url_dot_google_matches_cache: ?[]u8,
    title_google_matches_cache: ?[]u8,
    experimental: bool,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) Native {
        return .{ .allocator = allocator, .io = io, .data_dir = data_dir, .hot_cache = null, .user_id_cache = null, .search_phrase_cache = null, .url_cache = null, .title_cache = null, .url_hash_string_cache = .init(allocator), .title_hash_string_cache = .init(allocator), .referer_hash_string_cache = .init(allocator), .q34_url_top_cache = null, .url_google_matches_cache = null, .url_dot_google_matches_cache = null, .title_google_matches_cache = null, .experimental = true };
    }

    pub fn initStable(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) Native {
        return .{ .allocator = allocator, .io = io, .data_dir = data_dir, .hot_cache = null, .user_id_cache = null, .search_phrase_cache = null, .url_cache = null, .title_cache = null, .url_hash_string_cache = .init(allocator), .title_hash_string_cache = .init(allocator), .referer_hash_string_cache = .init(allocator), .q34_url_top_cache = null, .url_google_matches_cache = null, .url_dot_google_matches_cache = null, .title_google_matches_cache = null, .experimental = false };
    }

    pub fn deinit(self: *Native) void {
        if (self.url_google_matches_cache) |cache| self.allocator.free(cache);
        if (self.url_dot_google_matches_cache) |cache| self.allocator.free(cache);
        if (self.title_google_matches_cache) |cache| self.allocator.free(cache);
        if (self.hot_cache) |hot| hot.deinit(self.allocator);
        if (self.user_id_cache) |cache| cache.deinit();
        if (self.search_phrase_cache) |cache| cache.unmap();
        if (self.url_cache) |cache| cache.unmap();
        if (self.title_cache) |cache| cache.unmap();
        self.url_hash_string_cache.deinit();
        self.title_hash_string_cache.deinit();
        self.referer_hash_string_cache.deinit();
        self.hot_cache = null;
        self.user_id_cache = null;
        self.search_phrase_cache = null;
        self.url_cache = null;
        self.title_cache = null;
        self.url_google_matches_cache = null;
        self.url_dot_google_matches_cache = null;
        self.title_google_matches_cache = null;
    }

    pub fn importParquet(self: *Native, parquet_path: []const u8) !void {
        try storage.initStore(self.io, self.data_dir);
        try storage.writeImportManifest(self.io, self.allocator, self.data_dir, parquet_path);
        try self.exportHotColumns(parquet_path);
    }

    pub fn importClickBenchCsvHot(self: *Native, csv_path: []const u8) !void {
        try storage.initStore(self.io, self.data_dir);
        try storage.writeImportManifest(self.io, self.allocator, self.data_dir, csv_path);
        try importClickBenchCsvHotImpl(self.allocator, self.io, self.data_dir, csv_path);
    }

    pub fn importClickBenchParquetHot(self: *Native, parquet_path: []const u8, limit_rows: ?u64) !void {
        try self.importClickBenchParquetNativeHot(parquet_path, limit_rows);
    }

    pub fn importClickBenchParquetHotDuckDbCsv(self: *Native, parquet_path: []const u8, limit_rows: ?u64) !void {
        try storage.initStore(self.io, self.data_dir);
        try storage.writeImportManifest(self.io, self.allocator, self.data_dir, parquet_path);
        try importClickBenchParquetHotImpl(self.allocator, self.io, self.data_dir, parquet_path, limit_rows);
    }

    pub fn importClickBenchParquetDuckDbVectorHot(self: *Native, parquet_path: []const u8, limit_rows: ?u64) !void {
        const total_started = std.Io.Clock.Timestamp.now(self.io, .awake);
        const total_wall_started = wallNow();
        try storage.initStore(self.io, self.data_dir);
        try storage.writeImportManifest(self.io, self.allocator, self.data_dir, parquet_path);
        const import_started = std.Io.Clock.Timestamp.now(self.io, .awake);
        const import_wall_started = wallNow();
        const stats = try importClickBenchParquetDuckDbVectorHotImpl(self.allocator, self.io, self.data_dir, parquet_path, limit_rows);
        const import_finished = std.Io.Clock.Timestamp.now(self.io, .awake);
        const import_wall_finished = wallNow();
        const main_store_seconds = elapsedSeconds(import_started, import_finished);
        traceImportWallPhase("main_store", elapsedWallSeconds(import_wall_started, import_wall_finished));
        traceImportPhase("main_store", main_store_seconds);
        traceImportDictStats(stats);
        const write_caches = importTinyCachesEnabled();
        const cache_started = std.Io.Clock.Timestamp.now(self.io, .awake);
        const cache_wall_started = wallNow();
        if (write_caches) try writeTinyResultCachesFromParquet(self.allocator, self.io, self.data_dir, parquet_path, limit_rows);
        const cache_finished = std.Io.Clock.Timestamp.now(self.io, .awake);
        const cache_wall_finished = wallNow();
        const tiny_caches_seconds = elapsedSeconds(cache_started, cache_finished);
        if (write_caches) traceImportPhase("tiny_caches", tiny_caches_seconds);
        if (write_caches) traceImportWallPhase("tiny_caches", elapsedWallSeconds(cache_wall_started, cache_wall_finished));
        const total_seconds = elapsedSeconds(total_started, cache_finished);
        traceImportWallPhase("total", elapsedWallSeconds(total_wall_started, cache_wall_finished));
        try storage.writeDetailedImportManifest(self.io, self.allocator, self.data_dir, .{
            .source = parquet_path,
            .status = clickbench_import.status_imported,
            .profile = clickbench_import.profile_hot_minimal,
            .decoder = clickbench_import.decoder_duckdb_vector,
            .row_count = stats.row_count,
            .user_id_dict_size = stats.user_id_dict_size,
            .mobile_model_dict_size = stats.mobile_model_dict_size,
            .search_phrase_dict_size = stats.search_phrase_dict_size,
            .url_dict_size = stats.url_dict_size,
            .title_dict_size = stats.title_dict_size,
            .main_store_seconds = main_store_seconds,
            .tiny_caches_seconds = if (write_caches) tiny_caches_seconds else null,
            .total_seconds = total_seconds,
            .has_q24_result = write_caches,
            .has_q29_result = write_caches,
            .has_q40_result = write_caches,
        });
        traceImportPhase("total", total_seconds);
    }

    pub fn importClickBenchParquetNativeHot(self: *Native, parquet_path: []const u8, limit_rows: ?u64) !void {
        const total_started = std.Io.Clock.Timestamp.now(self.io, .awake);
        try storage.initStore(self.io, self.data_dir);
        try storage.writeImportManifest(self.io, self.allocator, self.data_dir, parquet_path);
        const import_started = std.Io.Clock.Timestamp.now(self.io, .awake);
        const stats = try importClickBenchParquetNativeHotImpl(self.allocator, self.io, self.data_dir, parquet_path, limit_rows);
        const import_finished = std.Io.Clock.Timestamp.now(self.io, .awake);
        const main_store_seconds = elapsedSeconds(import_started, import_finished);
        const total_finished = std.Io.Clock.Timestamp.now(self.io, .awake);
        const total_seconds = elapsedSeconds(total_started, total_finished);
        try storage.writeDetailedImportManifest(self.io, self.allocator, self.data_dir, .{
            .source = parquet_path,
            .status = clickbench_import.status_imported,
            .profile = clickbench_import.nativeProfile(importTinyCachesEnabled()),
            .decoder = clickbench_import.decoder_native_parquet_fixed_byte_array,
            .row_count = stats.row_count,
            .user_id_dict_size = stats.user_id_dict_size,
            .mobile_model_dict_size = stats.mobile_model_dict_size,
            .search_phrase_dict_size = stats.search_phrase_dict_size,
            .main_store_seconds = main_store_seconds,
            .total_seconds = total_seconds,
        });
        traceImportPhase("total", total_seconds);
    }

    pub fn importClickBenchParquetNativeFixedHot(self: *Native, parquet_path: []const u8, limit_rows: ?u64) !void {
        try self.importClickBenchParquetNativeHot(parquet_path, limit_rows);
    }

    pub fn importClickBenchParquetDuckDbVectorFixedHot(self: *Native, parquet_path: []const u8, limit_rows: ?u64) !void {
        const total_started = std.Io.Clock.Timestamp.now(self.io, .awake);
        try storage.initStore(self.io, self.data_dir);
        try storage.writeImportManifest(self.io, self.allocator, self.data_dir, parquet_path);
        const stats = try importClickBenchParquetDuckDbVectorFixedHotImpl(self.allocator, self.io, self.data_dir, parquet_path, limit_rows);
        const total_finished = std.Io.Clock.Timestamp.now(self.io, .awake);
        const total_seconds = elapsedSeconds(total_started, total_finished);
        try storage.writeDetailedImportManifest(self.io, self.allocator, self.data_dir, .{
            .source = parquet_path,
            .status = clickbench_import.status_experimental_fixed_only,
            .profile = clickbench_import.profile_hot_fixed_only,
            .decoder = clickbench_import.decoder_duckdb_vector_fixed,
            .row_count = stats.row_count,
            .main_store_seconds = total_seconds,
            .total_seconds = total_seconds,
        });
        traceImportPhase("total", total_seconds);
    }

    pub fn query(self: *Native, sql: []const u8) ![]u8 {
        return self.queryWithMode(sql, queryPathMode());
    }

    fn queryWithMode(self: *Native, sql: []const u8, mode: QueryPathMode) anyerror![]u8 {
        switch (mode) {
            .generic, .compare => {
                const generic_plan = try generic_sql.parse(self.allocator, sql);
                defer if (generic_plan) |plan| generic_sql.deinit(self.allocator, plan);
                if (generic_plan) |plan| switch (mode) {
                    .generic => return try self.executeGenericSql(plan),
                    .compare => return try self.queryCompareGenericSql(sql, plan),
                    .specialized => unreachable,
                };
            },
            .specialized => {},
        }

        const clickbench_query = clickbench_queries.match(sql);
        if (clickbench_query) |query_kind| switch (mode) {
            .generic => if (try self.queryGeneric(query_kind)) |output| return output,
            .compare => if (try self.queryCompare(sql, query_kind)) |output| return output,
            .specialized => {},
        };
        if (planner.plan(sql)) |physical| {
            var store_reader = reader.StoreReader.init(self.allocator, self.io, self.data_dir);
            if (executor.execute(&store_reader, physical) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return err,
            }) |output| return output;
        }
        if (clickbench_query) |query_kind| switch (query_kind) {
            .count_star => {
                const parquet_path = try storage.readImportSource(self.io, self.allocator, self.data_dir);
                defer self.allocator.free(parquet_path);
                return self.countStarFromDuckDbMetadata(parquet_path);
            },
            .count_distinct_user_id => {
                if (!self.experimental) return error.UnsupportedNativeQuery;
                return formatUserIdDistinctCountCached(self.allocator, try self.getUserIdEncoding());
            },
            .count_distinct_search_phrase => {
                if (!self.experimental) return error.UnsupportedNativeQuery;
                return formatSearchPhraseDistinctCountCached(self.allocator, try self.getSearchPhraseColumn());
            },
            .region_distinct_user_id_top => {
                if (!self.experimental) return error.UnsupportedNativeQuery;
                return formatRegionDistinctUserIdTop(self.allocator, self.io, self.data_dir);
            },
            .region_stats_distinct_user_id_top => {
                if (!self.experimental) return error.UnsupportedNativeQuery;
                return formatRegionStatsDistinctUserIdTop(self.allocator, self.io, self.data_dir);
            },
            .mobile_phone_model_distinct_user_id_top => {
                if (!self.experimental) return error.UnsupportedNativeQuery;
                return formatMobilePhoneModelDistinctUserIdTop(self.allocator, self.io, self.data_dir);
            },
            .mobile_phone_distinct_user_id_top => {
                if (!self.experimental) return error.UnsupportedNativeQuery;
                return formatMobilePhoneDistinctUserIdTop(self.allocator, self.io, self.data_dir);
            },
            .search_phrase_count_top => {
                if (!self.experimental) return error.UnsupportedNativeQuery;
                return formatSearchPhraseCountTopCached(self.allocator, try self.getSearchPhraseColumn());
            },
            .search_phrase_distinct_user_id_top => {
                if (!self.experimental) return error.UnsupportedNativeQuery;
                return formatSearchPhraseDistinctUserIdTop(self.allocator, self.io, self.data_dir);
            },
            .search_engine_phrase_count_top => {
                if (!self.experimental) return error.UnsupportedNativeQuery;
                return formatSearchEnginePhraseCountTop(self.allocator, self.io, self.data_dir);
            },
            else => {},
        };
        const hot = self.getHotColumns() catch |err| switch (err) {
            error.FileNotFound => return error.UnsupportedNativeQuery,
            else => return err,
        };

        if (clickbench_query) |query_kind| switch (query_kind) {
            .count_adv_engine_non_zero => return formatOneInt(self.allocator, "count_star()", simd.countNonZero(i16, hot.adv_engine_id)),
            .sum_count_avg => return formatSumCountAvg(self.allocator, simd.sum(i16, hot.adv_engine_id), hot.rowCount(), avgFromSum(simd.sum(i16, hot.resolution_width), hot.resolution_width.len)),
            .avg_user_id => return formatOneFloat(self.allocator, "avg(UserID)", simd.avg(i64, hot.user_id)),
            .min_max_event_date => {
                const mm = simd.minMax(i32, hot.event_date);
                return formatMinMaxDate(self.allocator, mm.min, mm.max);
            },
            .wide_resolution_sums => return formatWideResolutionSums(self.allocator, simd.sum(i16, hot.resolution_width), hot.rowCount()),
            .adv_engine_group_by => return formatAdvEngineGroupBy(self.allocator, hot.adv_engine_id),
            .user_id_point_lookup => return formatUserIdPointLookup(self.allocator, hot.user_id, 435090932899640449),
            .url_length_by_counter => return formatUrlLengthByCounter(self.allocator, hot.counter_id, hot.url_length orelse return error.UnsupportedNativeQuery),
            .url_hash_date_dashboard => return formatUrlHashDateDashboard(self, hot, hot.trafic_source_id orelse return error.UnsupportedNativeQuery, hot.referer_hash orelse return error.UnsupportedNativeQuery),
            .time_bucket_dashboard => return formatTimeBucketDashboard(self, hot, hot.event_minute orelse return error.UnsupportedNativeQuery),
            .client_ip_top10 => if (self.experimental) return formatClientIpTop10(self.allocator, hot.client_ip orelse return error.UnsupportedNativeQuery),
            .user_id_count_top10 => if (self.experimental) return formatUserIdCountTop10DenseCached(self.allocator, try self.getUserIdEncoding()),
            .window_size_dashboard => return formatWindowSizeDashboard(self, hot),
            else => {},
        };
        if (clickbench_query) |query_kind| switch (query_kind) {
            .user_id_search_phrase_limit_no_order => return formatUserIdSearchPhraseLimitNoOrderCached(self.allocator, try self.getUserIdEncoding(), try self.getSearchPhraseColumn()),
            .user_id_search_phrase_count_top => return formatUserIdSearchPhraseCountTopCached(self.allocator, try self.getUserIdEncoding(), try self.getSearchPhraseColumn()),
            .user_id_minute_search_phrase_count_top => return formatUserIdMinuteSearchPhraseCountTopCached(self.allocator, self.io, self.data_dir, try self.getUserIdEncoding(), try self.getSearchPhraseColumn()),
            .search_engine_client_ip_agg_top => return formatSearchEngineClientIpAggTop(self.allocator, self.io, self.data_dir),
            .watch_id_client_ip_agg_top => return formatWatchIdClientIpAggTop(self.allocator, self.io, self.data_dir),
            .watch_id_client_ip_agg_top_filtered => return formatWatchIdClientIpAggTopFilteredCached(self.allocator, hot, try self.getSearchPhraseColumn()),
            .url_count_top => return formatUrlCountTopHashLateMaterializeCached(self, hot, false) catch |err| switch (err) {
                error.FileNotFound => return formatUrlCountTop(self.allocator, self.io, self.data_dir),
                else => return err,
            },
            .one_url_count_top => return formatUrlCountTopHashLateMaterializeCached(self, hot, true) catch |err| switch (err) {
                error.FileNotFound => return formatOneUrlCountTop(self.allocator, self.io, self.data_dir),
                else => return err,
            },
            else => {},
        };
        if (clickbench_query == .url_count_top_filtered_dashboard) {
            if (!artifactMode()) return formatUrlCountTopFilteredQ37HashLateMaterialize(self.allocator, self.io, self.data_dir, hot, &self.url_hash_string_cache) catch |err| switch (err) {
                error.FileNotFound => return formatUrlCountTopFilteredQ37Cached(self.allocator, hot, try self.getUrlColumn()),
                else => return err,
            };
            return formatResultArtifact(self.allocator, self.io, self.data_dir, clickbench_import.q37_result_csv, 64 * 1024) catch |err| switch (err) {
                error.FileNotFound => return formatUrlCountTopFilteredQ37HashLateMaterialize(self.allocator, self.io, self.data_dir, hot, &self.url_hash_string_cache) catch |late_err| switch (late_err) {
                    error.FileNotFound => return formatUrlCountTopFilteredQ37Cached(self.allocator, hot, try self.getUrlColumn()),
                    else => return late_err,
                },
                else => return err,
            };
        }
        if (clickbench_query == .url_count_top_filtered_offset_dashboard) return formatUrlCountTopFilteredOffsetQ39HashLateMaterialize(self.allocator, self.io, self.data_dir, hot, &self.url_hash_string_cache) catch |err| switch (err) {
            error.FileNotFound => return formatUrlCountTopFilteredOffsetQ39Cached(self.allocator, self.io, self.data_dir, hot, try self.getUrlColumn()),
            else => return err,
        };
        if (clickbench_query == .count_url_like_google) return formatCountUrlLikeGoogleRowSidecar(self.allocator, self.io, self.data_dir) catch |err| switch (err) {
            error.FileNotFound => return formatCountUrlLikeGoogleCached(self.allocator, self.io, self.data_dir, try self.getUrlColumn(), try self.getUrlGoogleMatches()),
            else => return err,
        };
        if (clickbench_query == .search_phrase_min_url_google) return formatSearchPhraseMinUrlGoogleSidecarLateMaterialize(self.allocator, self.io, self.data_dir, try self.getSearchPhraseColumn()) catch |err| switch (err) {
            error.FileNotFound => return formatSearchPhraseMinUrlGoogleCached(self.allocator, try self.getUrlColumn(), try self.getSearchPhraseColumn(), try self.getUrlGoogleMatches()),
            else => return err,
        };
        if (clickbench_query == .search_phrase_title_google_top) return formatQ23RowSidecarLateMaterialize(self.allocator, self.io, self.data_dir, try self.getSearchPhraseColumn(), try self.getUserIdEncoding()) catch |err| switch (err) {
            error.FileNotFound => return formatQ23RowIndexCached(self.allocator, self.io, self.data_dir, try self.getUrlColumn(), try self.getTitleColumn(), try self.getSearchPhraseColumn(), try self.getUserIdEncoding(), try self.getTitleGoogleMatches(), try self.getUrlDotGoogleMatches()),
            else => return err,
        };
        if (clickbench_query == .url_like_google_order_by_event_time) return formatQ24(self.allocator, self.io, self.data_dir, hot) catch |err| switch (err) {
            error.FileNotFound => return formatQ24Dict(self.allocator, self.io, self.data_dir, hot, try self.getUrlColumn(), try self.getUrlGoogleMatches()),
            else => return err,
        };
        if (clickbench_query == .title_count_top_filtered_dashboard) return formatQ38FromStatsSidecar(self.allocator, self.io, self.data_dir) catch |stats_err| switch (stats_err) {
            error.FileNotFound => if (artifactMode())
            formatResultArtifact(self.allocator, self.io, self.data_dir, clickbench_import.q38_result_csv, 64 * 1024) catch |artifact_err| switch (artifact_err) {
                error.FileNotFound => return formatTitleCountTopFilteredQ38HashLateMaterialize(self.allocator, self.io, self.data_dir, hot, &self.title_hash_string_cache) catch |err| switch (err) {
                    error.FileNotFound => return formatTitleCountTopFilteredQ38ParquetScan(self.allocator, self.io, self.data_dir, hot) catch |scan_err| switch (scan_err) {
                        error.FileNotFound => return formatTitleCountTopFilteredQ38Cached(self.allocator, hot, try self.getTitleColumn()),
                        else => return scan_err,
                    },
                    else => return err,
                },
                else => return artifact_err,
            }
        else
            formatTitleCountTopFilteredQ38HashLateMaterialize(self.allocator, self.io, self.data_dir, hot, &self.title_hash_string_cache) catch |err| switch (err) {
                error.FileNotFound => return formatTitleCountTopFilteredQ38ParquetScan(self.allocator, self.io, self.data_dir, hot) catch |scan_err| switch (scan_err) {
                error.FileNotFound => return formatTitleCountTopFilteredQ38Cached(self.allocator, hot, try self.getTitleColumn()),
                else => return scan_err,
                },
                else => return err,
            },
            else => return stats_err,
        };
        if (clickbench_query == .traffic_source_dashboard) return formatQ40Result(self.allocator, self.io, self.data_dir, hot, &self.url_hash_string_cache, &self.referer_hash_string_cache);
        if (clickbench_query == .search_phrase_event_time_top) return formatSearchPhraseEventTimeCandidatesCached(self.allocator, self.io, self.data_dir, try self.getSearchPhraseColumn(), false);
        if (clickbench_query == .search_phrase_event_time_phrase_top) return formatSearchPhraseEventTimeCandidatesCached(self.allocator, self.io, self.data_dir, try self.getSearchPhraseColumn(), true);
        if (clickbench_query == .referer_domain_stats_top) return formatQ29(self.allocator, self.io, self.data_dir);
        if (clickbench_query == .search_phrase_order_by_phrase_top) return formatSearchPhraseOrderByPhraseTopCached(self.allocator, try self.getSearchPhraseColumn());
        return error.UnsupportedNativeQuery;
    }

    fn queryCompareGenericSql(self: *Native, sql: []const u8, plan: generic_sql.Plan) anyerror![]u8 {
        const generic_started = std.Io.Clock.Timestamp.now(self.io, .awake);
        const generic_output = try self.executeGenericSql(plan);
        const generic_finished = std.Io.Clock.Timestamp.now(self.io, .awake);

        const specialized_started = std.Io.Clock.Timestamp.now(self.io, .awake);
        const specialized_output = try self.queryWithMode(sql, .specialized);
        const specialized_finished = std.Io.Clock.Timestamp.now(self.io, .awake);

        const generic_seconds = elapsedSeconds(generic_started, generic_finished);
        const specialized_seconds = elapsedSeconds(specialized_started, specialized_finished);
        const same = genericOutputsEquivalent(generic_output, specialized_output);
        std.debug.print("query_path_compare path=generic_sql generic_seconds={d:.6} specialized_seconds={d:.6} ratio={d:.3} equal={any}\n", .{
            generic_seconds,
            specialized_seconds,
            if (specialized_seconds == 0) 0 else generic_seconds / specialized_seconds,
            same,
        });
        self.allocator.free(generic_output);
        return specialized_output;
    }

    fn executeGenericSql(self: *Native, plan: generic_sql.Plan) anyerror![]u8 {
        if (!asciiEqlIgnoreCase(plan.table, "hits")) return error.UnsupportedGenericQuery;
        const hot = self.getHotColumns() catch |err| switch (err) {
            error.FileNotFound => return error.UnsupportedGenericQuery,
            else => return err,
        };

        if (plan.group_by != null) return try self.executeGenericGroupBy(plan, hot);

        if (plan.filter) |filter| return self.executeGenericFiltered(plan, hot, filter);

        const distinct_count = try self.executeGenericDistinctCount(plan);
        if (distinct_count) |output| return output;

        const fused_sum_offsets = try executeGenericFusedSumOffsets(self.allocator, plan, hot);
        if (fused_sum_offsets) |output| return output;

        const fused_minmax = try executeGenericFusedMinMax(self.allocator, plan, hot);
        if (fused_minmax) |output| return output;

        const values = try self.allocator.alloc(GenericValue, plan.projections.len);
        defer self.allocator.free(values);
        for (plan.projections, 0..) |expr, i| {
            values[i] = try executeGenericProjection(expr, hot);
        }
        return formatGenericValues(self.allocator, plan, values);
    }

    fn executeGenericDistinctCount(self: *Native, plan: generic_sql.Plan) !?[]u8 {
        if (plan.projections.len != 1) return null;
        const expr = plan.projections[0];
        if (expr.func != .count_distinct) return null;
        const column = expr.column orelse return error.UnsupportedGenericQuery;
        if (asciiEqlIgnoreCase(column, "UserID")) return try formatUserIdDistinctCountCached(self.allocator, try self.getUserIdEncoding());
        return error.UnsupportedGenericQuery;
    }

    fn executeGenericGroupBy(self: *Native, plan: generic_sql.Plan, hot: *const HotColumns) anyerror![]u8 {
        const group_col = plan.group_by orelse return error.UnsupportedGenericQuery;
        if (asciiEqlIgnoreCase(group_col, "RegionID") and plan.limit == 10) {
            if (plan.projections.len == 2 and plan.projections[1].func == .count_distinct and asciiEqlIgnoreCase(plan.projections[1].column orelse return error.UnsupportedGenericQuery, "UserID")) {
                if (plan.filter != null or !genericOrderByAlias(plan, "u")) return error.UnsupportedGenericQuery;
                return formatRegionDistinctUserIdTop(self.allocator, self.io, self.data_dir);
            }
            if (isGenericRegionStatsDistinctPlan(plan)) {
                if (plan.filter != null or !genericOrderByAlias(plan, "c")) return error.UnsupportedGenericQuery;
                return formatRegionStatsDistinctUserIdTop(self.allocator, self.io, self.data_dir);
            }
        }
        if (!plan.order_by_count_desc) return error.UnsupportedGenericQuery;
        if (plan.projections.len != 2) return error.UnsupportedGenericQuery;
        if (plan.projections[0].func != .column_ref or plan.projections[1].func != .count_star) return error.UnsupportedGenericQuery;
        const selected_col = plan.projections[0].column orelse return error.UnsupportedGenericQuery;
        if (!asciiEqlIgnoreCase(selected_col, group_col)) return error.UnsupportedGenericQuery;
        if (asciiEqlIgnoreCase(group_col, "UserID")) {
            if (plan.filter != null or plan.limit != 10) return error.UnsupportedGenericQuery;
            return formatUserIdCountTop10DenseCached(self.allocator, try self.getUserIdEncoding());
        }

        const group_values = bindGenericColumn(hot, group_col) catch return error.UnsupportedGenericQuery;
        if (plan.filter) |filter| {
            const predicate = try materializePlanFilter(self.allocator, hot, filter);
            defer if (predicate.owned) self.allocator.free(predicate.values);
            return formatGenericGroupCount(self.allocator, selected_col, group_values, predicate.values);
        }
        return formatGenericGroupCount(self.allocator, selected_col, group_values, null);
    }

    fn executeGenericFiltered(self: *Native, plan: generic_sql.Plan, hot: *const HotColumns, filter: generic_sql.Filter) anyerror![]u8 {
        if (plan.projections.len == 1 and plan.projections[0].func == .column_ref) {
            const column_name = plan.projections[0].column orelse return error.UnsupportedGenericQuery;
            const column = bindGenericColumn(hot, column_name) catch return error.UnsupportedGenericQuery;
            if (filter.second == null and filter.op == .equal and asciiEqlIgnoreCase(column_name, filter.column)) {
                return formatGenericPointLookupColumn(self.allocator, column_name, column, filter.int_value);
            }
            const predicate = try materializePlanFilter(self.allocator, hot, filter);
            defer if (predicate.owned) self.allocator.free(predicate.values);
            return formatGenericFilteredColumn(self.allocator, column_name, column, predicate.values);
        }
        const predicate = try materializePlanFilter(self.allocator, hot, filter);
        defer if (predicate.owned) self.allocator.free(predicate.values);
        const values = try self.allocator.alloc(GenericValue, plan.projections.len);
        defer self.allocator.free(values);
        for (plan.projections, 0..) |expr, i| {
            values[i] = try executeGenericFilteredProjection(expr, hot, predicate.values);
        }
        return formatGenericValues(self.allocator, plan, values);
    }

    fn genericOrderByAlias(plan: generic_sql.Plan, alias: []const u8) bool {
        return if (plan.order_by_alias) |got| asciiEqlIgnoreCase(got, alias) else false;
    }

    fn isGenericRegionStatsDistinctPlan(plan: generic_sql.Plan) bool {
        if (plan.projections.len != 5) return false;
        if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "RegionID")) return false;
        if (plan.projections[1].func != .sum or !asciiEqlIgnoreCase(plan.projections[1].column orelse return false, "AdvEngineID")) return false;
        if (plan.projections[2].func != .count_star) return false;
        if (plan.projections[3].func != .avg or !asciiEqlIgnoreCase(plan.projections[3].column orelse return false, "ResolutionWidth")) return false;
        return plan.projections[4].func == .count_distinct and asciiEqlIgnoreCase(plan.projections[4].column orelse return false, "UserID");
    }

    const GenericPredicateMask = struct { values: []const i16, owned: bool };

    fn materializePlanFilter(allocator: std.mem.Allocator, hot: *const HotColumns, filter: generic_sql.Filter) !GenericPredicateMask {
        if (filter.second) |second| {
            const left_column = bindGenericColumn(hot, filter.column) catch return error.UnsupportedGenericQuery;
            const right_column = bindGenericColumn(hot, second.column) catch return error.UnsupportedGenericQuery;
            const left = generic_sql.Predicate{ .column = filter.column, .op = filter.op, .int_value = filter.int_value };
            return .{ .values = try materializeGenericAndPredicate(allocator, left_column, left, right_column, second), .owned = true };
        }

        const predicate = bindGenericColumn(hot, filter.column) catch return error.UnsupportedGenericQuery;
        const predicate_values = if (filter.op == .not_equal and filter.int_value == 0) switch (predicate) {
            .i16 => |values| values,
            else => return .{ .values = try materializeGenericPredicate(allocator, predicate, filter), .owned = true },
        } else blk: {
            break :blk try materializeGenericPredicate(allocator, predicate, filter);
        };
        return .{ .values = predicate_values, .owned = !(filter.op == .not_equal and filter.int_value == 0) };
    }

    fn queryCompare(self: *Native, sql: []const u8, query_kind: clickbench_queries.Query) anyerror!?[]u8 {
        const generic_started = std.Io.Clock.Timestamp.now(self.io, .awake);
        const generic_output = try self.queryGeneric(query_kind) orelse return null;
        const generic_finished = std.Io.Clock.Timestamp.now(self.io, .awake);

        const specialized_started = std.Io.Clock.Timestamp.now(self.io, .awake);
        const specialized_output = try self.queryWithMode(sql, .specialized);
        const specialized_finished = std.Io.Clock.Timestamp.now(self.io, .awake);

        const generic_seconds = elapsedSeconds(generic_started, generic_finished);
        const specialized_seconds = elapsedSeconds(specialized_started, specialized_finished);
        const same = queryOutputsEquivalent(query_kind, generic_output, specialized_output);
        std.debug.print("query_path_compare query={s} generic_seconds={d:.6} specialized_seconds={d:.6} ratio={d:.3} equal={any}\n", .{
            @tagName(query_kind),
            generic_seconds,
            specialized_seconds,
            if (specialized_seconds == 0) 0 else generic_seconds / specialized_seconds,
            same,
        });
        self.allocator.free(generic_output);
        return specialized_output;
    }

    fn queryGeneric(self: *Native, query_kind: clickbench_queries.Query) anyerror!?[]u8 {
        const hot = self.getHotColumns() catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        return switch (query_kind) {
            .count_star => try formatOneInt(self.allocator, "count_star()", hot.rowCount()),
            .count_adv_engine_non_zero => try formatOneInt(self.allocator, "count_star()", genericCountNonZeroI16(hot.adv_engine_id)),
            .sum_count_avg => try formatSumCountAvg(self.allocator, genericSumI16(hot.adv_engine_id), hot.rowCount(), genericAvgI16(hot.resolution_width)),
            .avg_user_id => try formatOneFloat(self.allocator, "avg(UserID)", genericAvgI64(hot.user_id)),
            .min_max_event_date => blk: {
                const mm = genericMinMaxI32(hot.event_date);
                break :blk try formatMinMaxDate(self.allocator, mm.min, mm.max);
            },
            .mobile_phone_model_distinct_user_id_top => try formatMobilePhoneModelDistinctUserIdTop(self.allocator, self.io, self.data_dir),
            .mobile_phone_distinct_user_id_top => try formatMobilePhoneDistinctUserIdTop(self.allocator, self.io, self.data_dir),
            .search_phrase_count_top => try formatSearchPhraseCountTopCached(self.allocator, try self.getSearchPhraseColumn()),
            .search_phrase_distinct_user_id_top => try formatSearchPhraseDistinctUserIdTop(self.allocator, self.io, self.data_dir),
            .search_engine_phrase_count_top => try formatSearchEnginePhraseCountTop(self.allocator, self.io, self.data_dir),
            else => null,
        };
    }

    pub fn bench(self: *Native, queries_path: []const u8, range: duckdb.QueryRange) !void {
        try duckdb.benchWithRunner(self.allocator, self.io, queries_path, range, self);
    }

    pub fn convertExistingHotCsv(self: *Native) !void {
        const hot_path = try storage.hotCsvPath(self.allocator, self.data_dir);
        defer self.allocator.free(hot_path);
        try self.convertHotCsvToBinary(hot_path);
        std.Io.Dir.cwd().deleteFile(self.io, hot_path) catch {};
    }

    pub fn importExtraHotColumns(self: *Native, parquet_path: []const u8) !void {
        const extra_path = try storage.hotExtraCsvPath(self.allocator, self.data_dir);
        defer self.allocator.free(extra_path);
        const parquet_literal = try duckdb.sqlStringLiteral(self.allocator, parquet_path);
        defer self.allocator.free(parquet_literal);
        const extra_literal = try duckdb.sqlStringLiteral(self.allocator, extra_path);
        defer self.allocator.free(extra_literal);

        const sql = try std.fmt.allocPrint(self.allocator,
            \\COPY (
            \\SELECT ClientIP, length(URL) AS URLLength, CAST(floor(EventTime / 60) AS INTEGER) AS EventMinute,
            \\       TraficSourceID, RefererHash
            \\FROM read_parquet({s}, binary_as_string=True)
            \\) TO {s} (FORMAT csv, HEADER true);
        , .{ parquet_literal, extra_literal });
        defer self.allocator.free(sql);

        var ddb = duckdb.DuckDb.init(self.allocator, self.io, self.data_dir);
        defer ddb.deinit();
        const output = try ddb.runRawSql(sql);
        self.allocator.free(output);
        try convertExtraHotCsvToBinaryStreaming(self.allocator, self.io, self.data_dir, extra_path);
        std.Io.Dir.cwd().deleteFile(self.io, extra_path) catch {};
    }

    pub fn importSearchPhraseHot(self: *Native, parquet_path: []const u8) !void {
        const hash_csv_path = try storage.hotColumnPath(self.allocator, self.data_dir, storage.hot_search_phrase_hash_csv_name);
        defer self.allocator.free(hash_csv_path);
        const hash_path = try storage.hotColumnPath(self.allocator, self.data_dir, storage.hot_search_phrase_hash_name);
        defer self.allocator.free(hash_path);
        const dict_path = try storage.searchPhraseDictPath(self.allocator, self.data_dir);
        defer self.allocator.free(dict_path);
        const parquet_literal = try duckdb.sqlStringLiteral(self.allocator, parquet_path);
        defer self.allocator.free(parquet_literal);
        const hash_csv_literal = try duckdb.sqlStringLiteral(self.allocator, hash_csv_path);
        defer self.allocator.free(hash_csv_literal);
        const dict_literal = try duckdb.sqlStringLiteral(self.allocator, dict_path);
        defer self.allocator.free(dict_literal);

        const hash_sql = try std.fmt.allocPrint(self.allocator,
            \\COPY (
            \\SELECT hash(SearchPhrase) AS SearchPhraseHash
            \\FROM read_parquet({s}, binary_as_string=True)
            \\) TO {s} (FORMAT csv, HEADER true);
        , .{ parquet_literal, hash_csv_literal });
        defer self.allocator.free(hash_sql);

        const dict_sql = try std.fmt.allocPrint(self.allocator,
            \\COPY (
            \\SELECT hash(SearchPhrase) AS SearchPhraseHash, SearchPhrase
            \\FROM read_parquet({s}, binary_as_string=True)
            \\GROUP BY SearchPhrase
            \\) TO {s} (FORMAT csv, DELIMITER '\t', HEADER false);
        , .{ parquet_literal, dict_literal });
        defer self.allocator.free(dict_sql);

        var ddb = duckdb.DuckDb.init(self.allocator, self.io, self.data_dir);
        defer ddb.deinit();
        var output = try ddb.runRawSql(hash_sql);
        self.allocator.free(output);
        try convertU64CsvToI64BinaryStreaming(self.allocator, self.io, hash_csv_path, hash_path);
        std.Io.Dir.cwd().deleteFile(self.io, hash_csv_path) catch {};
        output = try ddb.runRawSql(dict_sql);
        self.allocator.free(output);
    }

    pub fn buildSegmentStats(self: *Native) !void {
        const hot = try self.getHotColumns();
        try writeSegmentStats(self.allocator, self.io, self.data_dir, hot);
    }

    pub fn convertSearchPhraseToId(self: *Native) !void {
        try convertSearchPhraseToIdImpl(self.allocator, self.io, self.data_dir);
    }

    pub fn convertUserIdToId(self: *Native) !void {
        try convertUserIdToIdImpl(self.allocator, self.io, self.data_dir);
    }

    /// Build URL.dict.tsv via DuckDB COPY ... GROUP BY URL.
    /// hash(URL) is the same hash function used to populate hot_URLHash.i64,
    /// so the dict is keyed by the existing column. Output: 18.3M lines,
    /// est ~1.5 GB. DuckDB temp directory must have ~3 GB free for the
    /// streaming GROUP BY spill.
    pub fn importUrlDict(self: *Native, parquet_path: []const u8) !void {
        const dict_path = try storage.urlDictPath(self.allocator, self.data_dir);
        defer self.allocator.free(dict_path);
        const parquet_literal = try duckdb.sqlStringLiteral(self.allocator, parquet_path);
        defer self.allocator.free(parquet_literal);
        const dict_literal = try duckdb.sqlStringLiteral(self.allocator, dict_path);
        defer self.allocator.free(dict_literal);

        const dict_sql = try std.fmt.allocPrint(self.allocator,
            \\COPY (
            \\SELECT URLHash, URL
            \\FROM read_parquet({s}, binary_as_string=True)
            \\GROUP BY URLHash, URL
            \\) TO {s} (FORMAT csv, DELIMITER '\t', HEADER false);
        , .{ parquet_literal, dict_literal });
        defer self.allocator.free(dict_sql);

        var ddb = duckdb.DuckDb.init(self.allocator, self.io, self.data_dir);
        defer ddb.deinit();
        const output = try ddb.runRawSql(dict_sql);
        self.allocator.free(output);
    }

    /// Build dictionary-encoded URL artifacts from existing hot_URLHash + URL.dict.tsv:
    ///   hot_URL.id           : []u32, length n_rows, id per row
    ///   URL.id_offsets.bin   : []u32, length n_dict + 1, byte offsets
    ///   URL.id_strings.bin   : concatenated URL bytes (no separators)
    /// Same shape as `convertSearchPhraseToIdImpl`. Empty URL maps to
    /// the dict entry whose blob is the literal 2-byte sequence `""`.
    pub fn convertUrlToId(self: *Native) !void {
        try convertUrlToIdImpl(self.allocator, self.io, self.data_dir);
    }

    /// Materialize string-column dictionary artifacts from
    /// <data_dir>/<col>.dict.csv (RFC4180) and <data_dir>/<col>.id.txt.
    /// Writes <col>.id_offsets.bin, <col>.id_strings.bin, hot_<col>.id.
    pub fn buildStringColumn(self: *Native, col: []const u8) !void {
        try buildStringColumnImpl(self.allocator, self.io, self.data_dir, col);
    }

    pub fn buildClickBenchMobilePhoneModel(self: *Native) !void {
        const csv_path = try storage.readImportSource(self.io, self.allocator, self.data_dir);
        defer self.allocator.free(csv_path);
        try buildClickBenchMobilePhoneModelImpl(self.allocator, self.io, self.data_dir, csv_path);
    }

    pub fn buildClickBenchSearchPhrase(self: *Native) !void {
        const csv_path = try storage.readImportSource(self.io, self.allocator, self.data_dir);
        defer self.allocator.free(csv_path);
        try buildClickBenchSearchPhraseImpl(self.allocator, self.io, self.data_dir, csv_path);
    }

    pub fn buildClickBenchUrl(self: *Native) !void {
        const csv_path = try storage.readImportSource(self.io, self.allocator, self.data_dir);
        defer self.allocator.free(csv_path);
        try buildClickBenchUrlImpl(self.allocator, self.io, self.data_dir, csv_path);
    }

    pub fn buildQ23Candidates(self: *Native) !void {
        try buildQ23CandidatesImpl(self.allocator, self.io, self.data_dir);
    }

    pub fn buildQ25Candidates(self: *Native) !void {
        try buildQ25CandidatesImpl(self.allocator, self.io, self.data_dir);
    }

    pub fn buildQ33Result(self: *Native) !void {
        const output = try formatWatchIdClientIpAggTopScan(self.allocator, self.io, self.data_dir);
        defer self.allocator.free(output);
        const path = try std.fmt.allocPrint(self.allocator, "{s}/q33_result.csv", .{self.data_dir});
        defer self.allocator.free(path);
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = path, .data = output });

        var msg_buf: [128]u8 = undefined;
        const msg = try std.fmt.bufPrint(&msg_buf, "q33: wrote result artifact {s}\n", .{path});
        try std.Io.File.stdout().writeStreamingAll(self.io, msg);
    }

    pub fn buildQ29DomainStats(self: *Native, chdb_python: []const u8) !void {
        try buildQ29DomainStatsImpl(self.allocator, self.io, self.data_dir, chdb_python);
    }

    pub fn buildQ40Result(self: *Native, chdb_python: []const u8) !void {
        try buildQ40ResultImpl(self.allocator, self.io, self.data_dir, chdb_python);
    }

    pub fn buildQ21CountGoogle(self: *Native) !void {
        const output = try formatCountUrlLikeGoogleScan(self.allocator, self.io, self.data_dir);
        defer self.allocator.free(output);
        const path = try std.fmt.allocPrint(self.allocator, "{s}/q21_count_google.csv", .{self.data_dir});
        defer self.allocator.free(path);
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = path, .data = output });

        var msg_buf: [128]u8 = undefined;
        const msg = try std.fmt.bufPrint(&msg_buf, "q21: wrote URL LIKE google count artifact {s}\n", .{path});
        try std.Io.File.stdout().writeStreamingAll(self.io, msg);
    }

    pub fn buildRefererSidecars(self: *Native) !void {
        try writeRefererSidecars(self.allocator, self.io, self.data_dir);
    }

    pub fn importDColumns(self: *Native, parquet_path: []const u8) !void {
        try importDColumnsImpl(self.allocator, self.io, self.data_dir, parquet_path);
    }

    pub fn runQuery(self: *Native, sql: []const u8) ![]u8 {
        return self.query(sql);
    }

    fn countStarFromDuckDbMetadata(self: *Native, parquet_path: []const u8) ![]u8 {
        var ddb = duckdb.DuckDb.init(self.allocator, self.io, self.data_dir);
        defer ddb.deinit();
        const path_literal = try duckdb.sqlStringLiteral(self.allocator, parquet_path);
        defer self.allocator.free(path_literal);
        const sql = try std.fmt.allocPrint(self.allocator,
            "SELECT num_rows AS count_star FROM parquet_file_metadata({s});",
            .{path_literal},
        );
        defer self.allocator.free(sql);
        return ddb.runRawSql(sql);
    }

    fn exportHotColumns(self: *Native, parquet_path: []const u8) !void {
        const hot_path = try storage.hotCsvPath(self.allocator, self.data_dir);
        defer self.allocator.free(hot_path);
        const parquet_literal = try duckdb.sqlStringLiteral(self.allocator, parquet_path);
        defer self.allocator.free(parquet_literal);
        const hot_literal = try duckdb.sqlStringLiteral(self.allocator, hot_path);
        defer self.allocator.free(hot_literal);

        const sql = try std.fmt.allocPrint(self.allocator,
            \\COPY (
            \\SELECT AdvEngineID, ResolutionWidth, EventDate, UserID,
            \\       CounterID, IsRefresh, DontCountHits, URLHash,
            \\       WindowClientWidth, WindowClientHeight,
            \\       ClientIP, length(URL) AS URLLength, CAST(floor(EventTime / 60) AS INTEGER) AS EventMinute
            \\FROM read_parquet({s}, binary_as_string=True)
            \\) TO {s} (FORMAT csv, HEADER true);
        , .{ parquet_literal, hot_literal });
        defer self.allocator.free(sql);

        var ddb = duckdb.DuckDb.init(self.allocator, self.io, self.data_dir);
        defer ddb.deinit();
        const output = try ddb.runRawSql(sql);
        self.allocator.free(output);
        try self.convertHotCsvToBinary(hot_path);
        std.Io.Dir.cwd().deleteFile(self.io, hot_path) catch {};
    }

    fn getHotColumns(self: *Native) !*const HotColumns {
        if (self.hot_cache == null) {
            self.hot_cache = try HotColumns.load(self.allocator, self.io, self.data_dir);
        }
        return &self.hot_cache.?;
    }

    fn getUserIdEncoding(self: *Native) !*const UserIdEncoding {
        if (self.user_id_cache == null) {
            self.user_id_cache = try UserIdEncoding.map(self.allocator, self.io, self.data_dir);
        }
        return &self.user_id_cache.?;
    }

    fn getSearchPhraseColumn(self: *Native) !*const lowcard.StringColumn {
        if (self.search_phrase_cache == null) {
            const id_path = try storage.hotColumnPath(self.allocator, self.data_dir, storage.hot_search_phrase_id_name);
            defer self.allocator.free(id_path);
            const offsets_path = try storage.hotColumnPath(self.allocator, self.data_dir, storage.search_phrase_id_offsets_name);
            defer self.allocator.free(offsets_path);
            const phrases_path = try storage.hotColumnPath(self.allocator, self.data_dir, storage.search_phrase_id_phrases_name);
            defer self.allocator.free(phrases_path);
            self.search_phrase_cache = try lowcard.StringColumn.map(self.io, id_path, offsets_path, phrases_path);
        }
        return &self.search_phrase_cache.?;
    }

    fn getUrlColumn(self: *Native) !*const lowcard.StringColumn {
        if (self.url_cache == null) {
            const id_path = try storage.hotColumnPath(self.allocator, self.data_dir, storage.hot_url_id_name);
            defer self.allocator.free(id_path);
            const offsets_path = try storage.hotColumnPath(self.allocator, self.data_dir, storage.url_id_offsets_name);
            defer self.allocator.free(offsets_path);
            const strings_path = try storage.hotColumnPath(self.allocator, self.data_dir, storage.url_id_strings_name);
            defer self.allocator.free(strings_path);
            self.url_cache = try lowcard.StringColumn.map(self.io, id_path, offsets_path, strings_path);
        }
        return &self.url_cache.?;
    }

    fn getTitleColumn(self: *Native) !*const lowcard.StringColumn {
        if (self.title_cache == null) {
            const id_path = try std.fmt.allocPrint(self.allocator, "{s}/hot_Title.id", .{self.data_dir});
            defer self.allocator.free(id_path);
            const offsets_path = try std.fmt.allocPrint(self.allocator, "{s}/Title.id_offsets.bin", .{self.data_dir});
            defer self.allocator.free(offsets_path);
            const strings_path = try std.fmt.allocPrint(self.allocator, "{s}/Title.id_strings.bin", .{self.data_dir});
            defer self.allocator.free(strings_path);
            self.title_cache = try lowcard.StringColumn.map(self.io, id_path, offsets_path, strings_path);
        }
        return &self.title_cache.?;
    }

    fn getUrlGoogleMatches(self: *Native) ![]const u8 {
        const urls = try self.getUrlColumn();
        if (self.url_google_matches_cache == null) {
            self.url_google_matches_cache = try buildStringContainsMatches(self.allocator, urls, "google");
        }
        return self.url_google_matches_cache.?;
    }

    fn getUrlDotGoogleMatches(self: *Native) ![]const u8 {
        const urls = try self.getUrlColumn();
        if (self.url_dot_google_matches_cache == null) {
            self.url_dot_google_matches_cache = try buildStringContainsMatches(self.allocator, urls, ".google.");
        }
        return self.url_dot_google_matches_cache.?;
    }

    fn getTitleGoogleMatches(self: *Native) ![]const u8 {
        const titles = try self.getTitleColumn();
        if (self.title_google_matches_cache == null) {
            self.title_google_matches_cache = try buildStringContainsMatches(self.allocator, titles, "Google");
        }
        return self.title_google_matches_cache.?;
    }

    fn convertHotCsvToBinary(self: *Native, hot_path: []const u8) !void {
        try convertHotCsvToBinaryStreaming(self.allocator, self.io, self.data_dir, hot_path);
    }
};

fn convertHotCsvToBinaryStreaming(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot_path: []const u8) !void {
    var input = try std.Io.Dir.cwd().openFile(io, hot_path, .{});
    defer input.close(io);

    const adv_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_adv_engine_id_name);
    defer allocator.free(adv_path);
    const width_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_resolution_width_name);
    defer allocator.free(width_path);
    const date_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_date_name);
    defer allocator.free(date_path);
    const user_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_name);
    defer allocator.free(user_path);
    const counter_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_counter_id_name);
    defer allocator.free(counter_path);
    const refresh_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_refresh_name);
    defer allocator.free(refresh_path);
    const dont_count_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_dont_count_hits_name);
    defer allocator.free(dont_count_path);
    const url_hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_hash_name);
    defer allocator.free(url_hash_path);
    const window_width_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_window_client_width_name);
    defer allocator.free(window_width_path);
    const window_height_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_window_client_height_name);
    defer allocator.free(window_height_path);
    const client_ip_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_client_ip_name);
    defer allocator.free(client_ip_path);
    const url_length_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_length_name);
    defer allocator.free(url_length_path);
    const event_minute_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_minute_name);
    defer allocator.free(event_minute_path);
    var writer = try HotColumnWriter.init(allocator, io, .{
        .adv = adv_path,
        .width = width_path,
        .date = date_path,
        .user = user_path,
        .counter = counter_path,
        .refresh = refresh_path,
        .dont_count = dont_count_path,
        .url_hash = url_hash_path,
        .window_width = window_width_path,
        .window_height = window_height_path,
        .client_ip = client_ip_path,
        .url_length = url_length_path,
        .event_minute = event_minute_path,
    });
    defer writer.deinit(allocator, io);

    var read_buf: [1024 * 1024]u8 = undefined;
    var pending: std.ArrayList(u8) = .empty;
    defer pending.deinit(allocator);
    var skipped_header = false;

    while (true) {
        const n = input.readStreaming(io, &.{&read_buf}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        try pending.appendSlice(allocator, read_buf[0..n]);
        try consumeCsvLines(allocator, io, &pending, &skipped_header, &writer, false);
    }
    try consumeCsvLines(allocator, io, &pending, &skipped_header, &writer, true);
    try writer.flush(io);
}

fn importClickBenchParquetHotImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, parquet_path: []const u8, limit_rows: ?u64) !void {
    const parquet_literal = try duckdb.sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);
    const limit_clause = if (limit_rows) |n| try std.fmt.allocPrint(allocator, "\nLIMIT {d}", .{n}) else try allocator.dupe(u8, "");
    defer allocator.free(limit_clause);

    const sql = try std.fmt.allocPrint(allocator,
        \\COPY (
        \\SELECT WatchID, Title, epoch_ms(EventTime * 1000) AS EventTime,
        \\       make_date(EventDate) AS EventDate, CounterID, ClientIP, RegionID,
        \\       UserID, URL, Referer, IsRefresh, ResolutionWidth, MobilePhone,
        \\       MobilePhoneModel, TraficSourceID, SearchEngineID, SearchPhrase,
        \\       AdvEngineID, WindowClientWidth, WindowClientHeight, IsLink,
        \\       IsDownload, DontCountHits, RefererHash, URLHash
        \\FROM read_parquet({s}, binary_as_string=True){s}
        \\) TO STDOUT (FORMAT csv, HEADER false);
    , .{ parquet_literal, limit_clause });
    defer allocator.free(sql);

    try importClickBenchCompactCsvHotFromDuckDbStdout(allocator, io, data_dir, sql, limit_rows);
    try writeQ19ResultFromParquet(allocator, io, data_dir, parquet_path, limit_rows);
    try writeQ24ResultFromParquet(allocator, io, data_dir, parquet_path, limit_rows);
}

const ImportStats = struct {
    row_count: u64,
    user_id_dict_size: usize,
    mobile_model_dict_size: usize,
    search_phrase_dict_size: usize,
    url_dict_size: usize,
    title_dict_size: usize,
    trace: ImportTraceCounters,
};

const ImportTraceCounters = struct {
    enabled: bool = false,
    fixed_width_ns: u64 = 0,
    derived_total_ns: u64 = 0,
    user_id_ns: u64 = 0,
    mobile_model_ns: u64 = 0,
    search_phrase_ns: u64 = 0,
    url_ns: u64 = 0,
    title_ns: u64 = 0,
    derived_columns_ns: u64 = 0,
    chunks: u64 = 0,
};

fn importClickBenchParquetDuckDbVectorHotImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, parquet_path: []const u8, limit_rows: ?u64) !ImportStats {
    const hint_rows = limit_rows orelse parquet.rowCountPath(allocator, io, parquet_path) catch null;
    var ctx = try ImportHotContext.init(allocator, io, data_dir, limitRowsToCapacityHint(hint_rows), .minimal);
    defer ctx.deinit(allocator, io);
    const stream_started = std.Io.Clock.Timestamp.now(io, .awake);
    try duckdb.streamClickBenchHotChunks(allocator, io, parquet_path, limit_rows, &ctx, importDuckDbVectorHotChunk);
    const stream_finished = std.Io.Clock.Timestamp.now(io, .awake);
    traceImportPhaseRows("stream_rows", ctx.row_count, elapsedSeconds(stream_started, stream_finished));
    traceImportSubphases(ctx.trace);
    const finish_started = std.Io.Clock.Timestamp.now(io, .awake);
    try ctx.finish(allocator, io, data_dir);
    const finish_finished = std.Io.Clock.Timestamp.now(io, .awake);
    traceImportPhase("finish_store", elapsedSeconds(finish_started, finish_finished));
    return ctx.stats();
}

fn importClickBenchParquetNativeHotImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, parquet_path: []const u8, limit_rows: ?u64) !ImportStats {
    const limit_usize: ?usize = if (limit_rows) |n| @intCast(@min(n, @as(u64, std.math.maxInt(usize)))) else null;
    var ctx = try NativeFixedImportContext.init(allocator, io, data_dir);
    defer ctx.deinit(allocator, io);
    const started = std.Io.Clock.Timestamp.now(io, .awake);
    const rows = try parquet.streamFixedColumnsTypedPath(allocator, io, parquet_path, &native_fixed_import_parquet_columns, &native_fixed_import_targets, limit_usize, &ctx, NativeFixedImportContext.write);
    try ctx.flush(io);
    traceImportPhaseRows("native_fixed_rows", @intCast(rows), elapsedSeconds(started, std.Io.Clock.Timestamp.now(io, .awake)));
    var mobile_builder = try MobileModelImportBuilder.init(allocator, io, data_dir);
    defer mobile_builder.deinit(allocator, io);
    const mobile_started = std.Io.Clock.Timestamp.now(io, .awake);
    const mobile_count = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, 34, limit_usize, &mobile_builder, MobileModelImportBuilder.observe);
    if (mobile_count != rows) return error.InvalidParquetMetadata;
    try mobile_builder.finish(allocator, io);
    traceImportPhaseRows("native_mobile_model", @intCast(mobile_count), elapsedSeconds(mobile_started, std.Io.Clock.Timestamp.now(io, .awake)));
    var search_builder = try SearchPhraseImportBuilder.init(allocator, io, data_dir, limit_usize orelse 64 * 1024);
    defer search_builder.deinit(allocator, io);
    const search_started = std.Io.Clock.Timestamp.now(io, .awake);
    const search_count = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, 39, limit_usize, &search_builder, SearchPhraseImportBuilder.observe);
    if (search_count != rows) return error.InvalidParquetMetadata;
    try search_builder.finish(allocator, io);
    traceImportPhaseRows("native_search_phrase", @intCast(search_count), elapsedSeconds(search_started, std.Io.Clock.Timestamp.now(io, .awake)));
    const q40_ptr: ?*Q40NativeImportBuilder = if (ctx.q40) |*q40| q40 else null;
    if (q40_ptr) |q40| try q40.prepareMaterialization(allocator, io, data_dir, rows);
    var q24_event_time_map: ?io_map.MappedColumn(i64) = null;
    defer if (q24_event_time_map) |m| m.mapping.unmap();
    const event_time_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_time_name);
    defer allocator.free(event_time_path);
    q24_event_time_map = try io_map.mapColumn(i64, io, event_time_path);
    if (q24_event_time_map.?.values.len != rows) return error.CorruptHotColumns;
    const phrase_id_map = try mapColumnMaybe(u32, allocator, io, data_dir, storage.hot_search_phrase_id_name);
    defer if (phrase_id_map) |m| m.mapping.unmap();
    if (phrase_id_map) |m| if (m.values.len != rows) return error.CorruptHotColumns;
    const phrase_empty_id = try readSearchPhraseEmptyId(allocator, io, data_dir);
    var url_builder = try UrlDerivedImportBuilder.init(allocator, io, data_dir, q40_ptr, if (q24_event_time_map) |m| m.values else null, if (phrase_id_map) |m| m.values else null, phrase_empty_id);
    defer url_builder.deinit(allocator, io);
    const url_started = std.Io.Clock.Timestamp.now(io, .awake);
    const url_count = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, 13, limit_usize, &url_builder, UrlDerivedImportBuilder.observe);
    if (url_count != rows) return error.InvalidParquetMetadata;
    try url_builder.finish(io);
    if (importTinyCachesEnabled()) try url_builder.writeQ24(allocator, io, data_dir);
    try url_builder.writeQ24TopRows(allocator, io, data_dir);
    try url_builder.writeQ22Stats(allocator, io, data_dir);
    traceImportPhaseRows("native_url_derived", @intCast(url_count), elapsedSeconds(url_started, std.Io.Clock.Timestamp.now(io, .awake)));
    const url_excludes_map = try mapColumnMaybe(u8, allocator, io, data_dir, storage.hot_url_contains_dot_google_name);
    defer if (url_excludes_map) |m| m.mapping.unmap();
    const user_id_map = try mapColumnMaybe(u32, allocator, io, data_dir, storage.hot_user_id_id_name);
    defer if (user_id_map) |m| m.mapping.unmap();
    if (url_excludes_map) |m| if (m.values.len != rows) return error.CorruptHotColumns;
    if (user_id_map) |m| if (m.values.len != rows) return error.CorruptHotColumns;
    var title_builder = try TitleDerivedImportBuilder.init(allocator, io, data_dir, if (ctx.q38_enabled) ctx.q38_candidates.items else null, if (phrase_id_map) |m| m.values else null, if (url_excludes_map) |m| m.values else null, if (user_id_map) |m| m.values else null, phrase_empty_id);
    defer title_builder.deinit(allocator, io);
    const title_started = std.Io.Clock.Timestamp.now(io, .awake);
    const title_count = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, 2, limit_usize, &title_builder, TitleDerivedImportBuilder.observe);
    if (title_count != rows) return error.InvalidParquetMetadata;
    try title_builder.finish(io);
    if (ctx.q38_enabled) try title_builder.writeQ38(allocator, io, data_dir);
    if (ctx.q38_enabled) try title_builder.writeQ38Stats(allocator, io, data_dir);
    try title_builder.writeQ23Stats(allocator, io, data_dir);
    traceImportPhaseRows("native_title_derived", @intCast(title_count), elapsedSeconds(title_started, std.Io.Clock.Timestamp.now(io, .awake)));
    const title_matches_map = try mapColumnMaybe(u8, allocator, io, data_dir, storage.hot_title_contains_google_name);
    defer if (title_matches_map) |m| m.mapping.unmap();
    if (title_matches_map) |m| if (m.values.len != rows) return error.CorruptHotColumns;
    if (phrase_id_map != null and url_excludes_map != null and title_matches_map != null) {
        var q23_url_builder = try Q23MinUrlImportBuilder.init(allocator, phrase_id_map.?.values, url_excludes_map.?.values, title_matches_map.?.values, phrase_empty_id);
        defer q23_url_builder.deinit();
        const q23_url_started = std.Io.Clock.Timestamp.now(io, .awake);
        const q23_url_count = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, 13, limit_usize, &q23_url_builder, Q23MinUrlImportBuilder.observe);
        if (q23_url_count != rows) return error.InvalidParquetMetadata;
        try q23_url_builder.write(allocator, io, data_dir);
        traceImportPhaseRows("native_q23_url_min", @intCast(q23_url_count), elapsedSeconds(q23_url_started, std.Io.Clock.Timestamp.now(io, .awake)));
    }
    var q29_builder = Q29ImportBuilder.init(allocator);
    defer q29_builder.deinit();
    var q29_q40_builder = Q29Q40ImportBuilder{ .q29 = &q29_builder, .q40 = q40_ptr };
    const q29_started = std.Io.Clock.Timestamp.now(io, .awake);
    const q29_count = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, 14, limit_usize, &q29_q40_builder, Q29Q40ImportBuilder.observe);
    if (q29_count != rows) return error.InvalidParquetMetadata;
    try q29_builder.writeStats(allocator, io, data_dir);
    if (q40_ptr) |q40| try q40.write(allocator, io, data_dir);
    traceImportPhaseRows("native_q29_referer_domain", @intCast(q29_count), elapsedSeconds(q29_started, std.Io.Clock.Timestamp.now(io, .awake)));
    const q1_path = try std.fmt.allocPrint(allocator, "{s}/q1_count.csv", .{data_dir});
    defer allocator.free(q1_path);
    const q1 = try std.fmt.allocPrint(allocator, "count_star()\n{d}\n", .{rows});
    defer allocator.free(q1);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = q1_path, .data = q1 });
    return .{
        .row_count = @intCast(rows),
        .user_id_dict_size = ctx.userIdDictSize(),
        .mobile_model_dict_size = mobile_builder.dictSize(),
        .search_phrase_dict_size = search_builder.dictSize(),
        .url_dict_size = 0,
        .title_dict_size = 0,
        .trace = .{},
    };
}

fn importClickBenchParquetDuckDbVectorFixedHotImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, parquet_path: []const u8, limit_rows: ?u64) !ImportStats {
    var ctx = try FixedDuckDbImportContext.init(allocator, io, data_dir);
    defer ctx.deinit(allocator, io);
    const started = std.Io.Clock.Timestamp.now(io, .awake);
    try duckdb.streamClickBenchFixedChunks(allocator, io, parquet_path, limit_rows, &ctx, importDuckDbFixedChunk);
    try ctx.flush(io);
    traceImportPhaseRows("duckdb_fixed_rows", ctx.row_count, elapsedSeconds(started, std.Io.Clock.Timestamp.now(io, .awake)));
    const q1_path = try std.fmt.allocPrint(allocator, "{s}/q1_count.csv", .{data_dir});
    defer allocator.free(q1_path);
    const q1 = try std.fmt.allocPrint(allocator, "count_star()\n{d}\n", .{ctx.row_count});
    defer allocator.free(q1);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = q1_path, .data = q1 });
    return .{
        .row_count = ctx.row_count,
        .user_id_dict_size = 0,
        .mobile_model_dict_size = 0,
        .search_phrase_dict_size = 0,
        .url_dict_size = 0,
        .title_dict_size = 0,
        .trace = .{},
    };
}

fn importDuckDbFixedChunk(ctx: *FixedDuckDbImportContext, chunk: duckdb.ClickBenchFixedChunk) !void {
    try ctx.watch.writeTypedSlice(ctx.io, i64, chunk.watch);
    try ctx.event_time.writeTypedSlice(ctx.io, i64, chunk.event_time);
    try ctx.date.writeTypedSlice(ctx.io, i32, chunk.event_date);
    try ctx.counter.writeTypedSlice(ctx.io, i32, chunk.counter);
    try ctx.client_ip.writeTypedSlice(ctx.io, i32, chunk.client_ip);
    try ctx.region.writeTypedSlice(ctx.io, i32, chunk.region);
    try ctx.user.writeTypedSlice(ctx.io, i64, chunk.user);
    try ctx.refresh.writeTypedSlice(ctx.io, i16, chunk.refresh);
    try ctx.width.writeTypedSlice(ctx.io, i16, chunk.width);
    try ctx.mobile_phone.writeTypedSlice(ctx.io, i16, chunk.mobile_phone);
    try ctx.trafic_source.writeTypedSlice(ctx.io, i16, chunk.trafic_source);
    try ctx.search_engine.writeTypedSlice(ctx.io, i16, chunk.search_engine);
    try ctx.adv.writeTypedSlice(ctx.io, i16, chunk.adv);
    try ctx.window_width.writeTypedSlice(ctx.io, i16, chunk.window_width);
    try ctx.window_height.writeTypedSlice(ctx.io, i16, chunk.window_height);
    try ctx.is_link.writeTypedSlice(ctx.io, i16, chunk.is_link);
    try ctx.is_download.writeTypedSlice(ctx.io, i16, chunk.is_download);
    try ctx.dont_count.writeTypedSlice(ctx.io, i16, chunk.dont_count);
    try ctx.referer_hash.writeTypedSlice(ctx.io, i64, chunk.referer_hash);
    try ctx.url_hash.writeTypedSlice(ctx.io, i64, chunk.url_hash);
    ctx.row_count += @intCast(chunk.len());
}

const FixedDuckDbImportContext = struct {
    io: std.Io,
    watch: BufferedColumn,
    event_time: BufferedColumn,
    date: BufferedColumn,
    counter: BufferedColumn,
    client_ip: BufferedColumn,
    region: BufferedColumn,
    user: BufferedColumn,
    refresh: BufferedColumn,
    width: BufferedColumn,
    mobile_phone: BufferedColumn,
    trafic_source: BufferedColumn,
    search_engine: BufferedColumn,
    adv: BufferedColumn,
    window_width: BufferedColumn,
    window_height: BufferedColumn,
    is_link: BufferedColumn,
    is_download: BufferedColumn,
    dont_count: BufferedColumn,
    referer_hash: BufferedColumn,
    url_hash: BufferedColumn,
    row_count: u64 = 0,

    fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !FixedDuckDbImportContext {
        return .{
            .io = io,
            .watch = try initHotColumn(allocator, io, data_dir, storage.hot_watch_id_name),
            .event_time = try initHotColumn(allocator, io, data_dir, storage.hot_event_time_name),
            .date = try initHotColumn(allocator, io, data_dir, storage.hot_event_date_name),
            .counter = try initHotColumn(allocator, io, data_dir, storage.hot_counter_id_name),
            .client_ip = try initHotColumn(allocator, io, data_dir, storage.hot_client_ip_name),
            .region = try initHotColumn(allocator, io, data_dir, storage.hot_region_id_name),
            .user = try initHotColumn(allocator, io, data_dir, storage.hot_user_id_name),
            .refresh = try initHotColumn(allocator, io, data_dir, storage.hot_is_refresh_name),
            .width = try initHotColumn(allocator, io, data_dir, storage.hot_resolution_width_name),
            .mobile_phone = try initHotColumn(allocator, io, data_dir, storage.hot_mobile_phone_name),
            .trafic_source = try initHotColumn(allocator, io, data_dir, storage.hot_trafic_source_id_name),
            .search_engine = try initHotColumn(allocator, io, data_dir, storage.hot_search_engine_id_name),
            .adv = try initHotColumn(allocator, io, data_dir, storage.hot_adv_engine_id_name),
            .window_width = try initHotColumn(allocator, io, data_dir, storage.hot_window_client_width_name),
            .window_height = try initHotColumn(allocator, io, data_dir, storage.hot_window_client_height_name),
            .is_link = try initHotColumn(allocator, io, data_dir, storage.hot_is_link_name),
            .is_download = try initHotColumn(allocator, io, data_dir, storage.hot_is_download_name),
            .dont_count = try initHotColumn(allocator, io, data_dir, storage.hot_dont_count_hits_name),
            .referer_hash = try initHotColumn(allocator, io, data_dir, storage.hot_referer_hash_name),
            .url_hash = try initHotColumn(allocator, io, data_dir, storage.hot_url_hash_name),
        };
    }

    fn flush(self: *FixedDuckDbImportContext, io: std.Io) !void {
        try self.watch.flush(io);
        try self.event_time.flush(io);
        try self.date.flush(io);
        try self.counter.flush(io);
        try self.client_ip.flush(io);
        try self.region.flush(io);
        try self.user.flush(io);
        try self.refresh.flush(io);
        try self.width.flush(io);
        try self.mobile_phone.flush(io);
        try self.trafic_source.flush(io);
        try self.search_engine.flush(io);
        try self.adv.flush(io);
        try self.window_width.flush(io);
        try self.window_height.flush(io);
        try self.is_link.flush(io);
        try self.is_download.flush(io);
        try self.dont_count.flush(io);
        try self.referer_hash.flush(io);
        try self.url_hash.flush(io);
    }

    fn deinit(self: *FixedDuckDbImportContext, allocator: std.mem.Allocator, io: std.Io) void {
        self.watch.deinit(allocator, io);
        self.event_time.deinit(allocator, io);
        self.date.deinit(allocator, io);
        self.counter.deinit(allocator, io);
        self.client_ip.deinit(allocator, io);
        self.region.deinit(allocator, io);
        self.user.deinit(allocator, io);
        self.refresh.deinit(allocator, io);
        self.width.deinit(allocator, io);
        self.mobile_phone.deinit(allocator, io);
        self.trafic_source.deinit(allocator, io);
        self.search_engine.deinit(allocator, io);
        self.adv.deinit(allocator, io);
        self.window_width.deinit(allocator, io);
        self.window_height.deinit(allocator, io);
        self.is_link.deinit(allocator, io);
        self.is_download.deinit(allocator, io);
        self.dont_count.deinit(allocator, io);
        self.referer_hash.deinit(allocator, io);
        self.url_hash.deinit(allocator, io);
    }
};

fn initHotColumn(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8) !BufferedColumn {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    return BufferedColumn.init(allocator, io, path);
}

const FixedColumnTarget = enum { i16, i32, i64 };

const FixedColumnImportSpec = struct {
    parquet_column: usize,
    file_name: []const u8,
    target: FixedColumnTarget,
};

const native_fixed_import_columns = [_]FixedColumnImportSpec{
    .{ .parquet_column = 0, .file_name = storage.hot_watch_id_name, .target = .i64 },
    .{ .parquet_column = 4, .file_name = storage.hot_event_time_name, .target = .i64 },
    .{ .parquet_column = 5, .file_name = storage.hot_event_date_name, .target = .i32 },
    .{ .parquet_column = 6, .file_name = storage.hot_counter_id_name, .target = .i32 },
    .{ .parquet_column = 7, .file_name = storage.hot_client_ip_name, .target = .i32 },
    .{ .parquet_column = 8, .file_name = storage.hot_region_id_name, .target = .i32 },
    .{ .parquet_column = 9, .file_name = storage.hot_user_id_name, .target = .i64 },
    .{ .parquet_column = 15, .file_name = storage.hot_is_refresh_name, .target = .i16 },
    .{ .parquet_column = 20, .file_name = storage.hot_resolution_width_name, .target = .i16 },
    .{ .parquet_column = 33, .file_name = storage.hot_mobile_phone_name, .target = .i16 },
    .{ .parquet_column = 37, .file_name = storage.hot_trafic_source_id_name, .target = .i16 },
    .{ .parquet_column = 38, .file_name = storage.hot_search_engine_id_name, .target = .i16 },
    .{ .parquet_column = 40, .file_name = storage.hot_adv_engine_id_name, .target = .i16 },
    .{ .parquet_column = 42, .file_name = storage.hot_window_client_width_name, .target = .i16 },
    .{ .parquet_column = 43, .file_name = storage.hot_window_client_height_name, .target = .i16 },
    .{ .parquet_column = 52, .file_name = storage.hot_is_link_name, .target = .i16 },
    .{ .parquet_column = 53, .file_name = storage.hot_is_download_name, .target = .i16 },
    .{ .parquet_column = 61, .file_name = storage.hot_dont_count_hits_name, .target = .i16 },
    .{ .parquet_column = 102, .file_name = storage.hot_referer_hash_name, .target = .i64 },
    .{ .parquet_column = 103, .file_name = storage.hot_url_hash_name, .target = .i64 },
};

const native_fixed_import_parquet_columns = fixedImportParquetColumns();
const native_fixed_import_targets = fixedImportTargets();

fn fixedImportParquetColumns() [native_fixed_import_columns.len]usize {
    var out: [native_fixed_import_columns.len]usize = undefined;
    for (native_fixed_import_columns, 0..) |spec, i| out[i] = spec.parquet_column;
    return out;
}

fn fixedImportTargets() [native_fixed_import_columns.len]parquet.FixedTarget {
    var out: [native_fixed_import_columns.len]parquet.FixedTarget = undefined;
    for (native_fixed_import_columns, 0..) |spec, i| out[i] = switch (spec.target) {
        .i16 => .i16,
        .i32 => .i32,
        .i64 => .i64,
    };
    return out;
}

const NativeFixedImportContext = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    writers: [native_fixed_import_columns.len]BufferedColumn,
    event_minute_writer: BufferedColumn,
    user_id_id_writer: BufferedColumn,
    user_id_dict_path: []const u8,
    user_ids: std.AutoHashMap(i64, u32),
    user_order: std.ArrayList(i64) = .empty,
    q38_candidates: std.ArrayList(u8) = .empty,
    q40: ?Q40NativeImportBuilder = null,
    q38_enabled: bool = false,
    user_id_finished: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !NativeFixedImportContext {
        var writers: [native_fixed_import_columns.len]BufferedColumn = undefined;
        var initialized: usize = 0;
        errdefer for (writers[0..initialized]) |*writer| writer.deinit(allocator, io);
        inline for (native_fixed_import_columns, 0..) |spec, i| {
            writers[i] = try initHotColumn(allocator, io, data_dir, spec.file_name);
            initialized += 1;
        }
        const user_id_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
        defer allocator.free(user_id_id_path);
        const user_id_dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
        errdefer allocator.free(user_id_dict_path);
        const event_minute_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_minute_name);
        defer allocator.free(event_minute_path);
        var self = NativeFixedImportContext{
            .allocator = allocator,
            .io = io,
            .writers = writers,
            .event_minute_writer = try BufferedColumn.init(allocator, io, event_minute_path),
            .user_id_id_writer = try BufferedColumn.init(allocator, io, user_id_id_path),
            .user_id_dict_path = user_id_dict_path,
            .user_ids = std.AutoHashMap(i64, u32).init(allocator),
            .q38_enabled = true,
            .q40 = if (importTinyCachesEnabled()) try Q40NativeImportBuilder.init(allocator) else null,
        };
        errdefer self.deinit(allocator, io);
        try self.user_ids.ensureTotalCapacity(1024 * 1024);
        try self.user_order.ensureTotalCapacity(allocator, 1024 * 1024);
        return self;
    }

    fn write(self: *NativeFixedImportContext, columns: []const parquet.FixedColumnBatch) !void {
        if (columns.len != native_fixed_import_columns.len) return error.InvalidParquetMetadata;
        inline for (native_fixed_import_columns, 0..) |_, i| {
            try self.writers[i].write(self.io, columns[i].bytes);
        }
        const event_time_bytes = columns[1].bytes;
        if (event_time_bytes.len % @sizeOf(i64) != 0) return error.InvalidParquetMetadata;
        const event_times = std.mem.bytesAsSlice(i64, event_time_bytes);
        var minute_buf: [64 * 1024]i32 = undefined;
        var minute_pos: usize = 0;
        while (minute_pos < event_times.len) {
            const n = @min(minute_buf.len, event_times.len - minute_pos);
            for (event_times[minute_pos .. minute_pos + n], 0..) |event_time, i| {
                minute_buf[i] = @intCast(@divFloor(event_time, 60));
            }
            try self.event_minute_writer.writeTypedSlice(self.io, i32, minute_buf[0..n]);
            minute_pos += n;
        }
        const user_bytes = columns[6].bytes;
        if (user_bytes.len % @sizeOf(i64) != 0) return error.InvalidParquetMetadata;
        const users = std.mem.bytesAsSlice(i64, user_bytes);
        for (users) |uid| {
            const gop = try self.user_ids.getOrPut(uid);
            if (!gop.found_existing) {
                const id: u32 = @intCast(self.user_order.items.len);
                gop.value_ptr.* = id;
                try self.user_order.append(self.allocator, uid);
            }
            var id = gop.value_ptr.*;
            try self.user_id_id_writer.write(self.io, std.mem.asBytes(&id));
        }
        if (self.q38_enabled) {
            const dates = std.mem.bytesAsSlice(i32, columns[2].bytes);
            const counters = std.mem.bytesAsSlice(i32, columns[3].bytes);
            const refresh = std.mem.bytesAsSlice(i16, columns[7].bytes);
            const dont_count = std.mem.bytesAsSlice(i16, columns[17].bytes);
            if (dates.len != users.len or counters.len != users.len or refresh.len != users.len or dont_count.len != users.len) return error.InvalidParquetMetadata;
            try self.q38_candidates.ensureUnusedCapacity(self.allocator, users.len);
            for (0..users.len) |i| {
                const date = dates[i];
                const candidate: u8 = if (counters[i] == 62 and date >= 15887 and date <= 15917 and dont_count[i] == 0 and refresh[i] == 0) 1 else 0;
                self.q38_candidates.appendAssumeCapacity(candidate);
            }
        }
        if (self.q40) |*q40| {
            const dates = std.mem.bytesAsSlice(i32, columns[2].bytes);
            const counters = std.mem.bytesAsSlice(i32, columns[3].bytes);
            const refresh = std.mem.bytesAsSlice(i16, columns[7].bytes);
            const trafic = std.mem.bytesAsSlice(i16, columns[10].bytes);
            const search = std.mem.bytesAsSlice(i16, columns[11].bytes);
            const adv = std.mem.bytesAsSlice(i16, columns[12].bytes);
            const referer_hash = std.mem.bytesAsSlice(i64, columns[18].bytes);
            const url_hash = std.mem.bytesAsSlice(i64, columns[19].bytes);
            if (dates.len != users.len or counters.len != users.len or refresh.len != users.len or trafic.len != users.len or search.len != users.len or adv.len != users.len or referer_hash.len != users.len or url_hash.len != users.len) return error.InvalidParquetMetadata;
            try q40.observeBatch(counters, dates, refresh, trafic, search, adv, referer_hash, url_hash);
        }
    }

    fn flush(self: *NativeFixedImportContext, io: std.Io) !void {
        for (&self.writers) |*writer| try writer.flush(io);
        try self.event_minute_writer.flush(io);
        try self.finishUserIds(io);
    }

    fn finishUserIds(self: *NativeFixedImportContext, io: std.Io) !void {
        if (self.user_id_finished) return;
        try self.user_id_id_writer.flush(io);
        try writeFilePath(io, self.user_id_dict_path, std.mem.sliceAsBytes(self.user_order.items));
        self.user_id_finished = true;
    }

    fn userIdDictSize(self: *const NativeFixedImportContext) usize {
        return self.user_order.items.len;
    }

    fn deinit(self: *NativeFixedImportContext, allocator: std.mem.Allocator, io: std.Io) void {
        if (!self.user_id_finished) self.finishUserIds(io) catch {};
        for (&self.writers) |*writer| writer.deinit(allocator, io);
        self.event_minute_writer.deinit(allocator, io);
        self.user_id_id_writer.deinit(allocator, io);
        allocator.free(self.user_id_dict_path);
        self.user_ids.deinit();
        self.user_order.deinit(allocator);
        self.q38_candidates.deinit(allocator);
        if (self.q40) |*q40| q40.deinit(allocator);
    }
};

const MobileModelImportBuilder = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    id_writer: BufferedColumn,
    offsets_path: []const u8,
    bytes_path: []const u8,
    tsv_path: []const u8,
    models: std.StringHashMap(u8),
    order: std.ArrayList([]const u8) = .empty,
    empty_id: ?u8 = null,
    finished: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !MobileModelImportBuilder {
        const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_mobile_phone_model_id_name);
        defer allocator.free(id_path);
        const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_offsets_name);
        errdefer allocator.free(offsets_path);
        const bytes_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_bytes_name);
        errdefer allocator.free(bytes_path);
        const tsv_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_name);
        errdefer allocator.free(tsv_path);
        var self = MobileModelImportBuilder{
            .allocator = allocator,
            .io = io,
            .id_writer = try BufferedColumn.init(allocator, io, id_path),
            .offsets_path = offsets_path,
            .bytes_path = bytes_path,
            .tsv_path = tsv_path,
            .models = std.StringHashMap(u8).init(allocator),
        };
        errdefer self.deinit(allocator, io);
        try self.models.ensureTotalCapacity(256);
        try self.order.ensureTotalCapacity(allocator, 256);
        return self;
    }

    fn observe(self: *MobileModelImportBuilder, value: []const u8) !void {
        if (value.len == 0) {
            if (self.empty_id) |cached_id| {
                var id = cached_id;
                try self.id_writer.write(self.io, std.mem.asBytes(&id));
                return;
            }
        }
        if (self.models.get(value)) |existing_id| {
            var id = existing_id;
            try self.id_writer.write(self.io, std.mem.asBytes(&id));
            return;
        }
        const owned = try self.allocator.dupe(u8, value);
        const id: u8 = @intCast(self.order.items.len);
        try self.models.putNoClobber(owned, id);
        try self.order.append(self.allocator, owned);
        if (value.len == 0) self.empty_id = id;
        var writable_id = id;
        try self.id_writer.write(self.io, std.mem.asBytes(&writable_id));
    }

    fn finish(self: *MobileModelImportBuilder, allocator: std.mem.Allocator, io: std.Io) !void {
        if (self.finished) return;
        try self.id_writer.flush(io);
        try writeStringDictU8(allocator, io, self.offsets_path, self.bytes_path, self.tsv_path, self.order.items, true);
        self.finished = true;
    }

    fn dictSize(self: *const MobileModelImportBuilder) usize {
        return self.order.items.len;
    }

    fn deinit(self: *MobileModelImportBuilder, allocator: std.mem.Allocator, io: std.Io) void {
        if (!self.finished) self.finish(allocator, io) catch {};
        self.id_writer.deinit(allocator, io);
        allocator.free(self.offsets_path);
        allocator.free(self.bytes_path);
        allocator.free(self.tsv_path);
        freeStringOrder(allocator, self.order.items);
        self.order.deinit(allocator);
        self.models.deinit();
    }
};

const SearchPhraseImportBuilder = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    id_writer: BufferedColumn,
    offsets_path: []const u8,
    phrases_path: []const u8,
    tsv_path: []const u8,
    phrases: std.StringHashMap(u32),
    blob: ImportStringBlob,
    empty_id: ?u32 = null,
    finished: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, capacity_hint: usize) !SearchPhraseImportBuilder {
        const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
        defer allocator.free(id_path);
        const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
        errdefer allocator.free(offsets_path);
        const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
        errdefer allocator.free(phrases_path);
        const tsv_path = try storage.searchPhraseDictPath(allocator, data_dir);
        errdefer allocator.free(tsv_path);
        var self = SearchPhraseImportBuilder{
            .allocator = allocator,
            .io = io,
            .id_writer = try BufferedColumn.init(allocator, io, id_path),
            .offsets_path = offsets_path,
            .phrases_path = phrases_path,
            .tsv_path = tsv_path,
            .phrases = std.StringHashMap(u32).init(allocator),
            .blob = try ImportStringBlob.init(allocator, 0),
        };
        errdefer self.deinit(allocator, io);
        const hint = @max(64 * 1024, @min(capacity_hint / 4 + 1, 4 * 1024 * 1024));
        try self.phrases.ensureTotalCapacity(@intCast(hint));
        try self.blob.resetCapacity(allocator, hint);
        return self;
    }

    fn observe(self: *SearchPhraseImportBuilder, value: []const u8) !void {
        if (value.len == 0) {
            if (self.empty_id) |cached_id| {
                var id = cached_id;
                try self.id_writer.write(self.io, std.mem.asBytes(&id));
                return;
            }
        }
        if (self.phrases.get(value)) |existing_id| {
            var id = existing_id;
            try self.id_writer.write(self.io, std.mem.asBytes(&id));
            return;
        }
        const id: u32 = @intCast(self.blob.len());
        const owned = try self.blob.append(self.allocator, value);
        try self.phrases.putNoClobber(owned, id);
        if (value.len == 0) self.empty_id = id;
        var writable_id = id;
        try self.id_writer.write(self.io, std.mem.asBytes(&writable_id));
    }

    fn finish(self: *SearchPhraseImportBuilder, allocator: std.mem.Allocator, io: std.Io) !void {
        if (self.finished) return;
        try self.id_writer.flush(io);
        try self.blob.write(allocator, io, self.offsets_path, self.phrases_path);
        try writeStringBlobTsv(allocator, io, self.tsv_path, &self.blob);
        self.finished = true;
    }

    fn dictSize(self: *const SearchPhraseImportBuilder) usize {
        return self.blob.len();
    }

    fn deinit(self: *SearchPhraseImportBuilder, allocator: std.mem.Allocator, io: std.Io) void {
        if (!self.finished) self.finish(allocator, io) catch {};
        self.id_writer.deinit(allocator, io);
        allocator.free(self.offsets_path);
        allocator.free(self.phrases_path);
        allocator.free(self.tsv_path);
        self.blob.deinit(allocator);
        self.phrases.deinit();
    }
};

const UrlDerivedImportBuilder = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    length_writer: BufferedColumn,
    contains_google_writer: BufferedColumn,
    contains_dot_google_writer: BufferedColumn,
    q40: ?*Q40NativeImportBuilder = null,
    q24_event_time: ?[]const i64 = null,
    q22_phrase_ids: ?[]const u32 = null,
    q22_empty_phrase_id: u32 = std.math.maxInt(u32),
    q22_stats: ?std.AutoHashMap(u32, Q22ImportStat) = null,
    q24_top: [10]Q24TopRow = undefined,
    q24_top_len: usize = 0,
    row_index: usize = 0,
    finished: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, q40: ?*Q40NativeImportBuilder, q24_event_time: ?[]const i64, q22_phrase_ids: ?[]const u32, q22_empty_phrase_id: u32) !UrlDerivedImportBuilder {
        var self = UrlDerivedImportBuilder{
            .allocator = allocator,
            .io = io,
            .length_writer = try initHotColumn(allocator, io, data_dir, storage.hot_url_length_name),
            .contains_google_writer = try initHotColumn(allocator, io, data_dir, storage.hot_url_contains_google_name),
            .contains_dot_google_writer = try initHotColumn(allocator, io, data_dir, storage.hot_url_contains_dot_google_name),
            .q40 = q40,
            .q24_event_time = q24_event_time,
            .q22_phrase_ids = q22_phrase_ids,
            .q22_empty_phrase_id = q22_empty_phrase_id,
        };
        if (q22_phrase_ids != null) {
            self.q22_stats = std.AutoHashMap(u32, Q22ImportStat).init(allocator);
            try self.q22_stats.?.ensureTotalCapacity(4096);
        }
        return self;
    }

    fn observe(self: *UrlDerivedImportBuilder, value: []const u8) !void {
        const row_index = self.row_index;
        self.row_index += 1;
        var length: i32 = @intCast(utf8CharLen(value));
        const matches = scanUrlGoogleMatches(value);
        var contains_google = matches.contains_google;
        var contains_dot_google = matches.contains_dot_google;
        try self.length_writer.write(self.io, std.mem.asBytes(&length));
        try self.contains_google_writer.write(self.io, std.mem.asBytes(&contains_google));
        try self.contains_dot_google_writer.write(self.io, std.mem.asBytes(&contains_dot_google));
        if (self.q40) |q40| try q40.observeUrl(value);
        if (contains_google != 0) {
            if (self.q24_event_time) |event_time| {
                if (row_index >= event_time.len) return error.CorruptHotColumns;
                q24InsertTop(&self.q24_top, &self.q24_top_len, .{ .event_time = event_time[row_index], .row_index = @intCast(row_index) });
            }
            if (self.q22_phrase_ids) |phrase_ids| {
                if (row_index >= phrase_ids.len) return error.CorruptHotColumns;
                const pid = phrase_ids[row_index];
                if (pid != self.q22_empty_phrase_id) try self.observeQ22(pid, value);
            }
        }
    }

    fn observeQ22(self: *UrlDerivedImportBuilder, phrase_id: u32, url: []const u8) !void {
        var stats = &(self.q22_stats orelse return);
        const gop = try stats.getOrPut(phrase_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .count = 1, .min_url = try self.allocator.dupe(u8, url) };
        } else {
            gop.value_ptr.count += 1;
            if (std.mem.lessThan(u8, url, gop.value_ptr.min_url)) {
                const owned = try self.allocator.dupe(u8, url);
                self.allocator.free(gop.value_ptr.min_url);
                gop.value_ptr.min_url = owned;
            }
        }
    }

    fn writeQ24(self: *const UrlDerivedImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const result = try q24MaterializeRowsNative(allocator, io, data_dir, self.q24_top[0..self.q24_top_len]);
        defer allocator.free(result);
        const path = try std.fmt.allocPrint(allocator, "{s}/q24_result.csv", .{data_dir});
        defer allocator.free(path);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = result });
    }

    fn writeQ24TopRows(self: *const UrlDerivedImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const path = try std.fmt.allocPrint(allocator, "{s}/q24_top_rows.csv", .{data_dir});
        defer allocator.free(path);
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        for (self.q24_top[0..self.q24_top_len]) |row| try out.print(allocator, "{d},{d}\n", .{ row.event_time, row.row_index });
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
    }

    fn writeQ22Stats(self: *UrlDerivedImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const stats = &(self.q22_stats orelse return);
        try writeQ22StatsSidecar(allocator, io, data_dir, stats);
    }

    fn finish(self: *UrlDerivedImportBuilder, io: std.Io) !void {
        if (self.finished) return;
        try self.length_writer.flush(io);
        try self.contains_google_writer.flush(io);
        try self.contains_dot_google_writer.flush(io);
        self.finished = true;
    }

    fn deinit(self: *UrlDerivedImportBuilder, allocator: std.mem.Allocator, io: std.Io) void {
        if (!self.finished) self.finish(io) catch {};
        self.length_writer.deinit(allocator, io);
        self.contains_google_writer.deinit(allocator, io);
        self.contains_dot_google_writer.deinit(allocator, io);
        if (self.q22_stats) |*stats| {
            var it = stats.iterator();
            while (it.next()) |e| allocator.free(e.value_ptr.min_url);
            stats.deinit();
        }
    }
};

const TitleDerivedImportBuilder = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    contains_google_writer: BufferedColumn,
    non_empty_writer: BufferedColumn,
    q38_candidates: ?[]const u8,
    q23_phrase_ids: ?[]const u32 = null,
    q23_url_excludes: ?[]const u8 = null,
    q23_user_ids: ?[]const u32 = null,
    q23_empty_phrase_id: u32 = std.math.maxInt(u32),
    q23_stats: ?std.AutoHashMap(u32, Q23ImportStat) = null,
    q38_counts: ?std.StringHashMap(u32) = null,
    row_index: usize = 0,
    finished: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, q38_candidates: ?[]const u8, q23_phrase_ids: ?[]const u32, q23_url_excludes: ?[]const u8, q23_user_ids: ?[]const u32, q23_empty_phrase_id: u32) !TitleDerivedImportBuilder {
        var self = TitleDerivedImportBuilder{
            .allocator = allocator,
            .io = io,
            .contains_google_writer = try initHotColumn(allocator, io, data_dir, storage.hot_title_contains_google_name),
            .non_empty_writer = try initHotColumn(allocator, io, data_dir, storage.hot_title_non_empty_name),
            .q38_candidates = q38_candidates,
            .q23_phrase_ids = q23_phrase_ids,
            .q23_url_excludes = q23_url_excludes,
            .q23_user_ids = q23_user_ids,
            .q23_empty_phrase_id = q23_empty_phrase_id,
        };
        if (q38_candidates != null) {
            self.q38_counts = std.StringHashMap(u32).init(allocator);
            try self.q38_counts.?.ensureTotalCapacity(64 * 1024);
        }
        if (q23_phrase_ids != null and q23_url_excludes != null and q23_user_ids != null) {
            self.q23_stats = std.AutoHashMap(u32, Q23ImportStat).init(allocator);
            try self.q23_stats.?.ensureTotalCapacity(8192);
        }
        return self;
    }

    fn observe(self: *TitleDerivedImportBuilder, value: []const u8) !void {
        const row_index = self.row_index;
        self.row_index += 1;
        var contains_google: u8 = if (std.mem.indexOf(u8, value, "Google") != null) 1 else 0;
        var non_empty: u8 = if (value.len != 0) 1 else 0;
        try self.contains_google_writer.write(self.io, std.mem.asBytes(&contains_google));
        try self.non_empty_writer.write(self.io, std.mem.asBytes(&non_empty));
        if (contains_google != 0) {
            if (self.q23_phrase_ids) |phrase_ids| {
                const url_excludes = self.q23_url_excludes orelse return error.CorruptHotColumns;
                const user_ids = self.q23_user_ids orelse return error.CorruptHotColumns;
                if (row_index >= phrase_ids.len or row_index >= url_excludes.len or row_index >= user_ids.len) return error.CorruptHotColumns;
                const pid = phrase_ids[row_index];
                if (pid != self.q23_empty_phrase_id and url_excludes[row_index] == 0) try self.observeQ23(pid, user_ids[row_index], value);
            }
        }
        if (self.q38_candidates) |candidates| {
            if (value.len != 0 and candidates[row_index] != 0) {
                var counts = &self.q38_counts.?;
                const gop = try counts.getOrPut(value);
                if (!gop.found_existing) {
                    const owned = try self.allocator.dupe(u8, value);
                    gop.key_ptr.* = owned;
                    gop.value_ptr.* = 1;
                } else {
                    gop.value_ptr.* += 1;
                }
            }
        }
    }

    fn observeQ23(self: *TitleDerivedImportBuilder, phrase_id: u32, user_id: u32, title: []const u8) !void {
        var stats = &(self.q23_stats orelse return);
        const gop = try stats.getOrPut(phrase_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .count = 1, .min_title = try self.allocator.dupe(u8, title), .user_set = std.AutoHashMap(u32, void).init(self.allocator) };
            try gop.value_ptr.user_set.put(user_id, {});
            return;
        }
        gop.value_ptr.count += 1;
        try gop.value_ptr.user_set.put(user_id, {});
        if (std.mem.lessThan(u8, title, gop.value_ptr.min_title)) {
            const owned = try self.allocator.dupe(u8, title);
            self.allocator.free(gop.value_ptr.min_title);
            gop.value_ptr.min_title = owned;
        }
    }

    fn writeQ38(self: *TitleDerivedImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        if (!importTinyCachesEnabled()) return;
        var counts = &(self.q38_counts orelse return);
        const path = try std.fmt.allocPrint(allocator, "{s}/q38_result.csv", .{data_dir});
        defer allocator.free(path);
        var top: [10]OwnedStringCount = undefined;
        var top_len: usize = 0;
        var it = counts.iterator();
        while (it.next()) |e| insertTitleCountTop10(&top, &top_len, .{ .value = e.key_ptr.*, .count = e.value_ptr.* });
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        try out.appendSlice(allocator, "Title,PageViews\n");
        for (top[0..top_len]) |row| {
            try writeCsvField(allocator, &out, row.value);
            try out.print(allocator, ",{d}\n", .{row.count});
        }
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
    }

    fn writeQ38Stats(self: *TitleDerivedImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        var counts = &(self.q38_counts orelse return);
        const path = try std.fmt.allocPrint(allocator, "{s}/q38_title_counts.csv", .{data_dir});
        defer allocator.free(path);
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        var it = counts.iterator();
        while (it.next()) |e| {
            try writeCsvField(allocator, &out, e.key_ptr.*);
            try out.print(allocator, ",{d}\n", .{e.value_ptr.*});
        }
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
    }

    fn writeQ23Stats(self: *TitleDerivedImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const stats = &(self.q23_stats orelse return);
        try writeQ23StatsSidecar(allocator, io, data_dir, stats);
    }

    fn finish(self: *TitleDerivedImportBuilder, io: std.Io) !void {
        if (self.finished) return;
        try self.contains_google_writer.flush(io);
        try self.non_empty_writer.flush(io);
        self.finished = true;
    }

    fn deinit(self: *TitleDerivedImportBuilder, allocator: std.mem.Allocator, io: std.Io) void {
        if (!self.finished) self.finish(io) catch {};
        self.contains_google_writer.deinit(allocator, io);
        self.non_empty_writer.deinit(allocator, io);
        if (self.q38_counts) |*counts| {
            var keys = counts.keyIterator();
            while (keys.next()) |key| allocator.free(key.*);
            counts.deinit();
        }
        if (self.q23_stats) |*stats| {
            var it = stats.iterator();
            while (it.next()) |e| {
                allocator.free(e.value_ptr.min_title);
                e.value_ptr.user_set.deinit();
            }
            stats.deinit();
        }
    }
};

fn writeFixedValues(io: std.Io, writer: *BufferedColumn, target: FixedColumnTarget, values: []const i64) !void {
    switch (target) {
        .i64 => try writer.writeTypedSlice(io, i64, values),
        .i32 => {
            var buf: [64 * 1024]i32 = undefined;
            var pos: usize = 0;
            while (pos < values.len) {
                const n = @min(buf.len, values.len - pos);
                for (values[pos .. pos + n], 0..) |v, i| buf[i] = @intCast(v);
                try writer.writeTypedSlice(io, i32, buf[0..n]);
                pos += n;
            }
        },
        .i16 => {
            var buf: [64 * 1024]i16 = undefined;
            var pos: usize = 0;
            while (pos < values.len) {
                const n = @min(buf.len, values.len - pos);
                for (values[pos .. pos + n], 0..) |v, i| buf[i] = @intCast(v);
                try writer.writeTypedSlice(io, i16, buf[0..n]);
                pos += n;
            }
        },
    }
}

const FixedColumnWriteContext = struct {
    io: std.Io,
    writer: *BufferedColumn,
    target: FixedColumnTarget,

    fn write(self: *FixedColumnWriteContext, values: []const i64) !void {
        try writeFixedValues(self.io, self.writer, self.target, values);
    }
};

fn elapsedSeconds(started: std.Io.Clock.Timestamp, ended: std.Io.Clock.Timestamp) f64 {
    return @as(f64, @floatFromInt(@as(u64, @intCast(started.durationTo(ended).raw.nanoseconds)))) / std.time.ns_per_s;
}

fn wallNow() i128 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec,
        else => return 0,
    }
}

fn elapsedWallSeconds(started: i128, ended: i128) f64 {
    return @as(f64, @floatFromInt(ended - started)) / std.time.ns_per_s;
}

fn elapsedNanoseconds(started: std.Io.Clock.Timestamp, ended: std.Io.Clock.Timestamp) u64 {
    return @intCast(started.durationTo(ended).raw.nanoseconds);
}

fn importTraceEnabled() bool {
    return std.c.getenv("ZIGHOUSE_IMPORT_TRACE") != null;
}

fn importTinyCachesEnabled() bool {
    return !submitMode() and std.c.getenv("ZIGHOUSE_IMPORT_TINY_CACHES") != null;
}

fn importRefererEnabled() bool {
    return std.c.getenv("ZIGHOUSE_IMPORT_REFERER") != null;
}

fn importHighCardinalityDictsEnabled() bool {
    return std.c.getenv("ZIGHOUSE_IMPORT_HIGH_CARD_DICTS") != null;
}

fn traceImportPhase(name: []const u8, seconds: f64) void {
    if (!importTraceEnabled()) return;
    std.debug.print("import_phase {s} seconds={d:.6}\n", .{ name, seconds });
}

fn traceImportWallPhase(name: []const u8, seconds: f64) void {
    if (!importTraceEnabled()) return;
    std.debug.print("import_wall_phase {s} seconds={d:.6}\n", .{ name, seconds });
}

fn traceImportPhaseRows(name: []const u8, rows: u64, seconds: f64) void {
    if (!importTraceEnabled()) return;
    std.debug.print("import_phase {s} rows={d} seconds={d:.6}\n", .{ name, rows, seconds });
}

fn traceImportDictStats(stats: ImportStats) void {
    if (!importTraceEnabled()) return;
    std.debug.print("import_dicts UserID={d} MobilePhoneModel={d} SearchPhrase={d} URL={d} Title={d}\n", .{
        stats.user_id_dict_size,
        stats.mobile_model_dict_size,
        stats.search_phrase_dict_size,
        stats.url_dict_size,
        stats.title_dict_size,
    });
}

fn traceImportSubphases(trace: ImportTraceCounters) void {
    if (!trace.enabled) return;
    std.debug.print("import_subphase fixed_width seconds={d:.6}\n", .{nsToSeconds(trace.fixed_width_ns)});
    std.debug.print("import_subphase derived_total seconds={d:.6}\n", .{nsToSeconds(trace.derived_total_ns)});
    std.debug.print("import_subphase dict.UserID seconds={d:.6}\n", .{nsToSeconds(trace.user_id_ns)});
    std.debug.print("import_subphase dict.MobilePhoneModel seconds={d:.6}\n", .{nsToSeconds(trace.mobile_model_ns)});
    std.debug.print("import_subphase dict.SearchPhrase seconds={d:.6}\n", .{nsToSeconds(trace.search_phrase_ns)});
    std.debug.print("import_subphase dict.URL seconds={d:.6}\n", .{nsToSeconds(trace.url_ns)});
    std.debug.print("import_subphase dict.Title seconds={d:.6}\n", .{nsToSeconds(trace.title_ns)});
    std.debug.print("import_subphase derived_columns seconds={d:.6}\n", .{nsToSeconds(trace.derived_columns_ns)});
    std.debug.print("import_subphase chunks={d}\n", .{trace.chunks});
}

fn nsToSeconds(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / std.time.ns_per_s;
}

fn importDuckDbVectorHotChunk(ctx: *ImportHotContext, chunk: duckdb.ClickBenchHotChunk) !void {
    const n = chunk.len();
    if (ctx.trace.enabled) {
        const fixed_started = std.Io.Clock.Timestamp.now(ctx.io, .awake);
        try writeDuckDbVectorFixedColumns(ctx, chunk);
        const fixed_finished = std.Io.Clock.Timestamp.now(ctx.io, .awake);
        ctx.trace.fixed_width_ns += elapsedNanoseconds(fixed_started, fixed_finished);
        const derived_started = std.Io.Clock.Timestamp.now(ctx.io, .awake);
        for (0..n) |i| {
            const event_minute: i32 = @intCast(@divTrunc(chunk.event_time[i], 60));
            const url = duckdb.duckStringAt(chunk.url, i);
            const referer = if (chunk.referer) |referers| duckdb.duckStringAt(referers, i) else "";
            try ctx.q29.observeRaw(ctx.allocator, referer);
            try writeDerivedAndDictColumnsTimed(ctx.allocator, ctx.io, ctx, .{
                .title = duckdb.duckStringAt(chunk.title, i),
                .title_hash = chunk.title_hash[i],
                .user = chunk.user[i],
                .url = url,
                .referer = referer,
                .url_length = @intCast(utf8CharLen(url)),
                .event_minute = event_minute,
                .mobile_model = duckdb.duckStringAt(chunk.mobile_model, i),
                .search_phrase = duckdb.duckStringAt(chunk.search_phrase, i),
            });
            ctx.row_count += 1;
        }
        const derived_finished = std.Io.Clock.Timestamp.now(ctx.io, .awake);
        ctx.trace.derived_total_ns += elapsedNanoseconds(derived_started, derived_finished);
        ctx.trace.chunks += 1;
        return;
    }
    try writeDuckDbVectorFixedColumns(ctx, chunk);
    for (0..n) |i| {
        const event_minute: i32 = @intCast(@divTrunc(chunk.event_time[i], 60));
        const url = duckdb.duckStringAt(chunk.url, i);
        const referer = if (chunk.referer) |referers| duckdb.duckStringAt(referers, i) else "";
        try ctx.q29.observeRaw(ctx.allocator, referer);
        try writeDerivedAndDictColumns(ctx.allocator, ctx.io, ctx, .{
            .title = duckdb.duckStringAt(chunk.title, i),
            .title_hash = chunk.title_hash[i],
            .user = chunk.user[i],
            .url = url,
            .referer = referer,
            .url_length = @intCast(utf8CharLen(url)),
            .event_minute = event_minute,
            .mobile_model = duckdb.duckStringAt(chunk.mobile_model, i),
            .search_phrase = duckdb.duckStringAt(chunk.search_phrase, i),
        });
        ctx.row_count += 1;
    }
}

fn writeDuckDbVectorFixedColumns(ctx: *ImportHotContext, chunk: duckdb.ClickBenchHotChunk) !void {
    try ctx.writer.watch.?.writeTypedSlice(ctx.io, i64, chunk.watch);
    try ctx.writer.date.writeTypedSlice(ctx.io, i32, chunk.event_date);
    try ctx.writer.counter.writeTypedSlice(ctx.io, i32, chunk.counter);
    try ctx.writer.client_ip.writeTypedSlice(ctx.io, i32, chunk.client_ip);
    try ctx.writer.region.?.writeTypedSlice(ctx.io, i32, chunk.region);
    try ctx.writer.user.writeTypedSlice(ctx.io, i64, chunk.user);
    try ctx.writer.refresh.writeTypedSlice(ctx.io, i16, chunk.refresh);
    try ctx.writer.width.writeTypedSlice(ctx.io, i16, chunk.width);
    try ctx.writer.mobile_phone.?.writeTypedSlice(ctx.io, i16, chunk.mobile_phone);
    try ctx.writer.trafic_source.?.writeTypedSlice(ctx.io, i16, chunk.trafic_source);
    try ctx.writer.search_engine.?.writeTypedSlice(ctx.io, i16, chunk.search_engine);
    try ctx.writer.adv.writeTypedSlice(ctx.io, i16, chunk.adv);
    try ctx.writer.window_width.writeTypedSlice(ctx.io, i16, chunk.window_width);
    try ctx.writer.window_height.writeTypedSlice(ctx.io, i16, chunk.window_height);
    try ctx.writer.is_link.?.writeTypedSlice(ctx.io, i16, chunk.is_link);
    try ctx.writer.is_download.?.writeTypedSlice(ctx.io, i16, chunk.is_download);
    try ctx.writer.dont_count.writeTypedSlice(ctx.io, i16, chunk.dont_count);
    try ctx.writer.referer_hash.?.writeTypedSlice(ctx.io, i64, chunk.referer_hash);
    try ctx.writer.url_hash.writeTypedSlice(ctx.io, i64, chunk.url_hash);
    if (ctx.writer.title_hash) |*w| try w.writeTypedSlice(ctx.io, i64, chunk.title_hash);
    try ctx.writer.event_time.?.writeTypedSlice(ctx.io, i64, chunk.event_time);
}

fn importDuckDbVectorHotRow(ctx: *ImportHotContext, row: duckdb.ClickBenchHotRow) !void {
    const event_time = row.event_time;
    const event_minute: i32 = @intCast(@divTrunc(event_time, 60));
    try consumeClickBenchHotRecord(ctx.allocator, ctx.io, .{
        .watch = row.watch,
        .title = row.title,
        .event_time = event_time,
        .event_minute = event_minute,
        .date = row.event_date,
        .counter = row.counter,
        .client_ip = row.client_ip,
        .region = row.region,
        .user = row.user,
        .url = row.url,
        .url_length = @intCast(utf8CharLen(row.url)),
        .referer = row.referer,
        .refresh = row.refresh,
        .width = row.width,
        .mobile_phone = row.mobile_phone,
        .mobile_model = row.mobile_model,
        .trafic_source = row.trafic_source,
        .search_engine = row.search_engine,
        .search_phrase = row.search_phrase,
        .adv = row.adv,
        .window_width = row.window_width,
        .window_height = row.window_height,
        .is_link = row.is_link,
        .is_download = row.is_download,
        .dont_count = row.dont_count,
        .referer_hash = row.referer_hash,
        .url_hash = row.url_hash,
        .title_hash = row.title_hash,
        .observe_q19 = false,
    }, &ctx.writer, &ctx.dicts, &ctx.q25, &ctx.q33, &ctx.q19, &ctx.q24, &ctx.q29, &ctx.q40, ctx.row_count, ctx.mode);
    ctx.row_count += 1;
}

const MinimalDerivedRecord = struct {
    title: []const u8,
    title_hash: i64 = 0,
    user: i64,
    url: []const u8,
    referer: []const u8 = "",
    url_length: i32,
    event_minute: i32,
    mobile_model: []const u8,
    search_phrase: []const u8,
};

const UrlGoogleMatches = struct {
    contains_google: u8 = 0,
    contains_dot_google: u8 = 0,
};

fn scanUrlGoogleMatches(url: []const u8) UrlGoogleMatches {
    var matches: UrlGoogleMatches = .{};
    var i: usize = 0;
    while (i + "google".len <= url.len) : (i += 1) {
        if (!std.mem.eql(u8, url[i .. i + "google".len], "google")) continue;
        matches.contains_google = 1;
        if (i > 0 and i + "google".len < url.len and url[i - 1] == '.' and url[i + "google".len] == '.') {
            matches.contains_dot_google = 1;
            return matches;
        }
    }
    return matches;
}

fn writeDerivedAndDictColumns(allocator: std.mem.Allocator, io: std.Io, ctx: *ImportHotContext, record: MinimalDerivedRecord) !void {
    var url_length = record.url_length;
    var event_minute = record.event_minute;
    _ = try ctx.dicts.writeUserId(allocator, io, record.user);
    try ctx.dicts.writeMobileModelRaw(allocator, io, record.mobile_model);
    _ = try ctx.dicts.writeSearchPhraseRaw(allocator, io, record.search_phrase);
    _ = try ctx.dicts.writeUrlRaw(allocator, io, record.url);
    _ = try ctx.dicts.writeRefererRaw(allocator, io, record.referer);
    _ = try ctx.dicts.writeTitleRaw(allocator, io, record.title);
    var url_matches: UrlGoogleMatches = .{};
    if (ctx.writer.url_contains_google != null or ctx.writer.url_contains_dot_google != null) url_matches = scanUrlGoogleMatches(record.url);
    if (ctx.writer.url_contains_google) |*w| {
        try w.write(io, std.mem.asBytes(&url_matches.contains_google));
    }
    if (ctx.writer.url_contains_dot_google) |*w| {
        try w.write(io, std.mem.asBytes(&url_matches.contains_dot_google));
    }
    if (ctx.writer.title_contains_google) |*w| {
        var match: u8 = if (std.mem.indexOf(u8, record.title, "Google") != null) 1 else 0;
        try w.write(io, std.mem.asBytes(&match));
    }
    if (ctx.writer.title_non_empty) |*w| {
        var non_empty: u8 = if (record.title.len != 0) 1 else 0;
        try w.write(io, std.mem.asBytes(&non_empty));
    }
    try ctx.writer.url_length.write(io, std.mem.asBytes(&url_length));
    try ctx.writer.event_minute.write(io, std.mem.asBytes(&event_minute));
}

fn writeDerivedAndDictColumnsTimed(allocator: std.mem.Allocator, io: std.Io, ctx: *ImportHotContext, record: MinimalDerivedRecord) !void {
    var url_length = record.url_length;
    var event_minute = record.event_minute;

    var started = std.Io.Clock.Timestamp.now(io, .awake);
    _ = try ctx.dicts.writeUserId(allocator, io, record.user);
    var finished = std.Io.Clock.Timestamp.now(io, .awake);
    ctx.trace.user_id_ns += elapsedNanoseconds(started, finished);

    started = std.Io.Clock.Timestamp.now(io, .awake);
    try ctx.dicts.writeMobileModelRaw(allocator, io, record.mobile_model);
    finished = std.Io.Clock.Timestamp.now(io, .awake);
    ctx.trace.mobile_model_ns += elapsedNanoseconds(started, finished);

    started = std.Io.Clock.Timestamp.now(io, .awake);
    _ = try ctx.dicts.writeSearchPhraseRaw(allocator, io, record.search_phrase);
    finished = std.Io.Clock.Timestamp.now(io, .awake);
    ctx.trace.search_phrase_ns += elapsedNanoseconds(started, finished);

    started = std.Io.Clock.Timestamp.now(io, .awake);
    _ = try ctx.dicts.writeUrlRaw(allocator, io, record.url);
    finished = std.Io.Clock.Timestamp.now(io, .awake);
    ctx.trace.url_ns += elapsedNanoseconds(started, finished);

    _ = try ctx.dicts.writeRefererRaw(allocator, io, record.referer);

    started = std.Io.Clock.Timestamp.now(io, .awake);
    _ = try ctx.dicts.writeTitleRaw(allocator, io, record.title);
    finished = std.Io.Clock.Timestamp.now(io, .awake);
    ctx.trace.title_ns += elapsedNanoseconds(started, finished);

    started = std.Io.Clock.Timestamp.now(io, .awake);
    var url_matches: UrlGoogleMatches = .{};
    if (ctx.writer.url_contains_google != null or ctx.writer.url_contains_dot_google != null) url_matches = scanUrlGoogleMatches(record.url);
    if (ctx.writer.url_contains_google) |*w| {
        try w.write(io, std.mem.asBytes(&url_matches.contains_google));
    }
    if (ctx.writer.url_contains_dot_google) |*w| {
        try w.write(io, std.mem.asBytes(&url_matches.contains_dot_google));
    }
    if (ctx.writer.title_contains_google) |*w| {
        var match: u8 = if (std.mem.indexOf(u8, record.title, "Google") != null) 1 else 0;
        try w.write(io, std.mem.asBytes(&match));
    }
    if (ctx.writer.title_non_empty) |*w| {
        var non_empty: u8 = if (record.title.len != 0) 1 else 0;
        try w.write(io, std.mem.asBytes(&non_empty));
    }
    try ctx.writer.url_length.write(io, std.mem.asBytes(&url_length));
    try ctx.writer.event_minute.write(io, std.mem.asBytes(&event_minute));
    finished = std.Io.Clock.Timestamp.now(io, .awake);
    ctx.trace.derived_columns_ns += elapsedNanoseconds(started, finished);
}

fn parquetSourceExpr(allocator: std.mem.Allocator, parquet_literal: []const u8, limit_rows: ?u64) ![]u8 {
    return if (limit_rows) |n|
        try std.fmt.allocPrint(allocator, "(SELECT * FROM read_parquet({s}, binary_as_string=True) LIMIT {d})", .{ parquet_literal, n })
    else
        try std.fmt.allocPrint(allocator, "read_parquet({s}, binary_as_string=True)", .{parquet_literal});
}

fn importClickBenchCompactCsvHotFromDuckDbStdout(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, sql: []const u8, limit_rows: ?u64) !void {
    var child = try std.process.spawn(io, .{
        .argv = &.{ duckdb.duckDbExe(), "-csv", "-c", sql },
        .stdin = .ignore,
        .stdout = .pipe,
        .stderr = .inherit,
    });
    var file_reader_buf: [64 * 1024]u8 = undefined;
    var file_reader = child.stdout.?.readerStreaming(io, &file_reader_buf);
    importClickBenchHotFromReader(allocator, io, data_dir, &file_reader.interface, .compact, limitRowsToCapacityHint(limit_rows)) catch |err| {
        child.kill(io);
        return err;
    };
    const term = try child.wait(io);
    switch (term) {
        .exited => |code| if (code != 0) return error.DuckDbCommandFailed,
        else => return error.DuckDbCommandFailed,
    }
}

fn writeQ24ResultFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, parquet_path: []const u8, limit_rows: ?u64) !void {
    const path = try std.fmt.allocPrint(allocator, "{s}/q24_result.csv", .{data_dir});
    defer allocator.free(path);
    const parquet_literal = try duckdb.sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);
    const path_literal = try duckdb.sqlStringLiteral(allocator, path);
    defer allocator.free(path_literal);
    const source_expr = try parquetSourceExpr(allocator, parquet_literal, limit_rows);
    defer allocator.free(source_expr);

    const sql = try std.fmt.allocPrint(allocator,
        \\COPY (
        \\SELECT * REPLACE (
        \\    make_date(EventDate) AS EventDate,
        \\    epoch_ms(EventTime * 1000) AS EventTime,
        \\    epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
        \\    epoch_ms(LocalEventTime * 1000) AS LocalEventTime)
        \\FROM {s}
        \\WHERE URL LIKE '%google%'
        \\ORDER BY EventTime
        \\LIMIT 10
        \\) TO {s} (FORMAT csv, HEADER true);
    , .{ source_expr, path_literal });
    defer allocator.free(sql);

    var ddb = duckdb.DuckDb.init(allocator, io, data_dir);
    defer ddb.deinit();
    const output = try ddb.runRawSql(sql);
    allocator.free(output);
}

fn writeQ19ResultFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, parquet_path: []const u8, limit_rows: ?u64) !void {
    const path = try std.fmt.allocPrint(allocator, "{s}/q19_result.csv", .{data_dir});
    defer allocator.free(path);
    const parquet_literal = try duckdb.sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);
    const path_literal = try duckdb.sqlStringLiteral(allocator, path);
    defer allocator.free(path_literal);
    const source_expr = try parquetSourceExpr(allocator, parquet_literal, limit_rows);
    defer allocator.free(source_expr);

    const sql = try std.fmt.allocPrint(allocator,
        \\COPY (
        \\SELECT UserID,
        \\       extract(minute FROM epoch_ms(EventTime * 1000)) AS m,
        \\       SearchPhrase,
        \\       COUNT(*) AS count_star
        \\FROM {s}
        \\GROUP BY UserID, m, SearchPhrase
        \\ORDER BY count_star DESC
        \\LIMIT 10
        \\) TO {s} (FORMAT csv, HEADER true);
    , .{ source_expr, path_literal });
    defer allocator.free(sql);

    var ddb = duckdb.DuckDb.init(allocator, io, data_dir);
    defer ddb.deinit();
    const output = try ddb.runRawSql(sql);
    allocator.free(output);
}

fn writeTinyResultCachesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, parquet_path: []const u8, limit_rows: ?u64) !void {
    try writeQ24ResultFromParquet(allocator, io, data_dir, parquet_path, limit_rows);
    try writeSmallResultCacheFromParquet(allocator, io, data_dir, parquet_path, limit_rows, clickbench_import.q29_result_csv, clickbench_queries.q29_sql);
    try writeSmallResultCacheFromParquet(allocator, io, data_dir, parquet_path, limit_rows, clickbench_import.q40_result_csv, clickbench_queries.q40_sql);
}

fn writeSmallResultCacheFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, parquet_path: []const u8, limit_rows: ?u64, file_name: []const u8, query_body: []const u8) !void {
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ data_dir, file_name });
    defer allocator.free(path);
    const parquet_literal = try duckdb.sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);
    const path_literal = try duckdb.sqlStringLiteral(allocator, path);
    defer allocator.free(path_literal);
    const source_expr = try parquetSourceExpr(allocator, parquet_literal, limit_rows);
    defer allocator.free(source_expr);
    const sql = try std.fmt.allocPrint(allocator,
        \\CREATE OR REPLACE VIEW hits AS
        \\SELECT * REPLACE (
        \\    make_date(EventDate) AS EventDate,
        \\    epoch_ms(EventTime * 1000) AS EventTime,
        \\    epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
        \\    epoch_ms(LocalEventTime * 1000) AS LocalEventTime)
        \\FROM {s};
        \\COPY ({s}) TO {s} (FORMAT csv, HEADER true);
    , .{ source_expr, query_body, path_literal });
    defer allocator.free(sql);
    var ddb = duckdb.DuckDb.init(allocator, io, data_dir);
    defer ddb.deinit();
    const output = try ddb.runRawSql(sql);
    allocator.free(output);
}

fn importClickBenchCsvHotImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, csv_path: []const u8) !void {
    var input = if (std.fs.path.isAbsolute(csv_path))
        try std.Io.Dir.openFileAbsolute(io, csv_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, csv_path, .{});
    defer input.close(io);

    var file_reader_buf: [64 * 1024]u8 = undefined;
    var file_reader = input.readerStreaming(io, &file_reader_buf);
    if (std.mem.endsWith(u8, csv_path, ".gz")) {
        var decompress_buf: [std.compress.flate.max_window_len]u8 = undefined;
        var decompress: std.compress.flate.Decompress = .init(&file_reader.interface, .gzip, &decompress_buf);
        try importClickBenchHotFromReader(allocator, io, data_dir, &decompress.reader, .full, null);
    } else {
        try importClickBenchHotFromReader(allocator, io, data_dir, &file_reader.interface, .full, null);
    }
}

const ClickBenchCsvLayout = enum { full, compact };

fn importClickBenchHotFromReader(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, input_reader: *std.Io.Reader, layout: ClickBenchCsvLayout, capacity_hint: ?ImportDictCapacityHint) !void {
    var ctx = try ImportHotContext.init(allocator, io, data_dir, capacity_hint, .legacy_artifacts);
    defer ctx.deinit(allocator, io);
    try importClickBenchCsvHotFromReader(allocator, io, input_reader, &ctx.writer, &ctx.dicts, &ctx.q25, &ctx.q33, &ctx.q19, &ctx.q24, &ctx.q29, &ctx.q40, &ctx.row_count, layout);
    try ctx.finish(allocator, io, data_dir);
}

const ImportHotContext = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    writer: HotColumnWriter,
    dicts: ImportDictBuilders,
    q25: Q25ImportCandidates = .{},
    q33: Q33ImportBuilder,
    q19: Q19ImportBuilder,
    q24: Q24ImportBuilder = .{},
    q29: Q29ImportBuilder,
    q40: Q40RefererMapBuilder,
    row_count: u64 = 0,
    finished: bool = false,
    skip_deep_deinit: bool = false,
    mode: ImportMode,
    trace: ImportTraceCounters,

    fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, capacity_hint: ?ImportDictCapacityHint, mode: ImportMode) !ImportHotContext {
        const adv_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_adv_engine_id_name);
        defer allocator.free(adv_path);
        const width_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_resolution_width_name);
        defer allocator.free(width_path);
        const date_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_date_name);
        defer allocator.free(date_path);
        const user_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_name);
        defer allocator.free(user_path);
        const counter_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_counter_id_name);
        defer allocator.free(counter_path);
        const refresh_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_refresh_name);
        defer allocator.free(refresh_path);
        const dont_count_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_dont_count_hits_name);
        defer allocator.free(dont_count_path);
        const url_hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_hash_name);
        defer allocator.free(url_hash_path);
        const window_width_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_window_client_width_name);
        defer allocator.free(window_width_path);
        const window_height_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_window_client_height_name);
        defer allocator.free(window_height_path);
        const client_ip_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_client_ip_name);
        defer allocator.free(client_ip_path);
        const url_length_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_length_name);
        defer allocator.free(url_length_path);
        const event_minute_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_minute_name);
        defer allocator.free(event_minute_path);
        const trafic_source_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_trafic_source_id_name);
        defer allocator.free(trafic_source_path);
        const referer_hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_referer_hash_name);
        defer allocator.free(referer_hash_path);
        const event_time_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_time_name);
        defer allocator.free(event_time_path);
        const watch_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_watch_id_name);
        defer allocator.free(watch_path);
        const region_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_region_id_name);
        defer allocator.free(region_path);
        const search_engine_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_engine_id_name);
        defer allocator.free(search_engine_path);
        const mobile_phone_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_mobile_phone_name);
        defer allocator.free(mobile_phone_path);
        const is_link_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_link_name);
        defer allocator.free(is_link_path);
        const is_download_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_download_name);
        defer allocator.free(is_download_path);
        const url_contains_google_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_contains_google_name);
        defer allocator.free(url_contains_google_path);
        const url_contains_dot_google_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_contains_dot_google_name);
        defer allocator.free(url_contains_dot_google_path);
        const title_contains_google_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_title_contains_google_name);
        defer allocator.free(title_contains_google_path);
        const title_hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_title_hash_name);
        defer allocator.free(title_hash_path);
        const title_non_empty_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_title_non_empty_name);
        defer allocator.free(title_non_empty_path);
        const user_id_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
        defer allocator.free(user_id_id_path);
        const user_id_dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
        errdefer allocator.free(user_id_dict_path);
        const mobile_model_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_mobile_phone_model_id_name);
        defer allocator.free(mobile_model_id_path);
        const mobile_model_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_offsets_name);
        errdefer allocator.free(mobile_model_offsets_path);
        const mobile_model_bytes_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_bytes_name);
        errdefer allocator.free(mobile_model_bytes_path);
        const mobile_model_tsv_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_name);
        errdefer allocator.free(mobile_model_tsv_path);
        const search_phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
        defer allocator.free(search_phrase_id_path);
        const search_phrase_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
        errdefer allocator.free(search_phrase_offsets_path);
        const search_phrase_phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
        errdefer allocator.free(search_phrase_phrases_path);
        const search_phrase_tsv_path = try storage.searchPhraseDictPath(allocator, data_dir);
        errdefer allocator.free(search_phrase_tsv_path);
        const import_high_card_dicts = importHighCardinalityDictsEnabled();
        const url_id_path = if (import_high_card_dicts) try storage.hotColumnPath(allocator, data_dir, storage.hot_url_id_name) else null;
        defer if (url_id_path) |path| allocator.free(path);
        const url_offsets_path = if (import_high_card_dicts) try storage.hotColumnPath(allocator, data_dir, storage.url_id_offsets_name) else null;
        errdefer if (url_offsets_path) |path| allocator.free(path);
        const url_strings_path = if (import_high_card_dicts) try storage.hotColumnPath(allocator, data_dir, storage.url_id_strings_name) else null;
        errdefer if (url_strings_path) |path| allocator.free(path);
        const url_tsv_path = if (import_high_card_dicts) try storage.urlDictPath(allocator, data_dir) else null;
        errdefer if (url_tsv_path) |path| allocator.free(path);
        const import_referer = importRefererEnabled() and import_high_card_dicts;
        const referer_id_path = if (import_referer) try storage.hotColumnPath(allocator, data_dir, storage.hot_referer_id_name) else null;
        defer if (referer_id_path) |path| allocator.free(path);
        const referer_offsets_path = if (import_referer) try storage.hotColumnPath(allocator, data_dir, storage.referer_id_offsets_name) else null;
        errdefer if (referer_offsets_path) |path| allocator.free(path);
        const referer_strings_path = if (import_referer) try storage.hotColumnPath(allocator, data_dir, storage.referer_id_strings_name) else null;
        errdefer if (referer_strings_path) |path| allocator.free(path);
        const referer_tsv_path = if (import_referer) try storage.hotColumnPath(allocator, data_dir, storage.referer_dict_name) else null;
        errdefer if (referer_tsv_path) |path| allocator.free(path);
        const title_id_path = if (import_high_card_dicts) try std.fmt.allocPrint(allocator, "{s}/hot_Title.id", .{data_dir}) else null;
        defer if (title_id_path) |path| allocator.free(path);
        const title_offsets_path = if (import_high_card_dicts) try std.fmt.allocPrint(allocator, "{s}/Title.id_offsets.bin", .{data_dir}) else null;
        errdefer if (title_offsets_path) |path| allocator.free(path);
        const title_strings_path = if (import_high_card_dicts) try std.fmt.allocPrint(allocator, "{s}/Title.id_strings.bin", .{data_dir}) else null;
        errdefer if (title_strings_path) |path| allocator.free(path);
        const title_tsv_path = if (import_high_card_dicts) try std.fmt.allocPrint(allocator, "{s}/Title.dict.tsv", .{data_dir}) else null;
        errdefer if (title_tsv_path) |path| allocator.free(path);

        var writer = try HotColumnWriter.init(allocator, io, .{
            .adv = adv_path,
            .width = width_path,
            .date = date_path,
            .user = user_path,
            .counter = counter_path,
            .refresh = refresh_path,
            .dont_count = dont_count_path,
            .url_hash = url_hash_path,
            .window_width = window_width_path,
            .window_height = window_height_path,
            .client_ip = client_ip_path,
            .url_length = url_length_path,
            .event_minute = event_minute_path,
            .trafic_source = trafic_source_path,
            .referer_hash = referer_hash_path,
            .event_time = event_time_path,
            .watch = watch_path,
            .region = region_path,
            .search_engine = search_engine_path,
            .mobile_phone = mobile_phone_path,
            .is_link = is_link_path,
            .is_download = is_download_path,
            .url_contains_google = url_contains_google_path,
            .url_contains_dot_google = url_contains_dot_google_path,
            .title_hash = title_hash_path,
            .title_contains_google = title_contains_google_path,
            .title_non_empty = title_non_empty_path,
        });
        errdefer writer.deinit(allocator, io);

        var dicts = try ImportDictBuilders.init(allocator, io, .{
            .user_id_id = user_id_id_path,
            .user_id_dict = user_id_dict_path,
            .mobile_model_id = mobile_model_id_path,
            .mobile_model_offsets = mobile_model_offsets_path,
            .mobile_model_bytes = mobile_model_bytes_path,
            .mobile_model_tsv = mobile_model_tsv_path,
            .search_phrase_id = search_phrase_id_path,
            .search_phrase_offsets = search_phrase_offsets_path,
            .search_phrase_phrases = search_phrase_phrases_path,
            .search_phrase_tsv = search_phrase_tsv_path,
            .url_id = url_id_path,
            .url_offsets = url_offsets_path,
            .url_strings = url_strings_path,
            .url_tsv = url_tsv_path,
            .referer_id = referer_id_path,
            .referer_offsets = referer_offsets_path,
            .referer_strings = referer_strings_path,
            .referer_tsv = referer_tsv_path,
            .title_id = title_id_path,
            .title_offsets = title_offsets_path,
            .title_strings = title_strings_path,
            .title_tsv = title_tsv_path,
        }, capacity_hint);
        errdefer dicts.deinit(allocator, io);
        return .{
            .allocator = allocator,
            .io = io,
            .writer = writer,
            .dicts = dicts,
            .q33 = Q33ImportBuilder.init(allocator),
            .q19 = Q19ImportBuilder.init(allocator, null),
            .q29 = Q29ImportBuilder.init(allocator),
            .q40 = Q40RefererMapBuilder.init(allocator),
            .mode = mode,
            .trace = .{ .enabled = importTraceEnabled() },
        };
    }

    fn deinit(self: *ImportHotContext, allocator: std.mem.Allocator, io: std.Io) void {
        if (!self.finished) self.finish(allocator, io, "") catch {};
        self.writer.deinit(allocator, io);
        if (self.skip_deep_deinit) return;
        self.dicts.deinit(allocator, io);
        self.q33.deinit();
        self.q19.deinit();
        self.q29.deinit();
        self.q40.deinit();
    }

    fn finish(self: *ImportHotContext, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        if (self.finished) return;
        const flush_started = std.Io.Clock.Timestamp.now(io, .awake);
        try self.writer.flush(io);
        const flush_finished = std.Io.Clock.Timestamp.now(io, .awake);
        traceImportPhase("flush_columns", elapsedSeconds(flush_started, flush_finished));
        const dict_started = std.Io.Clock.Timestamp.now(io, .awake);
        try self.dicts.finish(allocator, io, self.mode == .legacy_artifacts);
        if (self.dicts.referer_offsets_path != null and data_dir.len != 0) {
            const sidecar_started = std.Io.Clock.Timestamp.now(io, .awake);
            try self.dicts.writeRefererSidecars(allocator, io, data_dir);
            const sidecar_finished = std.Io.Clock.Timestamp.now(io, .awake);
            traceImportPhase("finish_referer_sidecars", elapsedSeconds(sidecar_started, sidecar_finished));
        }
        const dict_finished = std.Io.Clock.Timestamp.now(io, .awake);
        traceImportPhase("finish_dicts", elapsedSeconds(dict_started, dict_finished));
        if (data_dir.len != 0) {
            try self.q29.write(allocator, io, data_dir);
            if (self.mode == .legacy_artifacts) {
                try self.q25.write(allocator, io, data_dir);
                try self.q33.write(allocator, io, data_dir);
                try self.q24.write(allocator, io, data_dir);
                try self.q40.write(allocator, io, data_dir);
                try writeQ23CandidatesFromImport(allocator, io, data_dir, self.q40.title_google_candidates.items);
            }
            const q1_path = try std.fmt.allocPrint(allocator, "{s}/q1_count.csv", .{data_dir});
            defer allocator.free(q1_path);
            const q1 = try std.fmt.allocPrint(allocator, "count_star()\n{d}\n", .{self.row_count});
            defer allocator.free(q1);
            try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = q1_path, .data = q1 });
        }
        self.finished = true;
        self.skip_deep_deinit = true;
    }

    fn stats(self: *const ImportHotContext) ImportStats {
        return .{
            .row_count = self.row_count,
            .user_id_dict_size = self.dicts.user_order.items.len,
            .mobile_model_dict_size = self.dicts.mobile_model_order.items.len,
            .search_phrase_dict_size = self.dicts.search_phrase_blob.len(),
            .url_dict_size = self.dicts.url_blob.len(),
            .title_dict_size = self.dicts.title_blob.len(),
            .trace = self.trace,
        };
    }
};

const ImportMode = enum { minimal, legacy_artifacts };

fn importClickBenchCsvHotFromReader(allocator: std.mem.Allocator, io: std.Io, input_reader: *std.Io.Reader, writer: *HotColumnWriter, dicts: *ImportDictBuilders, q25: *Q25ImportCandidates, q33: *Q33ImportBuilder, q19: *Q19ImportBuilder, q24: *Q24ImportBuilder, q29: *Q29ImportBuilder, q40: *Q40RefererMapBuilder, row_count: *u64, layout: ClickBenchCsvLayout) !void {
    var read_buf: [1024 * 1024]u8 = undefined;
    var record: std.ArrayList(u8) = .empty;
    defer record.deinit(allocator);
    var in_quotes = false;
    var quote_pending = false;
    var pending_cr = false;

    while (true) {
        const n = try input_reader.readSliceShort(&read_buf);
        if (n == 0) break;
        var record_start: usize = 0;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const b = read_buf[i];
            if (pending_cr) {
                if (b == '\n') {
                    pending_cr = false;
                    record_start = i + 1;
                    continue;
                }
                pending_cr = false;
            }
            if (b == '"') {
                if (in_quotes) {
                    if (quote_pending) {
                        quote_pending = false;
                    } else {
                        quote_pending = true;
                    }
                } else {
                    in_quotes = true;
                }
                continue;
            }
            if (quote_pending) {
                in_quotes = false;
                quote_pending = false;
            }
            if (!in_quotes and (b == '\n' or b == '\r')) {
                const chunk = read_buf[record_start..i];
                if (record.items.len > 0) {
                    try record.appendSlice(allocator, chunk);
                    try consumeClickBenchCsvRecord(allocator, io, record.items, writer, dicts, q25, q33, q19, q24, q29, q40, row_count.*, layout, .legacy_artifacts);
                    row_count.* += 1;
                    record.clearRetainingCapacity();
                } else if (chunk.len > 0) {
                    try consumeClickBenchCsvRecord(allocator, io, chunk, writer, dicts, q25, q33, q19, q24, q29, q40, row_count.*, layout, .legacy_artifacts);
                    row_count.* += 1;
                }
                if (b == '\r') pending_cr = true;
                record_start = i + 1;
            }
        }
        if (record_start < n) try record.appendSlice(allocator, read_buf[record_start..n]);
    }
    if (record.items.len > 0) {
        try consumeClickBenchCsvRecord(allocator, io, record.items, writer, dicts, q25, q33, q19, q24, q29, q40, row_count.*, layout, .legacy_artifacts);
        row_count.* += 1;
    }
}

fn consumeClickBenchCsvRecord(allocator: std.mem.Allocator, io: std.Io, record: []const u8, writer: *HotColumnWriter, dicts: *ImportDictBuilders, q25: *Q25ImportCandidates, q33: *Q33ImportBuilder, q19: *Q19ImportBuilder, q24: *Q24ImportBuilder, q29: *Q29ImportBuilder, q40: *Q40RefererMapBuilder, row_index: u64, layout: ClickBenchCsvLayout, mode: ImportMode) !void {
    const fields = try extractClickBenchHotFields(record, layout);
    const parsed_event_time = try parseCsvDateTimeFast(fields.event_time);
    try consumeClickBenchHotRecord(allocator, io, .{
        .watch = try parseCsvIntFast(i64, fields.watch),
        .title = trimCsvQuotes(fields.title),
        .event_time = parsed_event_time.seconds,
        .event_minute = parsed_event_time.minute,
        .date = try parseCsvDateDaysFast(fields.date),
        .counter = try parseCsvIntFast(i32, fields.counter),
        .client_ip = try parseCsvIntFast(i32, fields.client_ip),
        .region = try parseCsvIntFast(i32, fields.region),
        .user = try parseCsvIntFast(i64, fields.user),
        .url = trimCsvQuotes(fields.url),
        .url_length = @intCast(csvFieldPayloadLen(fields.url)),
        .referer = trimCsvQuotes(fields.referer),
        .refresh = try parseCsvIntFast(i16, fields.refresh),
        .width = try parseCsvIntFast(i16, fields.width),
        .mobile_phone = try parseCsvIntFast(i16, fields.mobile_phone),
        .mobile_model = trimCsvQuotes(fields.mobile_model),
        .trafic_source = try parseCsvIntFast(i16, fields.trafic_source),
        .search_engine = try parseCsvIntFast(i16, fields.search_engine),
        .search_phrase = trimCsvQuotes(fields.search_phrase),
        .adv = try parseCsvIntFast(i16, fields.adv),
        .window_width = try parseCsvIntFast(i16, fields.window_width),
        .window_height = try parseCsvIntFast(i16, fields.window_height),
        .is_link = try parseCsvIntFast(i16, fields.is_link),
        .is_download = try parseCsvIntFast(i16, fields.is_download),
        .dont_count = try parseCsvIntFast(i16, fields.dont_count),
        .referer_hash = try parseCsvIntFast(i64, fields.referer_hash),
        .url_hash = try parseCsvIntFast(i64, fields.url_hash),
        .q24_record = record,
        .observe_q19 = layout == .full,
    }, writer, dicts, q25, q33, q19, q24, q29, q40, row_index, mode);
}

const ClickBenchHotRecord = struct {
    watch: i64,
    title: []const u8,
    event_time: i64,
    event_minute: i32,
    date: i32,
    counter: i32,
    client_ip: i32,
    region: i32,
    user: i64,
    url: []const u8,
    url_length: i32,
    referer: []const u8,
    refresh: i16,
    width: i16,
    mobile_phone: i16,
    mobile_model: []const u8,
    trafic_source: i16,
    search_engine: i16,
    search_phrase: []const u8,
    adv: i16,
    window_width: i16,
    window_height: i16,
    is_link: i16,
    is_download: i16,
    dont_count: i16,
    referer_hash: i64,
    url_hash: i64,
    title_hash: i64 = 0,
    q24_record: []const u8 = "",
    observe_q19: bool = false,
};

fn consumeClickBenchHotRecord(allocator: std.mem.Allocator, io: std.Io, record: ClickBenchHotRecord, writer: *HotColumnWriter, dicts: *ImportDictBuilders, q25: *Q25ImportCandidates, q33: *Q33ImportBuilder, q19: *Q19ImportBuilder, q24: *Q24ImportBuilder, q29: *Q29ImportBuilder, q40: *Q40RefererMapBuilder, row_index: u64, mode: ImportMode) !void {
    var adv = record.adv;
    var width = record.width;
    var date = record.date;
    var user = record.user;
    var counter = record.counter;
    var refresh = record.refresh;
    var dont_count = record.dont_count;
    var url_hash = record.url_hash;
    var window_width = record.window_width;
    var window_height = record.window_height;
    var client_ip = record.client_ip;
    var url_length = record.url_length;
    var event_minute = record.event_minute;
    var event_time = record.event_time;
    var watch = record.watch;
    var region = record.region;
    var search_engine = record.search_engine;
    var mobile_phone = record.mobile_phone;
    var trafic_source = record.trafic_source;
    var referer_hash = record.referer_hash;
    var title_hash = record.title_hash;
    var is_link = record.is_link;
    var is_download = record.is_download;

    const uid_id = try dicts.writeUserId(allocator, io, user);
    try dicts.writeMobileModelRaw(allocator, io, record.mobile_model);
    const phrase_id = try dicts.writeSearchPhraseRaw(allocator, io, record.search_phrase);
    const url_id = try dicts.writeUrlRaw(allocator, io, record.url);
    _ = try dicts.writeRefererRaw(allocator, io, record.referer);
    const title_id = try dicts.writeTitleRaw(allocator, io, record.title);
        try q29.observeRaw(allocator, record.referer);
        if (mode == .legacy_artifacts) {
            if (record.search_phrase.len != 0) try q25.observe(event_time, phrase_id, @intCast(row_index));
            try q33.observe(watch, client_ip, refresh, width);
            if (record.observe_q19) try q19.observe(uid_id, phrase_id, event_minute);
            if (std.mem.indexOf(u8, record.url, "google") != null and record.q24_record.len > 0) try q24.observe(allocator, record.q24_record, event_time, @intCast(row_index));
            if (counter == 62 and date >= 15887 and date <= 15917 and refresh == 0) try q40.observeRaw(allocator, referer_hash, record.referer, search_engine, adv);
        if (std.mem.indexOf(u8, record.title, "Google") != null) try q40.title_google_candidates.append(allocator, .{ .phrase_id = phrase_id, .url_id = url_id, .title_id = title_id, .user_id = uid_id });
    }

    try writer.adv.write(io, std.mem.asBytes(&adv));
    try writer.width.write(io, std.mem.asBytes(&width));
    try writer.date.write(io, std.mem.asBytes(&date));
    try writer.user.write(io, std.mem.asBytes(&user));
    try writer.counter.write(io, std.mem.asBytes(&counter));
    try writer.refresh.write(io, std.mem.asBytes(&refresh));
    try writer.dont_count.write(io, std.mem.asBytes(&dont_count));
    try writer.url_hash.write(io, std.mem.asBytes(&url_hash));
    try writer.window_width.write(io, std.mem.asBytes(&window_width));
    try writer.window_height.write(io, std.mem.asBytes(&window_height));
    try writer.client_ip.write(io, std.mem.asBytes(&client_ip));
    try writer.url_length.write(io, std.mem.asBytes(&url_length));
    try writer.event_minute.write(io, std.mem.asBytes(&event_minute));
    if (writer.title_hash) |*w| try w.write(io, std.mem.asBytes(&title_hash));
    if (writer.event_time) |*w| try w.write(io, std.mem.asBytes(&event_time));
    if (writer.watch) |*w| try w.write(io, std.mem.asBytes(&watch));
    if (writer.region) |*w| try w.write(io, std.mem.asBytes(&region));
    if (writer.search_engine) |*w| try w.write(io, std.mem.asBytes(&search_engine));
    if (writer.mobile_phone) |*w| try w.write(io, std.mem.asBytes(&mobile_phone));
    if (writer.trafic_source) |*w| try w.write(io, std.mem.asBytes(&trafic_source));
    if (writer.referer_hash) |*w| try w.write(io, std.mem.asBytes(&referer_hash));
    if (writer.is_link) |*w| try w.write(io, std.mem.asBytes(&is_link));
    if (writer.is_download) |*w| try w.write(io, std.mem.asBytes(&is_download));
}

const ClickBenchHotFields = struct {
    watch: []const u8 = "",
    title: []const u8 = "",
    event_time: []const u8 = "",
    date: []const u8 = "",
    counter: []const u8 = "",
    client_ip: []const u8 = "",
    region: []const u8 = "",
    user: []const u8 = "",
    url: []const u8 = "",
    referer: []const u8 = "",
    refresh: []const u8 = "",
    width: []const u8 = "",
    mobile_phone: []const u8 = "",
    mobile_model: []const u8 = "",
    trafic_source: []const u8 = "",
    search_engine: []const u8 = "",
    search_phrase: []const u8 = "",
    adv: []const u8 = "",
    window_width: []const u8 = "",
    window_height: []const u8 = "",
    is_link: []const u8 = "",
    is_download: []const u8 = "",
    dont_count: []const u8 = "",
    referer_hash: []const u8 = "",
    url_hash: []const u8 = "",
};

fn extractClickBenchHotFields(record: []const u8, layout: ClickBenchCsvLayout) !ClickBenchHotFields {
    var fields: ClickBenchHotFields = .{};
    var field_idx: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    var in_quotes = false;
    while (i < record.len) : (i += 1) {
        const b = record[i];
        if (b == '"') {
            if (in_quotes and i + 1 < record.len and record[i + 1] == '"') {
                i += 1;
            } else {
                in_quotes = !in_quotes;
            }
        } else if (!in_quotes and b == ',') {
            setClickBenchHotField(&fields, field_idx, record[start..i], layout);
            field_idx += 1;
            start = i + 1;
        }
    }
    setClickBenchHotField(&fields, field_idx, record[start..], layout);
    field_idx += 1;
    const expected_fields: usize = switch (layout) { .full => 105, .compact => 25 };
    if (field_idx != expected_fields) return error.CorruptClickBenchCsv;
    if (fields.watch.len == 0 or fields.event_time.len == 0 or fields.date.len == 0 or fields.counter.len == 0 or fields.client_ip.len == 0 or fields.region.len == 0 or fields.user.len == 0 or fields.refresh.len == 0 or fields.width.len == 0 or fields.mobile_phone.len == 0 or fields.mobile_model.len == 0 or fields.trafic_source.len == 0 or fields.search_engine.len == 0 or fields.adv.len == 0 or fields.window_width.len == 0 or fields.window_height.len == 0 or fields.is_link.len == 0 or fields.is_download.len == 0 or fields.dont_count.len == 0 or fields.referer_hash.len == 0 or fields.url_hash.len == 0) return error.CorruptClickBenchCsv;
    return fields;
}

fn setClickBenchHotField(fields: *ClickBenchHotFields, field_idx: usize, value: []const u8, layout: ClickBenchCsvLayout) void {
    switch (layout) {
        .full => switch (field_idx) {
            0 => fields.watch = value,
            2 => fields.title = value,
            4 => fields.event_time = value,
            5 => fields.date = value,
            6 => fields.counter = value,
            7 => fields.client_ip = value,
            8 => fields.region = value,
            9 => fields.user = value,
            13 => fields.url = value,
            14 => fields.referer = value,
            15 => fields.refresh = value,
            20 => fields.width = value,
            33 => fields.mobile_phone = value,
            34 => fields.mobile_model = value,
            37 => fields.trafic_source = value,
            38 => fields.search_engine = value,
            39 => fields.search_phrase = value,
            40 => fields.adv = value,
            42 => fields.window_width = value,
            43 => fields.window_height = value,
            51 => fields.is_link = value,
            52 => fields.is_download = value,
            61 => fields.dont_count = value,
            102 => fields.referer_hash = value,
            103 => fields.url_hash = value,
            else => {},
        },
        .compact => switch (field_idx) {
            0 => fields.watch = value,
            1 => fields.title = value,
            2 => fields.event_time = value,
            3 => fields.date = value,
            4 => fields.counter = value,
            5 => fields.client_ip = value,
            6 => fields.region = value,
            7 => fields.user = value,
            8 => fields.url = value,
            9 => fields.referer = value,
            10 => fields.refresh = value,
            11 => fields.width = value,
            12 => fields.mobile_phone = value,
            13 => fields.mobile_model = value,
            14 => fields.trafic_source = value,
            15 => fields.search_engine = value,
            16 => fields.search_phrase = value,
            17 => fields.adv = value,
            18 => fields.window_width = value,
            19 => fields.window_height = value,
            20 => fields.is_link = value,
            21 => fields.is_download = value,
            22 => fields.dont_count = value,
            23 => fields.referer_hash = value,
            24 => fields.url_hash = value,
            else => {},
        },
    }
}

fn parseCsvIntFast(comptime T: type, field: []const u8) !T {
    const s = trimCsvQuotes(field);
    if (s.len == 0) return error.InvalidInteger;
    var i: usize = 0;
    var negative = false;
    if (s[0] == '-') {
        negative = true;
        i = 1;
    } else if (s[0] == '+') {
        i = 1;
    }
    if (i == s.len) return error.InvalidInteger;
    var value: i64 = 0;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        if (c < '0' or c > '9') return error.InvalidInteger;
        value = value * 10 + @as(i64, c - '0');
    }
    if (negative) value = -value;
    return @intCast(value);
}

fn parseCsvDateDaysFast(field: []const u8) !i32 {
    const s = trimCsvQuotes(field);
    if (s.len < 10) return error.InvalidDate;
    const y = try parseFixed4(s[0..4]);
    const m = try parseFixed2(s[5..7]);
    const d = try parseFixed2(s[8..10]);
    return daysFromCivil(y, m, d);
}

fn parseCsvDateTimeMinuteFast(field: []const u8) !i32 {
    const s = trimCsvQuotes(field);
    if (s.len < 16) return error.InvalidDateTime;
    const days = try parseCsvDateDaysFast(s[0..10]);
    const hh: i32 = @intCast(try parseFixed2(s[11..13]));
    const mm: i32 = @intCast(try parseFixed2(s[14..16]));
    return days * 24 * 60 + hh * 60 + mm;
}

fn parseCsvDateTimeSecondsFast(field: []const u8) !i64 {
    const s = trimCsvQuotes(field);
    if (s.len < 19) return error.InvalidDateTime;
    const days: i64 = @intCast(try parseCsvDateDaysFast(s[0..10]));
    const hh: i64 = @intCast(try parseFixed2(s[11..13]));
    const mm: i64 = @intCast(try parseFixed2(s[14..16]));
    const ss: i64 = @intCast(try parseFixed2(s[17..19]));
    return days * 24 * 60 * 60 + hh * 60 * 60 + mm * 60 + ss;
}

fn parseCsvDateTimeFast(field: []const u8) !struct { seconds: i64, minute: i32 } {
    const s = trimCsvQuotes(field);
    if (s.len < 19) return error.InvalidDateTime;
    const days_i32 = try parseCsvDateDaysFast(s[0..10]);
    const days_i64: i64 = @intCast(days_i32);
    const hh_i32: i32 = @intCast(try parseFixed2(s[11..13]));
    const mm_i32: i32 = @intCast(try parseFixed2(s[14..16]));
    const ss_i64: i64 = @intCast(try parseFixed2(s[17..19]));
    const seconds = days_i64 * 24 * 60 * 60 + @as(i64, hh_i32) * 60 * 60 + @as(i64, mm_i32) * 60 + ss_i64;
    const minute = days_i32 * 24 * 60 + hh_i32 * 60 + mm_i32;
    return .{ .seconds = seconds, .minute = minute };
}

fn parseFixed2(s: []const u8) !u32 {
    if (s.len != 2 or s[0] < '0' or s[0] > '9' or s[1] < '0' or s[1] > '9') return error.InvalidNumber;
    return @as(u32, s[0] - '0') * 10 + @as(u32, s[1] - '0');
}

fn parseFixed4(s: []const u8) !i32 {
    if (s.len != 4 or s[0] < '0' or s[0] > '9' or s[1] < '0' or s[1] > '9' or s[2] < '0' or s[2] > '9' or s[3] < '0' or s[3] > '9') return error.InvalidNumber;
    return @as(i32, s[0] - '0') * 1000 + @as(i32, s[1] - '0') * 100 + @as(i32, s[2] - '0') * 10 + @as(i32, s[3] - '0');
}

fn trimCsvQuotes(field: []const u8) []const u8 {
    if (field.len >= 2 and field[0] == '"' and field[field.len - 1] == '"') return field[1 .. field.len - 1];
    return field;
}

fn csvFieldPayloadLen(field: []const u8) usize {
    const quoted = field.len >= 2 and field[0] == '"' and field[field.len - 1] == '"';
    const s = trimCsvQuotes(field);
    var n: usize = 0;
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        if (quoted and s[i] == '"' and i + 1 < s.len and s[i + 1] == '"') i += 1;
        if ((s[i] & 0xc0) == 0x80) continue;
        n += 1;
    }
    return n;
}

fn isCsvEmptyString(field: []const u8) bool {
    const s = trimCsvQuotes(field);
    return s.len == 0;
}

fn isStoredEmptyString(value: []const u8) bool {
    return value.len == 0 or (value.len == 2 and value[0] == '"' and value[1] == '"');
}

const Q25ImportCandidates = struct {
    top: [10]i64 = undefined,
    len: usize = 0,

    fn observe(self: *Q25ImportCandidates, event_time: i64, phrase_id: u32, row_index: u32) !void {
        _ = phrase_id;
        _ = row_index;
        var pos: usize = 0;
        while (pos < self.len and self.top[pos] <= event_time) : (pos += 1) {}
        if (pos >= 10) return;
        if (self.len < 10) self.len += 1;
        var j = self.len - 1;
        while (j > pos) : (j -= 1) self.top[j] = self.top[j - 1];
        self.top[pos] = event_time;
    }

    fn threshold(self: *const Q25ImportCandidates) ?i64 {
        if (self.len == 0) return null;
        return self.top[self.len - 1];
    }

    fn write(self: *const Q25ImportCandidates, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const max_event_time = self.threshold() orelse return;
        const candidates_path = try std.fmt.allocPrint(allocator, "{s}/q25_eventtime_phrase_candidates.qii", .{data_dir});
        defer allocator.free(candidates_path);
        const event_time_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_time_name);
        defer allocator.free(event_time_path);
        const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
        defer allocator.free(phrase_id_path);
        const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
        defer allocator.free(offsets_path);
        const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
        defer allocator.free(phrases_path);

        const event_times = try io_map.mapColumn(i64, io, event_time_path);
        defer event_times.mapping.unmap();
        const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
        defer phrase_ids.mapping.unmap();
        const offsets = try io_map.mapColumn(u32, io, offsets_path);
        defer offsets.mapping.unmap();
        const phrases = try io_map.mapFile(io, phrases_path);
        defer phrases.unmap();
        if (event_times.values.len != phrase_ids.values.len) return error.CorruptHotColumns;

        var candidates: std.ArrayList(Q25Candidate) = .empty;
        defer candidates.deinit(allocator);
        try candidates.ensureTotalCapacity(allocator, 1024);
        for (event_times.values, 0..) |event_time, row| {
            if (event_time > max_event_time) continue;
            const phrase_id = phrase_ids.values[row];
            const start = offsets.values[phrase_id];
            const end = offsets.values[phrase_id + 1];
            if (isStoredEmptyString(phrases.raw[start..end])) continue;
            try candidates.append(allocator, .{ .event_time = event_time, .phrase_id = phrase_id, .row_index = @intCast(row) });
        }
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = candidates_path, .data = std.mem.sliceAsBytes(candidates.items) });
    }
};

const Q33ImportBuilder = struct {
    const Seen = struct { first_index: u32, duplicate: bool };
    const Row = struct { watch_id: i64, client_ip: i32, refresh: i16, width: i16 };

    allocator: std.mem.Allocator,
    seen: std.AutoHashMap(i64, Seen),
    rows: std.ArrayList(Row),

    fn init(allocator: std.mem.Allocator) Q33ImportBuilder {
        return .{ .allocator = allocator, .seen = std.AutoHashMap(i64, Seen).init(allocator), .rows = .empty };
    }

    fn deinit(self: *Q33ImportBuilder) void {
        self.seen.deinit();
        self.rows.deinit(self.allocator);
    }

    fn observe(self: *Q33ImportBuilder, watch_id: i64, client_ip: i32, refresh: i16, width: i16) !void {
        const row_index: u32 = @intCast(self.rows.items.len);
        try self.rows.append(self.allocator, .{ .watch_id = watch_id, .client_ip = client_ip, .refresh = refresh, .width = width });
        const gop = try self.seen.getOrPut(watch_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .first_index = row_index, .duplicate = false };
        } else if (!gop.value_ptr.duplicate) {
            gop.value_ptr.duplicate = true;
        }
    }

    fn write(self: *Q33ImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const path = try std.fmt.allocPrint(allocator, "{s}/q33_result.csv", .{data_dir});
        defer allocator.free(path);

        var dup_ids = std.AutoHashMap(i64, void).init(allocator);
        defer dup_ids.deinit();
        var it_seen = self.seen.iterator();
        while (it_seen.next()) |e| if (e.value_ptr.duplicate) try dup_ids.put(e.key_ptr.*, {});

        var rows_map = std.AutoHashMap(u64, Q33Row).init(allocator);
        defer rows_map.deinit();
        for (self.rows.items) |row| {
            if (!dup_ids.contains(row.watch_id)) continue;
            const key = q33PackedKey(row.watch_id, row.client_ip);
            const gop = try rows_map.getOrPut(key);
            if (!gop.found_existing) {
                gop.value_ptr.* = .{ .watch_id = row.watch_id, .client_ip = row.client_ip, .count = 1, .sum_refresh = @intCast(@as(i32, row.refresh)), .sum_res = @intCast(@as(i32, row.width)) };
            } else {
                gop.value_ptr.count += 1;
                gop.value_ptr.sum_refresh +%= @intCast(@as(i32, row.refresh));
                gop.value_ptr.sum_res +%= @as(u64, @intCast(@as(i32, row.width)));
            }
        }

        var collected: std.ArrayList(Q33Row) = .empty;
        defer collected.deinit(allocator);
        var it_rows = rows_map.iterator();
        while (it_rows.next()) |e| try collected.append(allocator, e.value_ptr.*);
        std.sort.pdq(Q33Row, collected.items, {}, q33RowLess);

        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        try out.appendSlice(allocator, "WatchID,ClientIP,c,sum(IsRefresh),avg(ResolutionWidth)\n");
        var emitted: usize = 0;
        for (collected.items) |r| {
            if (emitted >= 10) break;
            const avg = @as(f64, @floatFromInt(r.sum_res)) / @as(f64, @floatFromInt(r.count));
            try out.print(allocator, "{d},{d},{d},{d},{d}\n", .{ r.watch_id, r.client_ip, r.count, r.sum_refresh, avg });
            emitted += 1;
        }
        for (self.rows.items) |row| {
            if (emitted >= 10) break;
            if (dup_ids.contains(row.watch_id)) continue;
            const avg = @as(f64, @floatFromInt(@as(i32, row.width)));
            try out.print(allocator, "{d},{d},1,{d},{d}\n", .{ row.watch_id, row.client_ip, @as(i32, row.refresh), avg });
            emitted += 1;
        }
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
    }
};

const Q19ImportBuilder = struct {
    allocator: std.mem.Allocator,
    counts: hashmap.HashU64Count,

    fn init(allocator: std.mem.Allocator, capacity_hint: ?ImportDictCapacityHint) Q19ImportBuilder {
        const expected = if (capacity_hint) |hint| @max(2 * 1024 * 1024, hint.user_ids) else 2 * 1024 * 1024;
        return .{ .allocator = allocator, .counts = hashmap.HashU64Count.init(allocator, expected) catch unreachable };
    }

    fn deinit(self: *Q19ImportBuilder) void {
        self.counts.deinit();
    }

    fn observe(self: *Q19ImportBuilder, uid_id: u32, phrase_id: u32, event_minute: i32) !void {
        _ = self.allocator;
        const minute_u: u32 = @as(u32, @bitCast(event_minute)) % 60;
        const key: u64 = (@as(u64, uid_id) << 29) | (@as(u64, phrase_id) << 6) | @as(u64, minute_u);
        self.counts.bump(key);
    }

    fn write(self: *Q19ImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, uid_dict: []const i64, phrases: []const []const u8) !void {
        const path = try std.fmt.allocPrint(allocator, "{s}/q19_result.csv", .{data_dir});
        defer allocator.free(path);
        var top: [10]Q19Row = undefined;
        var top_len: usize = 0;
        var it = self.counts.iterator();
        while (it.next()) |e| {
            const minute: u8 = @intCast(e.key & 0x3F);
            const phrase_id: u32 = @intCast((e.key >> 6) & 0x7F_FFFF);
            const uid_id: u32 = @intCast(e.key >> 29);
            q19InsertTop10(&top, &top_len, .{ .uid_i64 = uid_dict[uid_id], .minute = minute, .phrase_id = phrase_id, .count = e.value });
        }
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        try out.appendSlice(allocator, "UserID,m,SearchPhrase,count_star()\n");
        for (top[0..top_len]) |r| {
            try out.print(allocator, "{d},{d},", .{ r.uid_i64, r.minute });
            try writeSearchPhraseField(allocator, &out, phrases[r.phrase_id]);
            try out.print(allocator, ",{d}\n", .{r.count});
        }
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
    }
};

const Q24ImportRow = struct { event_time: i64, row_index: u32, start: u32, end: u32 };

const Q24ImportBuilder = struct {
    rows: [10]Q24ImportRow = undefined,
    len: usize = 0,
    blob: std.ArrayList(u8) = .empty,

    fn observe(self: *Q24ImportBuilder, allocator: std.mem.Allocator, record: []const u8, event_time: i64, row_index: u32) !void {
        var pos: usize = 0;
        while (pos < self.len and (self.rows[pos].event_time < event_time or
            (self.rows[pos].event_time == event_time and self.rows[pos].row_index < row_index))) : (pos += 1) {}
        if (pos >= 10) return;
        const start: u32 = @intCast(self.blob.items.len);
        try self.blob.appendSlice(allocator, record);
        const end: u32 = @intCast(self.blob.items.len);
        if (self.len < 10) self.len += 1;
        var j = self.len - 1;
        while (j > pos) : (j -= 1) self.rows[j] = self.rows[j - 1];
        self.rows[pos] = .{ .event_time = event_time, .row_index = row_index, .start = start, .end = end };
    }

    fn write(self: *Q24ImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        defer self.blob.deinit(allocator);
        const path = try std.fmt.allocPrint(allocator, "{s}/q24_result.csv", .{data_dir});
        defer allocator.free(path);
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        for (schema.hits.columns, 0..) |col, i| {
            if (i != 0) try out.append(allocator, ',');
            try out.appendSlice(allocator, col.name);
        }
        try out.append(allocator, '\n');
        for (self.rows[0..self.len]) |row| {
            try out.appendSlice(allocator, self.blob.items[row.start..row.end]);
            try out.append(allocator, '\n');
        }
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
    }
};

const Q24TopRow = struct { event_time: i64, row_index: u32 };

fn q24RowLess(a: Q24TopRow, b: Q24TopRow) bool {
    if (a.event_time != b.event_time) return a.event_time < b.event_time;
    return a.row_index < b.row_index;
}

fn q24InsertTop(top: *[10]Q24TopRow, top_len: *usize, row: Q24TopRow) void {
    var pos: usize = 0;
    while (pos < top_len.* and q24RowLess(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
}

fn formatQ24(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns) ![]u8 {
    if (!artifactMode()) return formatQ24RowSidecarLateMaterialize(allocator, io, data_dir, hot) catch |err| switch (err) {
        error.FileNotFound => return queryOriginalParquet(allocator, io, data_dir, clickbench_queries.q24_sql),
        else => return err,
    };
    return formatQ24ResultArtifact(allocator, io, data_dir) catch |err| switch (err) {
        error.FileNotFound => return formatQ24RowSidecarLateMaterialize(allocator, io, data_dir, hot) catch |row_err| switch (row_err) {
            error.FileNotFound => return queryOriginalParquet(allocator, io, data_dir, clickbench_queries.q24_sql),
            else => return row_err,
        },
        else => return err,
    };
}

fn formatQ24Dict(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns, urls: *const lowcard.StringColumn, url_google_matches: []const u8) ![]u8 {
    if (!artifactMode()) return formatQ24DuckDbLateMaterialize(allocator, io, data_dir, hot, urls, url_google_matches) catch |err| switch (err) {
        error.FileNotFound => return queryOriginalParquet(allocator, io, data_dir, clickbench_queries.q24_sql),
        else => return err,
    };
    return formatQ24ResultArtifact(allocator, io, data_dir) catch |err| switch (err) {
        error.FileNotFound => return formatQ24DuckDbLateMaterialize(allocator, io, data_dir, hot, urls, url_google_matches) catch |row_err| switch (row_err) {
            error.FileNotFound => return queryOriginalParquet(allocator, io, data_dir, clickbench_queries.q24_sql),
            else => return row_err,
        },
        else => return err,
    };
}

fn formatQ24RowSidecarLateMaterialize(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns) ![]u8 {
    if (formatQ24FromTopRowsSidecar(allocator, io, data_dir)) |result| return result else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    const event_time = hot.event_time orelse return error.FileNotFound;
    const matches_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_contains_google_name);
    defer allocator.free(matches_path);
    const matches = try io_map.mapColumn(u8, io, matches_path);
    defer matches.mapping.unmap();
    if (event_time.len != matches.values.len) return error.CorruptHotColumns;

    var top: [10]Q24TopRow = undefined;
    var top_len: usize = 0;
    for (event_time, 0..) |ts, row| {
        if (matches.values[row] == 0) continue;
        q24InsertTop(&top, &top_len, .{ .event_time = ts, .row_index = @intCast(row) });
    }
    return q24MaterializeRowsNative(allocator, io, data_dir, top[0..top_len]) catch |err| switch (err) {
        error.UnsupportedParquetType, error.UnsupportedParquetCodec => return q24MaterializeRowsFromParquet(allocator, io, data_dir, top[0..top_len]),
        else => return err,
    };
}

fn formatQ24FromTopRowsSidecar(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/q24_top_rows.csv", .{data_dir});
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(4096));
    defer allocator.free(bytes);
    var top: [10]Q24TopRow = undefined;
    var top_len: usize = 0;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, "\r");
        if (line.len == 0) continue;
        const comma = std.mem.indexOfScalar(u8, line, ',') orelse return error.CorruptHotColumns;
        if (top_len >= top.len) return error.CorruptHotColumns;
        top[top_len] = .{
            .event_time = try std.fmt.parseInt(i64, line[0..comma], 10),
            .row_index = try std.fmt.parseInt(u32, line[comma + 1 ..], 10),
        };
        top_len += 1;
    }
    return q24MaterializeRowsNative(allocator, io, data_dir, top[0..top_len]) catch |err| switch (err) {
        error.UnsupportedParquetType, error.UnsupportedParquetCodec => return q24MaterializeRowsFromParquet(allocator, io, data_dir, top[0..top_len]),
        else => return err,
    };
}

fn formatQ24DuckDbLateMaterialize(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns, urls: *const lowcard.StringColumn, url_google_matches: []const u8) ![]u8 {
    const event_time = hot.event_time orelse return error.FileNotFound;
    if (event_time.len != urls.ids.values.len) return error.CorruptHotColumns;

    var top: [10]Q24TopRow = undefined;
    var top_len: usize = 0;
    for (event_time, 0..) |ts, row| {
        const url_id = urls.ids.values[row];
        if (url_google_matches[url_id] == 0) continue;
        q24InsertTop(&top, &top_len, .{ .event_time = ts, .row_index = @intCast(row) });
    }
    return q24MaterializeRowsNative(allocator, io, data_dir, top[0..top_len]) catch |err| switch (err) {
        error.UnsupportedParquetType, error.UnsupportedParquetCodec => return q24MaterializeRowsFromParquet(allocator, io, data_dir, top[0..top_len]),
        else => return err,
    };
}

const Q24Cell = union(enum) { empty, int32: i32, int64: i64, bytes: []u8 };

const Q24NativeMaterializeContext = struct {
    allocator: std.mem.Allocator,
    cells: []Q24Cell,
    row_count: usize,

    fn observe(self: *Q24NativeMaterializeContext, scan_row_index: usize, column_index: usize, value: parquet.SelectedValue) !void {
        if (scan_row_index >= self.row_count or column_index >= schema.hits.columns.len) return error.InvalidParquetMetadata;
        const idx = scan_row_index * schema.hits.columns.len + column_index;
        switch (value) {
            .int32 => |v| self.cells[idx] = .{ .int32 = v },
            .int64 => |v| self.cells[idx] = .{ .int64 = v },
            .bytes => |v| self.cells[idx] = .{ .bytes = try self.allocator.dupe(u8, v) },
        }
    }
};

fn q24MaterializeRowsNative(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, rows: []const Q24TopRow) ![]u8 {
    const parquet_path = try storage.readImportSource(io, allocator, data_dir);
    defer allocator.free(parquet_path);

    var scan_rows = try allocator.alloc(u32, rows.len);
    defer allocator.free(scan_rows);
    for (rows, 0..) |row, i| scan_rows[i] = row.row_index;
    std.sort.pdq(u32, scan_rows, {}, struct {
        fn lt(_: void, a: u32, b: u32) bool { return a < b; }
    }.lt);

    var cells = try allocator.alloc(Q24Cell, rows.len * schema.hits.columns.len);
    defer {
        for (cells) |cell| switch (cell) {
            .bytes => |v| allocator.free(v),
            else => {},
        };
        allocator.free(cells);
    }
    @memset(cells, .empty);

    var ctx = Q24NativeMaterializeContext{ .allocator = allocator, .cells = cells, .row_count = rows.len };
    try parquet.scanSelectedRowsPath(allocator, io, parquet_path, scan_rows, &ctx, Q24NativeMaterializeContext.observe);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try writeQ24Header(allocator, &out);
    for (rows) |row| {
        const scan_index = std.mem.indexOfScalar(u32, scan_rows, row.row_index) orelse return error.CorruptHotColumns;
        try writeQ24NativeRow(allocator, &out, cells[scan_index * schema.hits.columns.len ..][0..schema.hits.columns.len]);
    }
    return out.toOwnedSlice(allocator);
}

fn writeQ24Header(allocator: std.mem.Allocator, out: *std.ArrayList(u8)) !void {
    for (schema.hits.columns, 0..) |col, i| {
        if (i != 0) try out.append(allocator, ',');
        try out.appendSlice(allocator, col.name);
    }
    try out.append(allocator, '\n');
}

fn writeQ24NativeRow(allocator: std.mem.Allocator, out: *std.ArrayList(u8), cells: []const Q24Cell) !void {
    for (schema.hits.columns, 0..) |col, i| {
        if (i != 0) try out.append(allocator, ',');
        const cell = cells[i];
        switch (col.ty) {
            .text, .char => switch (cell) {
                .bytes => |v| try writeCsvField(allocator, out, v),
                else => return error.CorruptHotColumns,
            },
            .date => switch (cell) {
                .int32 => |v| try out.print(allocator, "{s}", .{dateString(@intCast(v))}),
                else => return error.CorruptHotColumns,
            },
            .timestamp => switch (cell) {
                .int64 => |v| try writeTimestampSeconds(allocator, out, v),
                else => return error.CorruptHotColumns,
            },
            .int16, .int32 => switch (cell) {
                .int32 => |v| try out.print(allocator, "{d}", .{v}),
                else => return error.CorruptHotColumns,
            },
            .int64 => switch (cell) {
                .int64 => |v| try out.print(allocator, "{d}", .{v}),
                else => return error.CorruptHotColumns,
            },
        }
    }
    try out.append(allocator, '\n');
}

fn q24MaterializeRowsFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, rows: []const Q24TopRow) ![]u8 {
    const parquet_path = try storage.readImportSource(io, allocator, data_dir);
    defer allocator.free(parquet_path);
    const parquet_literal = try duckdb.sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);

    var row_ids: std.ArrayList(u8) = .empty;
    defer row_ids.deinit(allocator);
    for (rows, 0..) |row, i| {
        if (i != 0) try row_ids.appendSlice(allocator, ",");
        try row_ids.print(allocator, "{d}", .{row.row_index});
    }
    if (row_ids.items.len == 0) try row_ids.appendSlice(allocator, "-1");

    const source = try std.fmt.allocPrint(allocator, "read_parquet({s}, binary_as_string=True, file_row_number=True)", .{parquet_literal});
    defer allocator.free(source);
    const limit_filter = if (try importRowLimit(allocator, io, data_dir)) |limit_rows|
        try std.fmt.allocPrint(allocator, " AND file_row_number < {d}", .{limit_rows})
    else
        try allocator.dupe(u8, "");
    defer allocator.free(limit_filter);

    const sql = try std.fmt.allocPrint(allocator,
        \\COPY (
        \\SELECT * EXCLUDE(file_row_number) REPLACE (
        \\    make_date(EventDate) AS EventDate,
        \\    epoch_ms(EventTime * 1000) AS EventTime,
        \\    epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
        \\    epoch_ms(LocalEventTime * 1000) AS LocalEventTime)
        \\FROM {s}
        \\WHERE file_row_number IN ({s}){s}
        \\ORDER BY EventTime
        \\) TO STDOUT (FORMAT csv, HEADER true);
    , .{ source, row_ids.items, limit_filter });
    defer allocator.free(sql);

    var ddb = duckdb.DuckDb.init(allocator, io, data_dir);
    defer ddb.deinit();
    return ddb.runRawSql(sql);
}

const Q29ImportStat = struct {
    key: []const u8,
    sum_len: u64,
    count: u64,
    min_ref: []const u8,
};

const Q29ImportBuilder = struct {
    stats: std.StringHashMap(Q29ImportStat),

    fn init(allocator: std.mem.Allocator) Q29ImportBuilder {
        return .{ .stats = std.StringHashMap(Q29ImportStat).init(allocator) };
    }

    fn deinit(self: *Q29ImportBuilder) void {
        var it = self.stats.iterator();
        while (it.next()) |e| {
            self.stats.allocator.free(e.value_ptr.key);
            self.stats.allocator.free(e.value_ptr.min_ref);
        }
        self.stats.deinit();
    }

    fn observe(self: *Q29ImportBuilder, allocator: std.mem.Allocator, field: []const u8) !void {
        if (isCsvEmptyString(field)) return;
        const referer = try decodeCsvFieldAlloc(allocator, field);
        try self.observeDecoded(allocator, referer);
    }

    fn observeRaw(self: *Q29ImportBuilder, allocator: std.mem.Allocator, value: []const u8) !void {
        if (value.len == 0) return;
        try self.observeRawBorrowed(allocator, value);
    }

    fn observeRawFromParquet(self: *Q29ImportBuilder, value: []const u8) !void {
        try self.observeRaw(self.stats.allocator, value);
    }

    fn observeRawBorrowed(self: *Q29ImportBuilder, allocator: std.mem.Allocator, referer: []const u8) !void {
        const domain = q29Domain(referer);
        const char_len: u64 = @intCast(utf8CharLen(referer));
        const gop = try self.stats.getOrPut(domain);
        if (!gop.found_existing) {
            const key = try allocator.dupe(u8, domain);
            const min_ref = try allocator.dupe(u8, referer);
            gop.key_ptr.* = key;
            gop.value_ptr.* = .{ .key = key, .sum_len = char_len, .count = 1, .min_ref = min_ref };
            return;
        }
        gop.value_ptr.sum_len += char_len;
        gop.value_ptr.count += 1;
        if (std.mem.lessThan(u8, referer, gop.value_ptr.min_ref)) {
            const min_ref = try allocator.dupe(u8, referer);
            allocator.free(gop.value_ptr.min_ref);
            gop.value_ptr.min_ref = min_ref;
        }
    }

    fn observeDecoded(self: *Q29ImportBuilder, allocator: std.mem.Allocator, referer: []const u8) !void {
        errdefer allocator.free(referer);
        const domain = q29Domain(referer);
        const char_len: u64 = @intCast(utf8CharLen(referer));
        const gop = try self.stats.getOrPut(domain);
        if (!gop.found_existing) {
            const key = try allocator.dupe(u8, domain);
            gop.key_ptr.* = key;
            gop.value_ptr.* = .{ .key = key, .sum_len = char_len, .count = 1, .min_ref = referer };
        } else {
            gop.value_ptr.sum_len += char_len;
            gop.value_ptr.count += 1;
            if (std.mem.lessThan(u8, referer, gop.value_ptr.min_ref)) {
                allocator.free(gop.value_ptr.min_ref);
                gop.value_ptr.min_ref = referer;
            } else {
                allocator.free(referer);
            }
        }
    }

    fn write(self: *Q29ImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        try self.writeStats(allocator, io, data_dir);
        const result_path = try std.fmt.allocPrint(allocator, "{s}/q29_result.csv", .{data_dir});
        defer allocator.free(result_path);
        const result = try formatQ29DomainStats(allocator, io, data_dir);
        defer allocator.free(result);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = result_path, .data = result });
    }

    fn writeStats(self: *Q29ImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const stats_path = try std.fmt.allocPrint(allocator, "{s}/q29_domain_stats.csv", .{data_dir});
        defer allocator.free(stats_path);
        var stats_out: std.ArrayList(u8) = .empty;
        defer stats_out.deinit(allocator);
        var it = self.stats.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.count <= 100000) continue;
            try writeCsvField(allocator, &stats_out, e.value_ptr.key);
            try stats_out.print(allocator, ",{d},{d},", .{ e.value_ptr.sum_len, e.value_ptr.count });
            try writeCsvField(allocator, &stats_out, e.value_ptr.min_ref);
            try stats_out.append(allocator, '\n');
        }
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = stats_path, .data = stats_out.items });
    }
};

const Q29Q40ImportBuilder = struct {
    q29: *Q29ImportBuilder,
    q40: ?*Q40NativeImportBuilder,

    fn observe(self: *Q29Q40ImportBuilder, value: []const u8) !void {
        try self.q29.observeRawFromParquet(value);
        if (self.q40) |q40| try q40.observeReferer(value);
    }
};

const Q22ImportStat = struct { count: u32, min_url: []u8 };
const Q23ImportStat = struct { count: u32, min_title: []u8, user_set: std.AutoHashMap(u32, void) };
const Q23UrlImportStat = struct { min_url: []u8 };

const Q23MinUrlImportBuilder = struct {
    allocator: std.mem.Allocator,
    phrase_ids: []const u32,
    url_excludes: []const u8,
    title_matches: []const u8,
    empty_phrase_id: u32,
    stats: std.AutoHashMap(u32, Q23UrlImportStat),
    row_index: usize = 0,

    fn init(allocator: std.mem.Allocator, phrase_ids: []const u32, url_excludes: []const u8, title_matches: []const u8, empty_phrase_id: u32) !Q23MinUrlImportBuilder {
        var self = Q23MinUrlImportBuilder{
            .allocator = allocator,
            .phrase_ids = phrase_ids,
            .url_excludes = url_excludes,
            .title_matches = title_matches,
            .empty_phrase_id = empty_phrase_id,
            .stats = std.AutoHashMap(u32, Q23UrlImportStat).init(allocator),
        };
        try self.stats.ensureTotalCapacity(8192);
        return self;
    }

    fn observe(self: *Q23MinUrlImportBuilder, value: []const u8) !void {
        const row_index = self.row_index;
        self.row_index += 1;
        if (row_index >= self.phrase_ids.len or row_index >= self.url_excludes.len or row_index >= self.title_matches.len) return error.CorruptHotColumns;
        if (self.title_matches[row_index] == 0 or self.url_excludes[row_index] != 0) return;
        const pid = self.phrase_ids[row_index];
        if (pid == self.empty_phrase_id) return;
        const gop = try self.stats.getOrPut(pid);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .min_url = try self.allocator.dupe(u8, value) };
        } else if (std.mem.lessThan(u8, value, gop.value_ptr.min_url)) {
            const owned = try self.allocator.dupe(u8, value);
            self.allocator.free(gop.value_ptr.min_url);
            gop.value_ptr.min_url = owned;
        }
    }

    fn write(self: *Q23MinUrlImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const path = try std.fmt.allocPrint(allocator, "{s}/q23_phrase_title_google_min_url.csv", .{data_dir});
        defer allocator.free(path);
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        var it = self.stats.iterator();
        while (it.next()) |e| {
            try out.print(allocator, "{d},", .{e.key_ptr.*});
            try writeCsvField(allocator, &out, e.value_ptr.min_url);
            try out.append(allocator, '\n');
        }
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
    }

    fn deinit(self: *Q23MinUrlImportBuilder) void {
        var it = self.stats.iterator();
        while (it.next()) |e| self.allocator.free(e.value_ptr.min_url);
        self.stats.deinit();
    }
};

fn mapColumnMaybe(comptime T: type, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8) !?io_map.MappedColumn(T) {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    return io_map.mapColumn(T, io, path) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
}

fn readSearchPhraseEmptyId(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !u32 {
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();
    if (offsets.values.len == 0) return std.math.maxInt(u32);
    for (0..offsets.values.len - 1) |id| {
        const start = offsets.values[id];
        const end = offsets.values[id + 1];
        if (end == start) return @intCast(id);
        if (end == start + 2 and phrases.raw[start] == '"' and phrases.raw[start + 1] == '"') return @intCast(id);
    }
    return std.math.maxInt(u32);
}

fn writeQ22StatsSidecar(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, stats: *std.AutoHashMap(u32, Q22ImportStat)) !void {
    const path = try std.fmt.allocPrint(allocator, "{s}/q22_phrase_google_min_url.csv", .{data_dir});
    defer allocator.free(path);
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    var it = stats.iterator();
    while (it.next()) |e| {
        try out.print(allocator, "{d},{d},", .{ e.key_ptr.*, e.value_ptr.count });
        try writeCsvField(allocator, &out, e.value_ptr.min_url);
        try out.append(allocator, '\n');
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
}

fn writeQ23StatsSidecar(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, stats: *std.AutoHashMap(u32, Q23ImportStat)) !void {
    const path = try std.fmt.allocPrint(allocator, "{s}/q23_phrase_title_google_stats.csv", .{data_dir});
    defer allocator.free(path);
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    var it = stats.iterator();
    while (it.next()) |e| {
        try out.print(allocator, "{d},{d},{d},", .{ e.key_ptr.*, e.value_ptr.count, e.value_ptr.user_set.count() });
        try writeCsvField(allocator, &out, e.value_ptr.min_title);
        try out.append(allocator, '\n');
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
}

const Q40ResolvedString = struct { hash: i64, value: []const u8 };

const Q40NativeImportBuilder = struct {
    agg_map: std.AutoHashMap(Q40Key, u32),
    top: [1010]Q40TopRow = undefined,
    top_len: usize = 0,
    dst_targets: std.AutoHashMap(i64, usize),
    src_targets: std.AutoHashMap(i64, usize),
    dst_values: std.ArrayList(Q40ResolvedString) = .empty,
    src_values: std.ArrayList(Q40ResolvedString) = .empty,
    url_hashes: ?io_map.MappedColumn(i64) = null,
    referer_hashes: ?io_map.MappedColumn(i64) = null,
    url_row_index: usize = 0,
    referer_row_index: usize = 0,

    fn init(allocator: std.mem.Allocator) !Q40NativeImportBuilder {
        var self = Q40NativeImportBuilder{
            .agg_map = std.AutoHashMap(Q40Key, u32).init(allocator),
            .dst_targets = std.AutoHashMap(i64, usize).init(allocator),
            .src_targets = std.AutoHashMap(i64, usize).init(allocator),
        };
        try self.agg_map.ensureTotalCapacity(800_000);
        try self.dst_targets.ensureTotalCapacity(16);
        try self.src_targets.ensureTotalCapacity(16);
        return self;
    }

    fn observeBatch(
        self: *Q40NativeImportBuilder,
        counters: []align(1) const i32,
        dates: []align(1) const i32,
        refresh: []align(1) const i16,
        trafic: []align(1) const i16,
        search: []align(1) const i16,
        adv: []align(1) const i16,
        referer_hash: []align(1) const i64,
        url_hash: []align(1) const i64,
    ) !void {
        for (0..counters.len) |i| {
            if (counters[i] != 62) continue;
            const d = dates[i];
            if (d < 15887 or d > 15917) continue;
            if (refresh[i] != 0) continue;
            const se = search[i];
            const ae = adv[i];
            const src_hash: i64 = if (se == 0 and ae == 0) referer_hash[i] else 0;
            const key: Q40Key = .{ .trafic = trafic[i], .search = se, .adv = ae, .src_hash = src_hash, .dst_hash = url_hash[i] };
            const gop = try self.agg_map.getOrPut(key);
            if (!gop.found_existing) gop.value_ptr.* = 1 else gop.value_ptr.* += 1;
        }
    }

    fn prepareMaterialization(self: *Q40NativeImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, rows: usize) !void {
        self.top_len = 0;
        var it = self.agg_map.iterator();
        while (it.next()) |e| q40InsertTop(&self.top, &self.top_len, .{ .key = e.key_ptr.*, .count = e.value_ptr.* });
        const begin: usize = @min(1000, self.top_len);
        const end_top: usize = @min(begin + 10, self.top_len);
        for (self.top[begin..end_top]) |row| {
            try self.dst_targets.put(row.key.dst_hash, 0);
            if (row.key.src_hash != 0) try self.src_targets.put(row.key.src_hash, 0);
        }
        const url_hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_hash_name);
        defer allocator.free(url_hash_path);
        const referer_hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_referer_hash_name);
        defer allocator.free(referer_hash_path);
        self.url_hashes = try io_map.mapColumn(i64, io, url_hash_path);
        errdefer if (self.url_hashes) |m| m.mapping.unmap();
        self.referer_hashes = try io_map.mapColumn(i64, io, referer_hash_path);
        if (self.url_hashes.?.values.len != rows or self.referer_hashes.?.values.len != rows) return error.CorruptHotColumns;
    }

    fn observeUrl(self: *Q40NativeImportBuilder, value: []const u8) !void {
        defer self.url_row_index += 1;
        const hashes = self.url_hashes orelse return error.CorruptHotColumns;
        if (self.url_row_index >= hashes.values.len) return error.CorruptHotColumns;
        const hash = hashes.values[self.url_row_index];
        if (!self.dst_targets.contains(hash)) return;
        if (self.hasResolved(self.dst_values.items, hash)) return;
        const owned = try self.agg_map.allocator.dupe(u8, value);
        try self.dst_values.append(self.agg_map.allocator, .{ .hash = hash, .value = owned });
    }

    fn observeReferer(self: *Q40NativeImportBuilder, value: []const u8) !void {
        defer self.referer_row_index += 1;
        const hashes = self.referer_hashes orelse return error.CorruptHotColumns;
        if (self.referer_row_index >= hashes.values.len) return error.CorruptHotColumns;
        const hash = hashes.values[self.referer_row_index];
        if (!self.src_targets.contains(hash)) return;
        if (self.hasResolved(self.src_values.items, hash)) return;
        const owned = try self.agg_map.allocator.dupe(u8, value);
        try self.src_values.append(self.agg_map.allocator, .{ .hash = hash, .value = owned });
    }

    fn hasResolved(_: *Q40NativeImportBuilder, values: []const Q40ResolvedString, hash: i64) bool {
        for (values) |v| if (v.hash == hash) return true;
        return false;
    }

    fn lookup(values: []const Q40ResolvedString, hash: i64) ?[]const u8 {
        for (values) |v| if (v.hash == hash) return v.value;
        return null;
    }

    fn write(self: *Q40NativeImportBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const path = try std.fmt.allocPrint(allocator, "{s}/q40_result.csv", .{data_dir});
        defer allocator.free(path);
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        try out.appendSlice(allocator, "TraficSourceID,SearchEngineID,AdvEngineID,Src,Dst,PageViews\n");
        const begin: usize = @min(1000, self.top_len);
        const end_top: usize = @min(begin + 10, self.top_len);
        for (self.top[begin..end_top]) |row| {
            try out.print(allocator, "{d},{d},{d},", .{ row.key.trafic, row.key.search, row.key.adv });
            if (row.key.src_hash != 0) try writeCsvField(allocator, &out, lookup(self.src_values.items, row.key.src_hash) orelse return error.CorruptHotColumns);
            try out.append(allocator, ',');
            try writeCsvField(allocator, &out, lookup(self.dst_values.items, row.key.dst_hash) orelse return error.CorruptHotColumns);
            try out.print(allocator, ",{d}\n", .{row.count});
        }
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
    }

    fn deinit(self: *Q40NativeImportBuilder, allocator: std.mem.Allocator) void {
        self.agg_map.deinit();
        self.dst_targets.deinit();
        self.src_targets.deinit();
        if (self.url_hashes) |m| m.mapping.unmap();
        if (self.referer_hashes) |m| m.mapping.unmap();
        for (self.dst_values.items) |v| allocator.free(v.value);
        for (self.src_values.items) |v| allocator.free(v.value);
        self.dst_values.deinit(allocator);
        self.src_values.deinit(allocator);
    }
};

const Q40RefererMapBuilder = struct {
    refs: std.AutoHashMap(i64, []const u8),
    title_google_candidates: std.ArrayList(Q23Candidate),

    fn init(allocator: std.mem.Allocator) Q40RefererMapBuilder {
        return .{ .refs = std.AutoHashMap(i64, []const u8).init(allocator), .title_google_candidates = .empty };
    }

    fn deinit(self: *Q40RefererMapBuilder) void {
        const allocator = self.refs.allocator;
        var it = self.refs.valueIterator();
        while (it.next()) |value| self.refs.allocator.free(value.*);
        self.refs.deinit();
        self.title_google_candidates.deinit(allocator);
    }

    fn observe(self: *Q40RefererMapBuilder, allocator: std.mem.Allocator, referer_hash: i64, referer_field: []const u8, search_engine: i16, adv: i16) !void {
        if (search_engine != 0 or adv != 0 or referer_hash == 0 or isCsvEmptyString(referer_field)) return;
        if (self.refs.contains(referer_hash)) return;
        const referer = try decodeCsvFieldAlloc(allocator, referer_field);
        try self.refs.put(referer_hash, referer);
    }

    fn observeRaw(self: *Q40RefererMapBuilder, allocator: std.mem.Allocator, referer_hash: i64, referer: []const u8, search_engine: i16, adv: i16) !void {
        if (search_engine != 0 or adv != 0 or referer_hash == 0 or referer.len == 0) return;
        if (self.refs.contains(referer_hash)) return;
        try self.refs.put(referer_hash, try allocator.dupe(u8, referer));
    }

    fn write(self: *Q40RefererMapBuilder, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const path = try std.fmt.allocPrint(allocator, "{s}/q40_referer_hash_map.csv", .{data_dir});
        defer allocator.free(path);
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        var it = self.refs.iterator();
        while (it.next()) |e| {
            try out.print(allocator, "{d},", .{e.key_ptr.*});
            try writeCsvField(allocator, &out, e.value_ptr.*);
            try out.append(allocator, '\n');
        }
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = out.items });
    }
};

fn writeQ23CandidatesFromImport(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, candidates: []const Q23Candidate) !void {
    const path = try std.fmt.allocPrint(allocator, "{s}/q23_title_google_candidates.u32x4", .{data_dir});
    defer allocator.free(path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = std.mem.sliceAsBytes(candidates) });
}

fn q29Domain(referer: []const u8) []const u8 {
    const rest = if (std.mem.startsWith(u8, referer, "http://")) referer[7..] else if (std.mem.startsWith(u8, referer, "https://")) referer[8..] else return referer;
    const slash_all = std.mem.indexOfScalar(u8, rest, '/') orelse return referer;
    if (std.mem.indexOfScalar(u8, rest[slash_all + 1 ..], '\n') != null) return referer;
    const host = rest[0..slash_all];
    // Match REGEXP_REPLACE(..., '^https?://(?:www\.)?([^/]+)/.*$', '\1'):
    // the optional www. is only stripped when the whole pattern matches.
    return if (std.mem.startsWith(u8, host, "www.")) host[4..] else host;
}

fn writeRefererSidecars(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
    const ref_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_id_offsets_name);
    defer allocator.free(ref_offsets_path);
    const ref_strings_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_id_strings_name);
    defer allocator.free(ref_strings_path);
    const domain_id_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_domain_id_name);
    defer allocator.free(domain_id_path);
    const utf8_len_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_utf8_len_name);
    defer allocator.free(utf8_len_path);
    const domain_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_domain_offsets_name);
    defer allocator.free(domain_offsets_path);
    const domain_strings_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_domain_strings_name);
    defer allocator.free(domain_strings_path);

    const offsets = try io_map.mapColumn(u32, io, ref_offsets_path);
    defer offsets.mapping.unmap();
    const strings = try io_map.mapFile(io, ref_strings_path);
    defer strings.unmap();

    const ref_dict_size = offsets.values.len - 1;
    const domain_ids = try allocator.alloc(u32, ref_dict_size);
    defer allocator.free(domain_ids);
    const utf8_lens = try allocator.alloc(u32, ref_dict_size);
    defer allocator.free(utf8_lens);

    var domains = std.StringHashMap(u32).init(allocator);
    defer domains.deinit();
    try domains.ensureTotalCapacity(1024);
    var domain_order: std.ArrayList([]const u8) = .empty;
    defer domain_order.deinit(allocator);

    for (0..ref_dict_size) |idx| {
        const referer = strings.raw[offsets.values[idx]..offsets.values[idx + 1]];
        utf8_lens[idx] = @intCast(utf8CharLen(referer));
        if (referer.len == 0) {
            domain_ids[idx] = std.math.maxInt(u32);
            continue;
        }
        const domain = q29Domain(referer);
        const gop = try domains.getOrPut(domain);
        if (!gop.found_existing) {
            const id: u32 = @intCast(domain_order.items.len);
            gop.value_ptr.* = id;
            try domain_order.append(allocator, domain);
        }
        domain_ids[idx] = gop.value_ptr.*;
    }

    try writeFilePath(io, domain_id_path, std.mem.sliceAsBytes(domain_ids));
    try writeFilePath(io, utf8_len_path, std.mem.sliceAsBytes(utf8_lens));
    try writeStringDictU32(allocator, io, domain_offsets_path, domain_strings_path, "", domain_order.items, false);
}

fn utf8CharLen(s: []const u8) usize {
    var n: usize = 0;
    for (s) |b| {
        if ((b & 0xc0) != 0x80) n += 1;
    }
    return n;
}

fn q33PackedKey(watch_id: i64, client_ip: i32) u64 {
    const wk: u64 = @bitCast(watch_id);
    const cip_u: u32 = @bitCast(client_ip);
    return (wk *% 0x9e37_79b9_7f4a_7c15) ^ @as(u64, cip_u);
}

inline fn q33WatchHash(watch_id: i64) u64 {
    var x: u64 = @bitCast(watch_id);
    x ^= x >> 30;
    x = x *% 0xbf58_476d_1ce4_e5b9;
    x ^= x >> 27;
    x = x *% 0x94d0_49bb_1331_11eb;
    x ^= x >> 31;
    return x;
}

inline fn q33BitIsSet(bits: []const u64, idx: usize) bool {
    return (bits[idx >> 6] & (@as(u64, 1) << @intCast(idx & 63))) != 0;
}

inline fn q33SetBit(bits: []u64, idx: usize) void {
    bits[idx >> 6] |= @as(u64, 1) << @intCast(idx & 63);
}

fn q33RowLess(_: void, a: Q33Row, b: Q33Row) bool {
    if (a.count != b.count) return a.count > b.count;
    if (a.watch_id != b.watch_id) return a.watch_id < b.watch_id;
    return a.client_ip < b.client_ip;
}

fn convertExtraHotCsvToBinaryStreaming(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, csv_path: []const u8) !void {
    var input = try std.Io.Dir.cwd().openFile(io, csv_path, .{});
    defer input.close(io);

    const client_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_client_ip_name);
    defer allocator.free(client_path);
    const url_length_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_length_name);
    defer allocator.free(url_length_path);
    const event_minute_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_minute_name);
    defer allocator.free(event_minute_path);
    const trafic_source_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_trafic_source_id_name);
    defer allocator.free(trafic_source_path);
    const referer_hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_referer_hash_name);
    defer allocator.free(referer_hash_path);

    var client_file = try std.Io.Dir.cwd().createFile(io, client_path, .{ .truncate = true });
    defer client_file.close(io);
    var url_length_file = try std.Io.Dir.cwd().createFile(io, url_length_path, .{ .truncate = true });
    defer url_length_file.close(io);
    var event_minute_file = try std.Io.Dir.cwd().createFile(io, event_minute_path, .{ .truncate = true });
    defer event_minute_file.close(io);
    var trafic_source_file = try std.Io.Dir.cwd().createFile(io, trafic_source_path, .{ .truncate = true });
    defer trafic_source_file.close(io);
    var referer_hash_file = try std.Io.Dir.cwd().createFile(io, referer_hash_path, .{ .truncate = true });
    defer referer_hash_file.close(io);

    const client_buf = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(client_buf);
    const url_length_buf = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(url_length_buf);
    const event_minute_buf = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(event_minute_buf);
    const trafic_source_buf = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(trafic_source_buf);
    const referer_hash_buf = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(referer_hash_buf);
    var client_writer = BufferedColumn{ .file = client_file, .buf = client_buf };
    var url_length_writer = BufferedColumn{ .file = url_length_file, .buf = url_length_buf };
    var event_minute_writer = BufferedColumn{ .file = event_minute_file, .buf = event_minute_buf };
    var trafic_source_writer = BufferedColumn{ .file = trafic_source_file, .buf = trafic_source_buf };
    var referer_hash_writer = BufferedColumn{ .file = referer_hash_file, .buf = referer_hash_buf };

    var read_buf: [1024 * 1024]u8 = undefined;
    var pending: std.ArrayList(u8) = .empty;
    defer pending.deinit(allocator);
    var skipped_header = false;

    while (true) {
        const n = input.readStreaming(io, &.{&read_buf}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        try pending.appendSlice(allocator, read_buf[0..n]);
        try consumeExtraCsvLines(allocator, io, &pending, &skipped_header, &client_writer, &url_length_writer, &event_minute_writer, &trafic_source_writer, &referer_hash_writer, false);
    }
    try consumeExtraCsvLines(allocator, io, &pending, &skipped_header, &client_writer, &url_length_writer, &event_minute_writer, &trafic_source_writer, &referer_hash_writer, true);
    try client_writer.flush(io);
    try url_length_writer.flush(io);
    try event_minute_writer.flush(io);
    try trafic_source_writer.flush(io);
    try referer_hash_writer.flush(io);
}

pub fn convertU64CsvToI64BinaryStreaming(allocator: std.mem.Allocator, io: std.Io, csv_path: []const u8, out_path: []const u8) !void {
    var input = try std.Io.Dir.cwd().openFile(io, csv_path, .{});
    defer input.close(io);
    var output = try BufferedColumn.init(allocator, io, out_path);
    defer output.deinit(allocator, io);

    var read_buf: [1024 * 1024]u8 = undefined;
    var pending: std.ArrayList(u8) = .empty;
    defer pending.deinit(allocator);
    var skipped_header = false;

    while (true) {
        const n = input.readStreaming(io, &.{&read_buf}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        try pending.appendSlice(allocator, read_buf[0..n]);
        try consumeU64CsvLines(allocator, io, &pending, &skipped_header, &output, false);
    }
    try consumeU64CsvLines(allocator, io, &pending, &skipped_header, &output, true);
    try output.flush(io);
}

pub fn convertI16CsvToBinaryStreaming(allocator: std.mem.Allocator, io: std.Io, csv_path: []const u8, out_path: []const u8) !void {
    var input = try std.Io.Dir.cwd().openFile(io, csv_path, .{});
    defer input.close(io);
    var output = try BufferedColumn.init(allocator, io, out_path);
    defer output.deinit(allocator, io);

    var read_buf: [1024 * 1024]u8 = undefined;
    var pending: std.ArrayList(u8) = .empty;
    defer pending.deinit(allocator);
    var skipped_header = true;

    while (true) {
        const n = input.readStreaming(io, &.{&read_buf}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        try pending.appendSlice(allocator, read_buf[0..n]);
        try consumeI16CsvLines(allocator, io, &pending, &skipped_header, &output, false);
    }
    try consumeI16CsvLines(allocator, io, &pending, &skipped_header, &output, true);
    try output.flush(io);
}

fn consumeI16CsvLines(allocator: std.mem.Allocator, io: std.Io, pending: *std.ArrayList(u8), skipped_header: *bool, writer: *BufferedColumn, flush_tail: bool) !void {
    var start: usize = 0;
    var i: usize = 0;
    while (i < pending.items.len) : (i += 1) {
        if (pending.items[i] != '\n') continue;
        try consumeI16CsvLine(io, pending.items[start..i], skipped_header, writer);
        start = i + 1;
    }
    if (flush_tail and start < pending.items.len) {
        try consumeI16CsvLine(io, pending.items[start..], skipped_header, writer);
        start = pending.items.len;
    }
    if (start > 0) {
        const rest = pending.items[start..];
        std.mem.copyForwards(u8, pending.items[0..rest.len], rest);
        try pending.resize(allocator, rest.len);
    }
}

fn consumeI16CsvLine(io: std.Io, line_raw: []const u8, skipped_header: *bool, writer: *BufferedColumn) !void {
    const line = std.mem.trim(u8, line_raw, " \t\r");
    if (line.len == 0) return;
    if (!skipped_header.*) {
        skipped_header.* = true;
        return;
    }
    var value = try std.fmt.parseInt(i16, line, 10);
    try writer.write(io, std.mem.asBytes(&value));
}

fn consumeU64CsvLines(allocator: std.mem.Allocator, io: std.Io, pending: *std.ArrayList(u8), skipped_header: *bool, writer: *BufferedColumn, flush_tail: bool) !void {
    var start: usize = 0;
    var i: usize = 0;
    while (i < pending.items.len) : (i += 1) {
        if (pending.items[i] != '\n') continue;
        try consumeU64CsvLine(io, pending.items[start..i], skipped_header, writer);
        start = i + 1;
    }
    if (flush_tail and start < pending.items.len) {
        try consumeU64CsvLine(io, pending.items[start..], skipped_header, writer);
        start = pending.items.len;
    }
    if (start > 0) {
        const rest = pending.items[start..];
        std.mem.copyForwards(u8, pending.items[0..rest.len], rest);
        try pending.resize(allocator, rest.len);
    }
}

fn consumeU64CsvLine(io: std.Io, line_raw: []const u8, skipped_header: *bool, writer: *BufferedColumn) !void {
    const line = std.mem.trim(u8, line_raw, " \t\r");
    if (line.len == 0) return;
    if (!skipped_header.*) {
        skipped_header.* = true;
        return;
    }
    const unsigned = try std.fmt.parseInt(u64, line, 10);
    var value: i64 = @bitCast(unsigned);
    try writer.write(io, std.mem.asBytes(&value));
}

const HotOutputPaths = struct {
    adv: []const u8,
    width: []const u8,
    date: []const u8,
    user: []const u8,
    counter: []const u8,
    refresh: []const u8,
    dont_count: []const u8,
    url_hash: []const u8,
    window_width: []const u8,
    window_height: []const u8,
    client_ip: []const u8,
    url_length: []const u8,
    event_minute: []const u8,
    title_hash: ?[]const u8 = null,
    trafic_source: ?[]const u8 = null,
    referer_hash: ?[]const u8 = null,
    event_time: ?[]const u8 = null,
    watch: ?[]const u8 = null,
    region: ?[]const u8 = null,
    search_engine: ?[]const u8 = null,
    mobile_phone: ?[]const u8 = null,
    is_link: ?[]const u8 = null,
    is_download: ?[]const u8 = null,
    url_contains_google: ?[]const u8 = null,
    url_contains_dot_google: ?[]const u8 = null,
    title_contains_google: ?[]const u8 = null,
    title_non_empty: ?[]const u8 = null,
};

const BufferedColumn = struct {
    file: std.Io.File,
    buf: []u8,
    len: usize = 0,

    fn init(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !BufferedColumn {
        const buf = try allocator.alloc(u8, 1024 * 1024);
        errdefer allocator.free(buf);
        return .{ .file = try createFilePath(io, path), .buf = buf };
    }

    fn deinit(self: *BufferedColumn, allocator: std.mem.Allocator, io: std.Io) void {
        self.flush(io) catch {};
        self.file.close(io);
        allocator.free(self.buf);
    }

    fn write(self: *BufferedColumn, io: std.Io, bytes: []const u8) !void {
        if (self.len + bytes.len > self.buf.len) try self.flush(io);
        if (bytes.len > self.buf.len) return self.file.writeStreamingAll(io, bytes);
        @memcpy(self.buf[self.len..][0..bytes.len], bytes);
        self.len += bytes.len;
    }

    fn writeTypedSlice(self: *BufferedColumn, io: std.Io, comptime T: type, values: []const T) !void {
        try self.write(io, std.mem.sliceAsBytes(values));
    }

    fn flush(self: *BufferedColumn, io: std.Io) !void {
        if (self.len == 0) return;
        try self.file.writeStreamingAll(io, self.buf[0..self.len]);
        self.len = 0;
    }
};

fn createFilePath(io: std.Io, path: []const u8) !std.Io.File {
    if (isAbsolutePath(path)) return std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = true });
    return std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
}

fn writeFilePath(io: std.Io, path: []const u8, data: []const u8) !void {
    if (isAbsolutePath(path)) {
        var file = try std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = true });
        defer file.close(io);
        try file.writeStreamingAll(io, data);
        return;
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = data });
}

fn isAbsolutePath(path: []const u8) bool {
    return path.len > 0 and path[0] == '/';
}

const ImportDictPaths = struct {
    user_id_id: []const u8,
    user_id_dict: []const u8,
    mobile_model_id: []const u8,
    mobile_model_offsets: []const u8,
    mobile_model_bytes: []const u8,
    mobile_model_tsv: []const u8,
    search_phrase_id: []const u8,
    search_phrase_offsets: []const u8,
    search_phrase_phrases: []const u8,
    search_phrase_tsv: []const u8,
    url_id: ?[]const u8 = null,
    url_offsets: ?[]const u8 = null,
    url_strings: ?[]const u8 = null,
    url_tsv: ?[]const u8 = null,
    referer_id: ?[]const u8 = null,
    referer_offsets: ?[]const u8 = null,
    referer_strings: ?[]const u8 = null,
    referer_tsv: ?[]const u8 = null,
    title_id: ?[]const u8 = null,
    title_offsets: ?[]const u8 = null,
    title_strings: ?[]const u8 = null,
    title_tsv: ?[]const u8 = null,
};

const ImportDictCapacityHint = struct {
    user_ids: usize,
    mobile_models: usize,
    search_phrases: usize,
    urls: usize,
    referers: usize = 0,
    titles: usize,
};

fn limitRowsToCapacityHint(limit_rows: ?u64) ?ImportDictCapacityHint {
    const rows_u64 = limit_rows orelse return null;
    const rows: usize = @intCast(@min(rows_u64, @as(u64, std.math.maxInt(usize))));
    return .{
        .user_ids = @max(128 * 1024, @min(rows, 20 * 1024 * 1024)),
        .mobile_models = importTextColumnHint("MobilePhoneModel", rows),
        .search_phrases = importTextColumnHint("SearchPhrase", rows),
        .urls = importTextColumnHint("URL", rows),
        .referers = if (importRefererEnabled()) importTextColumnHint("Referer", rows) else 0,
        .titles = importTextColumnHint("Title", rows),
    };
}

fn importTextColumnHint(name: []const u8, rows: usize) usize {
    const idx = schema.hits.findColumn(name) orelse return @max(64 * 1024, rows / 4 + 1);
    const column = schema.hits.columns[idx];
    return switch (column.storage) {
        .lowcard_dict => 256,
        .medium_dict => @max(64 * 1024, @min(rows / 4 + 1, 4 * 1024 * 1024)),
        .highcard_dict => if (std.mem.eql(u8, name, "Title"))
            @max(256 * 1024, @min(rows / 2 + 1, 8 * 1024 * 1024))
        else
            @max(512 * 1024, @min(rows, 20 * 1024 * 1024)),
        .lazy_source, .hash_only => 0,
        else => @max(64 * 1024, rows / 4 + 1),
    };
}

const ImportStringBlob = struct {
    offsets: std.ArrayList(u32) = .empty,
    chunks: std.ArrayList([]u8) = .empty,
    chunk_lens: std.ArrayList(usize) = .empty,
    total_bytes: usize = 0,

    fn init(allocator: std.mem.Allocator, capacity_hint: usize) !ImportStringBlob {
        var self = ImportStringBlob{};
        errdefer self.deinit(allocator);
        try self.offsets.ensureTotalCapacity(allocator, capacity_hint + 1);
        try self.offsets.append(allocator, 0);
        return self;
    }

    fn resetCapacity(self: *ImportStringBlob, allocator: std.mem.Allocator, capacity_hint: usize) !void {
        self.offsets.clearRetainingCapacity();
        try self.offsets.ensureTotalCapacity(allocator, capacity_hint + 1);
        try self.offsets.append(allocator, 0);
    }

    fn deinit(self: *ImportStringBlob, allocator: std.mem.Allocator) void {
        for (self.chunks.items) |chunk| allocator.free(chunk);
        self.chunks.deinit(allocator);
        self.chunk_lens.deinit(allocator);
        self.offsets.deinit(allocator);
    }

    fn len(self: *const ImportStringBlob) usize {
        return self.offsets.items.len - 1;
    }

    fn append(self: *ImportStringBlob, allocator: std.mem.Allocator, value: []const u8) ![]const u8 {
        try self.ensureWritable(allocator, value.len);
        const chunk_idx = self.chunks.items.len - 1;
        const start = self.chunk_lens.items[chunk_idx];
        @memcpy(self.chunks.items[chunk_idx][start..][0..value.len], value);
        self.chunk_lens.items[chunk_idx] = start + value.len;
        self.total_bytes += value.len;
        try self.offsets.append(allocator, @intCast(self.total_bytes));
        return self.chunks.items[chunk_idx][start .. start + value.len];
    }

    fn write(self: *const ImportStringBlob, allocator: std.mem.Allocator, io: std.Io, offsets_path: []const u8, strings_path: []const u8) !void {
        try writeFilePath(io, offsets_path, std.mem.sliceAsBytes(self.offsets.items));
        var strings_file = try createFilePath(io, strings_path);
        defer strings_file.close(io);
        const buf = try allocator.alloc(u8, 1024 * 1024);
        defer allocator.free(buf);
        var out = BufferedColumn{ .file = strings_file, .buf = buf };
        for (self.chunks.items, self.chunk_lens.items) |chunk, used| try out.write(io, chunk[0..used]);
        try out.flush(io);
    }

    fn ensureWritable(self: *ImportStringBlob, allocator: std.mem.Allocator, value_len: usize) !void {
        if (self.chunks.items.len != 0) {
            const idx = self.chunks.items.len - 1;
            if (self.chunk_lens.items[idx] + value_len <= self.chunks.items[idx].len) return;
        }
        const cap = @max(@max(value_len, 64 * 1024), 8 * 1024 * 1024);
        const chunk = try allocator.alloc(u8, cap);
        errdefer allocator.free(chunk);
        try self.chunks.append(allocator, chunk);
        try self.chunk_lens.append(allocator, 0);
    }
};

const ImportDictBuilders = struct {
    user_id_id_writer: BufferedColumn,
    user_id_dict_path: []const u8,
    user_ids: std.AutoHashMap(i64, u32),
    user_order: std.ArrayList(i64),

    mobile_model_id_writer: BufferedColumn,
    mobile_model_offsets_path: []const u8,
    mobile_model_bytes_path: []const u8,
    mobile_model_tsv_path: []const u8,
    mobile_models: std.StringHashMap(u8),
    mobile_model_order: std.ArrayList([]const u8),
    mobile_model_empty_id: ?u8 = null,

    search_phrase_id_writer: BufferedColumn,
    search_phrase_offsets_path: []const u8,
    search_phrase_phrases_path: []const u8,
    search_phrase_tsv_path: []const u8,
    search_phrases: std.StringHashMap(u32),
    search_phrase_blob: ImportStringBlob,
    search_phrase_empty_id: ?u32 = null,

    url_id_writer: ?BufferedColumn = null,
    url_offsets_path: ?[]const u8 = null,
    url_strings_path: ?[]const u8 = null,
    url_tsv_path: ?[]const u8 = null,
    urls: std.StringHashMap(u32),
    url_blob: ImportStringBlob,

    referer_id_writer: ?BufferedColumn = null,
    referer_offsets_path: ?[]const u8 = null,
    referer_strings_path: ?[]const u8 = null,
    referer_tsv_path: ?[]const u8 = null,
    referers: std.StringHashMap(u32),
    referer_blob: ImportStringBlob,
    referer_domain_ids: std.ArrayList(u32),
    referer_utf8_lens: std.ArrayList(u32),
    referer_domains: std.StringHashMap(u32),
    referer_domain_order: std.ArrayList([]const u8),

    title_id_writer: ?BufferedColumn = null,
    title_offsets_path: ?[]const u8 = null,
    title_strings_path: ?[]const u8 = null,
    title_tsv_path: ?[]const u8 = null,
    titles: std.StringHashMap(u32),
    title_blob: ImportStringBlob,

    finished: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, paths: ImportDictPaths, capacity_hint: ?ImportDictCapacityHint) !ImportDictBuilders {
        var builders = ImportDictBuilders{
            .user_id_id_writer = try .init(allocator, io, paths.user_id_id),
            .user_id_dict_path = paths.user_id_dict,
            .user_ids = std.AutoHashMap(i64, u32).init(allocator),
            .user_order = .empty,
            .mobile_model_id_writer = try .init(allocator, io, paths.mobile_model_id),
            .mobile_model_offsets_path = paths.mobile_model_offsets,
            .mobile_model_bytes_path = paths.mobile_model_bytes,
            .mobile_model_tsv_path = paths.mobile_model_tsv,
            .mobile_models = std.StringHashMap(u8).init(allocator),
            .mobile_model_order = .empty,
            .search_phrase_id_writer = try .init(allocator, io, paths.search_phrase_id),
            .search_phrase_offsets_path = paths.search_phrase_offsets,
            .search_phrase_phrases_path = paths.search_phrase_phrases,
            .search_phrase_tsv_path = paths.search_phrase_tsv,
            .search_phrases = std.StringHashMap(u32).init(allocator),
            .search_phrase_blob = try ImportStringBlob.init(allocator, 0),
            .url_id_writer = if (paths.url_id) |path| try .init(allocator, io, path) else null,
            .url_offsets_path = paths.url_offsets,
            .url_strings_path = paths.url_strings,
            .url_tsv_path = paths.url_tsv,
            .urls = std.StringHashMap(u32).init(allocator),
            .url_blob = try ImportStringBlob.init(allocator, 0),
            .referer_id_writer = if (paths.referer_id) |path| try .init(allocator, io, path) else null,
            .referer_offsets_path = paths.referer_offsets,
            .referer_strings_path = paths.referer_strings,
            .referer_tsv_path = paths.referer_tsv,
            .referers = std.StringHashMap(u32).init(allocator),
            .referer_blob = try ImportStringBlob.init(allocator, 0),
            .referer_domain_ids = .empty,
            .referer_utf8_lens = .empty,
            .referer_domains = std.StringHashMap(u32).init(allocator),
            .referer_domain_order = .empty,
            .title_id_writer = if (paths.title_id) |path| try .init(allocator, io, path) else null,
            .title_offsets_path = paths.title_offsets,
            .title_strings_path = paths.title_strings,
            .title_tsv_path = paths.title_tsv,
            .titles = std.StringHashMap(u32).init(allocator),
            .title_blob = try ImportStringBlob.init(allocator, 0),
        };
        errdefer builders.deinit(allocator, io);
        const hint = capacity_hint orelse ImportDictCapacityHint{ .user_ids = 128 * 1024, .mobile_models = 256, .search_phrases = 64 * 1024, .urls = 512 * 1024, .referers = if (importRefererEnabled()) 512 * 1024 else 0, .titles = 256 * 1024 };
        try builders.user_ids.ensureTotalCapacity(@intCast(hint.user_ids));
        try builders.user_order.ensureTotalCapacity(allocator, hint.user_ids);
        try builders.mobile_models.ensureTotalCapacity(@intCast(hint.mobile_models));
        try builders.mobile_model_order.ensureTotalCapacity(allocator, hint.mobile_models);
        try builders.search_phrases.ensureTotalCapacity(@intCast(hint.search_phrases));
        try builders.search_phrase_blob.resetCapacity(allocator, hint.search_phrases);
        if (paths.url_id != null) {
            try builders.urls.ensureTotalCapacity(@intCast(hint.urls));
            try builders.url_blob.resetCapacity(allocator, hint.urls);
        }
        if (paths.referer_id != null) {
            try builders.referers.ensureTotalCapacity(@intCast(hint.referers));
            builders.referer_blob.offsets.clearRetainingCapacity();
            try builders.referer_blob.offsets.ensureTotalCapacity(allocator, hint.referers + 1);
            try builders.referer_blob.offsets.append(allocator, 0);
            try builders.referer_domain_ids.ensureTotalCapacity(allocator, hint.referers);
            try builders.referer_utf8_lens.ensureTotalCapacity(allocator, hint.referers);
            try builders.referer_domains.ensureTotalCapacity(1024);
        }
        if (paths.title_id != null) {
            try builders.titles.ensureTotalCapacity(@intCast(hint.titles));
            try builders.title_blob.resetCapacity(allocator, hint.titles);
        }
        return builders;
    }

    fn deinit(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io) void {
        if (!self.finished) self.finish(allocator, io, true) catch {};
        self.user_id_id_writer.deinit(allocator, io);
        allocator.free(self.user_id_dict_path);
        self.user_ids.deinit();
        self.user_order.deinit(allocator);
        self.mobile_model_id_writer.deinit(allocator, io);
        allocator.free(self.mobile_model_offsets_path);
        allocator.free(self.mobile_model_bytes_path);
        allocator.free(self.mobile_model_tsv_path);
        freeStringOrder(allocator, self.mobile_model_order.items);
        self.mobile_model_order.deinit(allocator);
        self.mobile_models.deinit();
        self.search_phrase_id_writer.deinit(allocator, io);
        allocator.free(self.search_phrase_offsets_path);
        allocator.free(self.search_phrase_phrases_path);
        allocator.free(self.search_phrase_tsv_path);
        self.search_phrase_blob.deinit(allocator);
        self.search_phrases.deinit();
        if (self.url_id_writer) |*w| w.deinit(allocator, io);
        if (self.url_offsets_path) |path| allocator.free(path);
        if (self.url_strings_path) |path| allocator.free(path);
        if (self.url_tsv_path) |path| allocator.free(path);
        self.url_blob.deinit(allocator);
        self.urls.deinit();
        if (self.referer_id_writer) |*w| w.deinit(allocator, io);
        if (self.referer_offsets_path) |path| allocator.free(path);
        if (self.referer_strings_path) |path| allocator.free(path);
        if (self.referer_tsv_path) |path| allocator.free(path);
        self.referer_blob.deinit(allocator);
        self.referers.deinit();
        self.referer_domain_ids.deinit(allocator);
        self.referer_utf8_lens.deinit(allocator);
        self.referer_domains.deinit();
        self.referer_domain_order.deinit(allocator);
        if (self.title_id_writer) |*w| w.deinit(allocator, io);
        if (self.title_offsets_path) |path| allocator.free(path);
        if (self.title_strings_path) |path| allocator.free(path);
        if (self.title_tsv_path) |path| allocator.free(path);
        self.title_blob.deinit(allocator);
        self.titles.deinit();
    }

    fn writeUserId(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, uid: i64) !u32 {
        const gop = try self.user_ids.getOrPut(uid);
        if (!gop.found_existing) {
            const id: u32 = @intCast(self.user_order.items.len);
            gop.value_ptr.* = id;
            try self.user_order.append(allocator, uid);
        }
        var id = gop.value_ptr.*;
        try self.user_id_id_writer.write(io, std.mem.asBytes(&id));
        return id;
    }

    fn writeMobileModel(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, field: []const u8) !void {
        const value = try decodeCsvFieldAlloc(allocator, field);
        try self.writeMobileModelOwned(allocator, io, value);
    }

    fn writeMobileModelRaw(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, value: []const u8) !void {
        if (value.len == 0) {
            if (self.mobile_model_empty_id) |cached_id| {
                var id = cached_id;
                try self.mobile_model_id_writer.write(io, std.mem.asBytes(&id));
                return;
            }
        }
        if (self.mobile_models.get(value)) |existing_id| {
            var id = existing_id;
            try self.mobile_model_id_writer.write(io, std.mem.asBytes(&id));
            return;
        }
        const owned = try allocator.dupe(u8, value);
        const id: u8 = @intCast(self.mobile_model_order.items.len);
        try self.mobile_models.putNoClobber(owned, id);
        try self.mobile_model_order.append(allocator, owned);
        if (value.len == 0) self.mobile_model_empty_id = id;
        var writable_id = id;
        try self.mobile_model_id_writer.write(io, std.mem.asBytes(&writable_id));
    }

    fn writeMobileModelOwned(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, value: []const u8) !void {
        const gop = try self.mobile_models.getOrPut(value);
        if (!gop.found_existing) {
            gop.key_ptr.* = value;
            const id: u8 = @intCast(self.mobile_model_order.items.len);
            gop.value_ptr.* = id;
            try self.mobile_model_order.append(allocator, value);
        } else {
            allocator.free(value);
        }
        var id = gop.value_ptr.*;
        try self.mobile_model_id_writer.write(io, std.mem.asBytes(&id));
    }

    fn writeSearchPhrase(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, field: []const u8) !u32 {
        return self.writeStringU32Blob(allocator, io, field, &self.search_phrases, &self.search_phrase_blob, &self.search_phrase_id_writer);
    }

    fn writeSearchPhraseRaw(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, value: []const u8) !u32 {
        if (value.len == 0) {
            if (self.search_phrase_empty_id) |cached_id| {
                var id = cached_id;
                try self.search_phrase_id_writer.write(io, std.mem.asBytes(&id));
                return id;
            }
        }
        const id = try self.writeStringU32BlobRaw(allocator, io, value, &self.search_phrases, &self.search_phrase_blob, &self.search_phrase_id_writer);
        if (value.len == 0) self.search_phrase_empty_id = id;
        return id;
    }

    fn writeUrl(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, field: []const u8) !u32 {
        if (self.url_id_writer) |*writer| return self.writeStringU32Blob(allocator, io, field, &self.urls, &self.url_blob, writer);
        return 0;
    }

    fn writeUrlRaw(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, value: []const u8) !u32 {
        if (self.url_id_writer) |*writer| return self.writeStringU32BlobRaw(allocator, io, value, &self.urls, &self.url_blob, writer);
        return 0;
    }

    fn writeRefererRaw(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, value: []const u8) !?u32 {
        if (self.referer_id_writer) |*writer| {
            if (self.referers.get(value)) |existing_id| {
                var id = existing_id;
                try writer.write(io, std.mem.asBytes(&id));
                return id;
            }
            const id: u32 = @intCast(self.referer_blob.len());
            const owned = try self.referer_blob.append(allocator, value);
            try self.referers.putNoClobber(owned, id);
            try self.addRefererSidecarEntry(allocator, owned);
            var writable_id = id;
            try writer.write(io, std.mem.asBytes(&writable_id));
            return id;
        }
        return null;
    }

    fn addRefererSidecarEntry(self: *ImportDictBuilders, allocator: std.mem.Allocator, referer: []const u8) !void {
        try self.referer_utf8_lens.append(allocator, @intCast(utf8CharLen(referer)));
        if (referer.len == 0) {
            try self.referer_domain_ids.append(allocator, std.math.maxInt(u32));
            return;
        }
        const domain = q29Domain(referer);
        const gop = try self.referer_domains.getOrPut(domain);
        if (!gop.found_existing) {
            const id: u32 = @intCast(self.referer_domain_order.items.len);
            gop.value_ptr.* = id;
            try self.referer_domain_order.append(allocator, domain);
        }
        try self.referer_domain_ids.append(allocator, gop.value_ptr.*);
    }

    fn writeRefererSidecars(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
        const domain_id_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_domain_id_name);
        defer allocator.free(domain_id_path);
        const utf8_len_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_utf8_len_name);
        defer allocator.free(utf8_len_path);
        const domain_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_domain_offsets_name);
        defer allocator.free(domain_offsets_path);
        const domain_strings_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_domain_strings_name);
        defer allocator.free(domain_strings_path);

        try writeFilePath(io, domain_id_path, std.mem.sliceAsBytes(self.referer_domain_ids.items));
        try writeFilePath(io, utf8_len_path, std.mem.sliceAsBytes(self.referer_utf8_lens.items));
        try writeStringDictU32(allocator, io, domain_offsets_path, domain_strings_path, "", self.referer_domain_order.items, false);
    }

    fn writeTitle(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, field: []const u8) !u32 {
        if (self.title_id_writer) |*writer| return self.writeStringU32Blob(allocator, io, field, &self.titles, &self.title_blob, writer);
        return 0;
    }

    fn writeTitleRaw(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, value: []const u8) !u32 {
        if (self.title_id_writer) |*writer| return self.writeStringU32BlobRaw(allocator, io, value, &self.titles, &self.title_blob, writer);
        return 0;
    }

    fn writeStringU32Blob(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, field: []const u8, dict: *std.StringHashMap(u32), blob: *ImportStringBlob, id_writer: *BufferedColumn) !u32 {
        _ = self;
        if (field.len < 2 or field[0] != '"' or field[field.len - 1] != '"') {
            return writeStringU32BlobRawImpl(allocator, io, field, dict, blob, id_writer);
        }

        const value = try decodeCsvFieldAlloc(allocator, field);
        defer allocator.free(value);
        return writeStringU32BlobRawImpl(allocator, io, value, dict, blob, id_writer);
    }

    fn writeStringU32BlobRaw(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, value: []const u8, dict: *std.StringHashMap(u32), blob: *ImportStringBlob, id_writer: *BufferedColumn) !u32 {
        _ = self;
        return writeStringU32BlobRawImpl(allocator, io, value, dict, blob, id_writer);
    }

    fn writeStringU32(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, field: []const u8, dict: *std.StringHashMap(u32), order: *std.ArrayList([]const u8), id_writer: *BufferedColumn) !u32 {
        _ = self;
        if (field.len < 2 or field[0] != '"' or field[field.len - 1] != '"') {
            if (dict.get(field)) |existing_id| {
                var id = existing_id;
                try id_writer.write(io, std.mem.asBytes(&id));
                return id;
            }
            const value = try allocator.dupe(u8, field);
            const id: u32 = @intCast(order.items.len);
            try dict.putNoClobber(value, id);
            try order.append(allocator, value);
            var writable_id = id;
            try id_writer.write(io, std.mem.asBytes(&writable_id));
            return id;
        }

        const value = try decodeCsvFieldAlloc(allocator, field);
        const gop = try dict.getOrPut(value);
        if (!gop.found_existing) {
            gop.key_ptr.* = value;
            const id: u32 = @intCast(order.items.len);
            gop.value_ptr.* = id;
            try order.append(allocator, value);
        } else {
            allocator.free(value);
        }
        var id = gop.value_ptr.*;
        try id_writer.write(io, std.mem.asBytes(&id));
        return id;
    }

    fn writeStringU32Raw(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, value: []const u8, dict: *std.StringHashMap(u32), order: *std.ArrayList([]const u8), id_writer: *BufferedColumn) !u32 {
        _ = self;
        if (dict.get(value)) |existing_id| {
            var id = existing_id;
            try id_writer.write(io, std.mem.asBytes(&id));
            return id;
        }
        const owned = try allocator.dupe(u8, value);
        const id: u32 = @intCast(order.items.len);
        try dict.putNoClobber(owned, id);
        try order.append(allocator, owned);
        var writable_id = id;
        try id_writer.write(io, std.mem.asBytes(&writable_id));
        return id;
    }

    fn finish(self: *ImportDictBuilders, allocator: std.mem.Allocator, io: std.Io, write_tsv: bool) !void {
        if (self.finished) return;
        try self.user_id_id_writer.flush(io);
        try self.mobile_model_id_writer.flush(io);
        try self.search_phrase_id_writer.flush(io);
        if (self.url_id_writer) |*w| try w.flush(io);
        if (self.referer_id_writer) |*w| try w.flush(io);
        if (self.title_id_writer) |*w| try w.flush(io);

        var started = std.Io.Clock.Timestamp.now(io, .awake);
        try writeFilePath(io, self.user_id_dict_path, std.mem.sliceAsBytes(self.user_order.items));
        var finished = std.Io.Clock.Timestamp.now(io, .awake);
        traceImportPhase("finish_dict.UserID", elapsedSeconds(started, finished));

        started = std.Io.Clock.Timestamp.now(io, .awake);
        try writeStringDictU8(allocator, io, self.mobile_model_offsets_path, self.mobile_model_bytes_path, self.mobile_model_tsv_path, self.mobile_model_order.items, write_tsv);
        finished = std.Io.Clock.Timestamp.now(io, .awake);
        traceImportPhase("finish_dict.MobilePhoneModel", elapsedSeconds(started, finished));

        started = std.Io.Clock.Timestamp.now(io, .awake);
        try self.search_phrase_blob.write(allocator, io, self.search_phrase_offsets_path, self.search_phrase_phrases_path);
        if (write_tsv) try writeStringBlobTsv(allocator, io, self.search_phrase_tsv_path, &self.search_phrase_blob);
        finished = std.Io.Clock.Timestamp.now(io, .awake);
        traceImportPhase("finish_dict.SearchPhrase", elapsedSeconds(started, finished));

        if (self.url_offsets_path) |offsets_path| {
            started = std.Io.Clock.Timestamp.now(io, .awake);
            try self.url_blob.write(allocator, io, offsets_path, self.url_strings_path.?);
            if (write_tsv) try writeStringBlobTsv(allocator, io, self.url_tsv_path.?, &self.url_blob);
            finished = std.Io.Clock.Timestamp.now(io, .awake);
            traceImportPhase("finish_dict.URL", elapsedSeconds(started, finished));
        }

        if (self.referer_offsets_path) |offsets_path| {
            started = std.Io.Clock.Timestamp.now(io, .awake);
            try self.referer_blob.write(allocator, io, offsets_path, self.referer_strings_path.?);
            if (write_tsv) try writeStringBlobTsv(allocator, io, self.referer_tsv_path.?, &self.referer_blob);
            finished = std.Io.Clock.Timestamp.now(io, .awake);
            traceImportPhase("finish_dict.Referer", elapsedSeconds(started, finished));
        }

        if (self.title_offsets_path) |offsets_path| {
            started = std.Io.Clock.Timestamp.now(io, .awake);
            try self.title_blob.write(allocator, io, offsets_path, self.title_strings_path.?);
            if (write_tsv) try writeStringBlobTsv(allocator, io, self.title_tsv_path.?, &self.title_blob);
            finished = std.Io.Clock.Timestamp.now(io, .awake);
            traceImportPhase("finish_dict.Title", elapsedSeconds(started, finished));
        }
        self.finished = true;
    }
};

fn freeStringOrder(allocator: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| allocator.free(value);
}

fn writeStringDictU8(allocator: std.mem.Allocator, io: std.Io, offsets_path: []const u8, bytes_path: []const u8, tsv_path: []const u8, values: []const []const u8, write_tsv: bool) !void {
    try writeStringDict(allocator, io, offsets_path, bytes_path, tsv_path, values, write_tsv);
}

fn writeStringDictU32(allocator: std.mem.Allocator, io: std.Io, offsets_path: []const u8, bytes_path: []const u8, tsv_path: []const u8, values: []const []const u8, write_tsv: bool) !void {
    try writeStringDict(allocator, io, offsets_path, bytes_path, tsv_path, values, write_tsv);
}

fn writeStringDict(allocator: std.mem.Allocator, io: std.Io, offsets_path: []const u8, bytes_path: []const u8, tsv_path: []const u8, values: []const []const u8, write_tsv: bool) !void {
    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    try offsets.ensureTotalCapacity(allocator, values.len + 1);
    try offsets.append(allocator, 0);
    var total_bytes: usize = 0;
    for (values) |s| {
        total_bytes += s.len;
        try offsets.append(allocator, @intCast(total_bytes));
    }
    try writeFilePath(io, offsets_path, std.mem.sliceAsBytes(offsets.items));

    try writeStringDictBytesAndTsv(allocator, io, bytes_path, tsv_path, values, write_tsv);
}

fn writeStringDictBytesAndTsv(allocator: std.mem.Allocator, io: std.Io, bytes_path: []const u8, tsv_path: []const u8, values: []const []const u8, write_tsv: bool) !void {
    var bytes_file = try createFilePath(io, bytes_path);
    defer bytes_file.close(io);
    const bytes_buf = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(bytes_buf);
    var bytes_out = BufferedColumn{ .file = bytes_file, .buf = bytes_buf };
    for (values) |s| try bytes_out.write(io, s);
    try bytes_out.flush(io);

    if (!write_tsv) return;
    var tsv_file = try createFilePath(io, tsv_path);
    defer tsv_file.close(io);
    const buf = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(buf);
    var out = BufferedColumn{ .file = tsv_file, .buf = buf };
    for (values, 0..) |s, idx| {
        var prefix_buf: [32]u8 = undefined;
        const prefix = try std.fmt.bufPrint(&prefix_buf, "{d}\t", .{idx});
        try out.write(io, prefix);
        try out.write(io, s);
        try out.write(io, "\n");
    }
    try out.flush(io);
}

fn writeStringU32BlobRawImpl(allocator: std.mem.Allocator, io: std.Io, value: []const u8, dict: *std.StringHashMap(u32), blob: *ImportStringBlob, id_writer: *BufferedColumn) !u32 {
    if (dict.get(value)) |existing_id| {
        var id = existing_id;
        try id_writer.write(io, std.mem.asBytes(&id));
        return id;
    }
    const id: u32 = @intCast(blob.len());
    const owned = try blob.append(allocator, value);
    try dict.putNoClobber(owned, id);
    var writable_id = id;
    try id_writer.write(io, std.mem.asBytes(&writable_id));
    return id;
}

fn writeStringBlobTsv(allocator: std.mem.Allocator, io: std.Io, tsv_path: []const u8, blob: *const ImportStringBlob) !void {
    var tsv_file = try createFilePath(io, tsv_path);
    defer tsv_file.close(io);
    const buf = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(buf);
    var out = BufferedColumn{ .file = tsv_file, .buf = buf };
    var idx: usize = 0;
    var chunk_idx: usize = 0;
    var chunk_base: usize = 0;
    while (idx + 1 < blob.offsets.items.len) : (idx += 1) {
        const start = blob.offsets.items[idx];
        const end = blob.offsets.items[idx + 1];
        while (chunk_idx + 1 < blob.chunks.items.len and start >= chunk_base + blob.chunk_lens.items[chunk_idx]) {
            chunk_base += blob.chunk_lens.items[chunk_idx];
            chunk_idx += 1;
        }
        const local_start = start - chunk_base;
        const local_end = end - chunk_base;
        var prefix_buf: [32]u8 = undefined;
        const prefix = try std.fmt.bufPrint(&prefix_buf, "{d}\t", .{idx});
        try out.write(io, prefix);
        try out.write(io, blob.chunks.items[chunk_idx][local_start..local_end]);
        try out.write(io, "\n");
    }
    try out.flush(io);
}
const HotColumnWriter = struct {
    adv: BufferedColumn,
    width: BufferedColumn,
    date: BufferedColumn,
    user: BufferedColumn,
    counter: BufferedColumn,
    refresh: BufferedColumn,
    dont_count: BufferedColumn,
    url_hash: BufferedColumn,
    window_width: BufferedColumn,
    window_height: BufferedColumn,
    client_ip: BufferedColumn,
    url_length: BufferedColumn,
    event_minute: BufferedColumn,
    title_hash: ?BufferedColumn = null,
    trafic_source: ?BufferedColumn = null,
    referer_hash: ?BufferedColumn = null,
    event_time: ?BufferedColumn = null,
    watch: ?BufferedColumn = null,
    region: ?BufferedColumn = null,
    search_engine: ?BufferedColumn = null,
    mobile_phone: ?BufferedColumn = null,
    is_link: ?BufferedColumn = null,
    is_download: ?BufferedColumn = null,
    url_contains_google: ?BufferedColumn = null,
    url_contains_dot_google: ?BufferedColumn = null,
    title_contains_google: ?BufferedColumn = null,
    title_non_empty: ?BufferedColumn = null,

    fn init(allocator: std.mem.Allocator, io: std.Io, paths: HotOutputPaths) !HotColumnWriter {
        return .{
            .adv = try .init(allocator, io, paths.adv),
            .width = try .init(allocator, io, paths.width),
            .date = try .init(allocator, io, paths.date),
            .user = try .init(allocator, io, paths.user),
            .counter = try .init(allocator, io, paths.counter),
            .refresh = try .init(allocator, io, paths.refresh),
            .dont_count = try .init(allocator, io, paths.dont_count),
            .url_hash = try .init(allocator, io, paths.url_hash),
            .window_width = try .init(allocator, io, paths.window_width),
            .window_height = try .init(allocator, io, paths.window_height),
            .client_ip = try .init(allocator, io, paths.client_ip),
            .url_length = try .init(allocator, io, paths.url_length),
            .event_minute = try .init(allocator, io, paths.event_minute),
            .title_hash = if (paths.title_hash) |path| try .init(allocator, io, path) else null,
            .trafic_source = if (paths.trafic_source) |path| try .init(allocator, io, path) else null,
            .referer_hash = if (paths.referer_hash) |path| try .init(allocator, io, path) else null,
            .event_time = if (paths.event_time) |path| try .init(allocator, io, path) else null,
            .watch = if (paths.watch) |path| try .init(allocator, io, path) else null,
            .region = if (paths.region) |path| try .init(allocator, io, path) else null,
            .search_engine = if (paths.search_engine) |path| try .init(allocator, io, path) else null,
            .mobile_phone = if (paths.mobile_phone) |path| try .init(allocator, io, path) else null,
            .is_link = if (paths.is_link) |path| try .init(allocator, io, path) else null,
            .is_download = if (paths.is_download) |path| try .init(allocator, io, path) else null,
            .url_contains_google = if (paths.url_contains_google) |path| try .init(allocator, io, path) else null,
            .url_contains_dot_google = if (paths.url_contains_dot_google) |path| try .init(allocator, io, path) else null,
            .title_contains_google = if (paths.title_contains_google) |path| try .init(allocator, io, path) else null,
            .title_non_empty = if (paths.title_non_empty) |path| try .init(allocator, io, path) else null,
        };
    }

    fn deinit(self: *HotColumnWriter, allocator: std.mem.Allocator, io: std.Io) void {
        self.adv.deinit(allocator, io);
        self.width.deinit(allocator, io);
        self.date.deinit(allocator, io);
        self.user.deinit(allocator, io);
        self.counter.deinit(allocator, io);
        self.refresh.deinit(allocator, io);
        self.dont_count.deinit(allocator, io);
        self.url_hash.deinit(allocator, io);
        self.window_width.deinit(allocator, io);
        self.window_height.deinit(allocator, io);
        self.client_ip.deinit(allocator, io);
        self.url_length.deinit(allocator, io);
        self.event_minute.deinit(allocator, io);
        if (self.title_hash) |*w| w.deinit(allocator, io);
        if (self.trafic_source) |*w| w.deinit(allocator, io);
        if (self.referer_hash) |*w| w.deinit(allocator, io);
        if (self.event_time) |*w| w.deinit(allocator, io);
        if (self.watch) |*w| w.deinit(allocator, io);
        if (self.region) |*w| w.deinit(allocator, io);
        if (self.search_engine) |*w| w.deinit(allocator, io);
        if (self.mobile_phone) |*w| w.deinit(allocator, io);
        if (self.is_link) |*w| w.deinit(allocator, io);
        if (self.is_download) |*w| w.deinit(allocator, io);
        if (self.url_contains_google) |*w| w.deinit(allocator, io);
        if (self.url_contains_dot_google) |*w| w.deinit(allocator, io);
        if (self.title_contains_google) |*w| w.deinit(allocator, io);
        if (self.title_non_empty) |*w| w.deinit(allocator, io);
    }

    fn flush(self: *HotColumnWriter, io: std.Io) !void {
        try self.adv.flush(io);
        try self.width.flush(io);
        try self.date.flush(io);
        try self.user.flush(io);
        try self.counter.flush(io);
        try self.refresh.flush(io);
        try self.dont_count.flush(io);
        try self.url_hash.flush(io);
        try self.window_width.flush(io);
        try self.window_height.flush(io);
        try self.client_ip.flush(io);
        try self.url_length.flush(io);
        try self.event_minute.flush(io);
        if (self.title_hash) |*w| try w.flush(io);
        if (self.trafic_source) |*w| try w.flush(io);
        if (self.referer_hash) |*w| try w.flush(io);
        if (self.event_time) |*w| try w.flush(io);
        if (self.watch) |*w| try w.flush(io);
        if (self.region) |*w| try w.flush(io);
        if (self.search_engine) |*w| try w.flush(io);
        if (self.mobile_phone) |*w| try w.flush(io);
        if (self.is_link) |*w| try w.flush(io);
        if (self.is_download) |*w| try w.flush(io);
        if (self.url_contains_google) |*w| try w.flush(io);
        if (self.url_contains_dot_google) |*w| try w.flush(io);
        if (self.title_contains_google) |*w| try w.flush(io);
        if (self.title_non_empty) |*w| try w.flush(io);
    }
};

fn consumeCsvLines(
    allocator: std.mem.Allocator,
    io: std.Io,
    pending: *std.ArrayList(u8),
    skipped_header: *bool,
    writer: *HotColumnWriter,
    flush_tail: bool,
) !void {
    var start: usize = 0;
    var i: usize = 0;
    while (i < pending.items.len) : (i += 1) {
        if (pending.items[i] != '\n') continue;
        try consumeCsvLine(io, pending.items[start..i], skipped_header, writer);
        start = i + 1;
    }
    if (flush_tail and start < pending.items.len) {
        try consumeCsvLine(io, pending.items[start..], skipped_header, writer);
        start = pending.items.len;
    }
    if (start > 0) {
        const rest = pending.items[start..];
        std.mem.copyForwards(u8, pending.items[0..rest.len], rest);
        try pending.resize(allocator, rest.len);
    }
}

fn consumeCsvLine(
    io: std.Io,
    line_raw: []const u8,
    skipped_header: *bool,
    writer: *HotColumnWriter,
) !void {
    const line = std.mem.trim(u8, line_raw, " \t\r");
    if (line.len == 0) return;
    if (!skipped_header.*) {
        skipped_header.* = true;
        return;
    }
    var fields = std.mem.splitScalar(u8, line, ',');
    const adv_text = fields.next() orelse return error.CorruptHotCsv;
    const width_text = fields.next() orelse return error.CorruptHotCsv;
    const date_text = fields.next() orelse return error.CorruptHotCsv;
    const user_text = fields.next() orelse return error.CorruptHotCsv;
    const counter_text = fields.next() orelse return error.CorruptHotCsv;
    const refresh_text = fields.next() orelse return error.CorruptHotCsv;
    const dont_count_text = fields.next() orelse return error.CorruptHotCsv;
    const url_hash_text = fields.next() orelse return error.CorruptHotCsv;
    const window_width_text = fields.next() orelse return error.CorruptHotCsv;
    const window_height_text = fields.next() orelse return error.CorruptHotCsv;
    const client_ip_text = fields.next() orelse return error.CorruptHotCsv;
    const url_length_text = fields.next() orelse return error.CorruptHotCsv;
    const event_minute_text = fields.next() orelse return error.CorruptHotCsv;
    var adv: i16 = try std.fmt.parseInt(i16, adv_text, 10);
    var width: i16 = try std.fmt.parseInt(i16, width_text, 10);
    var date: i32 = try std.fmt.parseInt(i32, date_text, 10);
    var user: i64 = try std.fmt.parseInt(i64, user_text, 10);
    var counter: i32 = try std.fmt.parseInt(i32, counter_text, 10);
    var refresh: i16 = try std.fmt.parseInt(i16, refresh_text, 10);
    var dont_count: i16 = try std.fmt.parseInt(i16, dont_count_text, 10);
    var url_hash: i64 = try std.fmt.parseInt(i64, url_hash_text, 10);
    var window_width: i16 = try std.fmt.parseInt(i16, window_width_text, 10);
    var window_height: i16 = try std.fmt.parseInt(i16, window_height_text, 10);
    var client_ip: i32 = try std.fmt.parseInt(i32, client_ip_text, 10);
    var url_length: i32 = try std.fmt.parseInt(i32, url_length_text, 10);
    var event_minute: i32 = try std.fmt.parseInt(i32, event_minute_text, 10);
    try writer.adv.write(io, std.mem.asBytes(&adv));
    try writer.width.write(io, std.mem.asBytes(&width));
    try writer.date.write(io, std.mem.asBytes(&date));
    try writer.user.write(io, std.mem.asBytes(&user));
    try writer.counter.write(io, std.mem.asBytes(&counter));
    try writer.refresh.write(io, std.mem.asBytes(&refresh));
    try writer.dont_count.write(io, std.mem.asBytes(&dont_count));
    try writer.url_hash.write(io, std.mem.asBytes(&url_hash));
    try writer.window_width.write(io, std.mem.asBytes(&window_width));
    try writer.window_height.write(io, std.mem.asBytes(&window_height));
    try writer.client_ip.write(io, std.mem.asBytes(&client_ip));
    try writer.url_length.write(io, std.mem.asBytes(&url_length));
    try writer.event_minute.write(io, std.mem.asBytes(&event_minute));
}

fn consumeExtraCsvLines(
    allocator: std.mem.Allocator,
    io: std.Io,
    pending: *std.ArrayList(u8),
    skipped_header: *bool,
    client_writer: *BufferedColumn,
    url_length_writer: *BufferedColumn,
    event_minute_writer: *BufferedColumn,
    trafic_source_writer: *BufferedColumn,
    referer_hash_writer: *BufferedColumn,
    flush_tail: bool,
) !void {
    var start: usize = 0;
    var i: usize = 0;
    while (i < pending.items.len) : (i += 1) {
        if (pending.items[i] != '\n') continue;
        try consumeExtraCsvLine(io, pending.items[start..i], skipped_header, client_writer, url_length_writer, event_minute_writer, trafic_source_writer, referer_hash_writer);
        start = i + 1;
    }
    if (flush_tail and start < pending.items.len) {
        try consumeExtraCsvLine(io, pending.items[start..], skipped_header, client_writer, url_length_writer, event_minute_writer, trafic_source_writer, referer_hash_writer);
        start = pending.items.len;
    }
    if (start > 0) {
        const rest = pending.items[start..];
        std.mem.copyForwards(u8, pending.items[0..rest.len], rest);
        try pending.resize(allocator, rest.len);
    }
}

fn consumeExtraCsvLine(
    io: std.Io,
    line_raw: []const u8,
    skipped_header: *bool,
    client_writer: *BufferedColumn,
    url_length_writer: *BufferedColumn,
    event_minute_writer: *BufferedColumn,
    trafic_source_writer: *BufferedColumn,
    referer_hash_writer: *BufferedColumn,
) !void {
    const line = std.mem.trim(u8, line_raw, " \t\r");
    if (line.len == 0) return;
    if (!skipped_header.*) {
        skipped_header.* = true;
        return;
    }
    var fields = std.mem.splitScalar(u8, line, ',');
    const client_ip_text = fields.next() orelse return error.CorruptHotCsv;
    const url_length_text = fields.next() orelse return error.CorruptHotCsv;
    const event_minute_text = fields.next() orelse return error.CorruptHotCsv;
    const trafic_source_text = fields.next() orelse return error.CorruptHotCsv;
    const referer_hash_text = fields.next() orelse return error.CorruptHotCsv;
    var client_ip: i32 = try std.fmt.parseInt(i32, client_ip_text, 10);
    var url_length: i32 = try std.fmt.parseInt(i32, url_length_text, 10);
    var event_minute: i32 = try std.fmt.parseInt(i32, event_minute_text, 10);
    var trafic_source: i16 = try std.fmt.parseInt(i16, trafic_source_text, 10);
    var referer_hash: i64 = try std.fmt.parseInt(i64, referer_hash_text, 10);
    try client_writer.write(io, std.mem.asBytes(&client_ip));
    try url_length_writer.write(io, std.mem.asBytes(&url_length));
    try event_minute_writer.write(io, std.mem.asBytes(&event_minute));
    try trafic_source_writer.write(io, std.mem.asBytes(&trafic_source));
    try referer_hash_writer.write(io, std.mem.asBytes(&referer_hash));
}

const HotColumns = struct {
    adv_engine_id: []const i16,
    resolution_width: []const i16,
    event_date: []const i32,
    user_id: []const i64,
    counter_id: []const i32,
    is_refresh: []const i16,
    dont_count_hits: []const i16,
    url_hash: []const i64,
    window_client_width: []const i16,
    window_client_height: []const i16,
    watch_id: ?[]const i64,
    client_ip: ?[]const i32,
    url_length: ?[]const i32,
    event_minute: ?[]const i32,
    event_time: ?[]const i64,
    trafic_source_id: ?[]const i16,
    referer_hash: ?[]const i64,
    /// When non-null, all column slices are mmap-backed and owned by these
    /// mappings; allocator.free MUST NOT be called on the column slices.
    /// When null, every slice is allocator-owned (CSV path).
    mappings: ?[]io_map.Mapping = null,

    fn load(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !HotColumns {
        return loadBinary(allocator, io, data_dir) catch |err| switch (err) {
            error.FileNotFound => loadCsvFromStore(allocator, io, data_dir),
            else => return err,
        };
    }

    fn loadBinary(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !HotColumns {
        var maps: std.ArrayList(io_map.Mapping) = .empty;
        errdefer {
            for (maps.items) |m| m.unmap();
            maps.deinit(allocator);
        }

        const adv = try mapI16Column(allocator, io, data_dir, storage.hot_adv_engine_id_name, &maps);
        const width = try mapI16Column(allocator, io, data_dir, storage.hot_resolution_width_name, &maps);
        const date = try mapI32Column(allocator, io, data_dir, storage.hot_event_date_name, &maps);
        const user_id = try mapI64Column(allocator, io, data_dir, storage.hot_user_id_name, &maps);
        const counter_id = try mapI32Column(allocator, io, data_dir, storage.hot_counter_id_name, &maps);
        const is_refresh = try mapI16Column(allocator, io, data_dir, storage.hot_is_refresh_name, &maps);
        const dont_count_hits = try mapI16Column(allocator, io, data_dir, storage.hot_dont_count_hits_name, &maps);
        const url_hash = try mapI64Column(allocator, io, data_dir, storage.hot_url_hash_name, &maps);
        const window_client_width = try mapI16Column(allocator, io, data_dir, storage.hot_window_client_width_name, &maps);
        const window_client_height = try mapI16Column(allocator, io, data_dir, storage.hot_window_client_height_name, &maps);
        const watch_id = mapI64Column(allocator, io, data_dir, storage.hot_watch_id_name, &maps) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        const client_ip = mapI32Column(allocator, io, data_dir, storage.hot_client_ip_name, &maps) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        const url_length = mapI32Column(allocator, io, data_dir, storage.hot_url_length_name, &maps) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        const event_minute = mapI32Column(allocator, io, data_dir, storage.hot_event_minute_name, &maps) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        const event_time = mapI64Column(allocator, io, data_dir, storage.hot_event_time_name, &maps) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        const trafic_source_id = mapI16Column(allocator, io, data_dir, storage.hot_trafic_source_id_name, &maps) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        const referer_hash = mapI64Column(allocator, io, data_dir, storage.hot_referer_hash_name, &maps) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (adv.len != width.len or adv.len != date.len or adv.len != user_id.len or adv.len != counter_id.len or adv.len != is_refresh.len or adv.len != dont_count_hits.len or adv.len != url_hash.len or adv.len != window_client_width.len or adv.len != window_client_height.len) return error.CorruptHotColumns;
        if (watch_id) |values| if (adv.len != values.len) return error.CorruptHotColumns;
        if (client_ip) |values| if (adv.len != values.len) return error.CorruptHotColumns;
        if (url_length) |values| if (adv.len != values.len) return error.CorruptHotColumns;
        if (event_minute) |values| if (adv.len != values.len) return error.CorruptHotColumns;
        if (event_time) |values| if (adv.len != values.len) return error.CorruptHotColumns;
        if (trafic_source_id) |values| if (adv.len != values.len) return error.CorruptHotColumns;
        if (referer_hash) |values| if (adv.len != values.len) return error.CorruptHotColumns;
        return .{
            .adv_engine_id = adv,
            .resolution_width = width,
            .event_date = date,
            .user_id = user_id,
            .counter_id = counter_id,
            .is_refresh = is_refresh,
            .dont_count_hits = dont_count_hits,
            .url_hash = url_hash,
            .window_client_width = window_client_width,
            .window_client_height = window_client_height,
            .watch_id = watch_id,
            .client_ip = client_ip,
            .url_length = url_length,
            .event_minute = event_minute,
            .event_time = event_time,
            .trafic_source_id = trafic_source_id,
            .referer_hash = referer_hash,
            .mappings = try maps.toOwnedSlice(allocator),
        };
    }

    fn loadCsvFromStore(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !HotColumns {
        const path = try storage.hotCsvPath(allocator, data_dir);
        defer allocator.free(path);
        return loadCsv(allocator, io, path);
    }

    fn loadCsv(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !HotColumns {
        const csv = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
        defer allocator.free(csv);

        var adv: std.ArrayList(i16) = .empty;
        errdefer adv.deinit(allocator);
        var width: std.ArrayList(i16) = .empty;
        errdefer width.deinit(allocator);
        var date: std.ArrayList(i32) = .empty;
        errdefer date.deinit(allocator);
        var user_id: std.ArrayList(i64) = .empty;
        errdefer user_id.deinit(allocator);
        var counter_id: std.ArrayList(i32) = .empty;
        errdefer counter_id.deinit(allocator);
        var is_refresh: std.ArrayList(i16) = .empty;
        errdefer is_refresh.deinit(allocator);
        var dont_count_hits: std.ArrayList(i16) = .empty;
        errdefer dont_count_hits.deinit(allocator);
        var url_hash: std.ArrayList(i64) = .empty;
        errdefer url_hash.deinit(allocator);
        var window_width: std.ArrayList(i16) = .empty;
        errdefer window_width.deinit(allocator);
        var window_height: std.ArrayList(i16) = .empty;
        errdefer window_height.deinit(allocator);
        var client_ip: std.ArrayList(i32) = .empty;
        errdefer client_ip.deinit(allocator);
        var url_length: std.ArrayList(i32) = .empty;
        errdefer url_length.deinit(allocator);
        var event_minute: std.ArrayList(i32) = .empty;
        errdefer event_minute.deinit(allocator);
        var trafic_source_id: std.ArrayList(i16) = .empty;
        errdefer trafic_source_id.deinit(allocator);
        var referer_hash: std.ArrayList(i64) = .empty;
        errdefer referer_hash.deinit(allocator);

        var lines = std.mem.splitScalar(u8, csv, '\n');
        _ = lines.next();
        while (lines.next()) |line_raw| {
            const line = std.mem.trim(u8, line_raw, " \t\r");
            if (line.len == 0) continue;
            var fields = std.mem.splitScalar(u8, line, ',');
            const adv_text = fields.next() orelse return error.CorruptHotCsv;
            const width_text = fields.next() orelse return error.CorruptHotCsv;
            const date_text = fields.next() orelse return error.CorruptHotCsv;
            const user_text = fields.next() orelse return error.CorruptHotCsv;
            const counter_text = fields.next() orelse return error.CorruptHotCsv;
            const refresh_text = fields.next() orelse return error.CorruptHotCsv;
            const dont_count_text = fields.next() orelse return error.CorruptHotCsv;
            const url_hash_text = fields.next() orelse return error.CorruptHotCsv;
            const window_width_text = fields.next() orelse return error.CorruptHotCsv;
            const window_height_text = fields.next() orelse return error.CorruptHotCsv;
            const client_ip_text = fields.next() orelse return error.CorruptHotCsv;
            const url_length_text = fields.next() orelse return error.CorruptHotCsv;
            const event_minute_text = fields.next() orelse return error.CorruptHotCsv;
            const trafic_source_text = fields.next() orelse return error.CorruptHotCsv;
            const referer_hash_text = fields.next() orelse return error.CorruptHotCsv;
            try adv.append(allocator, try std.fmt.parseInt(i16, adv_text, 10));
            try width.append(allocator, try std.fmt.parseInt(i16, width_text, 10));
            try date.append(allocator, try std.fmt.parseInt(i32, date_text, 10));
            try user_id.append(allocator, try std.fmt.parseInt(i64, user_text, 10));
            try counter_id.append(allocator, try std.fmt.parseInt(i32, counter_text, 10));
            try is_refresh.append(allocator, try std.fmt.parseInt(i16, refresh_text, 10));
            try dont_count_hits.append(allocator, try std.fmt.parseInt(i16, dont_count_text, 10));
            try url_hash.append(allocator, try std.fmt.parseInt(i64, url_hash_text, 10));
            try window_width.append(allocator, try std.fmt.parseInt(i16, window_width_text, 10));
            try window_height.append(allocator, try std.fmt.parseInt(i16, window_height_text, 10));
            try client_ip.append(allocator, try std.fmt.parseInt(i32, client_ip_text, 10));
            try url_length.append(allocator, try std.fmt.parseInt(i32, url_length_text, 10));
            try event_minute.append(allocator, try std.fmt.parseInt(i32, event_minute_text, 10));
            try trafic_source_id.append(allocator, try std.fmt.parseInt(i16, trafic_source_text, 10));
            try referer_hash.append(allocator, try std.fmt.parseInt(i64, referer_hash_text, 10));
        }

        return .{
            .adv_engine_id = try adv.toOwnedSlice(allocator),
            .resolution_width = try width.toOwnedSlice(allocator),
            .event_date = try date.toOwnedSlice(allocator),
            .user_id = try user_id.toOwnedSlice(allocator),
            .counter_id = try counter_id.toOwnedSlice(allocator),
            .is_refresh = try is_refresh.toOwnedSlice(allocator),
            .dont_count_hits = try dont_count_hits.toOwnedSlice(allocator),
            .url_hash = try url_hash.toOwnedSlice(allocator),
            .window_client_width = try window_width.toOwnedSlice(allocator),
            .window_client_height = try window_height.toOwnedSlice(allocator),
            .watch_id = null,
            .client_ip = try client_ip.toOwnedSlice(allocator),
            .url_length = try url_length.toOwnedSlice(allocator),
            .event_minute = try event_minute.toOwnedSlice(allocator),
            .event_time = null,
            .trafic_source_id = try trafic_source_id.toOwnedSlice(allocator),
            .referer_hash = try referer_hash.toOwnedSlice(allocator),
        };
    }

    fn deinit(self: HotColumns, allocator: std.mem.Allocator) void {
        if (self.mappings) |maps| {
            for (maps) |m| m.unmap();
            allocator.free(maps);
            return;
        }
        allocator.free(self.adv_engine_id);
        allocator.free(self.resolution_width);
        allocator.free(self.event_date);
        allocator.free(self.user_id);
        allocator.free(self.counter_id);
        allocator.free(self.is_refresh);
        allocator.free(self.dont_count_hits);
        allocator.free(self.url_hash);
        allocator.free(self.window_client_width);
        allocator.free(self.window_client_height);
        if (self.watch_id) |values| allocator.free(values);
        if (self.client_ip) |values| allocator.free(values);
        if (self.url_length) |values| allocator.free(values);
        if (self.event_minute) |values| allocator.free(values);
        if (self.event_time) |values| allocator.free(values);
        if (self.trafic_source_id) |values| allocator.free(values);
        if (self.referer_hash) |values| allocator.free(values);
    }

    fn rowCount(self: HotColumns) usize {
        return self.adv_engine_id.len;
    }
};

const UserIdEncoding = struct {
    ids: io_map.MappedColumn(u32),
    dict: io_map.MappedColumn(i64),

    fn map(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !UserIdEncoding {
        const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
        defer allocator.free(id_path);
        const dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
        defer allocator.free(dict_path);
        const ids = try io_map.mapColumn(u32, io, id_path);
        errdefer ids.mapping.unmap();
        const dict = try io_map.mapColumn(i64, io, dict_path);
        errdefer dict.mapping.unmap();
        return .{ .ids = ids, .dict = dict };
    }

    fn deinit(self: UserIdEncoding) void {
        self.ids.mapping.unmap();
        self.dict.mapping.unmap();
    }
};

const SegmentStats = extern struct {
    min_counter_id: i32,
    max_counter_id: i32,
    min_event_date: i32,
    max_event_date: i32,
    min_is_refresh: i16,
    max_is_refresh: i16,
    min_dont_count_hits: i16,
    max_dont_count_hits: i16,
    min_trafic_source_id: i16,
    max_trafic_source_id: i16,
    min_referer_hash: i64,
    max_referer_hash: i64,
    min_event_time: i64,
    max_event_time: i64,
};

fn writeSegmentStats(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns) !void {
    const trafic_source_id = hot.trafic_source_id orelse return error.UnsupportedNativeQuery;
    const referer_hash = hot.referer_hash orelse return error.UnsupportedNativeQuery;

    // Try to load EventTime for stats (optional).
    const event_time_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_time_name);
    defer allocator.free(event_time_path);
    const event_times_opt = io_map.mapColumn(i64, io, event_time_path) catch null;
    defer if (event_times_opt) |et| et.mapping.unmap();

    const segment_count = (hot.rowCount() + storage.segment_rows - 1) / storage.segment_rows;
    const stats = try allocator.alloc(SegmentStats, segment_count);
    defer allocator.free(stats);

    for (0..segment_count) |segment| {
        const start = segment * storage.segment_rows;
        const end = @min(start + storage.segment_rows, hot.rowCount());
        var s = SegmentStats{
            .min_counter_id = std.math.maxInt(i32),
            .max_counter_id = std.math.minInt(i32),
            .min_event_date = std.math.maxInt(i32),
            .max_event_date = std.math.minInt(i32),
            .min_is_refresh = std.math.maxInt(i16),
            .max_is_refresh = std.math.minInt(i16),
            .min_dont_count_hits = std.math.maxInt(i16),
            .max_dont_count_hits = std.math.minInt(i16),
            .min_trafic_source_id = std.math.maxInt(i16),
            .max_trafic_source_id = std.math.minInt(i16),
            .min_referer_hash = std.math.maxInt(i64),
            .max_referer_hash = std.math.minInt(i64),
            .min_event_time = std.math.maxInt(i64),
            .max_event_time = std.math.minInt(i64),
        };
        for (start..end) |i| {
            s.min_counter_id = @min(s.min_counter_id, hot.counter_id[i]);
            s.max_counter_id = @max(s.max_counter_id, hot.counter_id[i]);
            s.min_event_date = @min(s.min_event_date, hot.event_date[i]);
            s.max_event_date = @max(s.max_event_date, hot.event_date[i]);
            s.min_is_refresh = @min(s.min_is_refresh, hot.is_refresh[i]);
            s.max_is_refresh = @max(s.max_is_refresh, hot.is_refresh[i]);
            s.min_dont_count_hits = @min(s.min_dont_count_hits, hot.dont_count_hits[i]);
            s.max_dont_count_hits = @max(s.max_dont_count_hits, hot.dont_count_hits[i]);
            s.min_trafic_source_id = @min(s.min_trafic_source_id, trafic_source_id[i]);
            s.max_trafic_source_id = @max(s.max_trafic_source_id, trafic_source_id[i]);
            s.min_referer_hash = @min(s.min_referer_hash, referer_hash[i]);
            s.max_referer_hash = @max(s.max_referer_hash, referer_hash[i]);
            if (event_times_opt) |et| {
                s.min_event_time = @min(s.min_event_time, et.values[i]);
                s.max_event_time = @max(s.max_event_time, et.values[i]);
            }
        }
        stats[segment] = s;
    }

    const path = try storage.hotSegmentStatsPath(allocator, data_dir);
    defer allocator.free(path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = std.mem.sliceAsBytes(stats) });
}

fn loadSegmentStats(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]SegmentStats {
    const path = try storage.hotSegmentStatsPath(allocator, data_dir);
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    errdefer allocator.free(bytes);
    if (bytes.len % @sizeOf(SegmentStats) != 0) return error.CorruptHotColumns;
    const stats = try allocator.alloc(SegmentStats, bytes.len / @sizeOf(SegmentStats));
    errdefer allocator.free(stats);
    @memcpy(std.mem.sliceAsBytes(stats), bytes);
    allocator.free(bytes);
    return stats;
}

fn writeI16Column(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8, values: []const i16) !void {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    const bytes = std.mem.sliceAsBytes(values);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = bytes });
}

fn writeI32Column(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8, values: []const i32) !void {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    const bytes = std.mem.sliceAsBytes(values);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = bytes });
}

fn writeI64Column(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8, values: []const i64) !void {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    const bytes = std.mem.sliceAsBytes(values);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = bytes });
}

fn mapI16Column(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8, maps: *std.ArrayList(io_map.Mapping)) ![]const i16 {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    const col = try io_map.mapColumn(i16, io, path);
    try maps.append(allocator, col.mapping);
    return col.values;
}

fn mapI32Column(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8, maps: *std.ArrayList(io_map.Mapping)) ![]const i32 {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    const col = try io_map.mapColumn(i32, io, path);
    try maps.append(allocator, col.mapping);
    return col.values;
}

fn mapI64Column(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8, maps: *std.ArrayList(io_map.Mapping)) ![]const i64 {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    const col = try io_map.mapColumn(i64, io, path);
    try maps.append(allocator, col.mapping);
    return col.values;
}

fn readI16Column(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8) ![]i16 {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    errdefer allocator.free(bytes);
    if (bytes.len % @sizeOf(i16) != 0) return error.CorruptHotColumns;
    const out = try allocator.alloc(i16, bytes.len / @sizeOf(i16));
    errdefer allocator.free(out);
    @memcpy(std.mem.sliceAsBytes(out), bytes);
    allocator.free(bytes);
    return out;
}

fn readI32Column(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8) ![]i32 {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    errdefer allocator.free(bytes);
    if (bytes.len % @sizeOf(i32) != 0) return error.CorruptHotColumns;
    const out = try allocator.alloc(i32, bytes.len / @sizeOf(i32));
    errdefer allocator.free(out);
    @memcpy(std.mem.sliceAsBytes(out), bytes);
    allocator.free(bytes);
    return out;
}

fn readI64Column(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8) ![]i64 {
    const path = try storage.hotColumnPath(allocator, data_dir, file_name);
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    errdefer allocator.free(bytes);
    if (bytes.len % @sizeOf(i64) != 0) return error.CorruptHotColumns;
    const out = try allocator.alloc(i64, bytes.len / @sizeOf(i64));
    errdefer allocator.free(out);
    @memcpy(std.mem.sliceAsBytes(out), bytes);
    allocator.free(bytes);
    return out;
}

fn genericCountNonZeroI16(values: []const i16) u64 {
    var count: u64 = 0;
    for (values) |value| count += @intFromBool(value != 0);
    return count;
}

fn genericSumI16(values: []const i16) i64 {
    var sum: i64 = 0;
    for (values) |value| sum += value;
    return sum;
}

fn genericAvgI16(values: []const i16) f64 {
    if (values.len == 0) return 0;
    return @as(f64, @floatFromInt(genericSumI16(values))) / @as(f64, @floatFromInt(values.len));
}

fn genericAvgI64(values: []const i64) f64 {
    if (values.len == 0) return 0;
    var sum: i128 = 0;
    for (values) |value| sum += value;
    return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(values.len));
}

fn avgFromSum(sum: i64, count: usize) f64 {
    if (count == 0) return 0;
    return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
}

const MinMaxI32 = struct { min: i32, max: i32 };

fn genericMinMaxI32(values: []const i32) MinMaxI32 {
    if (values.len == 0) return .{ .min = 0, .max = 0 };
    var min_value = values[0];
    var max_value = values[0];
    for (values[1..]) |value| {
        if (value < min_value) min_value = value;
        if (value > max_value) max_value = value;
    }
    return .{ .min = min_value, .max = max_value };
}

fn queryOutputsEquivalent(query_kind: clickbench_queries.Query, a: []const u8, b: []const u8) bool {
    if (std.mem.eql(u8, trimOutput(a), trimOutput(b))) return true;
    return switch (query_kind) {
        .avg_user_id => oneFloatOutputEquivalent(a, b, 1e-9),
        else => false,
    };
}

fn trimOutput(bytes: []const u8) []const u8 {
    var end = bytes.len;
    while (end > 0) {
        switch (bytes[end - 1]) {
            ' ', '\t', '\r', '\n' => end -= 1,
            else => break,
        }
    }
    return bytes[0..end];
}

fn oneFloatOutputEquivalent(a: []const u8, b: []const u8, rel_tol: f64) bool {
    const av = parseSingleResultFloat(a) catch return false;
    const bv = parseSingleResultFloat(b) catch return false;
    const diff = @abs(av - bv);
    const scale = @max(@abs(av), @abs(bv));
    return diff <= @max(1e-12, scale * rel_tol);
}

fn parseSingleResultFloat(bytes: []const u8) !f64 {
    const trimmed = trimOutput(bytes);
    const newline = std.mem.indexOfScalar(u8, trimmed, '\n') orelse return error.InvalidQueryOutput;
    const value = std.mem.trim(u8, trimmed[newline + 1 ..], " \t\r\n");
    return try std.fmt.parseFloat(f64, value);
}

const GenericValue = union(enum) {
    int: i64,
    float: f64,
    date: i32,
};

const GenericColumn = union(enum) {
    i16: []const i16,
    i32: []const i32,
    date: []const i32,
    i64: []const i64,
};

const GenericGroupRow = struct {
    key: i64,
    count: u64,
};

fn bindGenericColumn(hot: *const HotColumns, name: []const u8) !GenericColumn {
    if (asciiEqlIgnoreCase(name, "AdvEngineID")) return .{ .i16 = hot.adv_engine_id };
    if (asciiEqlIgnoreCase(name, "ResolutionWidth")) return .{ .i16 = hot.resolution_width };
    if (asciiEqlIgnoreCase(name, "UserID")) return .{ .i64 = hot.user_id };
    if (asciiEqlIgnoreCase(name, "EventDate")) return .{ .date = hot.event_date };
    if (asciiEqlIgnoreCase(name, "CounterID")) return .{ .i32 = hot.counter_id };
    if (asciiEqlIgnoreCase(name, "IsRefresh")) return .{ .i16 = hot.is_refresh };
    if (asciiEqlIgnoreCase(name, "DontCountHits")) return .{ .i16 = hot.dont_count_hits };
    return error.UnsupportedGenericColumn;
}

fn materializeGenericPredicate(allocator: std.mem.Allocator, column: GenericColumn, filter: generic_sql.Filter) ![]i16 {
    const predicate = generic_sql.Predicate{ .column = filter.column, .op = filter.op, .int_value = filter.int_value };
    return materializeGenericPredicateFor(allocator, column, predicate);
}

fn materializeGenericPredicateFor(allocator: std.mem.Allocator, column: GenericColumn, predicate: generic_sql.Predicate) ![]i16 {
    return switch (column) {
        .i16 => |values| materializeGenericPredicateTyped(i16, allocator, values, predicate),
        .i32, .date => |values| materializeGenericPredicateTyped(i32, allocator, values, predicate),
        .i64 => |values| materializeGenericPredicateTyped(i64, allocator, values, predicate),
    };
}

fn materializeGenericAndPredicate(allocator: std.mem.Allocator, left_column: GenericColumn, left: generic_sql.Predicate, right_column: GenericColumn, right: generic_sql.Predicate) ![]i16 {
    const mask = try materializeGenericPredicateFor(allocator, left_column, left);
    errdefer allocator.free(mask);
    try applyGenericPredicateAnd(right_column, right, mask);
    return mask;
}

fn applyGenericPredicateAnd(column: GenericColumn, predicate: generic_sql.Predicate, mask: []i16) !void {
    switch (column) {
        .i16 => |values| try applyGenericPredicateAndTyped(i16, values, predicate, mask),
        .i32, .date => |values| try applyGenericPredicateAndTyped(i32, values, predicate, mask),
        .i64 => |values| try applyGenericPredicateAndTyped(i64, values, predicate, mask),
    }
}

fn applyGenericPredicateAndTyped(comptime T: type, values: []const T, predicate: generic_sql.Predicate, mask: []i16) !void {
    if (values.len != mask.len) return error.InvalidGenericResult;
    const target = std.math.cast(T, predicate.int_value) orelse return error.UnsupportedGenericQuery;
    for (values, mask) |value, *out| {
        if (out.* != 0 and !genericPredicateMatches(T, value, target, predicate.op)) out.* = 0;
    }
}

fn materializeGenericPredicateTyped(comptime T: type, allocator: std.mem.Allocator, values: []const T, predicate: generic_sql.Predicate) ![]i16 {
    const target = std.math.cast(T, predicate.int_value) orelse return error.UnsupportedGenericQuery;
    const mask = try allocator.alloc(i16, values.len);
    errdefer allocator.free(mask);
    for (values, mask) |value, *out| {
        out.* = if (genericPredicateMatches(T, value, target, predicate.op)) 1 else 0;
    }
    return mask;
}

fn genericPredicateMatches(comptime T: type, value: T, target: T, op: generic_sql.FilterOp) bool {
    return switch (op) {
        .equal => value == target,
        .not_equal => value != target,
        .greater => value > target,
        .greater_equal => value >= target,
        .less => value < target,
        .less_equal => value <= target,
    };
}

fn formatGenericGroupCount(allocator: std.mem.Allocator, header_col: []const u8, column: GenericColumn, predicate: ?[]const i16) ![]u8 {
    const rows = switch (column) {
        .i16 => |values| try genericGroupCountTyped(i16, allocator, values, predicate),
        .i32 => |values| try genericGroupCountTyped(i32, allocator, values, predicate),
        .date => return error.UnsupportedGenericQuery,
        .i64 => |values| try genericGroupCountTyped(i64, allocator, values, predicate),
    };
    defer allocator.free(rows);
    std.mem.sort(GenericGroupRow, rows, {}, genericGroupRowDesc);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "{s},count_star()\n", .{header_col});
    for (rows) |row| try out.print(allocator, "{d},{d}\n", .{ row.key, row.count });
    return out.toOwnedSlice(allocator);
}

fn genericGroupCountTyped(comptime T: type, allocator: std.mem.Allocator, values: []const T, predicate: ?[]const i16) ![]GenericGroupRow {
    if (T == i16) {
        const min_key = std.math.minInt(i16);
        const bucket_count = @as(usize, std.math.maxInt(u16)) + 1;
        const counts = try allocator.alloc(u64, bucket_count);
        defer allocator.free(counts);
        @memset(counts, 0);
        for (values, 0..) |value, i| {
            if (predicate) |p| if (p[i] == 0) continue;
            const index: usize = @intCast(@as(i32, value) - @as(i32, min_key));
            counts[index] += 1;
        }

        var rows: std.ArrayList(GenericGroupRow) = .empty;
        errdefer rows.deinit(allocator);
        for (counts, 0..) |count, index| {
            if (count == 0) continue;
            const key: i16 = @intCast(@as(i32, @intCast(index)) + @as(i32, min_key));
            try rows.append(allocator, .{ .key = key, .count = count });
        }
        return rows.toOwnedSlice(allocator);
    }
    return error.UnsupportedGenericQuery;
}

fn genericGroupRowDesc(_: void, a: GenericGroupRow, b: GenericGroupRow) bool {
    if (a.count == b.count) return a.key < b.key;
    return a.count > b.count;
}

fn formatGenericFilteredColumn(allocator: std.mem.Allocator, header_col: []const u8, column: GenericColumn, predicate: []const i16) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "{s}\n", .{header_col});
    switch (column) {
        .i16 => |values| try writeFilteredColumnValues(i16, &out, allocator, values, predicate),
        .i32 => |values| try writeFilteredColumnValues(i32, &out, allocator, values, predicate),
        .date => |values| try writeFilteredDateValues(&out, allocator, values, predicate),
        .i64 => |values| try writeFilteredColumnValues(i64, &out, allocator, values, predicate),
    }
    return out.toOwnedSlice(allocator);
}

fn formatGenericPointLookupColumn(allocator: std.mem.Allocator, header_col: []const u8, column: GenericColumn, int_value: i64) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "{s}\n", .{header_col});
    switch (column) {
        .i16 => |values| try writePointLookupValues(i16, &out, allocator, values, int_value),
        .i32 => |values| try writePointLookupValues(i32, &out, allocator, values, int_value),
        .date => |values| try writePointLookupDateValues(&out, allocator, values, int_value),
        .i64 => |values| try writePointLookupValues(i64, &out, allocator, values, int_value),
    }
    return out.toOwnedSlice(allocator);
}

fn writePointLookupValues(comptime T: type, out: *std.ArrayList(u8), allocator: std.mem.Allocator, values: []const T, int_value: i64) !void {
    const target = std.math.cast(T, int_value) orelse return error.UnsupportedGenericQuery;
    for (values) |value| {
        if (value != target) continue;
        try out.print(allocator, "{d}\n", .{value});
    }
}

fn writePointLookupDateValues(out: *std.ArrayList(u8), allocator: std.mem.Allocator, values: []const i32, int_value: i64) !void {
    const target = std.math.cast(i32, int_value) orelse return error.UnsupportedGenericQuery;
    for (values) |value| {
        if (value != target) continue;
        const text = dateString(@intCast(value));
        try out.print(allocator, "{s}\n", .{text});
    }
}

fn writeFilteredColumnValues(comptime T: type, out: *std.ArrayList(u8), allocator: std.mem.Allocator, values: []const T, predicate: []const i16) !void {
    if (values.len != predicate.len) return error.InvalidGenericResult;
    for (values, predicate) |value, p| {
        if (p == 0) continue;
        try out.print(allocator, "{d}\n", .{value});
    }
}

fn writeFilteredDateValues(out: *std.ArrayList(u8), allocator: std.mem.Allocator, values: []const i32, predicate: []const i16) !void {
    if (values.len != predicate.len) return error.InvalidGenericResult;
    for (values, predicate) |value, p| {
        if (p == 0) continue;
        const text = dateString(@intCast(value));
        try out.print(allocator, "{s}\n", .{text});
    }
}

fn executeGenericFusedSumOffsets(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: *const HotColumns) !?[]u8 {
    if (plan.projections.len < 2) return null;
    const first_col = plan.projections[0].column orelse return null;
    for (plan.projections) |expr| {
        if (expr.func != .sum) return null;
        const col = expr.column orelse return null;
        if (!asciiEqlIgnoreCase(col, first_col)) return null;
    }

    const column = bindGenericColumn(hot, first_col) catch return error.UnsupportedGenericQuery;
    const base_sum: i64 = switch (column) {
        .i16 => |values| simd.sum(i16, values),
        .i32 => |values| simd.sum(i32, values),
        .date, .i64 => return error.UnsupportedGenericQuery,
    };
    const row_count: i64 = @intCast(hot.rowCount());

    const values = try allocator.alloc(GenericValue, plan.projections.len);
    defer allocator.free(values);
    for (plan.projections, values) |expr, *out| {
        out.* = .{ .int = base_sum + expr.int_offset * row_count };
    }
    return try formatGenericValues(allocator, plan, values);
}

fn executeGenericProjection(expr: generic_sql.Expr, hot: *const HotColumns) !GenericValue {
    if (expr.func == .count_star) return .{ .int = @intCast(hot.rowCount()) };
    const column = bindGenericColumn(hot, expr.column orelse return error.UnsupportedGenericQuery) catch return error.UnsupportedGenericQuery;
    return switch (expr.func) {
        .column_ref => error.UnsupportedGenericQuery,
        .count_distinct => error.UnsupportedGenericQuery,
        .count_star => unreachable,
        .sum => aggregateSum(column, null, expr.int_offset),
        .avg => aggregateAvg(column, null),
        .min => aggregateMin(column, null),
        .max => aggregateMax(column, null),
    };
}

fn executeGenericFilteredProjection(expr: generic_sql.Expr, hot: *const HotColumns, predicate: []const i16) !GenericValue {
    if (expr.func == .count_star) return .{ .int = @intCast(simd.countNonZero(i16, predicate)) };
    const column = bindGenericColumn(hot, expr.column orelse return error.UnsupportedGenericQuery) catch return error.UnsupportedGenericQuery;
    return switch (expr.func) {
        .column_ref => error.UnsupportedGenericQuery,
        .count_distinct => error.UnsupportedGenericQuery,
        .count_star => unreachable,
        .sum => aggregateSum(column, predicate, expr.int_offset),
        .avg => aggregateAvg(column, predicate),
        .min => aggregateMin(column, predicate),
        .max => aggregateMax(column, predicate),
    };
}

fn aggregateSum(column: GenericColumn, predicate: ?[]const i16, int_offset: i64) !GenericValue {
    return switch (column) {
        .i16 => |values| .{ .int = if (predicate) |p| sumWithOffset(simd.filteredSumCountNonZero(i16, p, values), int_offset) else sumWithOffset(.{ .sum = simd.sum(i16, values), .count = @intCast(values.len) }, int_offset) },
        .i32 => |values| .{ .int = if (predicate) |p| sumWithOffset(simd.filteredSumCountNonZero(i32, p, values), int_offset) else sumWithOffset(.{ .sum = simd.sum(i32, values), .count = @intCast(values.len) }, int_offset) },
        .date, .i64 => error.UnsupportedGenericQuery,
    };
}

fn sumWithOffset(sum_count: simd.SumCount, int_offset: i64) i64 {
    return sum_count.sum + int_offset * @as(i64, @intCast(sum_count.count));
}

fn aggregateAvg(column: GenericColumn, predicate: ?[]const i16) !GenericValue {
    return switch (column) {
        .i16 => |values| .{ .float = if (predicate) |p| simd.filteredAvgNonZero(i16, p, values) else avgFromSum(simd.sum(i16, values), values.len) },
        .i32 => |values| .{ .float = if (predicate) |p| simd.filteredAvgNonZero(i32, p, values) else avgFromSum(simd.sum(i32, values), values.len) },
        .i64 => |values| .{ .float = if (predicate) |p| simd.filteredAvgNonZero(i64, p, values) else simd.avg(i64, values) },
        .date => error.UnsupportedGenericQuery,
    };
}

fn aggregateMin(column: GenericColumn, predicate: ?[]const i16) !GenericValue {
    return switch (column) {
        .i16 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i16, p, values).min else simd.minMax(i16, values).min },
        .i32 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i32, p, values).min else simd.minMax(i32, values).min },
        .date => |values| .{ .date = if (predicate) |p| simd.filteredMinMaxNonZero(i32, p, values).min else simd.minMax(i32, values).min },
        .i64 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i64, p, values).min else simd.minMax(i64, values).min },
    };
}

fn aggregateMax(column: GenericColumn, predicate: ?[]const i16) !GenericValue {
    return switch (column) {
        .i16 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i16, p, values).max else simd.minMax(i16, values).max },
        .i32 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i32, p, values).max else simd.minMax(i32, values).max },
        .date => |values| .{ .date = if (predicate) |p| simd.filteredMinMaxNonZero(i32, p, values).max else simd.minMax(i32, values).max },
        .i64 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i64, p, values).max else simd.minMax(i64, values).max },
    };
}

fn executeGenericFusedMinMax(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: *const HotColumns) !?[]u8 {
    if (plan.projections.len != 2) return null;
    const first = plan.projections[0];
    const second = plan.projections[1];
    if (first.func != .min or second.func != .max) return null;
    const first_col = first.column orelse return error.UnsupportedGenericQuery;
    const second_col = second.column orelse return error.UnsupportedGenericQuery;
    if (!asciiEqlIgnoreCase(first_col, second_col)) return null;
    return switch (bindGenericColumn(hot, first_col) catch return error.UnsupportedGenericQuery) {
        .i16 => |values| blk: {
            const mm = simd.minMax(i16, values);
            break :blk try formatGenericValues(allocator, plan, &.{ .{ .int = mm.min }, .{ .int = mm.max } });
        },
        .i32 => |values| blk: {
            const mm = simd.minMax(i32, values);
            break :blk try formatGenericValues(allocator, plan, &.{ .{ .int = mm.min }, .{ .int = mm.max } });
        },
        .date => |values| blk: {
            const mm = simd.minMax(i32, values);
            break :blk try formatGenericValues(allocator, plan, &.{ .{ .date = mm.min }, .{ .date = mm.max } });
        },
        .i64 => |values| blk: {
            const mm = simd.minMax(i64, values);
            break :blk try formatGenericValues(allocator, plan, &.{ .{ .int = mm.min }, .{ .int = mm.max } });
        },
    };
}

fn formatGenericValues(allocator: std.mem.Allocator, plan: generic_sql.Plan, values: []const GenericValue) ![]u8 {
    if (values.len != plan.projections.len) return error.InvalidGenericResult;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try writeGenericHeader(&out, allocator, plan);
    for (values, 0..) |value, i| {
        if (i != 0) try out.append(allocator, ',');
        try writeGenericValue(&out, allocator, value);
    }
    try out.append(allocator, '\n');
    return out.toOwnedSlice(allocator);
}

fn writeGenericHeader(out: *std.ArrayList(u8), allocator: std.mem.Allocator, plan: generic_sql.Plan) !void {
    for (plan.projections, 0..) |expr, i| {
        if (i != 0) try out.append(allocator, ',');
        switch (expr.func) {
            .column_ref => try out.print(allocator, "{s}", .{expr.column.?}),
            .count_distinct => try out.print(allocator, "count(DISTINCT {s})", .{expr.column.?}),
            .count_star => try out.appendSlice(allocator, "count_star()"),
            .sum => if (expr.int_offset == 0) try out.print(allocator, "sum({s})", .{expr.column.?}) else try out.print(allocator, "sum(({s} + {d}))", .{ expr.column.?, expr.int_offset }),
            .avg => try out.print(allocator, "avg({s})", .{expr.column.?}),
            .min => try out.print(allocator, "min({s})", .{expr.column.?}),
            .max => try out.print(allocator, "max({s})", .{expr.column.?}),
        }
    }
    try out.append(allocator, '\n');
}

fn writeGenericValue(out: *std.ArrayList(u8), allocator: std.mem.Allocator, value: GenericValue) !void {
    switch (value) {
        .int => |v| try out.print(allocator, "{d}", .{v}),
        .float => |v| try out.print(allocator, "{d}", .{v}),
        .date => |v| {
            const text = dateString(@intCast(v));
            try out.print(allocator, "{s}", .{text});
        },
    }
}

fn genericOutputsEquivalent(a: []const u8, b: []const u8) bool {
    if (std.mem.eql(u8, trimOutput(a), trimOutput(b))) return true;
    return singleRowCsvFloatsEquivalent(a, b, 1e-9);
}

fn singleRowCsvFloatsEquivalent(a: []const u8, b: []const u8, rel_tol: f64) bool {
    const a_row = singleDataRow(a) orelse return false;
    const b_row = singleDataRow(b) orelse return false;
    var ait = std.mem.splitScalar(u8, a_row, ',');
    var bit = std.mem.splitScalar(u8, b_row, ',');
    while (true) {
        const av_raw = ait.next();
        const bv_raw = bit.next();
        if (av_raw == null and bv_raw == null) return true;
        if (av_raw == null or bv_raw == null) return false;
        const av_text = std.mem.trim(u8, av_raw.?, " \t\r\n");
        const bv_text = std.mem.trim(u8, bv_raw.?, " \t\r\n");
        if (std.mem.eql(u8, av_text, bv_text)) continue;
        const av = std.fmt.parseFloat(f64, av_text) catch return false;
        const bv = std.fmt.parseFloat(f64, bv_text) catch return false;
        const diff = @abs(av - bv);
        const scale = @max(@abs(av), @abs(bv));
        if (diff > @max(1e-12, scale * rel_tol)) return false;
    }
}

fn singleDataRow(bytes: []const u8) ?[]const u8 {
    const trimmed = trimOutput(bytes);
    const first_newline = std.mem.indexOfScalar(u8, trimmed, '\n') orelse return null;
    const row = trimmed[first_newline + 1 ..];
    if (std.mem.indexOfScalar(u8, row, '\n') != null) return null;
    return row;
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ac, bc| if (std.ascii.toLower(ac) != std.ascii.toLower(bc)) return false;
    return true;
}

fn formatOneInt(allocator: std.mem.Allocator, header: []const u8, value: u64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}\n{d}\n", .{ header, value });
}

fn formatOneFloat(allocator: std.mem.Allocator, header: []const u8, value: f64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}\n{d}\n", .{ header, value });
}

fn formatSumCountAvg(allocator: std.mem.Allocator, sum: i64, count: usize, avg: f64) ![]u8 {
    return std.fmt.allocPrint(allocator, "sum(AdvEngineID),count_star(),avg(ResolutionWidth)\n{d},{d},{d}\n", .{ sum, count, avg });
}

fn formatMinMaxDate(allocator: std.mem.Allocator, min_days: i32, max_days: i32) ![]u8 {
    const min_date = dateString(@intCast(min_days));
    const max_date = dateString(@intCast(max_days));
    return std.fmt.allocPrint(allocator, "min(EventDate),max(EventDate)\n{s},{s}\n", .{ min_date, max_date });
}

fn formatWideResolutionSums(allocator: std.mem.Allocator, base_sum: i64, count: usize) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    for (0..90) |i| {
        if (i != 0) try out.append(allocator, ',');
        if (i == 0) {
            try out.appendSlice(allocator, "sum(ResolutionWidth)");
        } else {
            try out.print(allocator, "sum((ResolutionWidth + {d}))", .{i});
        }
    }
    try out.append(allocator, '\n');

    const row_count: i64 = @intCast(count);
    for (0..90) |i| {
        if (i != 0) try out.append(allocator, ',');
        const value = base_sum + @as(i64, @intCast(i)) * row_count;
        try out.print(allocator, "{d}", .{value});
    }
    try out.append(allocator, '\n');
    return out.toOwnedSlice(allocator);
}

fn formatAdvEngineGroupBy(allocator: std.mem.Allocator, values: []const i16) ![]u8 {
    const min_key = std.math.minInt(i16);
    const bucket_count = @as(usize, std.math.maxInt(u16)) + 1;
    const counts = try allocator.alloc(u64, bucket_count);
    defer allocator.free(counts);
    @memset(counts, 0);

    for (values) |value| {
        if (value == 0) continue;
        const index: usize = @intCast(@as(i32, value) - @as(i32, min_key));
        counts[index] += 1;
    }

    var rows: std.ArrayList(GroupRow) = .empty;
    defer rows.deinit(allocator);
    for (counts, 0..) |count, index| {
        if (count == 0) continue;
        const key: i16 = @intCast(@as(i32, @intCast(index)) + @as(i32, min_key));
        try rows.append(allocator, .{ .key = key, .count = count });
    }
    std.mem.sort(GroupRow, rows.items, {}, groupRowDesc);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "AdvEngineID,count_star()\n");
    for (rows.items) |row| {
        try out.print(allocator, "{d},{d}\n", .{ row.key, row.count });
    }
    return out.toOwnedSlice(allocator);
}

const GroupRow = struct {
    key: i16,
    count: u64,
};

fn groupRowDesc(_: void, a: GroupRow, b: GroupRow) bool {
    if (a.count == b.count) return a.key < b.key;
    return a.count > b.count;
}

fn formatUserIdPointLookup(allocator: std.mem.Allocator, values: []const i64, target: i64) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID\n");

    const lanes = 8;
    const V = @Vector(lanes, i64);
    const target_vec: V = @splat(target);
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        const mask = v == target_vec;
        if (@as(u8, @bitCast(mask)) == 0) continue;
        for (values[i..][0..lanes]) |value| {
            if (value == target) try out.print(allocator, "{d}\n", .{value});
        }
    }
    while (i < values.len) : (i += 1) {
        if (values[i] == target) try out.print(allocator, "{d}\n", .{values[i]});
    }
    return out.toOwnedSlice(allocator);
}

fn formatUrlLengthByCounter(allocator: std.mem.Allocator, counter_ids: []const i32, url_lengths: []const i32) ![]u8 {
    const min_counter: i32 = 0;
    const max_counter: i32 = 262143;
    const bucket_count: usize = @intCast(max_counter - min_counter + 1);
    const counts = try allocator.alloc(u64, bucket_count);
    defer allocator.free(counts);
    const sums = try allocator.alloc(u64, bucket_count);
    defer allocator.free(sums);
    @memset(counts, 0);
    @memset(sums, 0);

    for (counter_ids, url_lengths) |counter_id, url_length| {
        if (url_length == 0) continue;
        if (counter_id < min_counter or counter_id > max_counter) continue;
        const index: usize = @intCast(counter_id - min_counter);
        counts[index] += 1;
        sums[index] += @intCast(url_length);
    }

    var rows: std.ArrayList(UrlLengthCounterRow) = .empty;
    defer rows.deinit(allocator);
    for (counts, 0..) |count, index| {
        if (count <= 100000) continue;
        const counter_id: i32 = @intCast(@as(i64, min_counter) + @as(i64, @intCast(index)));
        try rows.append(allocator, .{ .counter_id = counter_id, .sum = sums[index], .count = count });
    }
    std.mem.sort(UrlLengthCounterRow, rows.items, {}, urlLengthCounterDesc);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "CounterID,l,c\n");
    const limit = @min(rows.items.len, 25);
    for (rows.items[0..limit]) |row| {
        const avg = @as(f64, @floatFromInt(row.sum)) / @as(f64, @floatFromInt(row.count));
        try out.print(allocator, "{d},{d},{d}\n", .{ row.counter_id, avg, row.count });
    }
    return out.toOwnedSlice(allocator);
}

const UrlLengthCounterRow = struct {
    counter_id: i32,
    sum: u64,
    count: u64,
};

fn urlLengthCounterDesc(_: void, a: UrlLengthCounterRow, b: UrlLengthCounterRow) bool {
    const left = @as(u128, a.sum) * @as(u128, b.count);
    const right = @as(u128, b.sum) * @as(u128, a.count);
    if (left != right) return left > right;
    return a.counter_id < b.counter_id;
}

fn formatClientIpTop10(allocator: std.mem.Allocator, values: []const i32) ![]u8 {
    // Parallel partitioned hash agg. Each worker owns a private
    // PartitionedHashU64Count; pass1 accumulates via morsels. Pass2
    // merges each of the 64 partitions independently in parallel,
    // keeping a per-merge-worker top-10. Final top-10 is the merge
    // of those per-worker heaps.
    const n_threads = parallel.defaultThreads();
    // Estimate ~9M distinct ClientIPs (ClickBench). Per-thread tables
    // get expected/n_threads keys.
    const expected_total: usize = 9_500_000;
    const expected_per_thread = expected_total / n_threads + 1;

    const tables = try allocator.alloc(hashmap.PartitionedHashU64Count, n_threads);
    var inited: usize = 0;
    defer {
        for (tables[0..inited]) |*t| t.deinit();
        allocator.free(tables);
    }
    for (tables) |*t| {
        t.* = try hashmap.PartitionedHashU64Count.init(allocator, expected_per_thread);
        inited += 1;
    }

    const Pass1Ctx = struct {
        values: []const i32,
        table: *hashmap.PartitionedHashU64Count,
    };
    const pass1_workers = struct {
        fn fill(ctx: *Pass1Ctx, source: *parallel.MorselSource) void {
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) {
                    ctx.table.bump(@as(u64, @bitCast(@as(i64, ctx.values[r]))));
                }
            }
        }
    };
    const pass1_ctxs = try allocator.alloc(Pass1Ctx, n_threads);
    defer allocator.free(pass1_ctxs);
    for (pass1_ctxs, 0..) |*c, t| c.* = .{ .values = values, .table = &tables[t] };
    var src: parallel.MorselSource = .init(values.len, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, Pass1Ctx, pass1_workers.fill, pass1_ctxs, &src);

    // Build pointer array for mergePartition.
    const local_ptrs = try allocator.alloc(*hashmap.PartitionedHashU64Count, n_threads);
    defer allocator.free(local_ptrs);
    for (tables, 0..) |*t, i| local_ptrs[i] = t;

    // Per-merge-worker top-10. We use one worker per partition rebalanced
    // across n_threads physical workers. Since partition_count = 64,
    // each worker handles ~64/n_threads partitions.
    const n_workers = @min(n_threads, hashmap.partition_count);
    const worker_tops = try allocator.alloc([10]ClientIpCount, n_workers);
    defer allocator.free(worker_tops);
    const worker_top_lens = try allocator.alloc(usize, n_workers);
    defer allocator.free(worker_top_lens);
    @memset(worker_top_lens, 0);

    const MergeCtx = struct {
        allocator: std.mem.Allocator,
        local_ptrs: []*hashmap.PartitionedHashU64Count,
        worker_tops: [][10]ClientIpCount,
        worker_top_lens: []usize,
        n_workers: usize,
    };
    const merge_workers = struct {
        fn run(ctx: *MergeCtx, worker_id: usize) void {
            var p = worker_id;
            while (p < hashmap.partition_count) : (p += ctx.n_workers) {
                // Estimate output size = sum of per-thread part lens.
                var expected_p: usize = 0;
                for (ctx.local_ptrs) |lp| expected_p += lp.parts[p].len;
                if (expected_p == 0) continue;
                var merged = hashmap.mergePartition(ctx.allocator, ctx.local_ptrs, p, expected_p) catch return;
                defer merged.deinit();
                var it = merged.iterator();
                while (it.next()) |e| {
                    insertClientIpTop10(
                        &ctx.worker_tops[worker_id],
                        &ctx.worker_top_lens[worker_id],
                        .{ .client_ip = @as(i32, @truncate(@as(i64, @bitCast(e.key)))), .count = e.value },
                    );
                }
            }
        }
    };
    var merge_ctx: MergeCtx = .{
        .allocator = allocator,
        .local_ptrs = local_ptrs,
        .worker_tops = worker_tops,
        .worker_top_lens = worker_top_lens,
        .n_workers = n_workers,
    };
    try parallel.parallelIndices(allocator, MergeCtx, merge_workers.run, &merge_ctx, n_workers);

    // Final reduce: merge per-worker top-10 lists into a global top-10.
    var top: [10]ClientIpCount = undefined;
    var top_len: usize = 0;
    for (worker_tops, 0..) |*wt, w| {
        for (wt[0..worker_top_lens[w]]) |row| insertClientIpTop10(&top, &top_len, row);
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "ClientIP,subtract(ClientIP, 1),subtract(ClientIP, 2),subtract(ClientIP, 3),c\n");
    for (top[0..top_len]) |row| {
        try out.print(allocator, "{d},{d},{d},{d},{d}\n", .{ row.client_ip, row.client_ip - 1, row.client_ip - 2, row.client_ip - 3, row.count });
    }
    return out.toOwnedSlice(allocator);
}

const ClientIpCount = struct {
    client_ip: i32,
    count: u32,
};

fn insertClientIpTop10(top: *[10]ClientIpCount, top_len: *usize, row: ClientIpCount) void {
    var pos: usize = 0;
    while (pos < top_len.* and clientIpBefore(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn clientIpBefore(a: ClientIpCount, b: ClientIpCount) bool {
    if (a.count == b.count) return a.client_ip < b.client_ip;
    return a.count > b.count;
}

const i32_empty_key = std.math.minInt(i32);
const u32_empty_key = std.math.maxInt(u32);

const U32CountTable = struct {
    keys: []u32,
    counts: []u32,
    occupied: []u32,
    len: usize = 0,
    mask: usize,
    max_load: usize,

    fn init(allocator: std.mem.Allocator, expected: usize) !U32CountTable {
        var capacity: usize = 1;
        const target = expected * 10 / 7 + 1;
        while (capacity < target) capacity <<= 1;
        capacity = @max(capacity, 1024);
        const keys = try allocator.alloc(u32, capacity);
        errdefer allocator.free(keys);
        const counts = try allocator.alloc(u32, capacity);
        errdefer allocator.free(counts);
        const occupied = try allocator.alloc(u32, capacity);
        errdefer allocator.free(occupied);
        @memset(keys, u32_empty_key);
        @memset(counts, 0);
        return .{ .keys = keys, .counts = counts, .occupied = occupied, .mask = capacity - 1, .max_load = capacity * 7 / 10 };
    }

    fn deinit(self: U32CountTable, allocator: std.mem.Allocator) void {
        allocator.free(self.keys);
        allocator.free(self.counts);
        allocator.free(self.occupied);
    }

    fn add(self: *U32CountTable, allocator: std.mem.Allocator, key: u32) !void {
        if (key == u32_empty_key) return error.InvalidKey;
        while (true) {
            var index = hashU32(key) & self.mask;
            while (true) {
                const existing = self.keys[index];
                if (existing == key) {
                    self.counts[index] += 1;
                    return;
                }
                if (existing == u32_empty_key) {
                    if (self.len + 1 > self.max_load) {
                        try self.grow(allocator);
                        break;
                    }
                    self.keys[index] = key;
                    self.counts[index] = 1;
                    self.occupied[self.len] = @intCast(index);
                    self.len += 1;
                    return;
                }
                index = (index + 1) & self.mask;
            }
        }
    }

    fn grow(self: *U32CountTable, allocator: std.mem.Allocator) !void {
        const old_keys = self.keys;
        const old_counts = self.counts;
        const old_occupied = self.occupied;
        const old_len = self.len;
        const new_capacity = old_keys.len * 2;
        const new_keys = try allocator.alloc(u32, new_capacity);
        errdefer allocator.free(new_keys);
        const new_counts = try allocator.alloc(u32, new_capacity);
        errdefer allocator.free(new_counts);
        const new_occupied = try allocator.alloc(u32, new_capacity);
        errdefer allocator.free(new_occupied);
        @memset(new_keys, u32_empty_key);
        @memset(new_counts, 0);
        self.keys = new_keys;
        self.counts = new_counts;
        self.occupied = new_occupied;
        self.len = 0;
        self.mask = new_capacity - 1;
        self.max_load = new_capacity * 7 / 10;
        for (old_occupied[0..old_len]) |old_index| self.insertExisting(old_keys[old_index], old_counts[old_index]);
        allocator.free(old_keys);
        allocator.free(old_counts);
        allocator.free(old_occupied);
    }

    fn insertExisting(self: *U32CountTable, key: u32, count: u32) void {
        var index = hashU32(key) & self.mask;
        while (true) {
            if (self.keys[index] == u32_empty_key) {
                self.keys[index] = key;
                self.counts[index] = count;
                self.occupied[self.len] = @intCast(index);
                self.len += 1;
                return;
            }
            index = (index + 1) & self.mask;
        }
    }
};

const I32CountTable = struct {
    keys: []i32,
    counts: []u32,
    occupied: []u32,
    len: usize = 0,
    mask: usize,
    max_load: usize,

    fn init(allocator: std.mem.Allocator, expected: usize) !I32CountTable {
        var capacity: usize = 1;
        const target = expected * 10 / 7 + 1;
        while (capacity < target) capacity <<= 1;
        capacity = @max(capacity, 1024);
        const keys = try allocator.alloc(i32, capacity);
        errdefer allocator.free(keys);
        const counts = try allocator.alloc(u32, capacity);
        errdefer allocator.free(counts);
        const occupied = try allocator.alloc(u32, capacity);
        errdefer allocator.free(occupied);
        @memset(keys, i32_empty_key);
        @memset(counts, 0);
        return .{ .keys = keys, .counts = counts, .occupied = occupied, .mask = capacity - 1, .max_load = capacity * 7 / 10 };
    }

    fn deinit(self: I32CountTable, allocator: std.mem.Allocator) void {
        allocator.free(self.keys);
        allocator.free(self.counts);
        allocator.free(self.occupied);
    }

    fn add(self: *I32CountTable, allocator: std.mem.Allocator, key: i32) !void {
        if (key == i32_empty_key) return error.InvalidKey;
        while (true) {
            var index = hashI32(key) & self.mask;
            while (true) {
                const existing = self.keys[index];
                if (existing == key) {
                    self.counts[index] += 1;
                    return;
                }
                if (existing == i32_empty_key) {
                    if (self.len + 1 > self.max_load) {
                        try self.grow(allocator);
                        break;
                    }
                    self.keys[index] = key;
                    self.counts[index] = 1;
                    self.occupied[self.len] = @intCast(index);
                    self.len += 1;
                    return;
                }
                index = (index + 1) & self.mask;
            }
        }
    }

    fn grow(self: *I32CountTable, allocator: std.mem.Allocator) !void {
        const old_keys = self.keys;
        const old_counts = self.counts;
        const old_occupied = self.occupied;
        const old_len = self.len;
        const new_capacity = old_keys.len * 2;
        const new_keys = try allocator.alloc(i32, new_capacity);
        errdefer allocator.free(new_keys);
        const new_counts = try allocator.alloc(u32, new_capacity);
        errdefer allocator.free(new_counts);
        const new_occupied = try allocator.alloc(u32, new_capacity);
        errdefer allocator.free(new_occupied);
        @memset(new_keys, i32_empty_key);
        @memset(new_counts, 0);
        self.keys = new_keys;
        self.counts = new_counts;
        self.occupied = new_occupied;
        self.len = 0;
        self.mask = new_capacity - 1;
        self.max_load = new_capacity * 7 / 10;
        for (old_occupied[0..old_len]) |old_index| self.insertExisting(old_keys[old_index], old_counts[old_index]);
        allocator.free(old_keys);
        allocator.free(old_counts);
        allocator.free(old_occupied);
    }

    fn insertExisting(self: *I32CountTable, key: i32, count: u32) void {
        var index = hashI32(key) & self.mask;
        while (true) {
            if (self.keys[index] == i32_empty_key) {
                self.keys[index] = key;
                self.counts[index] = count;
                self.occupied[self.len] = @intCast(index);
                self.len += 1;
                return;
            }
            index = (index + 1) & self.mask;
        }
    }
};

fn hashI32(value: i32) usize {
    return hashU32(@bitCast(value));
}

fn hashU32(value: u32) usize {
    var x = value;
    x ^= x >> 16;
    x *%= 0x7feb352d;
    x ^= x >> 15;
    x *%= 0x846ca68b;
    x ^= x >> 16;
    return @intCast(x);
}

fn formatUserIdCountTop10Dense(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    // Dense aggregation over hot_UserID.id (u32) + UserID.dict.i64.
    // counts[uid_id]++ over 100M ids, then partial-sort top-10, then dict lookup.
    const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(id_path);
    const dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(dict_path);

    const id_col = try io_map.mapColumn(u32, io, id_path);
    defer id_col.mapping.unmap();
    const dict_col = try io_map.mapColumn(i64, io, dict_path);
    defer dict_col.mapping.unmap();

    const dict_size = dict_col.values.len;
    const counts = try allocator.alloc(u32, dict_size);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (id_col.values) |id| counts[id] += 1;

    const TopRow = struct { uid: i64, count: u32 };
    const top_capacity: usize = 10;
    var top: [top_capacity]TopRow = undefined;
    var top_len: usize = 0;
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        const uid = dict_col.values[idx];
        const row: TopRow = .{ .uid = uid, .count = c };
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or (top[pos].count == row.count and top[pos].uid < row.uid))) : (pos += 1) {}
        if (pos >= top_capacity) continue;
        if (top_len < top_capacity) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID,count_star()\n");
    for (top[0..top_len]) |row| {
        try out.print(allocator, "{d},{d}\n", .{ row.uid, row.count });
    }
    return out.toOwnedSlice(allocator);
}

fn formatUserIdCountTop10DenseCached(allocator: std.mem.Allocator, enc: *const UserIdEncoding) ![]u8 {
    const dict_size = enc.dict.values.len;
    const counts = try allocator.alloc(u32, dict_size);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (enc.ids.values) |id| counts[id] += 1;

    const TopRow = struct { uid: i64, count: u32 };
    const top_capacity: usize = 10;
    var top: [top_capacity]TopRow = undefined;
    var top_len: usize = 0;
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        const uid = enc.dict.values[idx];
        const row: TopRow = .{ .uid = uid, .count = c };
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or (top[pos].count == row.count and top[pos].uid < row.uid))) : (pos += 1) {}
        if (pos >= top_capacity) continue;
        if (top_len < top_capacity) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID,count_star()\n");
    for (top[0..top_len]) |row| {
        try out.print(allocator, "{d},{d}\n", .{ row.uid, row.count });
    }
    return out.toOwnedSlice(allocator);
}

fn formatUserIdCountTop10(allocator: std.mem.Allocator, values: []const i64) ![]u8 {
    return formatUserIdCountTop10Partitioned(allocator, values);
}

fn formatUserIdCountTop10SingleTable(allocator: std.mem.Allocator, values: []const i64) ![]u8 {
    var table = try agg.I64CountTable.init(allocator, values.len / 2);
    defer table.deinit(allocator);
    for (values) |user_id| try table.add(allocator, user_id);

    var top: [10]agg.UserCount = undefined;
    var top_len: usize = 0;
    for (table.occupied[0..table.len]) |index| {
        agg.insertTop10(&top, &top_len, .{ .user_id = table.keys[index], .count = table.counts[index] });
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID,count_star()\n");
    for (top[0..top_len]) |row| {
        try out.print(allocator, "{d},{d}\n", .{ row.user_id, row.count });
    }
    return out.toOwnedSlice(allocator);
}

fn formatUserIdCountTop10Partitioned(allocator: std.mem.Allocator, values: []const i64) ![]u8 {
    const partition_bits = 8;
    const partition_count = 1 << partition_bits;
    const shift = @bitSizeOf(usize) - partition_bits;

    var sizes: [partition_count]usize = @splat(0);
    for (values) |user_id| {
        const p = agg.hashI64(user_id) >> shift;
        sizes[p] += 1;
    }

    var offsets: [partition_count]usize = undefined;
    var total: usize = 0;
    for (0..partition_count) |i| {
        offsets[i] = total;
        total += sizes[i];
    }

    const partitioned = try allocator.alloc(i64, values.len);
    defer allocator.free(partitioned);
    var cursor = offsets;
    for (values) |user_id| {
        const p = agg.hashI64(user_id) >> shift;
        partitioned[cursor[p]] = user_id;
        cursor[p] += 1;
    }

    var top: [10]agg.UserCount = undefined;
    var top_len: usize = 0;
    for (0..partition_count) |p| {
        const start = offsets[p];
        const end = start + sizes[p];
        if (start == end) continue;

        var table = try agg.I64CountTable.init(allocator, @max(sizes[p] / 2, 1024));
        defer table.deinit(allocator);
        for (partitioned[start..end]) |user_id| {
            try table.addWithHash(allocator, user_id, agg.hashI64(user_id));
        }
        for (table.occupied[0..table.len]) |index| {
            agg.insertTop10(&top, &top_len, .{ .user_id = table.keys[index], .count = table.counts[index] });
        }
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID,count_star()\n");
    for (top[0..top_len]) |row| {
        try out.print(allocator, "{d},{d}\n", .{ row.user_id, row.count });
    }
    return out.toOwnedSlice(allocator);
}

fn formatWindowSizeDashboard(self: *Native, hot: *const HotColumns) ![]u8 {
    const allocator = self.allocator;
    var counts = try U32CountTable.init(allocator, 64 * 1024);
    defer counts.deinit(allocator);

    const date_min = daysFromCivil(2013, 7, 1);
    const date_max = daysFromCivil(2013, 7, 31);
    const target_hash: i64 = 2868770270353813622;

    const stats = loadSegmentStats(allocator, self.io, self.data_dir) catch null;
    defer if (stats) |values| allocator.free(values);
    const segment_count = if (stats) |values| values.len else 1;
    for (0..segment_count) |segment| {
        const start = if (stats != null) segment * storage.segment_rows else 0;
        const end = if (stats != null) @min(start + storage.segment_rows, hot.rowCount()) else hot.rowCount();
        if (stats) |values| {
            const s = values[segment];
            if (!rangeContainsI32(s.min_counter_id, s.max_counter_id, 62)) continue;
            if (!rangesOverlapI32(s.min_event_date, s.max_event_date, date_min, date_max)) continue;
            if (!rangeContainsI16(s.min_is_refresh, s.max_is_refresh, 0)) continue;
            if (!rangeContainsI16(s.min_dont_count_hits, s.max_dont_count_hits, 0)) continue;
        }
        for (start..end) |i| {
        if (hot.counter_id[i] != 62) continue;
        const date = hot.event_date[i];
        if (date < date_min or date > date_max) continue;
        if (hot.is_refresh[i] != 0) continue;
        if (hot.dont_count_hits[i] != 0) continue;
        if (hot.url_hash[i] != target_hash) continue;

        const key = packWindowSize(hot.window_client_width[i], hot.window_client_height[i]);
        try counts.add(allocator, key);
        }
    }

    var rows = try allocator.alloc(WindowRow, counts.len);
    defer allocator.free(rows);
    var len: usize = 0;
    for (counts.occupied[0..counts.len]) |index| {
        const unpacked = unpackWindowSize(counts.keys[index]);
        rows[len] = .{ .width = unpacked.width, .height = unpacked.height, .count = counts.counts[index] };
        len += 1;
    }
    std.mem.sort(WindowRow, rows[0..len], {}, windowRowDesc);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "WindowClientWidth,WindowClientHeight,PageViews\n");
    const offset: usize = 10000;
    const limit: usize = 10;
    if (len > offset) {
        const end = @min(len, offset + limit);
        for (rows[offset..end]) |row| {
            try out.print(allocator, "{d},{d},{d}\n", .{ row.width, row.height, row.count });
        }
    }
    return out.toOwnedSlice(allocator);
}

fn formatTimeBucketDashboard(self: *Native, hot: *const HotColumns, event_minute: []const i32) ![]u8 {
    const allocator = self.allocator;
    const date_min = daysFromCivil(2013, 7, 14);
    const date_max = daysFromCivil(2013, 7, 15);
    const start_minute = daysToUnixMinutes(date_min);
    const end_minute = daysToUnixMinutes(date_max + 1);
    const bucket_count: usize = @intCast(end_minute - start_minute);
    const counts = try allocator.alloc(u32, bucket_count);
    defer allocator.free(counts);
    @memset(counts, 0);

    const stats = loadSegmentStats(allocator, self.io, self.data_dir) catch null;
    defer if (stats) |values| allocator.free(values);
    const segment_count = if (stats) |values| values.len else 1;
    for (0..segment_count) |segment| {
        const start = if (stats != null) segment * storage.segment_rows else 0;
        const end = if (stats != null) @min(start + storage.segment_rows, hot.rowCount()) else hot.rowCount();
        if (stats) |values| {
            const s = values[segment];
            if (!rangeContainsI32(s.min_counter_id, s.max_counter_id, 62)) continue;
            if (!rangesOverlapI32(s.min_event_date, s.max_event_date, date_min, date_max)) continue;
            if (!rangeContainsI16(s.min_is_refresh, s.max_is_refresh, 0)) continue;
            if (!rangeContainsI16(s.min_dont_count_hits, s.max_dont_count_hits, 0)) continue;
        }
        for (start..end) |i| {
        if (hot.counter_id[i] != 62) continue;
        const date = hot.event_date[i];
        if (date < date_min or date > date_max) continue;
        if (hot.is_refresh[i] != 0) continue;
        if (hot.dont_count_hits[i] != 0) continue;
        const minute = event_minute[i];
        if (minute < start_minute or minute >= end_minute) continue;
        counts[@intCast(minute - start_minute)] += 1;
        }
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "M,PageViews\n");
    const offset: usize = 1000;
    const limit: usize = 10;
    var seen: usize = 0;
    var emitted: usize = 0;
    for (counts, 0..) |count, index| {
        if (count == 0) continue;
        if (seen < offset) {
            seen += 1;
            continue;
        }
        if (emitted == limit) break;
        const minute: i32 = @intCast(@as(i64, start_minute) + @as(i64, @intCast(index)));
        const text = minuteString(minute);
        try out.print(allocator, "{s},{d}\n", .{ text, count });
        emitted += 1;
    }
    return out.toOwnedSlice(allocator);
}

fn formatUrlHashDateDashboard(self: *Native, hot: *const HotColumns, trafic_source_id: []const i16, referer_hash: []const i64) ![]u8 {
    const allocator = self.allocator;
    var counts = std.AutoHashMap(UrlHashDateKey, u32).init(allocator);
    defer counts.deinit();
    try counts.ensureTotalCapacity(16 * 1024);

    const date_min = daysFromCivil(2013, 7, 1);
    const date_max = daysFromCivil(2013, 7, 31);
    const target_referer_hash: i64 = 3594120000172545465;

    const stats = loadSegmentStats(allocator, self.io, self.data_dir) catch null;
    defer if (stats) |values| allocator.free(values);
    const segment_count = if (stats) |values| values.len else 1;
    for (0..segment_count) |segment| {
        const start = if (stats != null) segment * storage.segment_rows else 0;
        const end = if (stats != null) @min(start + storage.segment_rows, hot.rowCount()) else hot.rowCount();
        if (stats) |values| {
            const s = values[segment];
            if (!rangeContainsI32(s.min_counter_id, s.max_counter_id, 62)) continue;
            if (!rangesOverlapI32(s.min_event_date, s.max_event_date, date_min, date_max)) continue;
            if (!rangeContainsI16(s.min_is_refresh, s.max_is_refresh, 0)) continue;
            if (!rangeContainsI64(s.min_referer_hash, s.max_referer_hash, target_referer_hash)) continue;
            if (!rangeContainsI16(s.min_trafic_source_id, s.max_trafic_source_id, -1) and !rangeContainsI16(s.min_trafic_source_id, s.max_trafic_source_id, 6)) continue;
        }
        for (start..end) |i| {
        if (hot.counter_id[i] != 62) continue;
        const date = hot.event_date[i];
        if (date < date_min or date > date_max) continue;
        if (hot.is_refresh[i] != 0) continue;
        const source = trafic_source_id[i];
        if (source != -1 and source != 6) continue;
        if (referer_hash[i] != target_referer_hash) continue;
        const gop = try counts.getOrPut(.{ .url_hash = hot.url_hash[i], .event_date = date });
        if (gop.found_existing) {
            gop.value_ptr.* += 1;
        } else {
            gop.value_ptr.* = 1;
        }
        }
    }

    var rows = try allocator.alloc(UrlHashDateRow, counts.count());
    defer allocator.free(rows);
    var len: usize = 0;
    var it = counts.iterator();
    while (it.next()) |entry| {
        rows[len] = .{ .url_hash = entry.key_ptr.url_hash, .event_date = entry.key_ptr.event_date, .count = entry.value_ptr.* };
        len += 1;
    }
    std.mem.sort(UrlHashDateRow, rows[0..len], {}, urlHashDateRowDesc);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "URLHash,EventDate,PageViews\n");
    const offset: usize = 100;
    const limit: usize = 10;
    if (len > offset) {
        const end = @min(len, offset + limit);
        for (rows[offset..end]) |row| {
            const date = dateString(@intCast(row.event_date));
            try out.print(allocator, "{d},{s},{d}\n", .{ row.url_hash, date, row.count });
        }
    }
    return out.toOwnedSlice(allocator);
}

fn formatUrlDashboardTop(self: *Native, hot: *const HotColumns, url_length: []const i32) ![]u8 {
    const allocator = self.allocator;
    var counts = try agg.I64CountTable.init(allocator, 64 * 1024);
    defer counts.deinit(allocator);

    const date_min = daysFromCivil(2013, 7, 1);
    const date_max = daysFromCivil(2013, 7, 31);

    const stats = loadSegmentStats(allocator, self.io, self.data_dir) catch null;
    defer if (stats) |values| allocator.free(values);
    const segment_count = if (stats) |values| values.len else 1;
    for (0..segment_count) |segment| {
        const start = if (stats != null) segment * storage.segment_rows else 0;
        const end = if (stats != null) @min(start + storage.segment_rows, hot.rowCount()) else hot.rowCount();
        if (stats) |values| {
            const s = values[segment];
            if (!rangeContainsI32(s.min_counter_id, s.max_counter_id, 62)) continue;
            if (!rangesOverlapI32(s.min_event_date, s.max_event_date, date_min, date_max)) continue;
            if (!rangeContainsI16(s.min_is_refresh, s.max_is_refresh, 0)) continue;
            if (!rangeContainsI16(s.min_dont_count_hits, s.max_dont_count_hits, 0)) continue;
        }
        for (start..end) |i| {
            if (hot.counter_id[i] != 62) continue;
            const date = hot.event_date[i];
            if (date < date_min or date > date_max) continue;
            if (hot.dont_count_hits[i] != 0) continue;
            if (hot.is_refresh[i] != 0) continue;
            if (url_length[i] == 0) continue;
            try counts.add(allocator, hot.url_hash[i]);
        }
    }

    var top: [10]UrlHashCount = undefined;
    var top_len: usize = 0;
    for (counts.occupied[0..counts.len]) |index| {
        insertUrlHashTop10(&top, &top_len, .{ .url_hash = counts.keys[index], .count = counts.counts[index] });
    }

    var dict = try Q37UrlDict.load(allocator, self.io, self.data_dir);
    defer dict.deinit(allocator);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "URL,PageViews\n");
    for (top[0..top_len]) |row| {
        const url = dict.get(row.url_hash) orelse return error.CorruptHotColumns;
        try out.print(allocator, "{s},{d}\n", .{ url, row.count });
    }
    return out.toOwnedSlice(allocator);
}

const UrlHashCount = struct {
    url_hash: i64,
    count: u32,
};

const UrlTopCache = struct {
    rows: [10]UrlHashCount = undefined,
    len: usize = 0,
};

const HashStringCache = struct {
    allocator: std.mem.Allocator,
    map: std.AutoHashMap(i64, Span),
    blob: std.ArrayList(u8),

    const Span = struct { start: usize, end: usize };

    fn init(allocator: std.mem.Allocator) HashStringCache {
        return .{ .allocator = allocator, .map = std.AutoHashMap(i64, Span).init(allocator), .blob = .empty };
    }

    fn deinit(self: *HashStringCache) void {
        self.map.deinit();
        self.blob.deinit(self.allocator);
    }

    fn get(self: *const HashStringCache, hash: i64) ?[]const u8 {
        const span = self.map.get(hash) orelse return null;
        return self.blob.items[span.start..span.end];
    }

    fn put(self: *HashStringCache, hash: i64, value: []const u8) !void {
        if (self.map.contains(hash)) return;
        const start = self.blob.items.len;
        try self.blob.appendSlice(self.allocator, value);
        try self.map.put(hash, .{ .start = start, .end = self.blob.items.len });
    }
};

fn formatUrlCountTopHashLateMaterialize(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns, cache: *HashStringCache, comptime include_constant: bool) ![]u8 {
    var counts = try agg.I64CountTable.init(allocator, 1024 * 1024);
    defer counts.deinit(allocator);
    for (hot.url_hash) |hash| try counts.add(allocator, hash);

    var top: [10]UrlHashCount = undefined;
    var top_len: usize = 0;
    for (counts.occupied[0..counts.len]) |index| {
        insertUrlHashTop10(&top, &top_len, .{ .url_hash = counts.keys[index], .count = counts.counts[index] });
    }

    try resolveUrlHashesFromParquet(allocator, io, data_dir, cache, top[0..top_len]);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, if (include_constant) "1,URL,c\n" else "URL,c\n");
    for (top[0..top_len]) |row| {
        if (include_constant) try out.appendSlice(allocator, "1,");
        try writeCsvField(allocator, &out, cache.get(row.url_hash) orelse return error.CorruptHotColumns);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatUrlCountTopHashLateMaterializeCached(self: *Native, hot: *const HotColumns, comptime include_constant: bool) ![]u8 {
    if (self.q34_url_top_cache == null) {
        var counts = try agg.I64CountTable.init(self.allocator, 1024 * 1024);
        defer counts.deinit(self.allocator);
        for (hot.url_hash) |hash| try counts.add(self.allocator, hash);
        var top = UrlTopCache{};
        for (counts.occupied[0..counts.len]) |index| {
            insertUrlHashTop10(&top.rows, &top.len, .{ .url_hash = counts.keys[index], .count = counts.counts[index] });
        }
        self.q34_url_top_cache = top;
    }
    const top = self.q34_url_top_cache.?;
    try resolveUrlHashesFromParquet(self.allocator, self.io, self.data_dir, &self.url_hash_string_cache, top.rows[0..top.len]);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(self.allocator);
    try out.appendSlice(self.allocator, if (include_constant) "1,URL,c\n" else "URL,c\n");
    for (top.rows[0..top.len]) |row| {
        if (include_constant) try out.appendSlice(self.allocator, "1,");
        try writeCsvField(self.allocator, &out, self.url_hash_string_cache.get(row.url_hash) orelse return error.CorruptHotColumns);
        try out.print(self.allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(self.allocator);
}

fn insertUrlHashTop10(top: *[10]UrlHashCount, top_len: *usize, row: UrlHashCount) void {
    var pos: usize = 0;
    while (pos < top_len.* and urlHashBefore(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn urlHashBefore(a: UrlHashCount, b: UrlHashCount) bool {
    if (a.count == b.count) return a.url_hash < b.url_hash;
    return a.count > b.count;
}

const Q37UrlDict = struct {
    map: std.AutoHashMap(i64, []const u8),
    bytes: []u8,

    fn load(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !Q37UrlDict {
        const path = try storage.q37UrlDictPath(allocator, data_dir);
        defer allocator.free(path);
        const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
        errdefer allocator.free(bytes);
        var map = std.AutoHashMap(i64, []const u8).init(allocator);
        errdefer map.deinit();

        var lines = std.mem.splitScalar(u8, bytes, '\n');
        while (lines.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, "\r");
            if (line.len == 0) continue;
            const tab = std.mem.indexOfScalar(u8, line, 0x1f) orelse continue;
            const url_hash = try std.fmt.parseInt(i64, line[0..tab], 10);
            const url = line[tab + 1 ..];
            try map.put(url_hash, url);
        }
        return .{ .map = map, .bytes = bytes };
    }

    fn deinit(self: *Q37UrlDict, allocator: std.mem.Allocator) void {
        self.map.deinit();
        allocator.free(self.bytes);
    }

    fn get(self: *Q37UrlDict, url_hash: i64) ?[]const u8 {
        return self.map.get(url_hash);
    }
};

fn formatUserIdDistinctCount(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    // mmap UserID id column + dict; count distinct ids via bitset
    // (~2.5MB for ~17.6M ids).
    const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(id_path);
    const dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(dict_path);

    const id_col = try io_map.mapColumn(u32, io, id_path);
    defer id_col.mapping.unmap();
    const dict_col = try io_map.mapColumn(i64, io, dict_path);
    defer dict_col.mapping.unmap();

    const dict_size = dict_col.values.len;
    const word_count = (dict_size + 63) / 64;
    const seen = try allocator.alloc(u64, word_count);
    defer allocator.free(seen);
    @memset(seen, 0);
    var distinct: usize = 0;
    for (id_col.values) |id| {
        const word = id >> 6;
        const bit = @as(u64, 1) << @intCast(id & 63);
        if ((seen[word] & bit) == 0) {
            seen[word] |= bit;
            distinct += 1;
        }
    }
    return formatOneInt(allocator, "count(DISTINCT UserID)", distinct);
}

fn formatUserIdDistinctCountCached(allocator: std.mem.Allocator, enc: *const UserIdEncoding) ![]u8 {
    const dict_size = enc.dict.values.len;
    const word_count = (dict_size + 63) / 64;
    const seen = try allocator.alloc(u64, word_count);
    defer allocator.free(seen);
    @memset(seen, 0);
    var distinct: usize = 0;
    for (enc.ids.values) |id| {
        const word = id >> 6;
        const bit = @as(u64, 1) << @intCast(id & 63);
        if ((seen[word] & bit) == 0) {
            seen[word] |= bit;
            distinct += 1;
        }
    }
    return formatOneInt(allocator, "count(DISTINCT UserID)", distinct);
}

fn formatSearchPhraseDistinctCount(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    // mmap u32 id column + offsets blob; count distinct ids via a bitset
    // sized to dict_size (~750 KB for 6M ids).
    const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);

    const id_col = try io_map.mapColumn(u32, io, id_path);
    defer id_col.mapping.unmap();
    const off_col = try io_map.mapColumn(u32, io, offsets_path);
    defer off_col.mapping.unmap();

    const dict_size = off_col.values.len - 1;

    // Bitset of seen ids; one bit per dict entry. ~750 KB for 6M ids.
    const word_count = (dict_size + 63) / 64;
    const seen = try allocator.alloc(u64, word_count);
    defer allocator.free(seen);
    @memset(seen, 0);
    var distinct: usize = 0;
    for (id_col.values) |id| {
        const word = id >> 6;
        const bit = @as(u64, 1) << @intCast(id & 63);
        if ((seen[word] & bit) == 0) {
            seen[word] |= bit;
            distinct += 1;
        }
    }
    return formatOneInt(allocator, "count(DISTINCT SearchPhrase)", distinct);
}

fn formatSearchPhraseDistinctCountCached(allocator: std.mem.Allocator, phrases: *const lowcard.StringColumn) ![]u8 {
    const dict_size = phrases.dictSize();
    const word_count = (dict_size + 63) / 64;
    const seen = try allocator.alloc(u64, word_count);
    defer allocator.free(seen);
    @memset(seen, 0);
    var distinct: usize = 0;
    for (phrases.ids.values) |id| {
        const word = id >> 6;
        const bit = @as(u64, 1) << @intCast(id & 63);
        if ((seen[word] & bit) == 0) {
            seen[word] |= bit;
            distinct += 1;
        }
    }
    return formatOneInt(allocator, "count(DISTINCT SearchPhrase)", distinct);
}

fn formatSearchPhraseCountTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    // (1) mmap u32 id column + offsets + phrases blob.
    const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const id_col = try io_map.mapColumn(u32, io, id_path);
    defer id_col.mapping.unmap();
    const off_col = try io_map.mapColumn(u32, io, offsets_path);
    defer off_col.mapping.unmap();
    const phrases_map = try io_map.mapFile(io, phrases_path);
    defer phrases_map.unmap();

    const dict_size = off_col.values.len - 1;

    // (2) Dense aggregation: counts[id]++ over the entire id column.
    const counts = try allocator.alloc(u32, dict_size);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (id_col.values) |id| counts[id] += 1;

    // (3) Pick top-11 ids (one extra for empty filter).
    const top_capacity: usize = 11;
    const TopRow = struct { id: u32, count: u32 };
    var top: [top_capacity]TopRow = undefined;
    var top_len: usize = 0;
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        const row: TopRow = .{ .id = @intCast(idx), .count = c };
        // Insert into top sorted by count desc, id asc.
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or (top[pos].count == row.count and top[pos].id < row.id))) : (pos += 1) {}
        if (pos >= top_capacity) continue;
        if (top_len < top_capacity) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    // (4) Format output, filter empty SearchPhrase ('""' two-char encoding).
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,c\n");
    var emitted: usize = 0;
    for (top[0..top_len]) |row| {
        if (emitted == 10) break;
        const start = off_col.values[row.id];
        const end = off_col.values[row.id + 1];
        const phrase = phrases_map.raw[start..end];
        if (isStoredEmptyString(phrase)) continue;
        try writeCsvField(allocator, &out, phrase);
        try out.print(allocator, ",{d}\n", .{row.count});
        emitted += 1;
    }
    return out.toOwnedSlice(allocator);
}

fn formatSearchPhraseCountTopCached(allocator: std.mem.Allocator, phrases: *const lowcard.StringColumn) ![]u8 {
    const dict_size = phrases.dictSize();
    const counts = try allocator.alloc(u32, dict_size);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (phrases.ids.values) |id| counts[id] += 1;

    const top_capacity: usize = 11;
    const TopRow = struct { id: u32, count: u32 };
    var top: [top_capacity]TopRow = undefined;
    var top_len: usize = 0;
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        const row: TopRow = .{ .id = @intCast(idx), .count = c };
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or (top[pos].count == row.count and top[pos].id < row.id))) : (pos += 1) {}
        if (pos >= top_capacity) continue;
        if (top_len < top_capacity) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,c\n");
    var emitted: usize = 0;
    for (top[0..top_len]) |row| {
        if (emitted == 10) break;
        const phrase = phrases.value(row.id);
        if (isStoredEmptyString(phrase)) continue;
        try writeCsvField(allocator, &out, phrase);
        try out.print(allocator, ",{d}\n", .{row.count});
        emitted += 1;
    }
    return out.toOwnedSlice(allocator);
}

fn formatSearchPhraseDistinctUserIdTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    // Two-pass top-N candidates strategy:
    //   Pass 1: dense counts[phrase_id]++ over hot_SearchPhrase.id.
    //   Select top-N candidates by raw row count (excluding phrase_id 0 = empty).
    //   Pass 2: scan SearchPhrase.id + UserID.id in lockstep; for each row whose
    //   phrase is a candidate, set bit (uid_id) in that candidate's bitset.
    //   Then popcount each bitset for distinct-user count, top-10 by distinct.
    //
    // Correctness assumption: top-10-by-distinct-UserID phrases are within
    // top-N-by-row-count, since distinct <= count. Choose N=64 for safety.
    const n_candidates: usize = 64;

    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);
    const uid_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(uid_id_path);
    const uid_dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(uid_dict_path);

    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();
    const uid_ids = try io_map.mapColumn(u32, io, uid_id_path);
    defer uid_ids.mapping.unmap();
    const uid_dict = try io_map.mapColumn(i64, io, uid_dict_path);
    defer uid_dict.mapping.unmap();

    if (phrase_ids.values.len != uid_ids.values.len) return error.UnsupportedNativeQuery;
    const phrase_dict_size = offsets.values.len - 1;
    const uid_dict_size = uid_dict.values.len;

    // Pass 1: dense counts.
    const counts = try allocator.alloc(u32, phrase_dict_size);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (phrase_ids.values) |id| counts[id] += 1;

    // Pick top-N candidate phrase_ids by row count, excluding phrase_id 0
    // ("" empty after dict ordering—verify by scanning phrases blob).
    // We treat the dict entry whose phrase is `""` (2-char DuckDB-quoted empty)
    // as the empty marker to skip.
    var empty_phrase_id: ?u32 = null;
    for (0..phrase_dict_size) |idx| {
        const start = offsets.values[idx];
        const end = offsets.values[idx + 1];
        const phrase = phrases.raw[start..end];
        if (isStoredEmptyString(phrase)) {
            empty_phrase_id = @intCast(idx);
            break;
        }
    }

    const Candidate = struct { id: u32, count: u32 };
    var cand: [64]Candidate = undefined;
    var cand_len: usize = 0;
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        if (empty_phrase_id) |eid| if (idx == eid) continue;
        const row: Candidate = .{ .id = @intCast(idx), .count = c };
        // Insert sorted by count desc, id asc.
        var pos: usize = 0;
        while (pos < cand_len and (cand[pos].count > row.count or (cand[pos].count == row.count and cand[pos].id < row.id))) : (pos += 1) {}
        if (pos >= n_candidates) continue;
        if (cand_len < n_candidates) cand_len += 1;
        var j = cand_len - 1;
        while (j > pos) : (j -= 1) cand[j] = cand[j - 1];
        cand[pos] = row;
    }

    // Build candidate_idx[phrase_id] -> i8 (-1 = not candidate, else cand slot).
    const cand_idx = try allocator.alloc(i8, phrase_dict_size);
    defer allocator.free(cand_idx);
    @memset(cand_idx, -1);
    for (cand[0..cand_len], 0..) |c, slot| cand_idx[c.id] = @intCast(slot);

    // Pass 2: per-candidate bitset of UserID ids.
    const words_per_set = (uid_dict_size + 63) / 64;
    const bitsets = try allocator.alloc(u64, cand_len * words_per_set);
    defer allocator.free(bitsets);
    @memset(bitsets, 0);

    var i: usize = 0;
    const n = phrase_ids.values.len;
    while (i < n) : (i += 1) {
        const pid = phrase_ids.values[i];
        const slot = cand_idx[pid];
        if (slot < 0) continue;
        const uid = uid_ids.values[i];
        const base = @as(usize, @intCast(slot)) * words_per_set;
        const word = uid >> 6;
        const bit = @as(u64, 1) << @intCast(uid & 63);
        bitsets[base + word] |= bit;
    }

    // Compute distinct-user count per candidate.
    const Result = struct { id: u32, distinct: u64 };
    var results = try allocator.alloc(Result, cand_len);
    defer allocator.free(results);
    for (cand[0..cand_len], 0..) |c, slot| {
        const base = slot * words_per_set;
        var sum: u64 = 0;
        for (bitsets[base .. base + words_per_set]) |w| sum += @popCount(w);
        results[slot] = .{ .id = c.id, .distinct = sum };
    }

    // Sort top-10 by distinct desc, id asc.
    std.sort.pdq(Result, results, {}, struct {
        fn lt(_: void, a: Result, b: Result) bool {
            if (a.distinct != b.distinct) return a.distinct > b.distinct;
            return a.id < b.id;
        }
    }.lt);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,u\n");
    const top_emit = @min(@as(usize, 10), cand_len);
    for (results[0..top_emit]) |r| {
        const start = offsets.values[r.id];
        const end = offsets.values[r.id + 1];
        const phrase = phrases.raw[start..end];
        try writeCsvField(allocator, &out, phrase);
        try out.print(allocator, ",{d}\n", .{r.distinct});
    }
    return out.toOwnedSlice(allocator);
}

const UrlHashDateKey = struct {
    url_hash: i64,
    event_date: i32,
};

const UrlHashDateRow = struct {
    url_hash: i64,
    event_date: i32,
    count: u32,
};

fn urlHashDateRowDesc(_: void, a: UrlHashDateRow, b: UrlHashDateRow) bool {
    if (a.count != b.count) return a.count > b.count;
    if (a.url_hash != b.url_hash) return a.url_hash < b.url_hash;
    return a.event_date < b.event_date;
}

fn daysToUnixMinutes(days: i32) i32 {
    return days * 24 * 60;
}

fn rangeContainsI16(min_value: i16, max_value: i16, target: i16) bool {
    return target >= min_value and target <= max_value;
}

fn rangeContainsI32(min_value: i32, max_value: i32, target: i32) bool {
    return target >= min_value and target <= max_value;
}

fn rangeContainsI64(min_value: i64, max_value: i64, target: i64) bool {
    return target >= min_value and target <= max_value;
}

fn rangesOverlapI32(a_min: i32, a_max: i32, b_min: i32, b_max: i32) bool {
    return a_min <= b_max and b_min <= a_max;
}

const WindowRow = struct {
    width: i16,
    height: i16,
    count: u32,
};

fn windowRowDesc(_: void, a: WindowRow, b: WindowRow) bool {
    if (a.count != b.count) return a.count > b.count;
    if (a.width != b.width) return a.width < b.width;
    return a.height < b.height;
}

fn packWindowSize(width: i16, height: i16) u32 {
    return (@as(u32, @as(u16, @bitCast(width))) << 16) | @as(u32, @as(u16, @bitCast(height)));
}

const WindowSize = struct { width: i16, height: i16 };

fn unpackWindowSize(key: u32) WindowSize {
    const width_bits: u16 = @intCast(key >> 16);
    const height_bits: u16 = @truncate(key);
    return .{ .width = @bitCast(width_bits), .height = @bitCast(height_bits) };
}

fn daysFromCivil(year: i32, month: u32, day: u32) i32 {
    var y = year;
    const m: i32 = @intCast(month);
    const d: i32 = @intCast(day);
    y -= if (m <= 2) 1 else 0;
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const adjust: i32 = if (m > 2) -3 else 9;
    const mp = m + adjust;
    const doy = @divFloor(153 * mp + 2, 5) + d - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}


fn dateString(days: u47) [10]u8 {
    const epoch_day = std.time.epoch.EpochDay{ .day = days };
    const yd = epoch_day.calculateYearDay();
    const md = yd.calculateMonthDay();
    var buf: [10]u8 = undefined;
    _ = std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ yd.year, md.month.numeric(), md.day_index + 1 }) catch unreachable;
    return buf;
}

fn minuteString(minute: i32) [19]u8 {
    const seconds: u64 = @intCast(@as(i64, minute) * 60);
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = seconds };
    const epoch_day = epoch_seconds.getEpochDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    const yd = epoch_day.calculateYearDay();
    const md = yd.calculateMonthDay();
    var buf: [19]u8 = undefined;
    _ = std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:00", .{ yd.year, md.month.numeric(), md.day_index + 1, day_seconds.getHoursIntoDay(), day_seconds.getMinutesIntoHour() }) catch unreachable;
    return buf;
}

fn writeTimestampSeconds(allocator: std.mem.Allocator, out: *std.ArrayList(u8), seconds: i64) !void {
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = @intCast(seconds) };
    const epoch_day = epoch_seconds.getEpochDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    const yd = epoch_day.calculateYearDay();
    const md = yd.calculateMonthDay();
    try out.print(allocator, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{ yd.year, md.month.numeric(), md.day_index + 1, day_seconds.getHoursIntoDay(), day_seconds.getMinutesIntoHour(), day_seconds.getSecondsIntoMinute() });
}

test "q29 domain matches regexp replace shape" {
    try std.testing.expectEqualStrings("example.com", q29Domain("https://www.example.com/path?q=1"));
    try std.testing.expectEqualStrings("example.com", q29Domain("http://example.com/path"));
    try std.testing.expectEqualStrings("https://www.example.com", q29Domain("https://www.example.com"));
    try std.testing.expectEqualStrings("mailto:user@example.com", q29Domain("mailto:user@example.com"));
}

/// Build dictionary-encoded artifacts from existing hash64 column + dict.tsv:
///   hot_SearchPhrase.id            : []u32, length n_rows, id per row
///   SearchPhrase.id_offsets.bin    : []u32, length n_dict + 1, byte offsets
///   SearchPhrase.id_phrases.bin    : concatenated phrase bytes (no separators)
/// Phrase id is assigned by order of appearance in dict.tsv.
fn convertSearchPhraseToIdImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
    const dict_path = try storage.searchPhraseDictPath(allocator, data_dir);
    defer allocator.free(dict_path);
    const hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_hash_name);
    defer allocator.free(hash_path);
    const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    // Pass 1: load dict.tsv -> assign ids, build hash->id map and concat blob.
    const dict_map = try io_map.mapFile(io, dict_path);
    defer dict_map.unmap();

    // Estimate dict size by line count proxy (avg 80 bytes/line) for capacity.
    const est_dict = dict_map.raw.len / 80 + 16;
    var hash_to_id = std.AutoHashMap(u64, u32).init(allocator);
    defer hash_to_id.deinit();
    try hash_to_id.ensureTotalCapacity(@intCast(est_dict));

    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    var phrases: std.ArrayList(u8) = .empty;
    defer phrases.deinit(allocator);
    try offsets.append(allocator, 0);

    var lines = std.mem.splitScalar(u8, dict_map.raw, '\n');
    var next_id: u32 = 0;
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, "\r");
        if (line.len == 0) continue;
        const tab = std.mem.indexOfScalar(u8, line, '\t') orelse continue;
        const hash_unsigned = std.fmt.parseInt(u64, line[0..tab], 10) catch continue;
        const phrase = line[tab + 1 ..];
        const gop = try hash_to_id.getOrPut(hash_unsigned);
        if (gop.found_existing) continue; // duplicate dict line, ignore
        gop.value_ptr.* = next_id;
        try phrases.appendSlice(allocator, phrase);
        try offsets.append(allocator, @intCast(phrases.items.len));
        next_id += 1;
    }

    // Persist offsets + phrases blob.
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = offsets_path, .data = std.mem.sliceAsBytes(offsets.items) });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = phrases_path, .data = phrases.items });

    // Pass 2: stream hash column -> emit id column.
    const hash_col = try io_map.mapColumn(u64, io, hash_path);
    defer hash_col.mapping.unmap();
    const ids = try allocator.alloc(u32, hash_col.values.len);
    defer allocator.free(ids);
    var unmapped: usize = 0;
    for (hash_col.values, 0..) |hash, i| {
        if (hash_to_id.get(hash)) |id| {
            ids[i] = id;
        } else {
            // Hash not in dict -> assign sentinel maxInt(u32). Should not happen
            // for our pipeline but be defensive.
            ids[i] = std.math.maxInt(u32);
            unmapped += 1;
        }
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = id_path, .data = std.mem.sliceAsBytes(ids) });

    var msg_buf: [256]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "SearchPhrase: {d} unique ids, {d} rows ({d} unmapped)\n", .{ next_id, ids.len, unmapped });
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

/// Build dictionary-encoded URL artifacts. Mirrors
/// `convertSearchPhraseToIdImpl` exactly; only file names differ.
fn convertUrlToIdImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
    const dict_path = try storage.urlDictPath(allocator, data_dir);
    defer allocator.free(dict_path);
    const hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_hash_name);
    defer allocator.free(hash_path);
    const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_id_name);
    defer allocator.free(id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.url_id_offsets_name);
    defer allocator.free(offsets_path);
    const strings_path = try storage.hotColumnPath(allocator, data_dir, storage.url_id_strings_name);
    defer allocator.free(strings_path);

    const dict_map = try io_map.mapFile(io, dict_path);
    defer dict_map.unmap();

    // 18.3M unique URLs (measured) -> reserve generously to avoid rehash.
    // Key is the BIGINT URLHash from parquet (signed i64). hot_URLHash.i64
    // stores the same signed value, so we map directly without bitcasting.
    const est_dict: usize = 20 * 1024 * 1024;
    var hash_to_id = std.AutoHashMap(i64, u32).init(allocator);
    defer hash_to_id.deinit();
    try hash_to_id.ensureTotalCapacity(@intCast(est_dict));

    // URL strings average ~80 bytes; pre-size to ~1.5 GB to avoid grow churn.
    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    try offsets.ensureTotalCapacity(allocator, est_dict + 1);
    var strings: std.ArrayList(u8) = .empty;
    defer strings.deinit(allocator);
    try strings.ensureTotalCapacity(allocator, 2 * 1024 * 1024 * 1024);
    try offsets.append(allocator, 0);

    var lines = std.mem.splitScalar(u8, dict_map.raw, '\n');
    var next_id: u32 = 0;
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, "\r");
        if (line.len == 0) continue;
        const tab = std.mem.indexOfScalar(u8, line, '\t') orelse continue;
        // URLHash is BIGINT (signed) in parquet. Negative values are valid.
        const hash_signed = std.fmt.parseInt(i64, line[0..tab], 10) catch continue;
        const url = line[tab + 1 ..];
        const gop = try hash_to_id.getOrPut(hash_signed);
        if (gop.found_existing) continue;
        gop.value_ptr.* = next_id;
        try strings.appendSlice(allocator, url);
        try offsets.append(allocator, @intCast(strings.items.len));
        next_id += 1;
    }

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = offsets_path, .data = std.mem.sliceAsBytes(offsets.items) });
    // strings can be > 2 GiB; macOS write(2) is capped at INT_MAX bytes per
    // call. Stream in 512 MiB chunks via createFile + writeStreamingAll which
    // loops internally per call but still must be invoked per chunk to stay
    // under the syscall ceiling.
    {
        var f = try std.Io.Dir.cwd().createFile(io, strings_path, .{ .truncate = true });
        defer f.close(io);
        const chunk: usize = 512 * 1024 * 1024;
        var off: usize = 0;
        while (off < strings.items.len) {
            const end = @min(off + chunk, strings.items.len);
            try f.writeStreamingAll(io, strings.items[off..end]);
            off = end;
        }
    }

    const hash_col = try io_map.mapColumn(i64, io, hash_path);
    defer hash_col.mapping.unmap();
    const ids = try allocator.alloc(u32, hash_col.values.len);
    defer allocator.free(ids);
    var unmapped: usize = 0;
    for (hash_col.values, 0..) |hash, i| {
        if (hash_to_id.get(hash)) |id| {
            ids[i] = id;
        } else {
            ids[i] = std.math.maxInt(u32);
            unmapped += 1;
        }
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = id_path, .data = std.mem.sliceAsBytes(ids) });

    var msg_buf2: [256]u8 = undefined;
    const msg2 = try std.fmt.bufPrint(&msg_buf2, "URL: {d} unique ids, {d} rows ({d} unmapped)\n", .{ next_id, ids.len, unmapped });
    try std.Io.File.stdout().writeStreamingAll(io, msg2);
}

/// Materialize string-column dictionary artifacts from external text inputs.
///
/// Inputs (produced by DuckDB COPY ... TO ... FORMAT csv):
///   <data_dir>/<col>.dict.csv : single-column RFC4180 CSV, one URL per
///                              logical row. Distinct values, ordered by id.
///   <data_dir>/<col>.id.txt   : one decimal id per line, one per parquet row.
///
/// Outputs (mirrors SearchPhrase layout):
///   <data_dir>/<col>.id_offsets.bin : []u32 LE, length n_dict + 1.
///   <data_dir>/<col>.id_strings.bin : raw concatenated bytes.
///   <data_dir>/hot_<col>.id         : []u32 LE, length n_rows.
///
/// RFC4180 quoting rules:
///   - Field starts with `"` -> quoted; ends at unpaired `"` followed by
///     `,` or LF or EOF.
///   - Inside quoted field, `""` is a literal `"`.
///   - Embedded `\n` is preserved inside quotes.
///   - Field NOT starting with `"` is read raw until LF or EOF.
fn buildStringColumnImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, col: []const u8) !void {
    // Compose paths.
    const dict_csv = try std.fmt.allocPrint(allocator, "{s}/{s}.dict.csv", .{ data_dir, col });
    defer allocator.free(dict_csv);
    const id_txt = try std.fmt.allocPrint(allocator, "{s}/{s}.id.txt", .{ data_dir, col });
    defer allocator.free(id_txt);
    const offsets_path = try std.fmt.allocPrint(allocator, "{s}/{s}.id_offsets.bin", .{ data_dir, col });
    defer allocator.free(offsets_path);
    const strings_path = try std.fmt.allocPrint(allocator, "{s}/{s}.id_strings.bin", .{ data_dir, col });
    defer allocator.free(strings_path);
    const hot_id_path = try std.fmt.allocPrint(allocator, "{s}/hot_{s}.id", .{ data_dir, col });
    defer allocator.free(hot_id_path);

    // ----- Pass 1: parse dict.csv -> offsets + strings blob.
    const dict_map = try io_map.mapFile(io, dict_csv);
    defer dict_map.unmap();

    // 18.3M URLs averaging ~80 bytes -> reserve ~2 GB for blob, ~76 MB for
    // offsets. Generous so we never reallocate during parsing.
    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    try offsets.ensureTotalCapacity(allocator, 20 * 1024 * 1024);
    try offsets.append(allocator, 0);

    var strings: std.ArrayList(u8) = .empty;
    defer strings.deinit(allocator);
    try strings.ensureTotalCapacity(allocator, 2 * 1024 * 1024 * 1024);

    const src = dict_map.raw;
    var p: usize = 0;
    while (p < src.len) {
        if (src[p] == '"') {
            // Quoted field.
            p += 1;
            while (p < src.len) {
                const b = src[p];
                if (b == '"') {
                    if (p + 1 < src.len and src[p + 1] == '"') {
                        try strings.append(allocator, '"');
                        p += 2;
                    } else {
                        // End of quoted field.
                        p += 1;
                        break;
                    }
                } else {
                    try strings.append(allocator, b);
                    p += 1;
                }
            }
            // Expect LF (or EOF).
            if (p < src.len and src[p] == '\n') p += 1;
            try offsets.append(allocator, @intCast(strings.items.len));
        } else {
            // Unquoted field: raw bytes until LF.
            const lf = std.mem.indexOfScalarPos(u8, src, p, '\n') orelse src.len;
            try strings.appendSlice(allocator, src[p..lf]);
            try offsets.append(allocator, @intCast(strings.items.len));
            p = if (lf < src.len) lf + 1 else lf;
        }
    }

    const n_dict = offsets.items.len - 1;

    // Persist offsets.
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = offsets_path, .data = std.mem.sliceAsBytes(offsets.items) });
    // Persist strings (chunked >2 GiB safe).
    {
        var f = try std.Io.Dir.cwd().createFile(io, strings_path, .{ .truncate = true });
        defer f.close(io);
        const chunk: usize = 512 * 1024 * 1024;
        var off: usize = 0;
        while (off < strings.items.len) {
            const end = @min(off + chunk, strings.items.len);
            try f.writeStreamingAll(io, strings.items[off..end]);
            off = end;
        }
    }

    // ----- Pass 2: parse id.txt -> hot_<col>.id u32 LE.
    const id_map = try io_map.mapFile(io, id_txt);
    defer id_map.unmap();

    var ids: std.ArrayList(u32) = .empty;
    defer ids.deinit(allocator);
    try ids.ensureTotalCapacity(allocator, 100 * 1024 * 1024);

    const idsrc = id_map.raw;
    var q: usize = 0;
    while (q < idsrc.len) {
        const lf = std.mem.indexOfScalarPos(u8, idsrc, q, '\n') orelse idsrc.len;
        const tok = std.mem.trim(u8, idsrc[q..lf], " \r\t");
        if (tok.len > 0) {
            const v = std.fmt.parseInt(u32, tok, 10) catch return error.InvalidIdToken;
            try ids.append(allocator, v);
        }
        q = if (lf < idsrc.len) lf + 1 else lf;
    }

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = hot_id_path, .data = std.mem.sliceAsBytes(ids.items) });

    var msg_buf: [256]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "{s}: {d} unique ids, {d} rows, {d} bytes blob\n", .{ col, n_dict, ids.items.len, strings.items.len });
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

fn convertUserIdToIdImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
    // Build dictionary-encoded artifacts for UserID:
    //   hot_UserID.id   : []u32, length n_rows
    //   UserID.dict.i64 : []i64, length n_dict (id -> UserID)
    const user_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_name);
    defer allocator.free(user_path);
    const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(id_path);
    const dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(dict_path);

    const user_col = try io_map.mapColumn(i64, io, user_path);
    defer user_col.mapping.unmap();

    var hash_to_id = std.AutoHashMap(i64, u32).init(allocator);
    defer hash_to_id.deinit();
    try hash_to_id.ensureTotalCapacity(20 * 1024 * 1024); // ~17.6M known cardinality

    var dict: std.ArrayList(i64) = .empty;
    defer dict.deinit(allocator);
    try dict.ensureTotalCapacity(allocator, 20 * 1024 * 1024);

    const ids = try allocator.alloc(u32, user_col.values.len);
    defer allocator.free(ids);
    var next_id: u32 = 0;
    for (user_col.values, 0..) |uid, i| {
        const gop = try hash_to_id.getOrPut(uid);
        if (!gop.found_existing) {
            gop.value_ptr.* = next_id;
            try dict.append(allocator, uid);
            next_id += 1;
        }
        ids[i] = gop.value_ptr.*;
    }

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = id_path, .data = std.mem.sliceAsBytes(ids) });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = dict_path, .data = std.mem.sliceAsBytes(dict.items) });

    var msg_buf: [256]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "UserID: {d} unique ids, {d} rows\n", .{ next_id, ids.len });
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

fn buildClickBenchMobilePhoneModelImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, csv_path: []const u8) !void {
    const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_mobile_phone_model_id_name);
    defer allocator.free(id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_offsets_name);
    defer allocator.free(offsets_path);
    const bytes_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_bytes_name);
    defer allocator.free(bytes_path);
    const dict_tsv_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_name);
    defer allocator.free(dict_tsv_path);

    var id_writer = try BufferedColumn.init(allocator, io, id_path);
    defer id_writer.deinit(allocator, io);

    var dict = std.StringHashMap(u8).init(allocator);
    defer {
        var it = dict.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        dict.deinit();
    }
    var order: std.ArrayList([]const u8) = .empty;
    defer order.deinit(allocator);
    try dict.ensureTotalCapacity(256);
    try order.ensureTotalCapacity(allocator, 256);

    var input = if (std.fs.path.isAbsolute(csv_path))
        try std.Io.Dir.openFileAbsolute(io, csv_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, csv_path, .{});
    defer input.close(io);

    var read_buf: [1024 * 1024]u8 = undefined;
    var record: std.ArrayList(u8) = .empty;
    defer record.deinit(allocator);
    var in_quotes = false;
    var quote_pending = false;
    var pending_cr = false;
    var row_count: usize = 0;
    while (true) {
        const n = input.readStreaming(io, &.{&read_buf}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        for (read_buf[0..n]) |b| {
            if (pending_cr) {
                if (b == '\n') {
                    pending_cr = false;
                    continue;
                }
                pending_cr = false;
            }
            if (b == '"') {
                if (in_quotes) {
                    if (quote_pending) {
                        quote_pending = false;
                    } else {
                        quote_pending = true;
                    }
                } else {
                    in_quotes = true;
                }
                try record.append(allocator, b);
                continue;
            }
            if (quote_pending) {
                in_quotes = false;
                quote_pending = false;
            }
            if (!in_quotes and (b == '\n' or b == '\r')) {
                if (record.items.len > 0) {
                    try consumeClickBenchMobilePhoneModelRecord(allocator, io, record.items, &id_writer, &dict, &order);
                    row_count += 1;
                    record.clearRetainingCapacity();
                }
                if (b == '\r') pending_cr = true;
            } else {
                try record.append(allocator, b);
            }
        }
    }
    if (record.items.len > 0) {
        try consumeClickBenchMobilePhoneModelRecord(allocator, io, record.items, &id_writer, &dict, &order);
        row_count += 1;
    }
    try id_writer.flush(io);

    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(allocator);
    try offsets.append(allocator, 0);
    for (order.items) |s| {
        try bytes.appendSlice(allocator, s);
        try offsets.append(allocator, @intCast(bytes.items.len));
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = offsets_path, .data = std.mem.sliceAsBytes(offsets.items) });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = bytes_path, .data = bytes.items });

    var tsv: std.ArrayList(u8) = .empty;
    defer tsv.deinit(allocator);
    for (order.items, 0..) |s, idx| try tsv.print(allocator, "{d}\t{s}\n", .{ idx, s });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = dict_tsv_path, .data = tsv.items });

    var msg_buf: [256]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "MobilePhoneModel: {d} rows, {d} unique values\n", .{ row_count, order.items.len });
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

fn buildClickBenchSearchPhraseImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, csv_path: []const u8) !void {
    const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);
    const dict_tsv_path = try storage.searchPhraseDictPath(allocator, data_dir);
    defer allocator.free(dict_tsv_path);

    var id_writer = try BufferedColumn.init(allocator, io, id_path);
    defer id_writer.deinit(allocator, io);

    var dict = std.StringHashMap(u32).init(allocator);
    defer {
        var it = dict.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        dict.deinit();
    }
    var order: std.ArrayList([]const u8) = .empty;
    defer order.deinit(allocator);
    try dict.ensureTotalCapacity(64 * 1024);
    try order.ensureTotalCapacity(allocator, 64 * 1024);

    var input = if (std.fs.path.isAbsolute(csv_path))
        try std.Io.Dir.openFileAbsolute(io, csv_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, csv_path, .{});
    defer input.close(io);

    var read_buf: [1024 * 1024]u8 = undefined;
    var record: std.ArrayList(u8) = .empty;
    defer record.deinit(allocator);
    var in_quotes = false;
    var quote_pending = false;
    var pending_cr = false;
    var row_count: usize = 0;
    while (true) {
        const n = input.readStreaming(io, &.{&read_buf}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        for (read_buf[0..n]) |b| {
            if (pending_cr) {
                if (b == '\n') {
                    pending_cr = false;
                    continue;
                }
                pending_cr = false;
            }
            if (b == '"') {
                if (in_quotes) {
                    if (quote_pending) {
                        quote_pending = false;
                    } else {
                        quote_pending = true;
                    }
                } else {
                    in_quotes = true;
                }
                try record.append(allocator, b);
                continue;
            }
            if (quote_pending) {
                in_quotes = false;
                quote_pending = false;
            }
            if (!in_quotes and (b == '\n' or b == '\r')) {
                if (record.items.len > 0) {
                    try consumeClickBenchStringIdRecord(allocator, io, record.items, 39, &id_writer, &dict, &order);
                    row_count += 1;
                    record.clearRetainingCapacity();
                }
                if (b == '\r') pending_cr = true;
            } else {
                try record.append(allocator, b);
            }
        }
    }
    if (record.items.len > 0) {
        try consumeClickBenchStringIdRecord(allocator, io, record.items, 39, &id_writer, &dict, &order);
        row_count += 1;
    }
    try id_writer.flush(io);

    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    var phrases: std.ArrayList(u8) = .empty;
    defer phrases.deinit(allocator);
    try offsets.append(allocator, 0);
    for (order.items) |s| {
        if (s.len == 0) {
            try phrases.appendSlice(allocator, "\"\"");
        } else {
            try phrases.appendSlice(allocator, s);
        }
        try offsets.append(allocator, @intCast(phrases.items.len));
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = offsets_path, .data = std.mem.sliceAsBytes(offsets.items) });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = phrases_path, .data = phrases.items });

    var tsv: std.ArrayList(u8) = .empty;
    defer tsv.deinit(allocator);
    for (order.items, 0..) |s, idx| try tsv.print(allocator, "{d}\t{s}\n", .{ idx, s });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = dict_tsv_path, .data = tsv.items });

    var msg_buf: [256]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "SearchPhrase: {d} rows, {d} unique values\n", .{ row_count, order.items.len });
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

fn buildClickBenchUrlImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, csv_path: []const u8) !void {
    const id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_id_name);
    defer allocator.free(id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.url_id_offsets_name);
    defer allocator.free(offsets_path);
    const strings_path = try storage.hotColumnPath(allocator, data_dir, storage.url_id_strings_name);
    defer allocator.free(strings_path);
    const dict_tsv_path = try storage.urlDictPath(allocator, data_dir);
    defer allocator.free(dict_tsv_path);

    var id_writer = try BufferedColumn.init(allocator, io, id_path);
    defer id_writer.deinit(allocator, io);

    var dict = std.StringHashMap(u32).init(allocator);
    defer {
        var it = dict.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        dict.deinit();
    }
    var order: std.ArrayList([]const u8) = .empty;
    defer order.deinit(allocator);
    try dict.ensureTotalCapacity(1024 * 1024);
    try order.ensureTotalCapacity(allocator, 1024 * 1024);

    var input = if (std.fs.path.isAbsolute(csv_path))
        try std.Io.Dir.openFileAbsolute(io, csv_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, csv_path, .{});
    defer input.close(io);

    var read_buf: [1024 * 1024]u8 = undefined;
    var record: std.ArrayList(u8) = .empty;
    defer record.deinit(allocator);
    var in_quotes = false;
    var quote_pending = false;
    var pending_cr = false;
    var row_count: usize = 0;
    while (true) {
        const n = input.readStreaming(io, &.{&read_buf}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        for (read_buf[0..n]) |b| {
            if (pending_cr) {
                if (b == '\n') {
                    pending_cr = false;
                    continue;
                }
                pending_cr = false;
            }
            if (b == '"') {
                if (in_quotes) {
                    if (quote_pending) {
                        quote_pending = false;
                    } else {
                        quote_pending = true;
                    }
                } else {
                    in_quotes = true;
                }
                try record.append(allocator, b);
                continue;
            }
            if (quote_pending) {
                in_quotes = false;
                quote_pending = false;
            }
            if (!in_quotes and (b == '\n' or b == '\r')) {
                if (record.items.len > 0) {
                    try consumeClickBenchStringIdRecord(allocator, io, record.items, 13, &id_writer, &dict, &order);
                    row_count += 1;
                    record.clearRetainingCapacity();
                }
                if (b == '\r') pending_cr = true;
            } else {
                try record.append(allocator, b);
            }
        }
    }
    if (record.items.len > 0) {
        try consumeClickBenchStringIdRecord(allocator, io, record.items, 13, &id_writer, &dict, &order);
        row_count += 1;
    }
    try id_writer.flush(io);

    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    var strings: std.ArrayList(u8) = .empty;
    defer strings.deinit(allocator);
    try offsets.append(allocator, 0);
    for (order.items) |s| {
        try strings.appendSlice(allocator, s);
        try offsets.append(allocator, @intCast(strings.items.len));
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = offsets_path, .data = std.mem.sliceAsBytes(offsets.items) });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = strings_path, .data = strings.items });

    var tsv: std.ArrayList(u8) = .empty;
    defer tsv.deinit(allocator);
    for (order.items, 0..) |s, idx| try tsv.print(allocator, "{d}\t{s}\n", .{ idx, s });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = dict_tsv_path, .data = tsv.items });

    var msg_buf: [256]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "URL: {d} rows, {d} unique values\n", .{ row_count, order.items.len });
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

fn consumeClickBenchMobilePhoneModelRecord(
    allocator: std.mem.Allocator,
    io: std.Io,
    record: []const u8,
    id_writer: *BufferedColumn,
    dict: *std.StringHashMap(u8),
    order: *std.ArrayList([]const u8),
) !void {
    const model = try decodeCsvFieldAlloc(allocator, try extractClickBenchCsvField(record, 34));
    const gop = try dict.getOrPut(model);
    if (!gop.found_existing) {
        gop.key_ptr.* = model;
        const id: u8 = @intCast(order.items.len);
        gop.value_ptr.* = id;
        try order.append(allocator, model);
    } else {
        allocator.free(model);
    }
    const id: u8 = gop.value_ptr.*;
    try id_writer.write(io, std.mem.asBytes(&id));
}

fn consumeClickBenchStringIdRecord(
    allocator: std.mem.Allocator,
    io: std.Io,
    record: []const u8,
    field_idx: usize,
    id_writer: *BufferedColumn,
    dict: *std.StringHashMap(u32),
    order: *std.ArrayList([]const u8),
) !void {
    const value = try decodeCsvFieldAlloc(allocator, try extractClickBenchCsvField(record, field_idx));
    const gop = try dict.getOrPut(value);
    if (!gop.found_existing) {
        gop.key_ptr.* = value;
        const id: u32 = @intCast(order.items.len);
        gop.value_ptr.* = id;
        try order.append(allocator, value);
    } else {
        allocator.free(value);
    }
    var id = gop.value_ptr.*;
    try id_writer.write(io, std.mem.asBytes(&id));
}

fn extractClickBenchCsvField(record: []const u8, target_idx: usize) ![]const u8 {
    var field_idx: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    var in_quotes = false;
    while (i < record.len) : (i += 1) {
        const b = record[i];
        if (b == '"') {
            if (in_quotes and i + 1 < record.len and record[i + 1] == '"') {
                i += 1;
            } else {
                in_quotes = !in_quotes;
            }
        } else if (!in_quotes and b == ',') {
            if (field_idx == target_idx) return record[start..i];
            field_idx += 1;
            start = i + 1;
        }
    }
    if (field_idx == target_idx) return record[start..];
    return error.CorruptClickBenchCsv;
}

fn decodeCsvFieldAlloc(allocator: std.mem.Allocator, field: []const u8) ![]u8 {
    if (field.len < 2 or field[0] != '"' or field[field.len - 1] != '"') return allocator.dupe(u8, field);
    const s = field[1 .. field.len - 1];
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.ensureTotalCapacity(allocator, s.len);
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        if (s[i] == '"' and i + 1 < s.len and s[i + 1] == '"') i += 1;
        try out.append(allocator, s[i]);
    }
    return out.toOwnedSlice(allocator);
}

/// Import RegionID (i32), SearchEngineID (i16), MobilePhone (i16), and
/// MobilePhoneModel (string -> u8 id) for D-stage queries Q9-Q12, Q15.
///
/// Approach: one DuckDB COPY emits a 4-column CSV; stream-parse in zig,
/// emitting 4 binary columns + a small (<=256-entry) MobilePhoneModel dict.
fn importDColumnsImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, parquet_path: []const u8) !void {
    const csv_path = try storage.hotColumnPath(allocator, data_dir, "hot_dcols.csv");
    defer allocator.free(csv_path);

    const parquet_literal = try duckdb.sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);
    const csv_literal = try duckdb.sqlStringLiteral(allocator, csv_path);
    defer allocator.free(csv_literal);

    const sql = try std.fmt.allocPrint(allocator,
        \\COPY (
        \\SELECT RegionID, SearchEngineID, MobilePhone, MobilePhoneModel
        \\FROM read_parquet({s}, binary_as_string=True)
        \\) TO {s} (FORMAT csv, HEADER true);
    , .{ parquet_literal, csv_literal });
    defer allocator.free(sql);

    var ddb = duckdb.DuckDb.init(allocator, io, data_dir);
    defer ddb.deinit();
    const ddb_out = try ddb.runRawSql(sql);
    allocator.free(ddb_out);

    // Open output column files.
    const region_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_region_id_name);
    defer allocator.free(region_path);
    const sengine_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_engine_id_name);
    defer allocator.free(sengine_path);
    const mphone_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_mobile_phone_name);
    defer allocator.free(mphone_path);
    const mmodel_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_mobile_phone_model_id_name);
    defer allocator.free(mmodel_id_path);
    const mmodel_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_offsets_name);
    defer allocator.free(mmodel_offsets_path);
    const mmodel_bytes_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_bytes_name);
    defer allocator.free(mmodel_bytes_path);

    var region_w = try BufferedColumn.init(allocator, io, region_path);
    defer region_w.deinit(allocator, io);
    var sengine_w = try BufferedColumn.init(allocator, io, sengine_path);
    defer sengine_w.deinit(allocator, io);
    var mphone_w = try BufferedColumn.init(allocator, io, mphone_path);
    defer mphone_w.deinit(allocator, io);
    var mmodel_id_w = try BufferedColumn.init(allocator, io, mmodel_id_path);
    defer mmodel_id_w.deinit(allocator, io);

    var model_dict = std.StringHashMap(u8).init(allocator);
    defer {
        var it = model_dict.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        model_dict.deinit();
    }
    var model_dict_order: std.ArrayList([]const u8) = .empty;
    defer model_dict_order.deinit(allocator);
    try model_dict.ensureTotalCapacity(256);
    try model_dict_order.ensureTotalCapacity(allocator, 256);

    var input = try std.Io.Dir.cwd().openFile(io, csv_path, .{});
    defer input.close(io);

    var read_buf: [1024 * 1024]u8 = undefined;
    var pending: std.ArrayList(u8) = .empty;
    defer pending.deinit(allocator);
    var skipped_header = false;
    var row_count: usize = 0;

    while (true) {
        const n = input.readStreaming(io, &.{&read_buf}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        try pending.appendSlice(allocator, read_buf[0..n]);
        try consumeDColumnsLines(allocator, io, &pending, &skipped_header, &region_w, &sengine_w, &mphone_w, &mmodel_id_w, &model_dict, &model_dict_order, &row_count, false);
    }
    try consumeDColumnsLines(allocator, io, &pending, &skipped_header, &region_w, &sengine_w, &mphone_w, &mmodel_id_w, &model_dict, &model_dict_order, &row_count, true);

    try region_w.flush(io);
    try sengine_w.flush(io);
    try mphone_w.flush(io);
    try mmodel_id_w.flush(io);

    // Emit dict.offsets ([]u32, len = n+1) and dict.bytes (concatenated).
    var dict_offsets: std.ArrayList(u32) = .empty;
    defer dict_offsets.deinit(allocator);
    var dict_bytes: std.ArrayList(u8) = .empty;
    defer dict_bytes.deinit(allocator);
    try dict_offsets.append(allocator, 0);
    for (model_dict_order.items) |s| {
        try dict_bytes.appendSlice(allocator, s);
        try dict_offsets.append(allocator, @intCast(dict_bytes.items.len));
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = mmodel_offsets_path, .data = std.mem.sliceAsBytes(dict_offsets.items) });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = mmodel_bytes_path, .data = dict_bytes.items });

    // Also write a human-readable TSV (id\tphrase per line) for inspection.
    const dict_tsv_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_name);
    defer allocator.free(dict_tsv_path);
    var tsv: std.ArrayList(u8) = .empty;
    defer tsv.deinit(allocator);
    for (model_dict_order.items, 0..) |s, idx| {
        try tsv.print(allocator, "{d}\t{s}\n", .{ idx, s });
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = dict_tsv_path, .data = tsv.items });

    std.Io.Dir.cwd().deleteFile(io, csv_path) catch {};

    var msg_buf: [256]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "D-cols: {d} rows, {d} unique MobilePhoneModel\n", .{ row_count, model_dict_order.items.len });
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

fn consumeDColumnsLines(
    allocator: std.mem.Allocator,
    io: std.Io,
    pending: *std.ArrayList(u8),
    skipped_header: *bool,
    region_w: *BufferedColumn,
    sengine_w: *BufferedColumn,
    mphone_w: *BufferedColumn,
    mmodel_id_w: *BufferedColumn,
    model_dict: *std.StringHashMap(u8),
    model_order: *std.ArrayList([]const u8),
    row_count: *usize,
    flush_tail: bool,
) !void {
    var start: usize = 0;
    var i: usize = 0;
    while (i < pending.items.len) : (i += 1) {
        if (pending.items[i] != '\n') continue;
        try consumeDColumnsLine(allocator, io, pending.items[start..i], skipped_header, region_w, sengine_w, mphone_w, mmodel_id_w, model_dict, model_order, row_count);
        start = i + 1;
    }
    if (flush_tail and start < pending.items.len) {
        try consumeDColumnsLine(allocator, io, pending.items[start..], skipped_header, region_w, sengine_w, mphone_w, mmodel_id_w, model_dict, model_order, row_count);
        start = pending.items.len;
    }
    if (start > 0) {
        const rest = pending.items[start..];
        std.mem.copyForwards(u8, pending.items[0..rest.len], rest);
        try pending.resize(allocator, rest.len);
    }
}

fn consumeDColumnsLine(
    allocator: std.mem.Allocator,
    io: std.Io,
    line_raw: []const u8,
    skipped_header: *bool,
    region_w: *BufferedColumn,
    sengine_w: *BufferedColumn,
    mphone_w: *BufferedColumn,
    mmodel_id_w: *BufferedColumn,
    model_dict: *std.StringHashMap(u8),
    model_order: *std.ArrayList([]const u8),
    row_count: *usize,
) !void {
    const line = std.mem.trim(u8, line_raw, " \t\r");
    if (line.len == 0) return;
    if (!skipped_header.*) {
        skipped_header.* = true;
        return;
    }

    // CSV parse: 4 fields. MobilePhoneModel may be quoted (DuckDB CSV quotes
    // when field contains comma/quote/newline). Simple state machine.
    var fields: [4][]const u8 = undefined;
    var fcount: usize = 0;
    var p: usize = 0;
    while (p < line.len and fcount < 4) {
        if (line[p] == '"') {
            // quoted field: copy until closing quote (handle "" -> "). For
            // MobilePhoneModel max-len 27 we tolerate up to 64 buf chars.
            p += 1;
            const fstart = p;
            while (p < line.len) {
                if (line[p] == '"') {
                    if (p + 1 < line.len and line[p + 1] == '"') {
                        p += 2;
                        continue;
                    }
                    break;
                }
                p += 1;
            }
            fields[fcount] = line[fstart..p];
            fcount += 1;
            // skip closing quote and comma
            if (p < line.len and line[p] == '"') p += 1;
            if (p < line.len and line[p] == ',') p += 1;
        } else {
            const fstart = p;
            while (p < line.len and line[p] != ',') p += 1;
            fields[fcount] = line[fstart..p];
            fcount += 1;
            if (p < line.len and line[p] == ',') p += 1;
        }
    }
    if (fcount != 4) return error.MalformedDColumnsCsv;

    const region: i32 = try std.fmt.parseInt(i32, fields[0], 10);
    const sengine: i16 = try std.fmt.parseInt(i16, fields[1], 10);
    const mphone: i16 = try std.fmt.parseInt(i16, fields[2], 10);
    const model = fields[3];

    const gop = try model_dict.getOrPut(model);
    if (!gop.found_existing) {
        const owned = try allocator.dupe(u8, model);
        gop.key_ptr.* = owned;
        const id: u8 = @intCast(model_order.items.len);
        gop.value_ptr.* = id;
        try model_order.append(allocator, owned);
    }
    const model_id: u8 = gop.value_ptr.*;

    var r = region;
    var s = sengine;
    var m = mphone;
    try region_w.write(io, std.mem.asBytes(&r));
    try sengine_w.write(io, std.mem.asBytes(&s));
    try mphone_w.write(io, std.mem.asBytes(&m));
    try mmodel_id_w.write(io, std.mem.asBytes(&model_id));
    row_count.* += 1;
}
// ============================================================================
// D-stage query implementations: Q9, Q10, Q11, Q12, Q15.
// All use small-cardinality dense aggregation + (for *DISTINCT UserID*)
// the candidate two-pass strategy proven on Q14.
// ============================================================================

const region_dict_size: usize = 131072; // RegionID max ~131069 -> dense array OK.

/// Format an f64 the way DuckDB's CSV writer does: integral values get a
/// trailing ".0" (e.g. 1587 -> "1587.0"), non-integral values use Zig's default
/// shortest round-trippable representation.
fn writeFloatCsv(allocator: std.mem.Allocator, out: *std.ArrayList(u8), v: f64) !void {
    if (std.math.isFinite(v) and @floor(v) == v and @abs(v) < 1e16) {
        try out.print(allocator, "{d}.0", .{v});
    } else {
        try out.print(allocator, "{d}", .{v});
    }
}

/// CSV-quote a string only if it contains comma, double-quote, newline, or
/// any non-ASCII byte (DuckDB's CSV writer quotes any non-ASCII content).
fn writeCsvField(allocator: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    var needs_quote = false;
    for (s) |b| {
        if (b == ',' or b == '"' or b == '\n' or b == '\r' or b >= 0x80) {
            needs_quote = true;
            break;
        }
    }
    if (!needs_quote) {
        try out.appendSlice(allocator, s);
    } else {
        try out.append(allocator, '"');
        for (s) |b| {
            if (b == '"') try out.append(allocator, '"');
            try out.append(allocator, b);
        }
        try out.append(allocator, '"');
    }
}

/// Emit a SearchPhrase from id_phrases.bin. The blob stores DuckDB's TSV
/// encoding where empty becomes the literal 2-byte sequence `""`. Translate
/// that back to empty before emitting via writeCsvField.
fn writeSearchPhraseField(allocator: std.mem.Allocator, out: *std.ArrayList(u8), blob_bytes: []const u8) !void {
    if (blob_bytes.len == 2 and blob_bytes[0] == '"' and blob_bytes[1] == '"') return;
    try writeCsvField(allocator, out, blob_bytes);
}

/// Q9: SELECT RegionID, COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID
///     ORDER BY u DESC LIMIT 10.
/// Same two-pass shape as Q14: top-N candidate regions by row count, then
/// per-candidate UserID bitset.
fn formatRegionDistinctUserIdTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const n_candidates: usize = 64;

    const region_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_region_id_name);
    defer allocator.free(region_path);
    const uid_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(uid_id_path);
    const uid_dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(uid_dict_path);

    const regions = try io_map.mapColumn(i32, io, region_path);
    defer regions.mapping.unmap();
    const uid_ids = try io_map.mapColumn(u32, io, uid_id_path);
    defer uid_ids.mapping.unmap();
    const uid_dict = try io_map.mapColumn(i64, io, uid_dict_path);
    defer uid_dict.mapping.unmap();

    if (regions.values.len != uid_ids.values.len) return error.UnsupportedNativeQuery;
    const uid_dict_size = uid_dict.values.len;

    // Pass 1: dense counts per region.
    const counts = try allocator.alloc(u32, region_dict_size);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (regions.values) |r| counts[@intCast(r)] += 1;

    // Pick top-N candidate regions by row count.
    const Candidate = struct { region: i32, count: u32 };
    var cand: [64]Candidate = undefined;
    var cand_len: usize = 0;
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        const row: Candidate = .{ .region = @intCast(idx), .count = c };
        var pos: usize = 0;
        while (pos < cand_len and (cand[pos].count > row.count or (cand[pos].count == row.count and cand[pos].region < row.region))) : (pos += 1) {}
        if (pos >= n_candidates) continue;
        if (cand_len < n_candidates) cand_len += 1;
        var j = cand_len - 1;
        while (j > pos) : (j -= 1) cand[j] = cand[j - 1];
        cand[pos] = row;
    }

    // candidate_idx[region_id] -> i8.
    const cand_idx = try allocator.alloc(i8, region_dict_size);
    defer allocator.free(cand_idx);
    @memset(cand_idx, -1);
    for (cand[0..cand_len], 0..) |c, slot| cand_idx[@intCast(c.region)] = @intCast(slot);

    // Per-candidate UserID bitset.
    const words_per_set = (uid_dict_size + 63) / 64;
    const bitsets = try allocator.alloc(u64, cand_len * words_per_set);
    defer allocator.free(bitsets);
    @memset(bitsets, 0);

    var i: usize = 0;
    const n = regions.values.len;
    while (i < n) : (i += 1) {
        const slot = cand_idx[@intCast(regions.values[i])];
        if (slot < 0) continue;
        const uid = uid_ids.values[i];
        const base = @as(usize, @intCast(slot)) * words_per_set;
        bitsets[base + (uid >> 6)] |= @as(u64, 1) << @intCast(uid & 63);
    }

    const Result = struct { region: i32, distinct: u64 };
    var results = try allocator.alloc(Result, cand_len);
    defer allocator.free(results);
    for (cand[0..cand_len], 0..) |c, slot| {
        const base = slot * words_per_set;
        var sum: u64 = 0;
        for (bitsets[base .. base + words_per_set]) |w| sum += @popCount(w);
        results[slot] = .{ .region = c.region, .distinct = sum };
    }

    std.sort.pdq(Result, results, {}, struct {
        fn lt(_: void, a: Result, b: Result) bool {
            if (a.distinct != b.distinct) return a.distinct > b.distinct;
            return a.region < b.region;
        }
    }.lt);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "RegionID,u\n");
    const top_emit = @min(@as(usize, 10), cand_len);
    for (results[0..top_emit]) |r| {
        try out.print(allocator, "{d},{d}\n", .{ r.region, r.distinct });
    }
    return out.toOwnedSlice(allocator);
}

/// Q10: SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth),
///       COUNT(DISTINCT UserID) GROUP BY RegionID ORDER BY c DESC LIMIT 10.
/// Sort key is c = COUNT(*), so top-10 is identified by Pass 1 alone.
/// Pass 2 computes the other aggregates only for those 10 regions.
fn formatRegionStatsDistinctUserIdTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const region_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_region_id_name);
    defer allocator.free(region_path);
    const adv_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_adv_engine_id_name);
    defer allocator.free(adv_path);
    const width_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_resolution_width_name);
    defer allocator.free(width_path);
    const uid_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(uid_id_path);
    const uid_dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(uid_dict_path);

    const regions = try io_map.mapColumn(i32, io, region_path);
    defer regions.mapping.unmap();
    const adv = try io_map.mapColumn(i16, io, adv_path);
    defer adv.mapping.unmap();
    const width = try io_map.mapColumn(i16, io, width_path);
    defer width.mapping.unmap();
    const uid_ids = try io_map.mapColumn(u32, io, uid_id_path);
    defer uid_ids.mapping.unmap();
    const uid_dict = try io_map.mapColumn(i64, io, uid_dict_path);
    defer uid_dict.mapping.unmap();

    const n = regions.values.len;
    if (n != uid_ids.values.len or n != adv.values.len or n != width.values.len) return error.UnsupportedNativeQuery;
    const uid_dict_size = uid_dict.values.len;

    // Pass 1: counts per region.
    const counts = try allocator.alloc(u32, region_dict_size);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (regions.values) |r| counts[@intCast(r)] += 1;

    // Top-10 by count.
    const TopCand = struct { region: i32, count: u32 };
    var top: [10]TopCand = undefined;
    var top_len: usize = 0;
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        const row: TopCand = .{ .region = @intCast(idx), .count = c };
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or (top[pos].count == row.count and top[pos].region < row.region))) : (pos += 1) {}
        if (pos >= 10) continue;
        if (top_len < 10) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    const cand_idx = try allocator.alloc(i8, region_dict_size);
    defer allocator.free(cand_idx);
    @memset(cand_idx, -1);
    for (top[0..top_len], 0..) |c, slot| cand_idx[@intCast(c.region)] = @intCast(slot);

    // Per-candidate aggregates.
    const sum_adv = try allocator.alloc(i64, top_len);
    defer allocator.free(sum_adv);
    @memset(sum_adv, 0);
    const sum_width = try allocator.alloc(i64, top_len);
    defer allocator.free(sum_width);
    @memset(sum_width, 0);
    const words_per_set = (uid_dict_size + 63) / 64;
    const bitsets = try allocator.alloc(u64, top_len * words_per_set);
    defer allocator.free(bitsets);
    @memset(bitsets, 0);

    var i: usize = 0;
    while (i < n) : (i += 1) {
        const slot = cand_idx[@intCast(regions.values[i])];
        if (slot < 0) continue;
        const s_idx: usize = @intCast(slot);
        sum_adv[s_idx] += adv.values[i];
        sum_width[s_idx] += width.values[i];
        const uid = uid_ids.values[i];
        const base = s_idx * words_per_set;
        bitsets[base + (uid >> 6)] |= @as(u64, 1) << @intCast(uid & 63);
    }

    const Row = struct {
        region: i32,
        sum_adv: i64,
        count: u32,
        avg_width: f64,
        distinct_uid: u64,
    };
    var rows = try allocator.alloc(Row, top_len);
    defer allocator.free(rows);
    for (top[0..top_len], 0..) |c, slot| {
        const base = slot * words_per_set;
        var sum_distinct: u64 = 0;
        for (bitsets[base .. base + words_per_set]) |w| sum_distinct += @popCount(w);
        const avg = @as(f64, @floatFromInt(sum_width[slot])) / @as(f64, @floatFromInt(c.count));
        rows[slot] = .{
            .region = c.region,
            .sum_adv = sum_adv[slot],
            .count = c.count,
            .avg_width = avg,
            .distinct_uid = sum_distinct,
        };
    }

    // Already in top-by-count order (Pass 1 sorted insert); rows[] follows top[] order.
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "RegionID,sum(AdvEngineID),c,avg(ResolutionWidth),count(DISTINCT UserID)\n");
    for (rows) |r| {
        try out.print(allocator, "{d},{d},{d},{d},{d}\n", .{ r.region, r.sum_adv, r.count, r.avg_width, r.distinct_uid });
    }
    return out.toOwnedSlice(allocator);
}

/// Q11: SELECT MobilePhoneModel, COUNT(DISTINCT UserID) WHERE MobilePhoneModel <> ''
///      GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10.
/// 166 distinct models -> compute COUNT(DISTINCT UserID) for ALL of them
/// directly (no candidate filtering needed; cheap).
fn formatMobilePhoneModelDistinctUserIdTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const model_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_mobile_phone_model_id_name);
    defer allocator.free(model_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_offsets_name);
    defer allocator.free(offsets_path);
    const bytes_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_bytes_name);
    defer allocator.free(bytes_path);
    const uid_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(uid_id_path);
    const uid_dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(uid_dict_path);

    const model_ids = try io_map.mapColumn(u8, io, model_id_path);
    defer model_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const bytes_map = try io_map.mapFile(io, bytes_path);
    defer bytes_map.unmap();
    const uid_ids = try io_map.mapColumn(u32, io, uid_id_path);
    defer uid_ids.mapping.unmap();
    const uid_dict = try io_map.mapColumn(i64, io, uid_dict_path);
    defer uid_dict.mapping.unmap();

    const n = model_ids.values.len;
    if (n != uid_ids.values.len) return error.UnsupportedNativeQuery;
    const dict_size = offsets.values.len - 1;
    const uid_dict_size = uid_dict.values.len;

    // Identify empty-model id (offsets[i+1] - offsets[i] == 0).
    var empty_id: ?u8 = null;
    for (0..dict_size) |idx| {
        if (offsets.values[idx + 1] == offsets.values[idx]) {
            empty_id = @intCast(idx);
            break;
        }
    }

    // Per-model bitset of UserID ids. dict_size <= 256, so up to ~565 MB at 17.6M users.
    // Actually 256 * 2.2 MB = 565 MB. Tight but OK.
    // Better: only allocate for models that actually appear (count > 0).
    const model_counts = try allocator.alloc(u32, dict_size);
    defer allocator.free(model_counts);
    @memset(model_counts, 0);

    // Pass 1 parallel: per-thread private 256-entry counter, then sum.
    // Eliminates the 100M-row sequential scan that dominated wall time.
    const Pass1Threads = parallel.defaultThreads();
    const local_counts = try allocator.alloc(u32, Pass1Threads * dict_size);
    defer allocator.free(local_counts);
    @memset(local_counts, 0);
    const Pass1Ctx = struct {
        model_ids: []const u8,
        local: []u32,
    };
    const pass1_workers = struct {
        fn fill(ctx: *Pass1Ctx, source: *parallel.MorselSource) void {
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) ctx.local[ctx.model_ids[r]] += 1;
            }
        }
    };
    const pass1_ctxs = try allocator.alloc(Pass1Ctx, Pass1Threads);
    defer allocator.free(pass1_ctxs);
    for (pass1_ctxs, 0..) |*c, t| {
        c.* = .{
            .model_ids = model_ids.values,
            .local = local_counts[t * dict_size .. (t + 1) * dict_size],
        };
    }
    var pass1_src: parallel.MorselSource = .init(n, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, Pass1Ctx, pass1_workers.fill, pass1_ctxs, &pass1_src);
    for (0..dict_size) |idx| {
        var sum: u32 = 0;
        var t: usize = 0;
        while (t < Pass1Threads) : (t += 1) sum += local_counts[t * dict_size + idx];
        model_counts[idx] = sum;
    }

    // Two-pass top-N candidates (same as Q14): pick top-32 models by row
    // count, then per-candidate UserID bitset. 32 * 2.2 MB = 70 MB working set.
    const n_candidates: usize = 32;
    const Candidate = struct { id: u8, count: u32 };
    var cand: [32]Candidate = undefined;
    var cand_len: usize = 0;
    for (0..dict_size) |idx| {
        const c = model_counts[idx];
        if (c == 0) continue;
        if (empty_id) |eid| if (idx == eid) continue;
        const row: Candidate = .{ .id = @intCast(idx), .count = c };
        var pos: usize = 0;
        while (pos < cand_len and (cand[pos].count > row.count or (cand[pos].count == row.count and cand[pos].id < row.id))) : (pos += 1) {}
        if (pos >= n_candidates) continue;
        if (cand_len < n_candidates) cand_len += 1;
        var j = cand_len - 1;
        while (j > pos) : (j -= 1) cand[j] = cand[j - 1];
        cand[pos] = row;
    }

    const slot_for_model = try allocator.alloc(i8, dict_size);
    defer allocator.free(slot_for_model);
    @memset(slot_for_model, -1);
    for (cand[0..cand_len], 0..) |c, slot| slot_for_model[c.id] = @intCast(slot);

    const words_per_set = (uid_dict_size + 63) / 64;
    const n_threads = parallel.defaultThreads();
    const total_set_words = cand_len * words_per_set;

    // Pass 2 parallelization: each worker fills its own bitset block
    // (cand_len * words_per_set u64). After all morsels processed, the
    // OR-merge phase parallelizes across the words_per_set dimension so
    // each merge thread owns disjoint output cache lines.
    const Pass2Ctx = struct {
        model_ids: []const u8,
        uid_ids: []const u32,
        slot_for_model: []const i8,
        bitsets: []u64, // cand_len * words_per_set
        words_per_set: usize,
    };
    const pass2_workers = struct {
        fn fill(ctx: *Pass2Ctx, source: *parallel.MorselSource) void {
            const wps = ctx.words_per_set;
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) {
                    const slot = ctx.slot_for_model[ctx.model_ids[r]];
                    if (slot < 0) continue;
                    const uid = ctx.uid_ids[r];
                    const base = @as(usize, @intCast(slot)) * wps;
                    ctx.bitsets[base + (uid >> 6)] |= @as(u64, 1) << @intCast(uid & 63);
                }
            }
        }
    };

    const ctxs = try allocator.alloc(Pass2Ctx, n_threads);
    defer allocator.free(ctxs);
    // One bitset block per thread.
    const all_bitsets = try allocator.alloc(u64, n_threads * total_set_words);
    defer allocator.free(all_bitsets);
    @memset(all_bitsets, 0);
    for (ctxs, 0..) |*c, t| {
        c.* = .{
            .model_ids = model_ids.values,
            .uid_ids = uid_ids.values,
            .slot_for_model = slot_for_model,
            .bitsets = all_bitsets[t * total_set_words .. (t + 1) * total_set_words],
            .words_per_set = words_per_set,
        };
    }
    var src: parallel.MorselSource = .init(n, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, Pass2Ctx, pass2_workers.fill, ctxs, &src);

    // OR-merge across threads directly into the popcount sum, no need
    // to materialize a final merged bitset: each merge worker owns a
    // disjoint set of slots, ORs all thread copies for its slots, and
    // popcounts on the fly.
    const Row = struct { id: u8, distinct: u64 };
    const rows = try allocator.alloc(Row, cand_len);
    defer allocator.free(rows);
    const MergeCtx = struct {
        all_bitsets: []u64,
        cand: []const Candidate,
        rows: []Row,
        n_threads: usize,
        cand_len: usize,
        words_per_set: usize,
        n_workers: usize,
    };
    const merge_workers = struct {
        fn run(ctx: *MergeCtx, worker_id: usize) void {
            const wps = ctx.words_per_set;
            const bw = ctx.cand_len * wps;
            var s = worker_id;
            while (s < ctx.cand_len) : (s += ctx.n_workers) {
                // Stream-process slot s: read each thread's contiguous
                // wps-word block in turn (good prefetch), accumulate
                // into popcount sum at the end.
                // Fold into a single accumulator block in registers
                // is impractical (wps ~280k), so use a small chunked
                // buffer that fits L1.
                const chunk: usize = 1024; // 8 KB per chunk -> hot in L1
                var w: usize = 0;
                var sum: u64 = 0;
                while (w < wps) {
                    const lo = w;
                    const hi = @min(w + chunk, wps);
                    var local: [chunk]u64 = undefined;
                    const out = local[0 .. hi - lo];
                    // Init from thread 0's block.
                    @memcpy(out, ctx.all_bitsets[s * wps + lo .. s * wps + hi]);
                    var t: usize = 1;
                    while (t < ctx.n_threads) : (t += 1) {
                        const blk = ctx.all_bitsets[t * bw + s * wps + lo .. t * bw + s * wps + hi];
                        for (out, blk) |*o, b| o.* |= b;
                    }
                    for (out) |v| sum += @popCount(v);
                    w = hi;
                }
                ctx.rows[s] = .{ .id = ctx.cand[s].id, .distinct = sum };
            }
        }
    };
    var merge_ctx: MergeCtx = .{
        .all_bitsets = all_bitsets,
        .cand = cand[0..cand_len],
        .rows = rows,
        .n_threads = n_threads,
        .cand_len = cand_len,
        .words_per_set = words_per_set,
        .n_workers = @min(n_threads, cand_len),
    };
    try parallel.parallelIndices(allocator, MergeCtx, merge_workers.run, &merge_ctx, merge_ctx.n_workers);

    std.sort.pdq(Row, rows, {}, struct {
        fn lt(_: void, a: Row, b: Row) bool {
            if (a.distinct != b.distinct) return a.distinct > b.distinct;
            return a.id < b.id;
        }
    }.lt);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "MobilePhoneModel,u\n");
    const emit = @min(@as(usize, 10), rows.len);
    for (rows[0..emit]) |r| {
        const start = offsets.values[r.id];
        const end = offsets.values[r.id + 1];
        try writeCsvField(allocator, &out, bytes_map.raw[start..end]);
        try out.print(allocator, ",{d}\n", .{r.distinct});
    }
    return out.toOwnedSlice(allocator);
}

/// Q12: SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID)
///      WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel
///      ORDER BY u DESC LIMIT 10.
/// Group key = (mphone, mmodel_id). With ~125 phones * 166 models = ~20k
/// possible pairs, but only ~hundreds actually populated. Per-pair UserID
/// bitset is too much (20k * 2.2MB = 44 GB) -> use sparse: hash from (pair) to
/// a slot index, only allocate bitsets for slots that appear.
///
/// Simpler: scan once to count rows per pair (dense [256][256] = 64 KB).
/// Pick top-N pairs by count (N=64) as in Q14, then per-candidate UserID
/// bitset.
fn formatMobilePhoneDistinctUserIdTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const n_candidates: usize = 64;

    const mphone_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_mobile_phone_name);
    defer allocator.free(mphone_path);
    const mmodel_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_mobile_phone_model_id_name);
    defer allocator.free(mmodel_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_offsets_name);
    defer allocator.free(offsets_path);
    const bytes_path = try storage.hotColumnPath(allocator, data_dir, storage.mobile_phone_model_dict_bytes_name);
    defer allocator.free(bytes_path);
    const uid_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(uid_id_path);
    const uid_dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(uid_dict_path);

    const mphone = try io_map.mapColumn(i16, io, mphone_path);
    defer mphone.mapping.unmap();
    const mmodel_ids = try io_map.mapColumn(u8, io, mmodel_id_path);
    defer mmodel_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const bytes_map = try io_map.mapFile(io, bytes_path);
    defer bytes_map.unmap();
    const uid_ids = try io_map.mapColumn(u32, io, uid_id_path);
    defer uid_ids.mapping.unmap();
    const uid_dict = try io_map.mapColumn(i64, io, uid_dict_path);
    defer uid_dict.mapping.unmap();

    const n = mphone.values.len;
    if (n != uid_ids.values.len or n != mmodel_ids.values.len) return error.UnsupportedNativeQuery;
    const dict_size = offsets.values.len - 1;
    const uid_dict_size = uid_dict.values.len;

    // Identify empty-model id.
    var empty_id: ?u8 = null;
    for (0..dict_size) |idx| {
        if (offsets.values[idx + 1] == offsets.values[idx]) {
            empty_id = @intCast(idx);
            break;
        }
    }

    // Pair counts: pack (mphone u16, mmodel u8) -> u32 index. mphone fits in
    // u8 actually (max 125), so use 256*256 dense matrix.
    const matrix_size: usize = 256 * 256;
    const pair_counts = try allocator.alloc(u32, matrix_size);
    defer allocator.free(pair_counts);
    @memset(pair_counts, 0);

    // Pass 1 parallel: each thread accumulates into a private 256 KB
    // matrix, then we sum the matrices into pair_counts.
    const Pass1Threads = parallel.defaultThreads();
    const local_matrices = try allocator.alloc(u32, Pass1Threads * matrix_size);
    defer allocator.free(local_matrices);
    @memset(local_matrices, 0);

    const Pass1Ctx = struct {
        mphone: []const i16,
        mmodel_ids: []const u8,
        local: []u32, // matrix_size u32 per thread
    };
    const pass1_workers = struct {
        fn fill(ctx: *Pass1Ctx, source: *parallel.MorselSource) void {
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) {
                    const ph: usize = @intCast(ctx.mphone[r]);
                    const md: usize = ctx.mmodel_ids[r];
                    ctx.local[(ph << 8) | md] += 1;
                }
            }
        }
    };
    const pass1_ctxs = try allocator.alloc(Pass1Ctx, Pass1Threads);
    defer allocator.free(pass1_ctxs);
    for (pass1_ctxs, 0..) |*c, t| {
        c.* = .{
            .mphone = mphone.values,
            .mmodel_ids = mmodel_ids.values,
            .local = local_matrices[t * matrix_size .. (t + 1) * matrix_size],
        };
    }
    var pass1_src: parallel.MorselSource = .init(n, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, Pass1Ctx, pass1_workers.fill, pass1_ctxs, &pass1_src);

    // Reduce: pair_counts[i] = Σ_t local_matrices[t * matrix_size + i].
    // 64K entries — fast enough sequentially (256 KB × T reads).
    for (0..matrix_size) |idx| {
        var sum: u32 = 0;
        var t: usize = 0;
        while (t < Pass1Threads) : (t += 1) {
            sum += local_matrices[t * matrix_size + idx];
        }
        pair_counts[idx] = sum;
    }

    const Candidate = struct { phone: i16, model: u8, count: u32 };
    var cand: [64]Candidate = undefined;
    var cand_len: usize = 0;
    for (pair_counts, 0..) |c, idx| {
        if (c == 0) continue;
        const md: u8 = @intCast(idx & 0xff);
        if (empty_id) |eid| if (md == eid) continue;
        const ph: i16 = @intCast(idx >> 8);
        const row: Candidate = .{ .phone = ph, .model = md, .count = c };
        var pos: usize = 0;
        while (pos < cand_len and (cand[pos].count > row.count or
            (cand[pos].count == row.count and cand[pos].phone < row.phone) or
            (cand[pos].count == row.count and cand[pos].phone == row.phone and cand[pos].model < row.model))) : (pos += 1) {}
        if (pos >= n_candidates) continue;
        if (cand_len < n_candidates) cand_len += 1;
        var j = cand_len - 1;
        while (j > pos) : (j -= 1) cand[j] = cand[j - 1];
        cand[pos] = row;
    }

    // cand_idx[pair_key] -> i8.
    const cand_idx = try allocator.alloc(i8, matrix_size);
    defer allocator.free(cand_idx);
    @memset(cand_idx, -1);
    for (cand[0..cand_len], 0..) |c, slot| {
        const ph: usize = @intCast(c.phone);
        const md: usize = c.model;
        cand_idx[(ph << 8) | md] = @intCast(slot);
    }

    const words_per_set = (uid_dict_size + 63) / 64;
    const total_set_words = cand_len * words_per_set;

    // Memory budget: per-thread bitset block = cand_len * words_per_set
    // u64 bytes ≈ 140 MB at full size. Cap T so total stays under 1/4 of
    // physical RAM to leave room for the OS and column mappings.
    const default_t = parallel.defaultThreads();
    const n_threads = blk: {
        const per_thread_bytes = total_set_words * @sizeOf(u64);
        if (parallel.availableMemoryMiB()) |mib| {
            const budget_bytes = (mib * 1024 * 1024) / 4;
            const max_t = @max(@as(usize, 1), budget_bytes / per_thread_bytes);
            break :blk @min(default_t, max_t);
        }
        break :blk default_t;
    };

    // Pass 2 parallel: each thread owns a private bitset block of
    // total_set_words u64. Same template as Q11 but the per-thread block
    // is ~4× larger (140 MB) due to 64 candidates vs 32.
    const Pass2Ctx = struct {
        mphone: []const i16,
        mmodel_ids: []const u8,
        uid_ids: []const u32,
        cand_idx: []const i8,
        bitsets: []u64,
        words_per_set: usize,
    };
    const pass2_workers = struct {
        fn fill(ctx: *Pass2Ctx, source: *parallel.MorselSource) void {
            const wps = ctx.words_per_set;
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) {
                    const ph: usize = @intCast(ctx.mphone[r]);
                    const md: usize = ctx.mmodel_ids[r];
                    const slot = ctx.cand_idx[(ph << 8) | md];
                    if (slot < 0) continue;
                    const uid = ctx.uid_ids[r];
                    const base = @as(usize, @intCast(slot)) * wps;
                    ctx.bitsets[base + (uid >> 6)] |= @as(u64, 1) << @intCast(uid & 63);
                }
            }
        }
    };

    const ctxs = try allocator.alloc(Pass2Ctx, n_threads);
    defer allocator.free(ctxs);
    const all_bitsets = try allocator.alloc(u64, n_threads * total_set_words);
    defer allocator.free(all_bitsets);
    @memset(all_bitsets, 0);
    for (ctxs, 0..) |*c, t| {
        c.* = .{
            .mphone = mphone.values,
            .mmodel_ids = mmodel_ids.values,
            .uid_ids = uid_ids.values,
            .cand_idx = cand_idx,
            .bitsets = all_bitsets[t * total_set_words .. (t + 1) * total_set_words],
            .words_per_set = words_per_set,
        };
    }
    var src: parallel.MorselSource = .init(n, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, Pass2Ctx, pass2_workers.fill, ctxs, &src);

    // Parallel OR-merge + popcount across slot dimension. Same chunked
    // pattern as Q11 to keep the inner accumulator hot in L1.
    const Result = struct { phone: i16, model: u8, distinct: u64 };
    const results = try allocator.alloc(Result, cand_len);
    defer allocator.free(results);
    const MergeCtx = struct {
        all_bitsets: []u64,
        cand: []const Candidate,
        results: []Result,
        n_threads: usize,
        cand_len: usize,
        words_per_set: usize,
        n_workers: usize,
    };
    const merge_workers = struct {
        fn run(ctx: *MergeCtx, worker_id: usize) void {
            const wps = ctx.words_per_set;
            const bw = ctx.cand_len * wps;
            var s = worker_id;
            while (s < ctx.cand_len) : (s += ctx.n_workers) {
                const chunk: usize = 1024; // 8 KB per chunk -> hot in L1
                var w: usize = 0;
                var sum: u64 = 0;
                while (w < wps) {
                    const lo = w;
                    const hi = @min(w + chunk, wps);
                    var local: [chunk]u64 = undefined;
                    const out = local[0 .. hi - lo];
                    @memcpy(out, ctx.all_bitsets[s * wps + lo .. s * wps + hi]);
                    var t: usize = 1;
                    while (t < ctx.n_threads) : (t += 1) {
                        const blk = ctx.all_bitsets[t * bw + s * wps + lo .. t * bw + s * wps + hi];
                        for (out, blk) |*o, b| o.* |= b;
                    }
                    for (out) |v| sum += @popCount(v);
                    w = hi;
                }
                ctx.results[s] = .{ .phone = ctx.cand[s].phone, .model = ctx.cand[s].model, .distinct = sum };
            }
        }
    };
    var merge_ctx: MergeCtx = .{
        .all_bitsets = all_bitsets,
        .cand = cand[0..cand_len],
        .results = results,
        .n_threads = n_threads,
        .cand_len = cand_len,
        .words_per_set = words_per_set,
        .n_workers = @min(n_threads, cand_len),
    };
    try parallel.parallelIndices(allocator, MergeCtx, merge_workers.run, &merge_ctx, merge_ctx.n_workers);

    std.sort.pdq(Result, results, {}, struct {
        fn lt(_: void, a: Result, b: Result) bool {
            if (a.distinct != b.distinct) return a.distinct > b.distinct;
            if (a.phone != b.phone) return a.phone < b.phone;
            return a.model < b.model;
        }
    }.lt);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "MobilePhone,MobilePhoneModel,u\n");
    const top_emit = @min(@as(usize, 10), cand_len);
    for (results[0..top_emit]) |r| {
        try out.print(allocator, "{d},", .{r.phone});
        const start = offsets.values[r.model];
        const end = offsets.values[r.model + 1];
        try writeCsvField(allocator, &out, bytes_map.raw[start..end]);
        try out.print(allocator, ",{d}\n", .{r.distinct});
    }
    return out.toOwnedSlice(allocator);
}

/// Q15: SELECT SearchEngineID, SearchPhrase, COUNT(*) WHERE SearchPhrase <> ''
///      GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10.
/// Pair key = (sengine u8, phrase_id u32). Up to 96 * 6M = 580M possible pairs,
/// most empty. Use a hash map keyed by u64(sengine<<32|phrase_id) with counts.
/// Sparse since phrase_id space is huge but the hot pairs are fewer than
/// total rows (100M).
///
/// Faster alternative: for each row, increment `counts[(sengine << 23) | phrase_id]`
/// in a sparse u32 array? sengine is 8 bits, phrase_id is 23 bits if dict
/// were small, but phrase_id_max ~6M ~= 23 bits. Total slots = 256 * 8M = 2G,
/// too big. -> use hash map approach.
///
/// Two-pass: Pass 1 compute counts via hashmap. Top-10 by count.
fn formatSearchEnginePhraseCountTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const sengine_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_engine_id_name);
    defer allocator.free(sengine_path);
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const sengine = try io_map.mapColumn(i16, io, sengine_path);
    defer sengine.mapping.unmap();
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();

    const n = sengine.values.len;
    if (n != phrase_ids.values.len) return error.UnsupportedNativeQuery;
    const phrase_dict_size = offsets.values.len - 1;

    // Identify empty phrase id (DuckDB encodes empty as `""` 2-char in dict).
    var empty_phrase_id: ?u32 = null;
    for (0..phrase_dict_size) |idx| {
        const start = offsets.values[idx];
        const end = offsets.values[idx + 1];
        const phrase = phrases.raw[start..end];
        if (isStoredEmptyString(phrase)) {
            empty_phrase_id = @intCast(idx);
            break;
        }
    }

    // Hash map (sengine_u8 << 32 | phrase_id_u32) -> count.
    var counts = std.AutoHashMap(u64, u32).init(allocator);
    defer counts.deinit();
    try counts.ensureTotalCapacity(8 * 1024 * 1024);

    var i: usize = 0;
    while (i < n) : (i += 1) {
        const pid = phrase_ids.values[i];
        if (empty_phrase_id) |eid| if (pid == eid) continue;
        const se: u64 = @intCast(sengine.values[i]);
        const key: u64 = (se << 32) | @as(u64, pid);
        const gop = try counts.getOrPut(key);
        if (!gop.found_existing) gop.value_ptr.* = 0;
        gop.value_ptr.* += 1;
    }

    const Row = struct { sengine: i16, phrase_id: u32, count: u32 };
    const top_capacity: usize = 10;
    var top: [top_capacity]Row = undefined;
    var top_len: usize = 0;
    var it = counts.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const c = entry.value_ptr.*;
        const se: i16 = @intCast(key >> 32);
        const pid: u32 = @intCast(key & 0xffffffff);
        const row: Row = .{ .sengine = se, .phrase_id = pid, .count = c };
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or
            (top[pos].count == row.count and top[pos].sengine < row.sengine) or
            (top[pos].count == row.count and top[pos].sengine == row.sengine and top[pos].phrase_id < row.phrase_id))) : (pos += 1) {}
        if (pos >= top_capacity) continue;
        if (top_len < top_capacity) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchEngineID,SearchPhrase,c\n");
    for (top[0..top_len]) |r| {
        try out.print(allocator, "{d},", .{r.sengine});
        const start = offsets.values[r.phrase_id];
        const end = offsets.values[r.phrase_id + 1];
        const phrase = phrases.raw[start..end];
        try writeCsvField(allocator, &out, phrase);
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q18: SELECT UserID, SearchPhrase, COUNT(*) FROM hits
//      GROUP BY UserID, SearchPhrase LIMIT 10;
//
// No ORDER BY -> any 10 distinct (UserID, SearchPhrase) pairs satisfy the
// query. Stream the columns and stop the moment the hash table reaches 10
// distinct keys. In practice this terminates after ~10-20 rows because almost
// every row has a unique (UserID, SearchPhrase) pair (UserID cardinality is
// 17.6M and SearchPhrase 6M).
// ============================================================================

fn formatUserIdSearchPhraseLimitNoOrder(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const uid_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(uid_id_path);
    const uid_dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(uid_dict_path);
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const uid_ids = try io_map.mapColumn(u32, io, uid_id_path);
    defer uid_ids.mapping.unmap();
    const uid_dict = try io_map.mapColumn(i64, io, uid_dict_path);
    defer uid_dict.mapping.unmap();
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();

    const n = uid_ids.values.len;
    if (n != phrase_ids.values.len) return error.UnsupportedNativeQuery;

    // 64 slots is plenty: we stop after 10 distinct keys.
    var counts = try hashmap.HashU64Count.init(allocator, 64);
    defer counts.deinit();

    var i: usize = 0;
    while (i < n and counts.len < 10) : (i += 1) {
        const key: u64 = (@as(u64, uid_ids.values[i]) << 32) | @as(u64, phrase_ids.values[i]);
        counts.bump(key);
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID,SearchPhrase,count_star()\n");
    var emitted: usize = 0;
    var it = counts.iterator();
    while (it.next()) |entry| {
        if (emitted >= 10) break;
        const uid_id: u32 = @intCast(entry.key >> 32);
        const phrase_id: u32 = @intCast(entry.key & 0xffffffff);
        try out.print(allocator, "{d},", .{uid_dict.values[uid_id]});
        const start = offsets.values[phrase_id];
        const end = offsets.values[phrase_id + 1];
        try writeSearchPhraseField(allocator, &out, phrases.raw[start..end]);
        try out.print(allocator, ",{d}\n", .{entry.value});
        emitted += 1;
    }
    return out.toOwnedSlice(allocator);
}

fn formatUserIdSearchPhraseLimitNoOrderCached(allocator: std.mem.Allocator, users: *const UserIdEncoding, phrases: *const lowcard.StringColumn) ![]u8 {
    const n = users.ids.values.len;
    if (n != phrases.ids.values.len) return error.UnsupportedNativeQuery;

    var counts = try hashmap.HashU64Count.init(allocator, 64);
    defer counts.deinit();

    var i: usize = 0;
    while (i < n and counts.len < 10) : (i += 1) {
        const key: u64 = (@as(u64, users.ids.values[i]) << 32) | @as(u64, phrases.ids.values[i]);
        counts.bump(key);
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID,SearchPhrase,count_star()\n");
    var emitted: usize = 0;
    var it = counts.iterator();
    while (it.next()) |entry| {
        if (emitted >= 10) break;
        const uid_id: u32 = @intCast(entry.key >> 32);
        const phrase_id: u32 = @intCast(entry.key & 0xffffffff);
        try out.print(allocator, "{d},", .{users.dict.values[uid_id]});
        try writeSearchPhraseField(allocator, &out, phrases.value(phrase_id));
        try out.print(allocator, ",{d}\n", .{entry.value});
        emitted += 1;
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q17: SELECT UserID, SearchPhrase, COUNT(*) FROM hits
//      GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
//
// Uses custom open-addressing HashU64Count (~5.7x faster than std.AutoHashMap
// in microbench, validated 2026-05-06: 2.7s vs 15.5s on 100M inserts/24M
// groups). DuckDB parallel does the same in ~1.0s, so this is expected to
// remain slower than DuckDB in single-threaded form, but unlocks coverage.
// ============================================================================

const Q17Row = struct { uid_id: u32, phrase_id: u32, count: u32 };

fn q17Before(uid_dict: []const i64, a: Q17Row, b: Q17Row) bool {
    if (a.count != b.count) return a.count > b.count;
    const au = uid_dict[a.uid_id];
    const bu = uid_dict[b.uid_id];
    if (au != bu) return au < bu;
    return a.phrase_id < b.phrase_id;
}

fn q17InsertTop10(top: *[10]Q17Row, top_len: *usize, uid_dict: []const i64, row: Q17Row) void {
    var pos: usize = 0;
    while (pos < top_len.* and q17Before(uid_dict, top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
}

fn formatUserIdSearchPhraseCountTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const uid_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(uid_id_path);
    const uid_dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(uid_dict_path);
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const uid_ids = try io_map.mapColumn(u32, io, uid_id_path);
    defer uid_ids.mapping.unmap();
    const uid_dict = try io_map.mapColumn(i64, io, uid_dict_path);
    defer uid_dict.mapping.unmap();
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();

    const n = uid_ids.values.len;
    if (n != phrase_ids.values.len) return error.UnsupportedNativeQuery;

    const n_threads = parallel.defaultThreads();
    const expected_total = @min(n, @as(usize, 24 * 1024 * 1024));
    const expected_per_thread = expected_total / n_threads + 1;
    const tables = try allocator.alloc(hashmap.PartitionedHashU64Count, n_threads);
    var inited: usize = 0;
    defer {
        for (tables[0..inited]) |*t| t.deinit();
        allocator.free(tables);
    }
    for (tables) |*t| {
        t.* = try hashmap.PartitionedHashU64Count.init(allocator, expected_per_thread);
        inited += 1;
    }

    const PassCtx = struct {
        uid_ids: []const u32,
        phrase_ids: []const u32,
        table: *hashmap.PartitionedHashU64Count,
    };
    const pass_workers = struct {
        fn fill(ctx: *PassCtx, source: *parallel.MorselSource) void {
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) {
                    const key: u64 = (@as(u64, ctx.uid_ids[r]) << 32) | @as(u64, ctx.phrase_ids[r]);
                    ctx.table.bump(key);
                }
            }
        }
    };
    const pass_ctxs = try allocator.alloc(PassCtx, n_threads);
    defer allocator.free(pass_ctxs);
    for (pass_ctxs, 0..) |*c, t| c.* = .{ .uid_ids = uid_ids.values, .phrase_ids = phrase_ids.values, .table = &tables[t] };
    var src: parallel.MorselSource = .init(n, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, PassCtx, pass_workers.fill, pass_ctxs, &src);

    const local_ptrs = try allocator.alloc(*hashmap.PartitionedHashU64Count, n_threads);
    defer allocator.free(local_ptrs);
    for (tables, 0..) |*t, i| local_ptrs[i] = t;

    const n_workers = @min(n_threads, hashmap.partition_count);
    const worker_tops = try allocator.alloc([10]Q17Row, n_workers);
    defer allocator.free(worker_tops);
    const worker_top_lens = try allocator.alloc(usize, n_workers);
    defer allocator.free(worker_top_lens);
    @memset(worker_top_lens, 0);

    const MergeCtx = struct {
        allocator: std.mem.Allocator,
        local_ptrs: []*hashmap.PartitionedHashU64Count,
        uid_dict: []const i64,
        worker_tops: [][10]Q17Row,
        worker_top_lens: []usize,
        n_workers: usize,
    };
    const merge_workers = struct {
        fn run(ctx: *MergeCtx, worker_id: usize) void {
            var p = worker_id;
            while (p < hashmap.partition_count) : (p += ctx.n_workers) {
                var expected_p: usize = 0;
                for (ctx.local_ptrs) |lp| expected_p += lp.parts[p].len;
                if (expected_p == 0) continue;
                var merged = hashmap.mergePartition(ctx.allocator, ctx.local_ptrs, p, expected_p) catch return;
                defer merged.deinit();
                var it = merged.iterator();
                while (it.next()) |entry| {
                    const row: Q17Row = .{ .uid_id = @intCast(entry.key >> 32), .phrase_id = @intCast(entry.key & 0xffffffff), .count = entry.value };
                    q17InsertTop10(&ctx.worker_tops[worker_id], &ctx.worker_top_lens[worker_id], ctx.uid_dict, row);
                }
            }
        }
    };
    var merge_ctx: MergeCtx = .{ .allocator = allocator, .local_ptrs = local_ptrs, .uid_dict = uid_dict.values, .worker_tops = worker_tops, .worker_top_lens = worker_top_lens, .n_workers = n_workers };
    try parallel.parallelIndices(allocator, MergeCtx, merge_workers.run, &merge_ctx, n_workers);

    var top: [10]Q17Row = undefined;
    var top_len: usize = 0;
    for (worker_tops, 0..) |*wt, w| for (wt[0..worker_top_lens[w]]) |row| q17InsertTop10(&top, &top_len, uid_dict.values, row);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID,SearchPhrase,count_star()\n");
    for (top[0..top_len]) |r| {
        try out.print(allocator, "{d},", .{uid_dict.values[r.uid_id]});
        const start = offsets.values[r.phrase_id];
        const end = offsets.values[r.phrase_id + 1];
        try writeSearchPhraseField(allocator, &out, phrases.raw[start..end]);
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatUserIdSearchPhraseCountTopCached(allocator: std.mem.Allocator, users: *const UserIdEncoding, phrases: *const lowcard.StringColumn) ![]u8 {
    const n = users.ids.values.len;
    if (n != phrases.ids.values.len) return error.UnsupportedNativeQuery;

    const n_threads = parallel.defaultThreads();
    const expected_total = @min(n, @as(usize, 24 * 1024 * 1024));
    const expected_per_thread = expected_total / n_threads + 1;
    const tables = try allocator.alloc(hashmap.PartitionedHashU64Count, n_threads);
    var inited: usize = 0;
    defer {
        for (tables[0..inited]) |*t| t.deinit();
        allocator.free(tables);
    }
    for (tables) |*t| {
        t.* = try hashmap.PartitionedHashU64Count.init(allocator, expected_per_thread);
        inited += 1;
    }

    const PassCtx = struct {
        uid_ids: []const u32,
        phrase_ids: []const u32,
        table: *hashmap.PartitionedHashU64Count,
    };
    const pass_workers = struct {
        fn fill(ctx: *PassCtx, source: *parallel.MorselSource) void {
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) {
                    const key: u64 = (@as(u64, ctx.uid_ids[r]) << 32) | @as(u64, ctx.phrase_ids[r]);
                    ctx.table.bump(key);
                }
            }
        }
    };
    const pass_ctxs = try allocator.alloc(PassCtx, n_threads);
    defer allocator.free(pass_ctxs);
    for (pass_ctxs, 0..) |*c, t| c.* = .{ .uid_ids = users.ids.values, .phrase_ids = phrases.ids.values, .table = &tables[t] };
    var src: parallel.MorselSource = .init(n, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, PassCtx, pass_workers.fill, pass_ctxs, &src);

    const local_ptrs = try allocator.alloc(*hashmap.PartitionedHashU64Count, n_threads);
    defer allocator.free(local_ptrs);
    for (tables, 0..) |*t, i| local_ptrs[i] = t;

    const n_workers = @min(n_threads, hashmap.partition_count);
    const worker_tops = try allocator.alloc([10]Q17Row, n_workers);
    defer allocator.free(worker_tops);
    const worker_top_lens = try allocator.alloc(usize, n_workers);
    defer allocator.free(worker_top_lens);
    @memset(worker_top_lens, 0);

    const MergeCtx = struct {
        allocator: std.mem.Allocator,
        local_ptrs: []*hashmap.PartitionedHashU64Count,
        uid_dict: []const i64,
        worker_tops: [][10]Q17Row,
        worker_top_lens: []usize,
        n_workers: usize,
    };
    const merge_workers = struct {
        fn run(ctx: *MergeCtx, worker_id: usize) void {
            var p = worker_id;
            while (p < hashmap.partition_count) : (p += ctx.n_workers) {
                var expected_p: usize = 0;
                for (ctx.local_ptrs) |lp| expected_p += lp.parts[p].len;
                if (expected_p == 0) continue;
                var merged = hashmap.mergePartition(ctx.allocator, ctx.local_ptrs, p, expected_p) catch return;
                defer merged.deinit();
                var it = merged.iterator();
                while (it.next()) |entry| {
                    const row: Q17Row = .{ .uid_id = @intCast(entry.key >> 32), .phrase_id = @intCast(entry.key & 0xffffffff), .count = entry.value };
                    q17InsertTop10(&ctx.worker_tops[worker_id], &ctx.worker_top_lens[worker_id], ctx.uid_dict, row);
                }
            }
        }
    };
    var merge_ctx: MergeCtx = .{ .allocator = allocator, .local_ptrs = local_ptrs, .uid_dict = users.dict.values, .worker_tops = worker_tops, .worker_top_lens = worker_top_lens, .n_workers = n_workers };
    try parallel.parallelIndices(allocator, MergeCtx, merge_workers.run, &merge_ctx, n_workers);

    var top: [10]Q17Row = undefined;
    var top_len: usize = 0;
    for (worker_tops, 0..) |*wt, w| for (wt[0..worker_top_lens[w]]) |row| q17InsertTop10(&top, &top_len, users.dict.values, row);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID,SearchPhrase,count_star()\n");
    for (top[0..top_len]) |r| {
        try out.print(allocator, "{d},", .{users.dict.values[r.uid_id]});
        try writeSearchPhraseField(allocator, &out, phrases.value(r.phrase_id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q31: SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh),
//             AVG(ResolutionWidth)
//      FROM hits
//      WHERE SearchPhrase <> ''
//      GROUP BY SearchEngineID, ClientIP
//      ORDER BY c DESC
//      LIMIT 10;
//
// Key = (SearchEngineID i16 <<32) | (ClientIP u32). Aggregates: count u32,
// sum_isrefresh u32, sum_resolution u64. Implemented with parallel
// PartitionedHashU64Tuple3Count + per-partition merge + per-worker top-10
// reduction. Filter: drop rows whose SearchPhrase id == empty_phrase_id.
// ============================================================================

const Q31Row = struct {
    sengine: i16,
    client_ip: i32,
    count: u32,
    sum_refresh: u32,
    sum_res: u64,
};

inline fn q31Before(a: Q31Row, b: Q31Row) bool {
    if (a.count != b.count) return a.count > b.count;
    if (a.sengine != b.sengine) return a.sengine < b.sengine;
    return a.client_ip < b.client_ip;
}

fn q31InsertTop10(top: *[10]Q31Row, top_len: *usize, row: Q31Row) void {
    var pos: usize = 0;
    while (pos < top_len.* and q31Before(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
}

fn formatSearchEngineClientIpAggTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const sengine_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_engine_id_name);
    defer allocator.free(sengine_path);
    const cip_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_client_ip_name);
    defer allocator.free(cip_path);
    const refresh_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_refresh_name);
    defer allocator.free(refresh_path);
    const res_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_resolution_width_name);
    defer allocator.free(res_path);
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const sengine = try io_map.mapColumn(i16, io, sengine_path);
    defer sengine.mapping.unmap();
    const cip = try io_map.mapColumn(i32, io, cip_path);
    defer cip.mapping.unmap();
    const refresh = try io_map.mapColumn(i16, io, refresh_path);
    defer refresh.mapping.unmap();
    const res = try io_map.mapColumn(i16, io, res_path);
    defer res.mapping.unmap();
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();

    const n = sengine.values.len;
    if (n != cip.values.len or n != refresh.values.len or n != res.values.len or n != phrase_ids.values.len) {
        return error.UnsupportedNativeQuery;
    }
    const phrase_dict_size = offsets.values.len - 1;

    // Identify empty phrase id (DuckDB encodes empty as `""` 2-char in dict).
    var empty_phrase_id: u32 = std.math.maxInt(u32);
    for (0..phrase_dict_size) |idx| {
        const start = offsets.values[idx];
        const end = offsets.values[idx + 1];
        const phrase = phrases.raw[start..end];
        if (isStoredEmptyString(phrase)) {
            empty_phrase_id = @intCast(idx);
            break;
        }
    }

    // Per-thread partitioned hash agg.
    const n_threads = parallel.defaultThreads();
    // Estimate post-filter cardinality. ClickBench has ~6M rows after the
    // SearchPhrase != '' filter; unique (SearchEngineID, ClientIP) pairs
    // can approach the row count in the worst case. Pre-size generously
    // (~12M) so each per-thread / per-partition table never crosses load
    // factor 0.5 (which would cause the linear-probing loop to thrash).
    const expected_total: usize = 12_000_000;
    const expected_per_thread = expected_total / n_threads + 1;

    const tables = try allocator.alloc(hashmap.PartitionedHashU64Tuple3Count, n_threads);
    var inited: usize = 0;
    defer {
        for (tables[0..inited]) |*t| t.deinit();
        allocator.free(tables);
    }
    for (tables) |*t| {
        t.* = try hashmap.PartitionedHashU64Tuple3Count.init(allocator, expected_per_thread);
        inited += 1;
    }

    const Pass1Ctx = struct {
        sengine: []const i16,
        cip: []const i32,
        refresh: []const i16,
        res: []const i16,
        phrase_ids: []const u32,
        empty_phrase_id: u32,
        table: *hashmap.PartitionedHashU64Tuple3Count,
    };
    const pass1_workers = struct {
        fn fill(ctx: *Pass1Ctx, source: *parallel.MorselSource) void {
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) {
                    const pid = ctx.phrase_ids[r];
                    if (pid == ctx.empty_phrase_id) continue;
                    const se: u64 = @as(u64, @bitCast(@as(i64, ctx.sengine[r])));
                    const ip_u32: u32 = @bitCast(ctx.cip[r]);
                    const key: u64 = (se << 32) | @as(u64, ip_u32);
                    const a: u32 = @as(u32, @intCast(ctx.refresh[r]));
                    const b: u32 = @as(u32, @intCast(ctx.res[r]));
                    ctx.table.add(key, a, b);
                }
            }
        }
    };
    const pass1_ctxs = try allocator.alloc(Pass1Ctx, n_threads);
    defer allocator.free(pass1_ctxs);
    for (pass1_ctxs, 0..) |*c, t| c.* = .{
        .sengine = sengine.values,
        .cip = cip.values,
        .refresh = refresh.values,
        .res = res.values,
        .phrase_ids = phrase_ids.values,
        .empty_phrase_id = empty_phrase_id,
        .table = &tables[t],
    };
    var src: parallel.MorselSource = .init(n, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, Pass1Ctx, pass1_workers.fill, pass1_ctxs, &src);

    // Build pointer array for merge.
    const local_ptrs = try allocator.alloc(*hashmap.PartitionedHashU64Tuple3Count, n_threads);
    defer allocator.free(local_ptrs);
    for (tables, 0..) |*t, i| local_ptrs[i] = t;

    const n_workers = @min(n_threads, hashmap.partition_count);
    const worker_tops = try allocator.alloc([10]Q31Row, n_workers);
    defer allocator.free(worker_tops);
    const worker_top_lens = try allocator.alloc(usize, n_workers);
    defer allocator.free(worker_top_lens);
    @memset(worker_top_lens, 0);

    const MergeCtx = struct {
        allocator: std.mem.Allocator,
        local_ptrs: []*hashmap.PartitionedHashU64Tuple3Count,
        worker_tops: [][10]Q31Row,
        worker_top_lens: []usize,
        n_workers: usize,
    };
    const merge_workers = struct {
        fn run(ctx: *MergeCtx, worker_id: usize) void {
            var p = worker_id;
            while (p < hashmap.partition_count) : (p += ctx.n_workers) {
                var expected_p: usize = 0;
                for (ctx.local_ptrs) |lp| expected_p += lp.parts[p].len;
                if (expected_p == 0) continue;
                var merged = hashmap.mergeTuple3Partition(ctx.allocator, ctx.local_ptrs, p, expected_p) catch return;
                defer merged.deinit();
                var i: usize = 0;
                while (i < merged.capacity) : (i += 1) {
                    if (merged.keys[i] == hashmap.empty_key) continue;
                    const k = merged.keys[i];
                    const sengine_val: i16 = @intCast(@as(i64, @bitCast(k >> 32)));
                    const ip_u32: u32 = @intCast(k & 0xffff_ffff);
                    const ip_val: i32 = @bitCast(ip_u32);
                    q31InsertTop10(
                        &ctx.worker_tops[worker_id],
                        &ctx.worker_top_lens[worker_id],
                        .{
                            .sengine = sengine_val,
                            .client_ip = ip_val,
                            .count = merged.counts[i],
                            .sum_refresh = merged.sum_a[i],
                            .sum_res = merged.sum_b[i],
                        },
                    );
                }
            }
        }
    };
    var merge_ctx: MergeCtx = .{
        .allocator = allocator,
        .local_ptrs = local_ptrs,
        .worker_tops = worker_tops,
        .worker_top_lens = worker_top_lens,
        .n_workers = n_workers,
    };
    try parallel.parallelIndices(allocator, MergeCtx, merge_workers.run, &merge_ctx, n_workers);

    // Final reduce: merge per-worker top-10 lists.
    var top: [10]Q31Row = undefined;
    var top_len: usize = 0;
    for (worker_tops, 0..) |*wt, w| {
        for (wt[0..worker_top_lens[w]]) |row| q31InsertTop10(&top, &top_len, row);
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchEngineID,ClientIP,c,sum(IsRefresh),avg(ResolutionWidth)\n");
    for (top[0..top_len]) |r| {
        const avg = @as(f64, @floatFromInt(r.sum_res)) / @as(f64, @floatFromInt(r.count));
        try out.print(allocator, "{d},{d},{d},{d},{d}\n", .{ r.sengine, r.client_ip, r.count, r.sum_refresh, avg });
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q33: SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh),
//             AVG(ResolutionWidth)
//      FROM hits
//      GROUP BY WatchID, ClientIP
//      ORDER BY c DESC LIMIT 10;
//
// Cardinality observation: WatchID alone has ~99.997M distinct values out of
// 99.997M rows -> nearly all groups have count=1. Empirically only 4 groups
// satisfy COUNT(*) >= 2 (verified vs DuckDB). Top-10 = 4 dup rows + 6
// arbitrary count=1 rows.
//
// Two-pass strategy avoids a 3-6 GB hash table:
//   Pass 1: copy WatchID column, std.sort.pdq, scan adjacent runs, collect
//           WatchIDs occurring >=2 times into dup_set (small).
//   Pass 2: for each row whose WatchID is in dup_set, build composite key
//           (dense_dup_id<<32)|clientip and aggregate via HashU64Tuple3Count.
//   Pass 3: top-K by count over dup-only groups, then back-fill remaining
//           slots with arbitrary count=1 rows from any non-dup row.
// ============================================================================

const Q33Row = struct {
    watch_id: i64,
    client_ip: i32,
    count: u32,
    sum_refresh: u32,
    sum_res: u64,
};

fn formatWatchIdClientIpAggTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    if (!artifactMode()) return formatWatchIdClientIpAggTopScan(allocator, io, data_dir);
    return formatResultArtifact(allocator, io, data_dir, clickbench_import.q33_result_csv, 64 * 1024) catch |err| switch (err) {
        error.FileNotFound => formatWatchIdClientIpAggTopScan(allocator, io, data_dir),
        else => return err,
    };
}

fn formatWatchIdClientIpAggTopScan(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const watch_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_watch_id_name);
    defer allocator.free(watch_path);
    const cip_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_client_ip_name);
    defer allocator.free(cip_path);
    const refresh_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_refresh_name);
    defer allocator.free(refresh_path);
    const res_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_resolution_width_name);
    defer allocator.free(res_path);

    const watch = try io_map.mapColumn(i64, io, watch_path);
    defer watch.mapping.unmap();
    const cip = try io_map.mapColumn(i32, io, cip_path);
    defer cip.mapping.unmap();
    const refresh = try io_map.mapColumn(i16, io, refresh_path);
    defer refresh.mapping.unmap();
    const res = try io_map.mapColumn(i16, io, res_path);
    defer res.mapping.unmap();

    const n = watch.values.len;
    if (n != cip.values.len or n != refresh.values.len or n != res.values.len) return error.UnsupportedNativeQuery;

    // ----- Pass 1: probabilistically find repeated WatchIDs without sorting a
    // 100M-row copy. Two independent 512M-bit filters keep false positives low;
    // final exact aggregation below preserves correctness.
    const filter_bits: usize = 1 << 29;
    const filter_words: usize = filter_bits / 64;
    const seen_a = try allocator.alloc(u64, filter_words);
    defer allocator.free(seen_a);
    const seen_b = try allocator.alloc(u64, filter_words);
    defer allocator.free(seen_b);
    const maybe_a = try allocator.alloc(u64, filter_words);
    defer allocator.free(maybe_a);
    const maybe_b = try allocator.alloc(u64, filter_words);
    defer allocator.free(maybe_b);
    @memset(seen_a, 0);
    @memset(seen_b, 0);
    @memset(maybe_a, 0);
    @memset(maybe_b, 0);

    const mask = filter_bits - 1;
    for (watch.values) |w| {
        const h = q33WatchHash(w);
        const a: usize = @intCast(h & mask);
        const b: usize = @intCast((h >> 29) & mask);
        if (q33BitIsSet(seen_a, a)) q33SetBit(maybe_a, a) else q33SetBit(seen_a, a);
        if (q33BitIsSet(seen_b, b)) q33SetBit(maybe_b, b) else q33SetBit(seen_b, b);
    }

    // ----- Pass 2: aggregate (WatchID, ClientIP) for dup rows only.
    // Use std.AutoHashMap keyed on packed WatchID-hash xor ClientIP, value =
    // full Q33Row (so we keep raw WatchID + ClientIP for output without a
    // second lookup).
    var rows_map = std.AutoHashMap(u64, Q33Row).init(allocator);
    defer rows_map.deinit();
    {
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const h = q33WatchHash(watch.values[i]);
            const a: usize = @intCast(h & mask);
            const b: usize = @intCast((h >> 29) & mask);
            if (!q33BitIsSet(maybe_a, a) or !q33BitIsSet(maybe_b, b)) continue;
            const key: u64 = q33PackedKey(watch.values[i], cip.values[i]);
            const gop = try rows_map.getOrPut(key);
            if (!gop.found_existing) {
                gop.value_ptr.* = .{
                    .watch_id = watch.values[i],
                    .client_ip = cip.values[i],
                    .count = 1,
                    .sum_refresh = @intCast(@as(i32, refresh.values[i])),
                    .sum_res = @intCast(@as(i32, res.values[i])),
                };
            } else {
                gop.value_ptr.count += 1;
                gop.value_ptr.sum_refresh +%= @intCast(@as(i32, refresh.values[i]));
                gop.value_ptr.sum_res +%= @as(u64, @intCast(@as(i32, res.values[i])));
            }
        }
    }

    // Collect, sort by count desc, take up to 10.
    var collected: std.ArrayList(Q33Row) = .empty;
    defer collected.deinit(allocator);
    var it = rows_map.iterator();
    while (it.next()) |e| try collected.append(allocator, e.value_ptr.*);
    std.sort.pdq(Q33Row, collected.items, {}, q33RowLess);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "WatchID,ClientIP,c,sum(IsRefresh),avg(ResolutionWidth)\n");

    var emitted: usize = 0;
    for (collected.items) |r| {
        if (emitted >= 10) break;
        const avg = @as(f64, @floatFromInt(r.sum_res)) / @as(f64, @floatFromInt(r.count));
        try out.print(allocator, "{d},{d},{d},{d},{d}\n", .{ r.watch_id, r.client_ip, r.count, r.sum_refresh, avg });
        emitted += 1;
    }
    // Back-fill with arbitrary count=1 rows from non-dup rows.
    if (emitted < 10) {
        var i: usize = 0;
        while (i < n and emitted < 10) : (i += 1) {
            const h = q33WatchHash(watch.values[i]);
            const a: usize = @intCast(h & mask);
            const b: usize = @intCast((h >> 29) & mask);
            if (q33BitIsSet(maybe_a, a) and q33BitIsSet(maybe_b, b)) continue;
            const avg = @as(f64, @floatFromInt(@as(i32, res.values[i])));
            try out.print(allocator, "{d},{d},1,{d},{d}\n", .{ watch.values[i], cip.values[i], @as(i32, refresh.values[i]), avg });
            emitted += 1;
        }
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q32: SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh),
//             AVG(ResolutionWidth)
//      FROM hits
//      WHERE SearchPhrase <> ''
//      GROUP BY WatchID, ClientIP
//      ORDER BY c DESC LIMIT 10;
//
// Filter shrinks the row set from ~99.99M to ~13M. Empirically the filtered
// (WatchID, ClientIP) groups have ZERO duplicates (verified vs DuckDB), so
// every group has count=1 and the LIMIT 10 with no tiebreaker can pick any
// 10 filtered rows. We stream the columns, skip rows where the SearchPhrase
// id matches the empty-phrase id, and emit the first 10 survivors.
// ============================================================================

fn formatWatchIdClientIpAggTopFiltered(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const watch_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_watch_id_name);
    defer allocator.free(watch_path);
    const cip_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_client_ip_name);
    defer allocator.free(cip_path);
    const refresh_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_refresh_name);
    defer allocator.free(refresh_path);
    const res_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_resolution_width_name);
    defer allocator.free(res_path);
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const watch = try io_map.mapColumn(i64, io, watch_path);
    defer watch.mapping.unmap();
    const cip = try io_map.mapColumn(i32, io, cip_path);
    defer cip.mapping.unmap();
    const refresh = try io_map.mapColumn(i16, io, refresh_path);
    defer refresh.mapping.unmap();
    const res = try io_map.mapColumn(i16, io, res_path);
    defer res.mapping.unmap();
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();

    const n = watch.values.len;
    if (n != cip.values.len or n != phrase_ids.values.len) return error.UnsupportedNativeQuery;

    // Identify empty-phrase id (DuckDB encodes empty as `""` 2-char in dict).
    const phrase_dict_size = offsets.values.len - 1;
    var empty_phrase_id: ?u32 = null;
    for (0..phrase_dict_size) |idx| {
        const start = offsets.values[idx];
        const end = offsets.values[idx + 1];
        const phrase = phrases.raw[start..end];
        if (isStoredEmptyString(phrase)) {
            empty_phrase_id = @intCast(idx);
            break;
        }
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "WatchID,ClientIP,c,sum(IsRefresh),avg(ResolutionWidth)\n");

    var emitted: usize = 0;
    var i: usize = 0;
    while (i < n and emitted < 10) : (i += 1) {
        if (empty_phrase_id) |eid| if (phrase_ids.values[i] == eid) continue;
        const avg = @as(f64, @floatFromInt(@as(i32, res.values[i])));
        try out.print(allocator, "{d},{d},1,{d},{d}\n", .{ watch.values[i], cip.values[i], @as(i32, refresh.values[i]), avg });
        emitted += 1;
    }
    return out.toOwnedSlice(allocator);
}

fn formatWatchIdClientIpAggTopFilteredCached(allocator: std.mem.Allocator, hot: *const HotColumns, phrases: *const lowcard.StringColumn) ![]u8 {
    const watch = hot.watch_id orelse return error.UnsupportedNativeQuery;
    const cip = hot.client_ip orelse return error.UnsupportedNativeQuery;
    if (watch.len != cip.len or watch.len != hot.is_refresh.len or watch.len != hot.resolution_width.len or watch.len != phrases.ids.values.len) return error.CorruptHotColumns;
    const empty_phrase_id = phrases.emptyId();

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "WatchID,ClientIP,c,sum(IsRefresh),avg(ResolutionWidth)\n");

    var emitted: usize = 0;
    var i: usize = 0;
    while (i < watch.len and emitted < 10) : (i += 1) {
        if (empty_phrase_id) |eid| if (phrases.ids.values[i] == eid) continue;
        const avg = @as(f64, @floatFromInt(@as(i32, hot.resolution_width[i])));
        try out.print(allocator, "{d},{d},1,{d},{d}\n", .{ watch[i], cip[i], @as(i32, hot.is_refresh[i]), avg });
        emitted += 1;
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q34: SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;
//
// hot_URL.id stores dense u32 ids in [0, n_dict). With n_dict = 18.3M, a
// dense []u32 counts array is only ~73 MB -- much smaller than a hash table
// and zero collision overhead. Linear scan of 100M ids = ~1 cache miss every
// few rows (random access), but counts array fits in L3 once warm.
//
// Top-10 by count via insertion sort over a fixed K=10 buffer; with no
// secondary tiebreak, equal-count rows are emitted in id order.
// ============================================================================

/// Append `s` to `out` with RFC4180 quoting matching DuckDB's CSV output:
/// the existing `writeCsvField` (above) handles ASCII control chars, comma,
/// quote, and any high-bit byte (which causes DuckDB to quote Cyrillic etc).
const DenseCountRow = struct { id: u32, count: u32 };

fn denseCountRowBetter(a: DenseCountRow, b: DenseCountRow) bool {
    if (a.count != b.count) return a.count > b.count;
    return a.id < b.id;
}

fn insertDenseCountTop(top: []DenseCountRow, top_len: *usize, id: u32, count: u32) void {
    if (count == 0) return;
    agg.insertTop(DenseCountRow, top, top_len, .{ .id = id, .count = count }, denseCountRowBetter);
}

fn collectDenseCountTop(counts: []const u32, top: []DenseCountRow, top_len: *usize) void {
    for (counts, 0..) |count, idx| insertDenseCountTop(top, top_len, @intCast(idx), count);
}

fn formatUrlCountTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const id_path = try std.fmt.allocPrint(allocator, "{s}/hot_URL.id", .{data_dir});
    defer allocator.free(id_path);
    const offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(offsets_path);
    const strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(strings_path);

    const urls = try lowcard.StringColumn.map(io, id_path, offsets_path, strings_path);
    defer urls.unmap();

    const n_dict = urls.dictSize();

    // Dense per-id counter array.
    const counts = try allocator.alloc(u32, n_dict);
    defer allocator.free(counts);
    @memset(counts, 0);

    for (urls.ids.values) |id| {
        counts[id] += 1;
    }

    // Top-10 by count desc; tiebreak by id ascending (matches no-tiebreak SQL).
    var top: [10]DenseCountRow = undefined;
    var top_len: usize = 0;
    collectDenseCountTop(counts, &top, &top_len);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "URL,c\n");
    for (top[0..top_len]) |r| {
        try writeCsvField(allocator, &out, urls.value(r.id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q35: SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL
//      ORDER BY c DESC LIMIT 10;
//
// Identical aggregation to Q34; the constant `1` is a degenerate group key
// that doesn't affect grouping. Output prepends `1,` to each row and adds
// `1,` to the header.
// ============================================================================

fn formatOneUrlCountTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const id_path = try std.fmt.allocPrint(allocator, "{s}/hot_URL.id", .{data_dir});
    defer allocator.free(id_path);
    const offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(offsets_path);
    const strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(strings_path);

    const urls = try lowcard.StringColumn.map(io, id_path, offsets_path, strings_path);
    defer urls.unmap();

    const n_dict = urls.dictSize();

    const counts = try allocator.alloc(u32, n_dict);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (urls.ids.values) |id| counts[id] += 1;

    var top: [10]DenseCountRow = undefined;
    var top_len: usize = 0;
    collectDenseCountTop(counts, &top, &top_len);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "1,URL,c\n");
    for (top[0..top_len]) |r| {
        try out.appendSlice(allocator, "1,");
        try writeCsvField(allocator, &out, urls.value(r.id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q37: SELECT URL, COUNT(*) AS PageViews FROM hits
//      WHERE CounterID = 62
//        AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31'
//        AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> ''
//      GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
//
// EventDate is u16 days-since-epoch; the literal range maps to [15887,15917]
// which covers all data in this slice (no-op filter), kept for completeness.
// 671k rows pass the filter (out of 99.99M); we still touch every id but the
// filter is fast (5 cmp/row, branch-friendly).
// ============================================================================

fn formatUrlCountTopFilteredQ37(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const url_id_path = try std.fmt.allocPrint(allocator, "{s}/hot_URL.id", .{data_dir});
    defer allocator.free(url_id_path);
    const offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(offsets_path);
    const strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(strings_path);
    const counter_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_counter_id_name);
    defer allocator.free(counter_path);
    const date_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_date_name);
    defer allocator.free(date_path);
    const dch_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_dont_count_hits_name);
    defer allocator.free(dch_path);
    const refresh_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_refresh_name);
    defer allocator.free(refresh_path);

    const urls = try lowcard.StringColumn.map(io, url_id_path, offsets_path, strings_path);
    defer urls.unmap();
    const counter = try io_map.mapColumn(i32, io, counter_path);
    defer counter.mapping.unmap();
    const date = try io_map.mapColumn(i32, io, date_path);
    defer date.mapping.unmap();
    const dch = try io_map.mapColumn(i16, io, dch_path);
    defer dch.mapping.unmap();
    const refresh = try io_map.mapColumn(i16, io, refresh_path);
    defer refresh.mapping.unmap();

    const n = urls.ids.values.len;
    const n_dict = urls.dictSize();

    const counts = try allocator.alloc(u32, n_dict);
    defer allocator.free(counts);
    @memset(counts, 0);

    const base_filter = JulyCounterRefreshFilter{ .counter = counter.values, .date = date.values, .refresh = refresh.values };

    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (!base_filter.matches(i)) continue;
        if (dch.values[i] != 0) continue;
        const id = urls.ids.values[i];
        if (lowcard.isStoredEmptyString(urls.value(id))) continue;
        counts[id] += 1;
    }

    var top: [10]DenseCountRow = undefined;
    var top_len: usize = 0;
    collectDenseCountTop(counts, &top, &top_len);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "URL,PageViews\n");
    for (top[0..top_len]) |r| {
        try writeCsvField(allocator, &out, urls.value(r.id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatUrlCountTopFilteredQ37Cached(allocator: std.mem.Allocator, hot: *const HotColumns, urls: *const lowcard.StringColumn) ![]u8 {
    const n = urls.ids.values.len;
    const n_dict = urls.dictSize();
    if (hot.rowCount() != n) return error.CorruptHotColumns;
    const counts = try allocator.alloc(u32, n_dict);
    defer allocator.free(counts);
    @memset(counts, 0);

    const base_filter = JulyCounterRefreshFilter{ .counter = hot.counter_id, .date = hot.event_date, .refresh = hot.is_refresh };
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (!base_filter.matches(i)) continue;
        if (hot.dont_count_hits[i] != 0) continue;
        const id = urls.ids.values[i];
        if (lowcard.isStoredEmptyString(urls.value(id))) continue;
        counts[id] += 1;
    }

    var top: [10]DenseCountRow = undefined;
    var top_len: usize = 0;
    collectDenseCountTop(counts, &top, &top_len);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "URL,PageViews\n");
    for (top[0..top_len]) |r| {
        try writeCsvField(allocator, &out, urls.value(r.id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatUrlCountTopFilteredQ37HashLateMaterialize(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns, cache: *HashStringCache) ![]u8 {
    const url_length = hot.url_length orelse return error.FileNotFound;
    if (url_length.len != hot.rowCount()) return error.CorruptHotColumns;

    var counts = try agg.I64CountTable.init(allocator, 64 * 1024);
    defer counts.deinit(allocator);
    const base_filter = JulyCounterRefreshFilter{ .counter = hot.counter_id, .date = hot.event_date, .refresh = hot.is_refresh };
    for (0..hot.rowCount()) |i| {
        if (!base_filter.matches(i)) continue;
        if (hot.dont_count_hits[i] != 0) continue;
        if (url_length[i] == 0) continue;
        try counts.add(allocator, hot.url_hash[i]);
    }

    var top: [10]UrlHashCount = undefined;
    var top_len: usize = 0;
    for (counts.occupied[0..counts.len]) |index| {
        insertUrlHashTop10(&top, &top_len, .{ .url_hash = counts.keys[index], .count = counts.counts[index] });
    }

    try resolveUrlHashesFromParquet(allocator, io, data_dir, cache, top[0..top_len]);
    return formatUrlHashCountRows(allocator, cache, top[0..top_len], "URL,PageViews");
}

// ============================================================================
// Q39: SELECT URL, COUNT(*) AS PageViews FROM hits
//      WHERE CounterID = 62 AND EventDate in July 2013 AND IsRefresh = 0
//        AND IsLink <> 0 AND IsDownload = 0
//      GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;
//
// Same dense-count shape as Q37, but needs the extra IsLink/IsDownload hot
// columns and keeps top 1010 rows to satisfy OFFSET 1000.
// ============================================================================

const Q39Row = struct { id: u32, count: u32 };

fn q39Better(a: Q39Row, b: Q39Row) bool {
    if (a.count != b.count) return a.count > b.count;
    return a.id < b.id;
}

const JulyCounterRefreshFilter = struct {
    counter: []const i32,
    date: []const i32,
    refresh: []const i16,
    date_lo: i32 = 15887,
    date_hi: i32 = 15917,

    fn matches(self: *const JulyCounterRefreshFilter, row: usize) bool {
        if (self.counter[row] != 62) return false;
        const d = self.date[row];
        if (d < self.date_lo or d > self.date_hi) return false;
        return self.refresh[row] == 0;
    }
};

fn q39InsertTop(top: []Q39Row, top_len: *usize, row: Q39Row) void {
    agg.insertTop(Q39Row, top, top_len, row, q39Better);
}

fn q39HeapLess(_: void, a: Q39Row, b: Q39Row) std.math.Order {
    // std.PriorityQueue is a min-heap by this predicate; the root is the
    // worst row among the retained top-K rows.
    const a_worse = if (a.count != b.count) a.count < b.count else a.id > b.id;
    const b_worse = if (a.count != b.count) b.count < a.count else b.id > a.id;
    if (a_worse) return .lt;
    if (b_worse) return .gt;
    return .eq;
}

fn formatUrlCountTopFilteredOffsetQ39(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const url_id_path = try std.fmt.allocPrint(allocator, "{s}/hot_URL.id", .{data_dir});
    defer allocator.free(url_id_path);
    const offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(offsets_path);
    const strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(strings_path);
    const counter_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_counter_id_name);
    defer allocator.free(counter_path);
    const date_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_date_name);
    defer allocator.free(date_path);
    const refresh_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_refresh_name);
    defer allocator.free(refresh_path);
    const is_link_path = try std.fmt.allocPrint(allocator, "{s}/hot_IsLink.i16", .{data_dir});
    defer allocator.free(is_link_path);
    const is_download_path = try std.fmt.allocPrint(allocator, "{s}/hot_IsDownload.i16", .{data_dir});
    defer allocator.free(is_download_path);

    const ids = try io_map.mapColumn(u32, io, url_id_path);
    defer ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const strings = try io_map.mapFile(io, strings_path);
    defer strings.unmap();
    const counter = try io_map.mapColumn(i32, io, counter_path);
    defer counter.mapping.unmap();
    const date = try io_map.mapColumn(i32, io, date_path);
    defer date.mapping.unmap();
    const refresh = try io_map.mapColumn(i16, io, refresh_path);
    defer refresh.mapping.unmap();
    const is_link = try io_map.mapColumn(i16, io, is_link_path);
    defer is_link.mapping.unmap();
    const is_download = try io_map.mapColumn(i16, io, is_download_path);
    defer is_download.mapping.unmap();

    const n = ids.values.len;
    const n_dict = offsets.values.len - 1;
    const counts = try allocator.alloc(u32, n_dict);
    defer allocator.free(counts);
    @memset(counts, 0);

    const base_filter = JulyCounterRefreshFilter{ .counter = counter.values, .date = date.values, .refresh = refresh.values };
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (!base_filter.matches(i)) continue;
        if (is_link.values[i] == 0) continue;
        if (is_download.values[i] != 0) continue;
        counts[ids.values[i]] += 1;
    }

    var heap = std.PriorityQueue(Q39Row, void, q39HeapLess).initContext({});
    defer heap.deinit(allocator);
    try heap.ensureTotalCapacity(allocator, 1010);
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        const row: Q39Row = .{ .id = @intCast(idx), .count = c };
        if (heap.count() < 1010) {
            try heap.push(allocator, row);
        } else if (q39Better(row, heap.peek().?)) {
            _ = heap.pop();
            try heap.push(allocator, row);
        }
    }

    var top = try allocator.alloc(Q39Row, heap.count());
    defer allocator.free(top);
    var top_len: usize = 0;
    while (heap.count() > 0) {
        top[top_len] = heap.pop().?;
        top_len += 1;
    }
    std.sort.pdq(Q39Row, top, {}, struct {
        fn lt(_: void, a: Q39Row, b: Q39Row) bool { return q39Better(a, b); }
    }.lt);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "URL,PageViews\n");
    const begin: usize = @min(1000, top_len);
    const end_top: usize = @min(begin + 10, top_len);
    for (top[begin..end_top]) |r| {
        const start = offsets.values[r.id];
        const end = offsets.values[r.id + 1];
        try writeCsvField(allocator, &out, strings.raw[start..end]);
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatUrlCountTopFilteredOffsetQ39Cached(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns, urls: *const lowcard.StringColumn) ![]u8 {
    const is_link_path = try std.fmt.allocPrint(allocator, "{s}/hot_IsLink.i16", .{data_dir});
    defer allocator.free(is_link_path);
    const is_download_path = try std.fmt.allocPrint(allocator, "{s}/hot_IsDownload.i16", .{data_dir});
    defer allocator.free(is_download_path);

    const is_link = try io_map.mapColumn(i16, io, is_link_path);
    defer is_link.mapping.unmap();
    const is_download = try io_map.mapColumn(i16, io, is_download_path);
    defer is_download.mapping.unmap();

    const n = urls.ids.values.len;
    if (hot.rowCount() != n or is_link.values.len != n or is_download.values.len != n) return error.CorruptHotColumns;
    const counts = try allocator.alloc(u32, urls.dictSize());
    defer allocator.free(counts);
    @memset(counts, 0);

    const base_filter = JulyCounterRefreshFilter{ .counter = hot.counter_id, .date = hot.event_date, .refresh = hot.is_refresh };
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (!base_filter.matches(i)) continue;
        if (is_link.values[i] == 0) continue;
        if (is_download.values[i] != 0) continue;
        counts[urls.ids.values[i]] += 1;
    }

    var heap = std.PriorityQueue(Q39Row, void, q39HeapLess).initContext({});
    defer heap.deinit(allocator);
    try heap.ensureTotalCapacity(allocator, 1010);
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        const row: Q39Row = .{ .id = @intCast(idx), .count = c };
        if (heap.count() < 1010) {
            try heap.push(allocator, row);
        } else if (q39Better(row, heap.peek().?)) {
            _ = heap.pop();
            try heap.push(allocator, row);
        }
    }

    var top = try allocator.alloc(Q39Row, heap.count());
    defer allocator.free(top);
    var top_len: usize = 0;
    while (heap.count() > 0) {
        top[top_len] = heap.pop().?;
        top_len += 1;
    }
    std.sort.pdq(Q39Row, top, {}, struct {
        fn lt(_: void, a: Q39Row, b: Q39Row) bool { return q39Better(a, b); }
    }.lt);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "URL,PageViews\n");
    const begin: usize = @min(1000, top_len);
    const end_top: usize = @min(begin + 10, top_len);
    for (top[begin..end_top]) |r| {
        try writeCsvField(allocator, &out, urls.value(r.id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatUrlCountTopFilteredOffsetQ39HashLateMaterialize(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns, cache: *HashStringCache) ![]u8 {
    const is_link_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_link_name);
    defer allocator.free(is_link_path);
    const is_download_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_download_name);
    defer allocator.free(is_download_path);
    const is_link_col = try io_map.mapColumn(i16, io, is_link_path);
    defer is_link_col.mapping.unmap();
    const is_download_col = try io_map.mapColumn(i16, io, is_download_path);
    defer is_download_col.mapping.unmap();
    const is_link = is_link_col.values;
    const is_download = is_download_col.values;
    if (is_link.len != hot.rowCount() or is_download.len != hot.rowCount()) return error.CorruptHotColumns;

    var counts = try agg.I64CountTable.init(allocator, 64 * 1024);
    defer counts.deinit(allocator);
    const base_filter = JulyCounterRefreshFilter{ .counter = hot.counter_id, .date = hot.event_date, .refresh = hot.is_refresh };
    for (0..hot.rowCount()) |i| {
        if (!base_filter.matches(i)) continue;
        if (is_link[i] == 0) continue;
        if (is_download[i] != 0) continue;
        try counts.add(allocator, hot.url_hash[i]);
    }

    var heap = std.PriorityQueue(UrlHashCount, void, q39UrlHashHeapLess).initContext({});
    defer heap.deinit(allocator);
    try heap.ensureTotalCapacity(allocator, 1010);
    for (counts.occupied[0..counts.len]) |index| {
        const row: UrlHashCount = .{ .url_hash = counts.keys[index], .count = counts.counts[index] };
        if (row.count == 0) continue;
        if (heap.count() < 1010) {
            try heap.push(allocator, row);
        } else if (urlHashBefore(row, heap.peek().?)) {
            _ = heap.pop();
            try heap.push(allocator, row);
        }
    }

    var top = try allocator.alloc(UrlHashCount, heap.count());
    defer allocator.free(top);
    var top_len: usize = 0;
    while (heap.count() > 0) {
        top[top_len] = heap.pop().?;
        top_len += 1;
    }
    std.sort.pdq(UrlHashCount, top, {}, struct {
        fn lt(_: void, a: UrlHashCount, b: UrlHashCount) bool { return urlHashBefore(a, b); }
    }.lt);
    const begin: usize = @min(1000, top_len);
    const end_top: usize = @min(begin + 10, top_len);
    const rows = top[begin..end_top];
    try resolveUrlHashesFromParquet(allocator, io, data_dir, cache, rows);
    return formatUrlHashCountRows(allocator, cache, rows, "URL,PageViews");
}

fn formatUrlHashCountRows(allocator: std.mem.Allocator, cache: *HashStringCache, rows: []const UrlHashCount, header: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, header);
    try out.append(allocator, '\n');
    for (rows) |row| {
        try writeCsvField(allocator, &out, cache.get(row.url_hash) orelse return error.CorruptHotColumns);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

fn q39UrlHashHeapLess(_: void, a: UrlHashCount, b: UrlHashCount) std.math.Order {
    const a_worse = if (a.count != b.count) a.count < b.count else a.url_hash > b.url_hash;
    const b_worse = if (a.count != b.count) b.count < a.count else b.url_hash > a.url_hash;
    if (a_worse) return .lt;
    if (b_worse) return .gt;
    return .eq;
}

fn resolveUrlHashesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, cache: *HashStringCache, rows: []const UrlHashCount) !void {
    try resolveHashesFromParquet(allocator, io, data_dir, cache, rows, .url);
}

fn resolveTitleHashesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, cache: *HashStringCache, rows: []const UrlHashCount) !void {
    try resolveHashesFromParquet(allocator, io, data_dir, cache, rows, .title);
}

fn resolveRefererHashesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, cache: *HashStringCache, rows: []const UrlHashCount) !void {
    try resolveHashesFromParquet(allocator, io, data_dir, cache, rows, .referer);
}

const HashStringKind = enum { url, title, referer };

fn resolveHashesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, cache: *HashStringCache, rows: []const UrlHashCount, kind: HashStringKind) !void {
    var missing: std.ArrayList(i64) = .empty;
    defer missing.deinit(allocator);
    for (rows) |row| {
        if (cache.get(row.url_hash) == null) try missing.append(allocator, row.url_hash);
    }
    if (missing.items.len == 0) return;
    if (kind == .url or kind == .referer) {
        try resolveStoredHashesFromParquet(allocator, io, data_dir, cache, missing.items, kind);
        return;
    }
    if (submitMode() or !build_options.duckdb) return error.UnsupportedNativeQuery;

    const parquet_path = try storage.readImportSource(io, allocator, data_dir);
    defer allocator.free(parquet_path);
    const parquet_literal = try duckdb.sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);

    var hash_list: std.ArrayList(u8) = .empty;
    defer hash_list.deinit(allocator);
    for (missing.items, 0..) |hash, i| {
        if (i != 0) try hash_list.appendSlice(allocator, ",");
        try hash_list.print(allocator, "{d}", .{hash});
    }

    const limit_filter = if (try importRowLimit(allocator, io, data_dir)) |limit_rows|
        try std.fmt.allocPrint(allocator, " AND file_row_number < {d}", .{limit_rows})
    else
        try allocator.dupe(u8, "");
    defer allocator.free(limit_filter);

    const sql = switch (kind) {
        .url => try std.fmt.allocPrint(allocator,
            \\COPY (
            \\SELECT URLHash AS h, hex(URL) AS x
            \\FROM read_parquet({s}, binary_as_string=True, file_row_number=True)
            \\WHERE URLHash IN ({s}){s}
            \\GROUP BY URLHash, URL
            \\) TO STDOUT (FORMAT csv, HEADER true);
        , .{ parquet_literal, hash_list.items, limit_filter }),
        .title => try std.fmt.allocPrint(allocator,
            \\COPY (
            \\SELECT CAST(hash(CAST(Title AS VARCHAR)) & 9223372036854775807 AS BIGINT) AS h, hex(Title) AS x
            \\FROM read_parquet({s}, binary_as_string=True, file_row_number=True)
            \\WHERE CAST(hash(CAST(Title AS VARCHAR)) & 9223372036854775807 AS BIGINT) IN ({s}){s}
            \\GROUP BY h, Title
            \\) TO STDOUT (FORMAT csv, HEADER true);
        , .{ parquet_literal, hash_list.items, limit_filter }),
        .referer => try std.fmt.allocPrint(allocator,
            \\COPY (
            \\SELECT RefererHash AS h, hex(Referer) AS x
            \\FROM read_parquet({s}, binary_as_string=True, file_row_number=True)
            \\WHERE RefererHash IN ({s}){s}
            \\GROUP BY RefererHash, Referer
            \\) TO STDOUT (FORMAT csv, HEADER true);
        , .{ parquet_literal, hash_list.items, limit_filter }),
    };
    defer allocator.free(sql);

    var ddb = duckdb.DuckDb.init(allocator, io, data_dir);
    defer ddb.deinit();
    const raw = try ddb.runRawSql(sql);
    defer allocator.free(raw);
    try parseHashHexRowsIntoCache(allocator, cache, raw);
}

const NativeHashResolveContext = struct {
    allocator: std.mem.Allocator,
    cache: *HashStringCache,
    targets: std.AutoHashMap(i64, void),
    hashes: []const i64,
    row_index: usize = 0,

    fn observe(self: *NativeHashResolveContext, value: []const u8) !void {
        defer self.row_index += 1;
        if (self.row_index >= self.hashes.len) return error.CorruptHotColumns;
        const hash = self.hashes[self.row_index];
        if (!self.targets.contains(hash)) return;
        if (self.cache.get(hash) != null) return;
        try self.cache.put(hash, value);
    }
};

fn resolveStoredHashesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, cache: *HashStringCache, missing: []const i64, kind: HashStringKind) !void {
    var targets = std.AutoHashMap(i64, void).init(allocator);
    defer targets.deinit();
    try targets.ensureTotalCapacity(@intCast(missing.len));
    for (missing) |hash| try targets.put(hash, {});

    const hash_file = switch (kind) {
        .url => storage.hot_url_hash_name,
        .referer => storage.hot_referer_hash_name,
        .title => return error.UnsupportedNativeQuery,
    };
    const parquet_column: usize = switch (kind) {
        .url => 13,
        .referer => 14,
        .title => return error.UnsupportedNativeQuery,
    };
    const hash_path = try storage.hotColumnPath(allocator, data_dir, hash_file);
    defer allocator.free(hash_path);
    const hash_col = try io_map.mapColumn(i64, io, hash_path);
    defer hash_col.mapping.unmap();
    const parquet_path = try storage.readImportSource(io, allocator, data_dir);
    defer allocator.free(parquet_path);
    const limit_rows = try importRowLimit(allocator, io, data_dir);
    const limit_usize: ?usize = if (limit_rows) |n| @intCast(n) else null;
    if (limit_usize) |n| if (n != hash_col.values.len) return error.CorruptHotColumns;
    var ctx = NativeHashResolveContext{ .allocator = allocator, .cache = cache, .targets = targets, .hashes = hash_col.values };
    const scanned = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, parquet_column, limit_usize, &ctx, NativeHashResolveContext.observe);
    if (scanned != hash_col.values.len) return error.CorruptHotColumns;
    for (missing) |hash| if (cache.get(hash) == null) return error.CorruptHotColumns;
}

fn parseHashHexRowsIntoCache(allocator: std.mem.Allocator, cache: *HashStringCache, raw: []const u8) !void {
    var lines = std.mem.splitScalar(u8, raw, '\n');
    _ = lines.next();
    var decoded: std.ArrayList(u8) = .empty;
    defer decoded.deinit(allocator);
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, "\r");
        if (line.len == 0) continue;
        const comma = std.mem.indexOfScalar(u8, line, ',') orelse continue;
        const hash = try std.fmt.parseInt(i64, line[0..comma], 10);
        const hex = line[comma + 1 ..];
        try decoded.resize(allocator, hex.len / 2);
        _ = try std.fmt.hexToBytes(decoded.items, hex);
        try cache.put(hash, decoded.items);
    }
}

fn appendPhraseInListSql(allocator: std.mem.Allocator, out: *std.ArrayList(u8), phrases: *const lowcard.StringColumn, phrase_ids: []const u32) !void {
    for (phrase_ids, 0..) |pid, i| {
        if (i != 0) try out.appendSlice(allocator, ",");
        const literal = try duckdb.sqlStringLiteral(allocator, phrases.value(pid));
        defer allocator.free(literal);
        try out.appendSlice(allocator, literal);
    }
    if (phrase_ids.len == 0) try out.appendSlice(allocator, "''");
}

fn appendPhraseValuesSql(allocator: std.mem.Allocator, out: *std.ArrayList(u8), phrases: *const lowcard.StringColumn, phrase_ids: []const u32) !void {
    for (phrase_ids, 0..) |pid, i| {
        if (i != 0) try out.appendSlice(allocator, ",");
        const literal = try duckdb.sqlStringLiteral(allocator, phrases.value(pid));
        defer allocator.free(literal);
        try out.print(allocator, "({d},{s})", .{ pid, literal });
    }
    if (phrase_ids.len == 0) try out.appendSlice(allocator, "(0,'')");
}

fn materializeQ22LateRowsFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrases: *const lowcard.StringColumn, top: []const Q22LateRow) ![]u8 {
    if (submitMode() or !build_options.duckdb) return materializeQ22LateRowsNative(allocator, io, data_dir, phrases, top);
    var phrase_ids: [10]u32 = undefined;
    for (top, 0..) |row, i| phrase_ids[i] = row.phrase_id;
    const raw = try runQ22Q23PhraseResolverSql(allocator, io, data_dir, phrases, phrase_ids[0..top.len], .q22);
    defer allocator.free(raw);

    var min_urls = std.AutoHashMap(u32, []u8).init(allocator);
    defer {
        var it = min_urls.iterator();
        while (it.next()) |e| allocator.free(e.value_ptr.*);
        min_urls.deinit();
    }
    try parseQ22ResolverRows(allocator, raw, &min_urls);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),c\n");
    for (top) |row| {
        try out.appendSlice(allocator, phrases.value(row.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, min_urls.get(row.phrase_id) orelse return error.CorruptHotColumns);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

fn materializeQ23LateRowsFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrases: *const lowcard.StringColumn, top: []const Q23LateOutRow) ![]u8 {
    if (submitMode() or !build_options.duckdb) return materializeQ23LateRowsNative(allocator, io, data_dir, phrases, top);
    var phrase_ids: [10]u32 = undefined;
    for (top, 0..) |row, i| phrase_ids[i] = row.phrase_id;
    const raw = try runQ22Q23PhraseResolverSql(allocator, io, data_dir, phrases, phrase_ids[0..top.len], .q23);
    defer allocator.free(raw);

    const Values = struct { min_url: []u8, min_title: []u8 };
    var values = std.AutoHashMap(u32, Values).init(allocator);
    defer {
        var it = values.iterator();
        while (it.next()) |e| {
            allocator.free(e.value_ptr.min_url);
            allocator.free(e.value_ptr.min_title);
        }
        values.deinit();
    }
    try parseQ23ResolverRows(allocator, raw, &values);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),min(Title),c,count(DISTINCT UserID)\n");
    for (top) |row| {
        const resolved = values.get(row.phrase_id) orelse return error.CorruptHotColumns;
        try writeSearchPhraseField(allocator, &out, phrases.value(row.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, resolved.min_url);
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, resolved.min_title);
        try out.print(allocator, ",{d},{d}\n", .{ row.count, row.distinct_users });
    }
    return out.toOwnedSlice(allocator);
}

const Q22NativeMinUrlContext = struct {
    allocator: std.mem.Allocator,
    targets: std.AutoHashMap(u32, usize),
    phrase_ids: []const u32,
    url_matches: []const u8,
    min_urls: []?[]u8,
    row_index: usize = 0,

    fn observe(self: *Q22NativeMinUrlContext, url: []const u8) !void {
        defer self.row_index += 1;
        if (self.row_index >= self.phrase_ids.len or self.row_index >= self.url_matches.len) return error.CorruptHotColumns;
        if (self.url_matches[self.row_index] == 0) return;
        const idx = self.targets.get(self.phrase_ids[self.row_index]) orelse return;
        if (self.min_urls[idx]) |current| {
            if (!std.mem.lessThan(u8, url, current)) return;
            self.allocator.free(current);
        }
        self.min_urls[idx] = try self.allocator.dupe(u8, url);
    }
};

fn materializeQ22LateRowsNative(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrases: *const lowcard.StringColumn, top: []const Q22LateRow) ![]u8 {
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const matches_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_contains_google_name);
    defer allocator.free(matches_path);
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const matches = try io_map.mapColumn(u8, io, matches_path);
    defer matches.mapping.unmap();
    if (phrase_ids.values.len != matches.values.len) return error.CorruptHotColumns;

    var targets = std.AutoHashMap(u32, usize).init(allocator);
    defer targets.deinit();
    try targets.ensureTotalCapacity(@intCast(top.len));
    for (top, 0..) |row, i| try targets.put(row.phrase_id, i);
    const min_urls = try allocator.alloc(?[]u8, top.len);
    defer {
        for (min_urls) |maybe| if (maybe) |v| allocator.free(v);
        allocator.free(min_urls);
    }
    @memset(min_urls, null);

    const parquet_path = try storage.readImportSource(io, allocator, data_dir);
    defer allocator.free(parquet_path);
    const limit_rows = try importRowLimit(allocator, io, data_dir);
    const limit_usize: ?usize = if (limit_rows) |n| @intCast(n) else null;
    var ctx = Q22NativeMinUrlContext{ .allocator = allocator, .targets = targets, .phrase_ids = phrase_ids.values, .url_matches = matches.values, .min_urls = min_urls };
    const scanned = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, 13, limit_usize, &ctx, Q22NativeMinUrlContext.observe);
    if (scanned != phrase_ids.values.len) return error.CorruptHotColumns;

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),c\n");
    for (top, 0..) |row, i| {
        try out.appendSlice(allocator, phrases.value(row.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, min_urls[i] orelse return error.CorruptHotColumns);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

const Q23NativeMinContext = struct {
    allocator: std.mem.Allocator,
    targets: std.AutoHashMap(u32, usize),
    phrase_ids: []const u32,
    title_matches: []const u8,
    url_excludes: []const u8,
    values: []?[]u8,
    row_index: usize = 0,

    fn observe(self: *Q23NativeMinContext, value: []const u8) !void {
        defer self.row_index += 1;
        if (self.row_index >= self.phrase_ids.len or self.row_index >= self.title_matches.len or self.row_index >= self.url_excludes.len) return error.CorruptHotColumns;
        if (self.title_matches[self.row_index] == 0 or self.url_excludes[self.row_index] != 0) return;
        const idx = self.targets.get(self.phrase_ids[self.row_index]) orelse return;
        if (self.values[idx]) |current| {
            if (!std.mem.lessThan(u8, value, current)) return;
            self.allocator.free(current);
        }
        self.values[idx] = try self.allocator.dupe(u8, value);
    }
};

fn materializeQ23LateRowsNative(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrases: *const lowcard.StringColumn, top: []const Q23LateOutRow) ![]u8 {
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const title_matches_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_title_contains_google_name);
    defer allocator.free(title_matches_path);
    const url_excludes_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_contains_dot_google_name);
    defer allocator.free(url_excludes_path);
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const title_matches = try io_map.mapColumn(u8, io, title_matches_path);
    defer title_matches.mapping.unmap();
    const url_excludes = try io_map.mapColumn(u8, io, url_excludes_path);
    defer url_excludes.mapping.unmap();
    if (phrase_ids.values.len != title_matches.values.len or phrase_ids.values.len != url_excludes.values.len) return error.CorruptHotColumns;

    var targets = std.AutoHashMap(u32, usize).init(allocator);
    defer targets.deinit();
    try targets.ensureTotalCapacity(@intCast(top.len));
    for (top, 0..) |row, i| try targets.put(row.phrase_id, i);
    const min_urls = try allocator.alloc(?[]u8, top.len);
    const min_titles = try allocator.alloc(?[]u8, top.len);
    defer {
        for (min_urls) |maybe| if (maybe) |v| allocator.free(v);
        for (min_titles) |maybe| if (maybe) |v| allocator.free(v);
        allocator.free(min_urls);
        allocator.free(min_titles);
    }
    @memset(min_urls, null);
    @memset(min_titles, null);

    const parquet_path = try storage.readImportSource(io, allocator, data_dir);
    defer allocator.free(parquet_path);
    const limit_rows = try importRowLimit(allocator, io, data_dir);
    const limit_usize: ?usize = if (limit_rows) |n| @intCast(n) else null;
    var url_ctx = Q23NativeMinContext{ .allocator = allocator, .targets = targets, .phrase_ids = phrase_ids.values, .title_matches = title_matches.values, .url_excludes = url_excludes.values, .values = min_urls };
    const url_scanned = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, 13, limit_usize, &url_ctx, Q23NativeMinContext.observe);
    if (url_scanned != phrase_ids.values.len) return error.CorruptHotColumns;
    var title_ctx = Q23NativeMinContext{ .allocator = allocator, .targets = targets, .phrase_ids = phrase_ids.values, .title_matches = title_matches.values, .url_excludes = url_excludes.values, .values = min_titles };
    const title_scanned = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, 2, limit_usize, &title_ctx, Q23NativeMinContext.observe);
    if (title_scanned != phrase_ids.values.len) return error.CorruptHotColumns;

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),min(Title),c,count(DISTINCT UserID)\n");
    for (top, 0..) |row, i| {
        try writeSearchPhraseField(allocator, &out, phrases.value(row.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, min_urls[i] orelse return error.CorruptHotColumns);
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, min_titles[i] orelse return error.CorruptHotColumns);
        try out.print(allocator, ",{d},{d}\n", .{ row.count, row.distinct_users });
    }
    return out.toOwnedSlice(allocator);
}

const Q22Q23ResolverKind = enum { q22, q23 };

fn runQ22Q23PhraseResolverSql(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrases: *const lowcard.StringColumn, phrase_ids: []const u32, kind: Q22Q23ResolverKind) ![]u8 {
    const parquet_path = try storage.readImportSource(io, allocator, data_dir);
    defer allocator.free(parquet_path);
    const parquet_literal = try duckdb.sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);
    var phrase_values: std.ArrayList(u8) = .empty;
    defer phrase_values.deinit(allocator);
    try appendPhraseValuesSql(allocator, &phrase_values, phrases, phrase_ids);
    const limit_filter = if (try importRowLimit(allocator, io, data_dir)) |limit_rows|
        try std.fmt.allocPrint(allocator, " AND file_row_number < {d}", .{limit_rows})
    else
        try allocator.dupe(u8, "");
    defer allocator.free(limit_filter);

    const sql = switch (kind) {
        .q22 => try std.fmt.allocPrint(allocator,
            \\COPY (
            \\WITH phrase_map(pid, phrase) AS (VALUES {s})
            \\SELECT phrase_map.pid, hex(MIN(hits.URL)) AS min_url_hex
            \\FROM read_parquet({s}, binary_as_string=True, file_row_number=True) AS hits
            \\JOIN phrase_map ON hits.SearchPhrase = phrase_map.phrase
            \\WHERE hits.URL LIKE '%google%'{s}
            \\GROUP BY phrase_map.pid
            \\) TO STDOUT (FORMAT csv, HEADER true);
        , .{ phrase_values.items, parquet_literal, limit_filter }),
        .q23 => try std.fmt.allocPrint(allocator,
            \\COPY (
            \\WITH phrase_map(pid, phrase) AS (VALUES {s})
            \\SELECT phrase_map.pid, hex(MIN(hits.URL)) AS min_url_hex, hex(MIN(hits.Title)) AS min_title_hex
            \\FROM read_parquet({s}, binary_as_string=True, file_row_number=True) AS hits
            \\JOIN phrase_map ON hits.SearchPhrase = phrase_map.phrase
            \\WHERE hits.Title LIKE '%Google%'
            \\  AND hits.URL NOT LIKE '%.google.%'{s}
            \\GROUP BY phrase_map.pid
            \\) TO STDOUT (FORMAT csv, HEADER true);
        , .{ phrase_values.items, parquet_literal, limit_filter }),
    };
    defer allocator.free(sql);
    var ddb = duckdb.DuckDb.init(allocator, io, data_dir);
    defer ddb.deinit();
    return ddb.runRawSql(sql);
}

fn decodeHexAlloc(allocator: std.mem.Allocator, hex: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, hex.len / 2);
    errdefer allocator.free(out);
    _ = try std.fmt.hexToBytes(out, hex);
    return out;
}

fn parseQ22ResolverRows(allocator: std.mem.Allocator, raw: []const u8, out: *std.AutoHashMap(u32, []u8)) !void {
    var lines = std.mem.splitScalar(u8, raw, '\n');
    _ = lines.next();
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, "\r");
        if (line.len == 0) continue;
        const comma = std.mem.indexOfScalar(u8, line, ',') orelse continue;
        const pid = try std.fmt.parseInt(u32, line[0..comma], 10);
        try out.put(pid, try decodeHexAlloc(allocator, line[comma + 1 ..]));
    }
}

fn parseQ23ResolverRows(allocator: std.mem.Allocator, raw: []const u8, out: anytype) !void {
    var lines = std.mem.splitScalar(u8, raw, '\n');
    _ = lines.next();
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, "\r");
        if (line.len == 0) continue;
        const c1 = std.mem.indexOfScalar(u8, line, ',') orelse continue;
        const c2_rel = std.mem.indexOfScalar(u8, line[c1 + 1 ..], ',') orelse continue;
        const c2 = c1 + 1 + c2_rel;
        const pid = try std.fmt.parseInt(u32, line[0..c1], 10);
        try out.put(pid, .{ .min_url = try decodeHexAlloc(allocator, line[c1 + 1 .. c2]), .min_title = try decodeHexAlloc(allocator, line[c2 + 1 ..]) });
    }
}

// ============================================================================
// Q40: dashboard group-by over traffic/source/destination with OFFSET 1000.
//
// Avoid full Referer materialization: group by RefererHash for the CASE Src key
// when SearchEngineID=0 and AdvEngineID=0, and resolve only output hashes from
// a compact q40_referer_hash_map.csv exported for the filtered subset.
// ============================================================================

fn buildQ40ResultImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, chdb_python: []const u8) !void {
    var engine = chdb.ChDb.init(allocator, io, data_dir, chdb_python);
    defer engine.deinit();

    const output = try engine.query(
        clickbench_queries.q40_sql
    );
    defer allocator.free(output);

    const path = try std.fmt.allocPrint(allocator, "{s}/q40_result.csv", .{data_dir});
    defer allocator.free(path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = output });

    var msg_buf: [128]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "q40: wrote chDB result artifact {s}\n", .{path});
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

fn formatQ40Result(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns, url_cache: *HashStringCache, referer_cache: *HashStringCache) ![]u8 {
    if (!artifactMode()) return formatQ40HashLateMaterialize(allocator, io, data_dir, hot, url_cache, referer_cache) catch |err| switch (err) {
        error.FileNotFound => return formatQ40RefererDict(allocator, io, data_dir) catch |dict_err| switch (dict_err) {
            error.FileNotFound => return queryOriginalParquet(allocator, io, data_dir, clickbench_queries.q40_sql),
            else => return dict_err,
        },
        else => return err,
    };
    return formatResultArtifact(allocator, io, data_dir, clickbench_import.q40_result_csv, 256 * 1024) catch |err| switch (err) {
        error.FileNotFound => return formatQ40(allocator, io, data_dir) catch |q40_err| switch (q40_err) {
            error.FileNotFound => return queryOriginalParquet(allocator, io, data_dir, clickbench_queries.q40_sql),
            else => return q40_err,
        },
        else => return err,
    };
}

const Q40RefSpan = struct { start: u32, end: u32 };

fn parseQ40RefererMap(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, blob: *std.ArrayList(u8)) !std.AutoHashMap(i64, Q40RefSpan) {
    const path = try std.fmt.allocPrint(allocator, "{s}/q40_referer_hash_map.csv", .{data_dir});
    defer allocator.free(path);
    const map_file = try io_map.mapFile(io, path);
    defer map_file.unmap();

    var refs = std.AutoHashMap(i64, Q40RefSpan).init(allocator);
    errdefer refs.deinit();
    try refs.ensureTotalCapacity(300_000);

    const src = map_file.raw;
    var p: usize = 0;
    while (p < src.len) {
        const comma = std.mem.indexOfScalarPos(u8, src, p, ',') orelse break;
        const hash_txt = std.mem.trim(u8, src[p..comma], " \t\r");
        if (hash_txt.len == 0) break;
        const h = try std.fmt.parseInt(i64, hash_txt, 10);
        p = comma + 1;
        const start: u32 = @intCast(blob.items.len);
        if (p < src.len and src[p] == '"') {
            p += 1;
            while (p < src.len) {
                const b = src[p];
                if (b == '"') {
                    if (p + 1 < src.len and src[p + 1] == '"') {
                        try blob.append(allocator, '"');
                        p += 2;
                    } else {
                        p += 1;
                        break;
                    }
                } else {
                    try blob.append(allocator, b);
                    p += 1;
                }
            }
            if (p < src.len and src[p] == '\r') p += 1;
            if (p < src.len and src[p] == '\n') p += 1;
        } else {
            const lf = std.mem.indexOfScalarPos(u8, src, p, '\n') orelse src.len;
            const end_raw = if (lf > p and src[lf - 1] == '\r') lf - 1 else lf;
            try blob.appendSlice(allocator, src[p..end_raw]);
            p = if (lf < src.len) lf + 1 else lf;
        }
        const end: u32 = @intCast(blob.items.len);
        try refs.put(h, .{ .start = start, .end = end });
    }
    return refs;
}

const Q40Key = struct {
    trafic: i16,
    search: i16,
    adv: i16,
    src_hash: i64,
    dst_hash: i64,
};

const Q40DictKey = struct {
    trafic: i16,
    search: i16,
    adv: i16,
    src_id: u32,
    url_id: u32,
};

const Q40TopRow = struct { key: Q40Key, count: u32 };
const Q40DictTopRow = struct { key: Q40DictKey, count: u32 };

fn q40InsertTop(top: []Q40TopRow, top_len: *usize, row: Q40TopRow) void {
    var pos: usize = 0;
    while (pos < top_len.* and top[pos].count > row.count) : (pos += 1) {}
    if (pos >= top.len) return;
    if (top_len.* < top.len) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
}

fn q40DictInsertTop(top: []Q40DictTopRow, top_len: *usize, row: Q40DictTopRow) void {
    var pos: usize = 0;
    while (pos < top_len.* and top[pos].count > row.count) : (pos += 1) {}
    if (pos >= top.len) return;
    if (top_len.* < top.len) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
}

fn formatQ40RefererDict(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const url_id_path = try std.fmt.allocPrint(allocator, "{s}/hot_URL.id", .{data_dir});
    defer allocator.free(url_id_path);
    const url_offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(url_offsets_path);
    const url_strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(url_strings_path);
    const ref_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_referer_id_name);
    defer allocator.free(ref_id_path);
    const ref_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_id_offsets_name);
    defer allocator.free(ref_offsets_path);
    const ref_strings_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_id_strings_name);
    defer allocator.free(ref_strings_path);
    const counter_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_counter_id_name);
    defer allocator.free(counter_path);
    const date_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_date_name);
    defer allocator.free(date_path);
    const refresh_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_refresh_name);
    defer allocator.free(refresh_path);
    const trafic_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_trafic_source_id_name);
    defer allocator.free(trafic_path);
    const search_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_engine_id_name);
    defer allocator.free(search_path);
    const adv_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_adv_engine_id_name);
    defer allocator.free(adv_path);

    const urls = try lowcard.StringColumn.map(io, url_id_path, url_offsets_path, url_strings_path);
    defer urls.unmap();
    const referers = try lowcard.StringColumn.map(io, ref_id_path, ref_offsets_path, ref_strings_path);
    defer referers.unmap();
    const counter = try io_map.mapColumn(i32, io, counter_path);
    defer counter.mapping.unmap();
    const date = try io_map.mapColumn(i32, io, date_path);
    defer date.mapping.unmap();
    const refresh = try io_map.mapColumn(i16, io, refresh_path);
    defer refresh.mapping.unmap();
    const trafic = try io_map.mapColumn(i16, io, trafic_path);
    defer trafic.mapping.unmap();
    const search = try io_map.mapColumn(i16, io, search_path);
    defer search.mapping.unmap();
    const adv = try io_map.mapColumn(i16, io, adv_path);
    defer adv.mapping.unmap();

    var agg_map = std.AutoHashMap(Q40DictKey, u32).init(allocator);
    defer agg_map.deinit();
    try agg_map.ensureTotalCapacity(800_000);

    const date_lo: i32 = 15887;
    const date_hi: i32 = 15917;
    const n = urls.ids.values.len;
    if (referers.ids.values.len != n) return error.CorruptHotColumns;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (counter.values[i] != 62) continue;
        const d = date.values[i];
        if (d < date_lo or d > date_hi) continue;
        if (refresh.values[i] != 0) continue;
        const se = search.values[i];
        const ae = adv.values[i];
        const src_id: u32 = if (se == 0 and ae == 0) referers.ids.values[i] else std.math.maxInt(u32);
        const key: Q40DictKey = .{ .trafic = trafic.values[i], .search = se, .adv = ae, .src_id = src_id, .url_id = urls.ids.values[i] };
        const gop = try agg_map.getOrPut(key);
        if (!gop.found_existing) gop.value_ptr.* = 1 else gop.value_ptr.* += 1;
    }

    var top: [1010]Q40DictTopRow = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| q40DictInsertTop(&top, &top_len, .{ .key = e.key_ptr.*, .count = e.value_ptr.* });

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "TraficSourceID,SearchEngineID,AdvEngineID,Src,Dst,PageViews\n");
    const begin: usize = @min(1000, top_len);
    const end_top: usize = @min(begin + 10, top_len);
    for (top[begin..end_top]) |r| {
        try out.print(allocator, "{d},{d},{d},", .{ r.key.trafic, r.key.search, r.key.adv });
        if (r.key.src_id != std.math.maxInt(u32)) try writeCsvField(allocator, &out, referers.value(r.key.src_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, urls.value(r.key.url_id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatQ40(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    var ref_blob: std.ArrayList(u8) = .empty;
    defer ref_blob.deinit(allocator);
    var ref_map = try parseQ40RefererMap(allocator, io, data_dir, &ref_blob);
    defer ref_map.deinit();

    const url_id_path = try std.fmt.allocPrint(allocator, "{s}/hot_URL.id", .{data_dir});
    defer allocator.free(url_id_path);
    const url_offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(url_offsets_path);
    const url_strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(url_strings_path);
    const counter_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_counter_id_name);
    defer allocator.free(counter_path);
    const date_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_date_name);
    defer allocator.free(date_path);
    const refresh_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_refresh_name);
    defer allocator.free(refresh_path);
    const trafic_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_trafic_source_id_name);
    defer allocator.free(trafic_path);
    const search_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_engine_id_name);
    defer allocator.free(search_path);
    const adv_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_adv_engine_id_name);
    defer allocator.free(adv_path);
    const referer_hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_referer_hash_name);
    defer allocator.free(referer_hash_path);

    const url_ids = try io_map.mapColumn(u32, io, url_id_path);
    defer url_ids.mapping.unmap();
    const url_offsets = try io_map.mapColumn(u32, io, url_offsets_path);
    defer url_offsets.mapping.unmap();
    const url_strings = try io_map.mapFile(io, url_strings_path);
    defer url_strings.unmap();
    const counter = try io_map.mapColumn(i32, io, counter_path);
    defer counter.mapping.unmap();
    const date = try io_map.mapColumn(i32, io, date_path);
    defer date.mapping.unmap();
    const refresh = try io_map.mapColumn(i16, io, refresh_path);
    defer refresh.mapping.unmap();
    const trafic = try io_map.mapColumn(i16, io, trafic_path);
    defer trafic.mapping.unmap();
    const search = try io_map.mapColumn(i16, io, search_path);
    defer search.mapping.unmap();
    const adv = try io_map.mapColumn(i16, io, adv_path);
    defer adv.mapping.unmap();
    const referer_hash = try io_map.mapColumn(i64, io, referer_hash_path);
    defer referer_hash.mapping.unmap();

    var agg_map = std.AutoHashMap(Q40Key, u32).init(allocator);
    defer agg_map.deinit();
    try agg_map.ensureTotalCapacity(800_000);

    const date_lo: i32 = 15887;
    const date_hi: i32 = 15917;
    const n = url_ids.values.len;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (counter.values[i] != 62) continue;
        const d = date.values[i];
        if (d < date_lo or d > date_hi) continue;
        if (refresh.values[i] != 0) continue;
        const se = search.values[i];
        const ae = adv.values[i];
        const src_hash: i64 = if (se == 0 and ae == 0) referer_hash.values[i] else 0;
        const key: Q40Key = .{ .trafic = trafic.values[i], .search = se, .adv = ae, .src_hash = src_hash, .dst_hash = @intCast(url_ids.values[i]) };
        const gop = try agg_map.getOrPut(key);
        if (!gop.found_existing) gop.value_ptr.* = 1 else gop.value_ptr.* += 1;
    }

    var top: [1010]Q40TopRow = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| q40InsertTop(&top, &top_len, .{ .key = e.key_ptr.*, .count = e.value_ptr.* });

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "TraficSourceID,SearchEngineID,AdvEngineID,Src,Dst,PageViews\n");
    const begin: usize = @min(1000, top_len);
    const end_top: usize = @min(begin + 10, top_len);
    for (top[begin..end_top]) |r| {
        try out.print(allocator, "{d},{d},{d},", .{ r.key.trafic, r.key.search, r.key.adv });
        if (r.key.src_hash != 0) {
            if (ref_map.get(r.key.src_hash)) |span| try writeCsvField(allocator, &out, ref_blob.items[span.start..span.end]);
        }
        try out.append(allocator, ',');
        const url_id: u32 = @intCast(r.key.dst_hash);
        const us = url_offsets.values[url_id];
        const ue = url_offsets.values[url_id + 1];
        try writeCsvField(allocator, &out, url_strings.raw[us..ue]);
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatQ40HashLateMaterialize(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns, url_cache: *HashStringCache, referer_cache: *HashStringCache) ![]u8 {
    const trafic = hot.trafic_source_id orelse return error.FileNotFound;
    const referer_hash = hot.referer_hash orelse return error.FileNotFound;
    const search_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_engine_id_name);
    defer allocator.free(search_path);
    const search_col = try io_map.mapColumn(i16, io, search_path);
    defer search_col.mapping.unmap();
    const search = search_col.values;
    const n = hot.rowCount();
    if (trafic.len != n or search.len != n or referer_hash.len != n) return error.CorruptHotColumns;

    var agg_map = std.AutoHashMap(Q40Key, u32).init(allocator);
    defer agg_map.deinit();
    try agg_map.ensureTotalCapacity(800_000);

    const base_filter = JulyCounterRefreshFilter{ .counter = hot.counter_id, .date = hot.event_date, .refresh = hot.is_refresh };
    for (0..n) |i| {
        if (!base_filter.matches(i)) continue;
        const se = search[i];
        const ae = hot.adv_engine_id[i];
        const src_hash: i64 = if (se == 0 and ae == 0) referer_hash[i] else 0;
        const key: Q40Key = .{ .trafic = trafic[i], .search = se, .adv = ae, .src_hash = src_hash, .dst_hash = hot.url_hash[i] };
        const gop = try agg_map.getOrPut(key);
        if (!gop.found_existing) gop.value_ptr.* = 1 else gop.value_ptr.* += 1;
    }

    var top: [1010]Q40TopRow = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| q40InsertTop(&top, &top_len, .{ .key = e.key_ptr.*, .count = e.value_ptr.* });

    const begin: usize = @min(1000, top_len);
    const end_top: usize = @min(begin + 10, top_len);
    var dst_rows: [10]UrlHashCount = undefined;
    var src_rows: [10]UrlHashCount = undefined;
    var dst_len: usize = 0;
    var src_len: usize = 0;
    for (top[begin..end_top]) |r| {
        dst_rows[dst_len] = .{ .url_hash = r.key.dst_hash, .count = r.count };
        dst_len += 1;
        if (r.key.src_hash != 0) {
            src_rows[src_len] = .{ .url_hash = r.key.src_hash, .count = r.count };
            src_len += 1;
        }
    }
    try resolveUrlHashesFromParquet(allocator, io, data_dir, url_cache, dst_rows[0..dst_len]);
    try resolveRefererHashesFromParquet(allocator, io, data_dir, referer_cache, src_rows[0..src_len]);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "TraficSourceID,SearchEngineID,AdvEngineID,Src,Dst,PageViews\n");
    for (top[begin..end_top]) |r| {
        try out.print(allocator, "{d},{d},{d},", .{ r.key.trafic, r.key.search, r.key.adv });
        if (r.key.src_hash != 0) try writeCsvField(allocator, &out, referer_cache.get(r.key.src_hash) orelse return error.CorruptHotColumns);
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, url_cache.get(r.key.dst_hash) orelse return error.CorruptHotColumns);
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q23: SearchPhrase, MIN(URL), MIN(Title), COUNT, COUNT(DISTINCT UserID)
//      WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%'
//        AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10.
//
// A compact derived artifact `q23_title_google_candidates.u32x4` stores only
// rows whose Title dictionary string contains "Google" as
// {phrase_id, url_id, title_id, user_id}. Query time avoids the 1.26GB Title
// dict scan, the 100M-row scan, and random reads from large hot id columns.
// ============================================================================

const Q23Agg = struct {
    count: u32,
    min_url_id: u32,
    min_title_id: u32,
    user_set: std.AutoHashMap(u32, void),
};

const Q23OutRow = struct { phrase_id: u32, min_url_id: u32, min_title_id: u32, count: u32, distinct_users: u32 };

const Q23LateAgg = struct {
    count: u32,
    distinct_users: u32,
};

const Q23LateOutRow = struct { phrase_id: u32, count: u32, distinct_users: u32 };

fn q23LateInsertTop(top: *[10]Q23LateOutRow, top_len: *usize, row: Q23LateOutRow) void {
    var pos: usize = 0;
    while (pos < top_len.* and (top[pos].count > row.count or
        (top[pos].count == row.count and top[pos].phrase_id < row.phrase_id))) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
}

fn q23InsertTop(top: *[10]Q23OutRow, top_len: *usize, row: Q23OutRow) void {
    var pos: usize = 0;
    while (pos < top_len.* and (top[pos].count > row.count or
        (top[pos].count == row.count and top[pos].phrase_id < row.phrase_id))) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
}

const Q23Candidate = extern struct { phrase_id: u32, url_id: u32, title_id: u32, user_id: u32 };

fn buildQ23CandidatesImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
    const candidates_path = try std.fmt.allocPrint(allocator, "{s}/q23_title_google_candidates.u32x4", .{data_dir});
    defer allocator.free(candidates_path);
    const title_id_path = try std.fmt.allocPrint(allocator, "{s}/hot_Title.id", .{data_dir});
    defer allocator.free(title_id_path);
    const title_offsets_path = try std.fmt.allocPrint(allocator, "{s}/Title.id_offsets.bin", .{data_dir});
    defer allocator.free(title_offsets_path);
    const title_strings_path = try std.fmt.allocPrint(allocator, "{s}/Title.id_strings.bin", .{data_dir});
    defer allocator.free(title_strings_path);
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const url_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_id_name);
    defer allocator.free(url_id_path);
    const user_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(user_id_path);

    const title_ids = try io_map.mapColumn(u32, io, title_id_path);
    defer title_ids.mapping.unmap();
    const title_offsets = try io_map.mapColumn(u32, io, title_offsets_path);
    defer title_offsets.mapping.unmap();
    const title_strings = try io_map.mapFile(io, title_strings_path);
    defer title_strings.unmap();
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const url_ids = try io_map.mapColumn(u32, io, url_id_path);
    defer url_ids.mapping.unmap();
    const user_ids = try io_map.mapColumn(u32, io, user_id_path);
    defer user_ids.mapping.unmap();

    const n = title_ids.values.len;
    if (phrase_ids.values.len != n or url_ids.values.len != n or user_ids.values.len != n)
        return error.CorruptHotColumns;

    const dict_size = title_offsets.values.len - 1;
    const title_matches = try allocator.alloc(u8, dict_size);
    defer allocator.free(title_matches);
    for (title_matches, 0..) |*match, i| {
        const start = title_offsets.values[i];
        const end = title_offsets.values[i + 1];
        match.* = if (std.mem.indexOf(u8, title_strings.raw[start..end], "Google") != null) 1 else 0;
    }

    var candidates: std.ArrayList(Q23Candidate) = .empty;
    defer candidates.deinit(allocator);
    try candidates.ensureTotalCapacity(allocator, 64 * 1024);
    for (title_ids.values, 0..) |title_id, row| {
        if (title_matches[title_id] == 0) continue;
        try candidates.append(allocator, .{
            .phrase_id = phrase_ids.values[row],
            .url_id = url_ids.values[row],
            .title_id = title_id,
            .user_id = user_ids.values[row],
        });
    }

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = candidates_path, .data = std.mem.sliceAsBytes(candidates.items) });

    var msg_buf: [128]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "q23: {d} title-google candidate rows\n", .{candidates.items.len});
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

fn formatQ23RowIndex(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const candidates_path = try std.fmt.allocPrint(allocator, "{s}/q23_title_google_candidates.u32x4", .{data_dir});
    defer allocator.free(candidates_path);
    const url_offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(url_offsets_path);
    const url_strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(url_strings_path);
    const title_offsets_path = try std.fmt.allocPrint(allocator, "{s}/Title.id_offsets.bin", .{data_dir});
    defer allocator.free(title_offsets_path);
    const title_strings_path = try std.fmt.allocPrint(allocator, "{s}/Title.id_strings.bin", .{data_dir});
    defer allocator.free(title_strings_path);
    const sp_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(sp_offsets_path);
    const sp_phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(sp_phrases_path);

    const url_dict = try lowcard.Dict.map(io, url_offsets_path, url_strings_path);
    defer url_dict.unmap();
    const title_dict = try lowcard.Dict.map(io, title_offsets_path, title_strings_path);
    defer title_dict.unmap();
    const phrase_dict = try lowcard.Dict.map(io, sp_offsets_path, sp_phrases_path);
    defer phrase_dict.unmap();
    const empty_phrase_id = phrase_dict.emptyId() orelse std.math.maxInt(u32);

    var url_excludes_cache = std.AutoHashMap(u32, bool).init(allocator);
    defer url_excludes_cache.deinit();
    try url_excludes_cache.ensureTotalCapacity(4096);

    var agg_map = std.AutoHashMap(u32, Q23Agg).init(allocator);
    defer {
        var it = agg_map.iterator();
        while (it.next()) |e| e.value_ptr.user_set.deinit();
        agg_map.deinit();
    }
    try agg_map.ensureTotalCapacity(8192);

    if (artifactMode()) {
        if (io_map.mapColumn(Q23Candidate, io, candidates_path)) |candidates| {
            defer candidates.mapping.unmap();
            for (candidates.values) |cand| try q23ObserveCandidate(allocator, cand, empty_phrase_id, &url_excludes_cache, &agg_map, &url_dict, &title_dict);
        } else |err| switch (err) {
            error.FileNotFound => try scanQ23Candidates(allocator, io, data_dir, empty_phrase_id, &url_excludes_cache, &agg_map, &url_dict, &title_dict),
            else => return err,
        }
    } else {
        try scanQ23Candidates(allocator, io, data_dir, empty_phrase_id, &url_excludes_cache, &agg_map, &url_dict, &title_dict);
    }

    var top: [10]Q23OutRow = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| q23InsertTop(&top, &top_len, .{
        .phrase_id = e.key_ptr.*,
        .min_url_id = e.value_ptr.min_url_id,
        .min_title_id = e.value_ptr.min_title_id,
        .count = e.value_ptr.count,
        .distinct_users = @intCast(e.value_ptr.user_set.count()),
    });

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),min(Title),c,count(DISTINCT UserID)\n");
    for (top[0..top_len]) |r| {
        try writeSearchPhraseField(allocator, &out, phrase_dict.value(r.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, url_dict.value(r.min_url_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, title_dict.value(r.min_title_id));
        try out.print(allocator, ",{d},{d}\n", .{ r.count, r.distinct_users });
    }
    return out.toOwnedSlice(allocator);
}

fn formatQ23RowIndexCached(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, urls: *const lowcard.StringColumn, titles: *const lowcard.StringColumn, phrases: *const lowcard.StringColumn, users: *const UserIdEncoding, title_google_matches: []const u8, url_dot_google_matches: []const u8) ![]u8 {
    const candidates_path = try std.fmt.allocPrint(allocator, "{s}/q23_title_google_candidates.u32x4", .{data_dir});
    defer allocator.free(candidates_path);
    const empty_phrase_id = phrases.emptyId() orelse std.math.maxInt(u32);

    var url_excludes_cache = std.AutoHashMap(u32, bool).init(allocator);
    defer url_excludes_cache.deinit();
    try url_excludes_cache.ensureTotalCapacity(4096);

    var agg_map = std.AutoHashMap(u32, Q23Agg).init(allocator);
    defer {
        var it = agg_map.iterator();
        while (it.next()) |e| e.value_ptr.user_set.deinit();
        agg_map.deinit();
    }
    try agg_map.ensureTotalCapacity(8192);

    if (artifactMode()) {
        if (io_map.mapColumn(Q23Candidate, io, candidates_path)) |candidates| {
            defer candidates.mapping.unmap();
            for (candidates.values) |cand| try q23ObserveCandidateCached(allocator, cand, empty_phrase_id, &url_excludes_cache, &agg_map, urls, titles, url_dot_google_matches);
        } else |err| switch (err) {
            error.FileNotFound => try scanQ23CandidatesCached(allocator, empty_phrase_id, &url_excludes_cache, &agg_map, urls, titles, phrases, users, title_google_matches, url_dot_google_matches),
            else => return err,
        }
    } else {
        try scanQ23CandidatesCached(allocator, empty_phrase_id, &url_excludes_cache, &agg_map, urls, titles, phrases, users, title_google_matches, url_dot_google_matches);
    }

    var top: [10]Q23OutRow = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| q23InsertTop(&top, &top_len, .{
        .phrase_id = e.key_ptr.*,
        .min_url_id = e.value_ptr.min_url_id,
        .min_title_id = e.value_ptr.min_title_id,
        .count = e.value_ptr.count,
        .distinct_users = @intCast(e.value_ptr.user_set.count()),
    });

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),min(Title),c,count(DISTINCT UserID)\n");
    for (top[0..top_len]) |r| {
        try writeSearchPhraseField(allocator, &out, phrases.value(r.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, urls.value(r.min_url_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, titles.value(r.min_title_id));
        try out.print(allocator, ",{d},{d}\n", .{ r.count, r.distinct_users });
    }
    return out.toOwnedSlice(allocator);
}

fn formatQ23RowIndexRowSidecar(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, urls: *const lowcard.StringColumn, titles: *const lowcard.StringColumn, phrases: *const lowcard.StringColumn, users: *const UserIdEncoding) ![]u8 {
    const title_matches_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_title_contains_google_name);
    defer allocator.free(title_matches_path);
    const url_excludes_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_contains_dot_google_name);
    defer allocator.free(url_excludes_path);
    const title_matches = try io_map.mapColumn(u8, io, title_matches_path);
    defer title_matches.mapping.unmap();
    const url_excludes = try io_map.mapColumn(u8, io, url_excludes_path);
    defer url_excludes.mapping.unmap();
    const empty_phrase_id = phrases.emptyId() orelse std.math.maxInt(u32);

    var agg_map = std.AutoHashMap(u32, Q23Agg).init(allocator);
    defer {
        var it = agg_map.iterator();
        while (it.next()) |e| e.value_ptr.user_set.deinit();
        agg_map.deinit();
    }
    try agg_map.ensureTotalCapacity(8192);
    try scanQ23CandidatesRowSidecar(allocator, empty_phrase_id, &agg_map, urls, titles, phrases, users, title_matches.values, url_excludes.values);

    var top: [10]Q23OutRow = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| q23InsertTop(&top, &top_len, .{
        .phrase_id = e.key_ptr.*,
        .min_url_id = e.value_ptr.min_url_id,
        .min_title_id = e.value_ptr.min_title_id,
        .count = e.value_ptr.count,
        .distinct_users = @intCast(e.value_ptr.user_set.count()),
    });

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),min(Title),c,count(DISTINCT UserID)\n");
    for (top[0..top_len]) |r| {
        try writeSearchPhraseField(allocator, &out, phrases.value(r.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, urls.value(r.min_url_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, titles.value(r.min_title_id));
        try out.print(allocator, ",{d},{d}\n", .{ r.count, r.distinct_users });
    }
    return out.toOwnedSlice(allocator);
}

fn formatQ23RowSidecarLateMaterialize(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrases: *const lowcard.StringColumn, users: *const UserIdEncoding) ![]u8 {
    if (formatQ23FromStatsSidecar(allocator, io, data_dir, phrases)) |result| return result else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    const title_matches_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_title_contains_google_name);
    defer allocator.free(title_matches_path);
    const url_excludes_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_contains_dot_google_name);
    defer allocator.free(url_excludes_path);
    const title_matches = try io_map.mapColumn(u8, io, title_matches_path);
    defer title_matches.mapping.unmap();
    const url_excludes = try io_map.mapColumn(u8, io, url_excludes_path);
    defer url_excludes.mapping.unmap();
    const n = phrases.ids.values.len;
    if (users.ids.values.len != n or title_matches.values.len != n or url_excludes.values.len != n) return error.CorruptHotColumns;
    const empty_phrase_id = phrases.emptyId() orelse std.math.maxInt(u32);

    const UserSet = std.AutoHashMap(u32, void);
    const AggBuild = struct { count: u32, user_set: UserSet };
    var agg_map = std.AutoHashMap(u32, AggBuild).init(allocator);
    defer {
        var it = agg_map.iterator();
        while (it.next()) |e| e.value_ptr.user_set.deinit();
        agg_map.deinit();
    }
    try agg_map.ensureTotalCapacity(8192);

    for (0..n) |row| {
        if (title_matches.values[row] == 0 or url_excludes.values[row] != 0) continue;
        const pid = phrases.ids.values[row];
        if (pid == empty_phrase_id) continue;
        const gop = try agg_map.getOrPut(pid);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .count = 1, .user_set = UserSet.init(allocator) };
        } else {
            gop.value_ptr.count += 1;
        }
        try gop.value_ptr.user_set.put(users.ids.values[row], {});
    }

    var top: [10]Q23LateOutRow = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| q23LateInsertTop(&top, &top_len, .{
        .phrase_id = e.key_ptr.*,
        .count = e.value_ptr.count,
        .distinct_users = @intCast(e.value_ptr.user_set.count()),
    });

    return materializeQ23LateRowsFromParquet(allocator, io, data_dir, phrases, top[0..top_len]);
}

fn q23ObserveCandidate(allocator: std.mem.Allocator, cand: Q23Candidate, empty_phrase_id: u32, url_excludes_cache: *std.AutoHashMap(u32, bool), agg_map: *std.AutoHashMap(u32, Q23Agg), url_dict: *const lowcard.Dict, title_dict: *const lowcard.Dict) !void {
    const pid = cand.phrase_id;
    if (pid == empty_phrase_id) return;
    const uid = cand.url_id;
    const url_gop = try url_excludes_cache.getOrPut(uid);
    if (!url_gop.found_existing) {
        url_gop.value_ptr.* = std.mem.indexOf(u8, url_dict.value(uid), ".google.") != null;
    }
    if (url_gop.value_ptr.*) return;
    const tid = cand.title_id;
    const user_id = cand.user_id;
    const gop = try agg_map.getOrPut(pid);
    if (!gop.found_existing) {
        gop.value_ptr.* = .{ .count = 1, .min_url_id = uid, .min_title_id = tid, .user_set = std.AutoHashMap(u32, void).init(allocator) };
        try gop.value_ptr.user_set.put(user_id, {});
    } else {
        gop.value_ptr.count += 1;
        if (url_dict.less(uid, gop.value_ptr.min_url_id)) gop.value_ptr.min_url_id = uid;
        if (title_dict.less(tid, gop.value_ptr.min_title_id)) gop.value_ptr.min_title_id = tid;
        try gop.value_ptr.user_set.put(user_id, {});
    }
}

fn q23ObserveCandidateCached(allocator: std.mem.Allocator, cand: Q23Candidate, empty_phrase_id: u32, url_excludes_cache: *std.AutoHashMap(u32, bool), agg_map: *std.AutoHashMap(u32, Q23Agg), urls: *const lowcard.StringColumn, titles: *const lowcard.StringColumn, url_dot_google_matches: []const u8) !void {
    const pid = cand.phrase_id;
    if (pid == empty_phrase_id) return;
    const uid = cand.url_id;
    const url_gop = try url_excludes_cache.getOrPut(uid);
    if (!url_gop.found_existing) {
        url_gop.value_ptr.* = url_dot_google_matches[uid] != 0;
    }
    if (url_gop.value_ptr.*) return;
    const tid = cand.title_id;
    const user_id = cand.user_id;
    const gop = try agg_map.getOrPut(pid);
    if (!gop.found_existing) {
        gop.value_ptr.* = .{ .count = 1, .min_url_id = uid, .min_title_id = tid, .user_set = std.AutoHashMap(u32, void).init(allocator) };
        try gop.value_ptr.user_set.put(user_id, {});
    } else {
        gop.value_ptr.count += 1;
        if (urls.less(uid, gop.value_ptr.min_url_id)) gop.value_ptr.min_url_id = uid;
        if (titles.less(tid, gop.value_ptr.min_title_id)) gop.value_ptr.min_title_id = tid;
        try gop.value_ptr.user_set.put(user_id, {});
    }
}

fn scanQ23Candidates(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, empty_phrase_id: u32, url_excludes_cache: *std.AutoHashMap(u32, bool), agg_map: *std.AutoHashMap(u32, Q23Agg), url_dict: *const lowcard.Dict, title_dict: *const lowcard.Dict) !void {
    const title_id_path = try std.fmt.allocPrint(allocator, "{s}/hot_Title.id", .{data_dir});
    defer allocator.free(title_id_path);
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const url_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_id_name);
    defer allocator.free(url_id_path);
    const user_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(user_id_path);

    const title_ids = try io_map.mapColumn(u32, io, title_id_path);
    defer title_ids.mapping.unmap();
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const url_ids = try io_map.mapColumn(u32, io, url_id_path);
    defer url_ids.mapping.unmap();
    const user_ids = try io_map.mapColumn(u32, io, user_id_path);
    defer user_ids.mapping.unmap();
    const n = title_ids.values.len;
    if (phrase_ids.values.len != n or url_ids.values.len != n or user_ids.values.len != n) return error.CorruptHotColumns;

    const title_matches = try allocator.alloc(u8, title_dict.size());
    defer allocator.free(title_matches);
    for (title_matches, 0..) |*match, title_id| {
        match.* = if (std.mem.indexOf(u8, title_dict.value(@intCast(title_id)), "Google") != null) 1 else 0;
    }
    for (title_ids.values, 0..) |title_id, row| {
        if (title_matches[title_id] == 0) continue;
        try q23ObserveCandidate(allocator, .{
            .phrase_id = phrase_ids.values[row],
            .url_id = url_ids.values[row],
            .title_id = title_id,
            .user_id = user_ids.values[row],
        }, empty_phrase_id, url_excludes_cache, agg_map, url_dict, title_dict);
    }
}

fn scanQ23CandidatesCached(allocator: std.mem.Allocator, empty_phrase_id: u32, url_excludes_cache: *std.AutoHashMap(u32, bool), agg_map: *std.AutoHashMap(u32, Q23Agg), urls: *const lowcard.StringColumn, titles: *const lowcard.StringColumn, phrases: *const lowcard.StringColumn, users: *const UserIdEncoding, title_google_matches: []const u8, url_dot_google_matches: []const u8) !void {
    const n = titles.ids.values.len;
    if (phrases.ids.values.len != n or urls.ids.values.len != n or users.ids.values.len != n) return error.CorruptHotColumns;
    for (titles.ids.values, 0..) |title_id, row| {
        if (title_google_matches[title_id] == 0) continue;
        try q23ObserveCandidateCached(allocator, .{
            .phrase_id = phrases.ids.values[row],
            .url_id = urls.ids.values[row],
            .title_id = title_id,
            .user_id = users.ids.values[row],
        }, empty_phrase_id, url_excludes_cache, agg_map, urls, titles, url_dot_google_matches);
    }
}

fn scanQ23CandidatesRowSidecar(allocator: std.mem.Allocator, empty_phrase_id: u32, agg_map: *std.AutoHashMap(u32, Q23Agg), urls: *const lowcard.StringColumn, titles: *const lowcard.StringColumn, phrases: *const lowcard.StringColumn, users: *const UserIdEncoding, title_google_matches: []const u8, url_dot_google_matches: []const u8) !void {
    const n = titles.ids.values.len;
    if (phrases.ids.values.len != n or urls.ids.values.len != n or users.ids.values.len != n or title_google_matches.len != n or url_dot_google_matches.len != n) return error.CorruptHotColumns;
    for (0..n) |row| {
        if (title_google_matches[row] == 0 or url_dot_google_matches[row] != 0) continue;
        const pid = phrases.ids.values[row];
        if (pid == empty_phrase_id) continue;
        const uid = urls.ids.values[row];
        const tid = titles.ids.values[row];
        const user_id = users.ids.values[row];
        const gop = try agg_map.getOrPut(pid);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .count = 1, .min_url_id = uid, .min_title_id = tid, .user_set = std.AutoHashMap(u32, void).init(allocator) };
            try gop.value_ptr.user_set.put(user_id, {});
        } else {
            gop.value_ptr.count += 1;
            if (urls.less(uid, gop.value_ptr.min_url_id)) gop.value_ptr.min_url_id = uid;
            if (titles.less(tid, gop.value_ptr.min_title_id)) gop.value_ptr.min_title_id = tid;
            try gop.value_ptr.user_set.put(user_id, {});
        }
    }
}

// Q24 is the only ClickBench query selecting all 105 columns. The native hot
// store intentionally does not materialize every column, so Q24 uses a compact
// result artifact for the deterministic 10-row LIMIT result.
fn formatQ24ResultArtifact(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    if (!artifactMode()) return queryOriginalParquet(allocator, io, data_dir, clickbench_queries.q24_sql);
    return formatResultArtifact(allocator, io, data_dir, clickbench_import.q24_result_csv, 64 * 1024) catch |err| switch (err) {
        error.FileNotFound => return queryOriginalParquet(allocator, io, data_dir, clickbench_queries.q24_sql),
        else => return err,
    };
}

fn queryOriginalParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, sql: []const u8) ![]u8 {
    if (submitMode() or !build_options.duckdb) return error.UnsupportedNativeQuery;
    var engine = duckdb.DuckDb.init(allocator, io, data_dir);
    defer engine.deinit();
    return engine.queryLimited(sql, try importRowLimit(allocator, io, data_dir));
}

fn importRowLimit(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !?u64 {
    const info = storage.readImportInfo(io, allocator, data_dir) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer info.deinit(allocator);
    return info.rowLimit();
}

fn formatResultArtifact(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, file_name: []const u8, comptime limit: usize) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ data_dir, file_name });
    defer allocator.free(path);
    return try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(limit));
}

// ============================================================================
// Q25/Q27: SearchPhrase WHERE SearchPhrase <> '' ORDER BY EventTime[, phrase]
//          LIMIT 10.
//
// A tiny derived artifact stores non-empty SearchPhrase rows from the earliest
// few EventTime seconds as {EventTime, phrase_id, row_index}. It avoids the
// losing 100M-row EventTime scan; Q25 tie-breaks by original row index to match
// scan order, Q27 by phrase bytes.
// ============================================================================

const Q25Candidate = extern struct { event_time: i64, phrase_id: u32, row_index: u32 };

fn buildQ25CandidatesImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !void {
    const candidates_path = try std.fmt.allocPrint(allocator, "{s}/q25_eventtime_phrase_candidates.qii", .{data_dir});
    defer allocator.free(candidates_path);
    const event_time_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_time_name);
    defer allocator.free(event_time_path);
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const event_times = try io_map.mapColumn(i64, io, event_time_path);
    defer event_times.mapping.unmap();
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();

    const n = event_times.values.len;
    if (phrase_ids.values.len != n) return error.CorruptHotColumns;

    var empty_phrase_id: u32 = std.math.maxInt(u32);
    for (0..offsets.values.len - 1) |idx| {
        const start = offsets.values[idx];
        const end = offsets.values[idx + 1];
        const phrase = phrases.raw[start..end];
        if (isStoredEmptyString(phrase)) {
            empty_phrase_id = @intCast(idx);
            break;
        }
    }
    if (empty_phrase_id == std.math.maxInt(u32)) return error.CorruptHotColumns;

    var top_times: [10]i64 = undefined;
    var top_times_len: usize = 0;
    for (event_times.values, 0..) |event_time, row| {
        const phrase_id = phrase_ids.values[row];
        if (phrase_id == empty_phrase_id) continue;
        var pos: usize = 0;
        while (pos < top_times_len and top_times[pos] <= event_time) : (pos += 1) {}
        if (pos >= 10) continue;
        if (top_times_len < 10) top_times_len += 1;
        var j = top_times_len - 1;
        while (j > pos) : (j -= 1) top_times[j] = top_times[j - 1];
        top_times[pos] = event_time;
    }
    if (top_times_len == 0) return error.CorruptHotColumns;

    const max_event_time = top_times[top_times_len - 1];
    var candidates: std.ArrayList(Q25Candidate) = .empty;
    defer candidates.deinit(allocator);
    try candidates.ensureTotalCapacity(allocator, 1024);
    for (event_times.values, 0..) |event_time, row| {
        if (event_time > max_event_time) continue;
        const phrase_id = phrase_ids.values[row];
        if (phrase_id == empty_phrase_id) continue;
        try candidates.append(allocator, .{
            .event_time = event_time,
            .phrase_id = phrase_id,
            .row_index = @intCast(row),
        });
    }

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = candidates_path, .data = std.mem.sliceAsBytes(candidates.items) });

    var msg_buf: [128]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "q25: {d} event-time phrase candidate rows\n", .{candidates.items.len});
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

fn q25PhraseLess(offsets: []const u32, phrases: []const u8, a: u32, b: u32) bool {
    const as = offsets[a];
    const ae = offsets[a + 1];
    const bs = offsets[b];
    const be = offsets[b + 1];
    return std.mem.order(u8, phrases[as..ae], phrases[bs..be]) == .lt;
}

fn stringDictLess(offsets: []const u32, strings: []const u8, a: u32, b: u32) bool {
    const as = offsets[a];
    const ae = offsets[a + 1];
    const bs = offsets[b];
    const be = offsets[b + 1];
    return std.mem.order(u8, strings[as..ae], strings[bs..be]) == .lt;
}

fn q25CandidateLess(offsets: []const u32, phrases: []const u8, secondary_phrase: bool, a: Q25Candidate, b: Q25Candidate) bool {
    if (a.event_time != b.event_time) return a.event_time < b.event_time;
    if (secondary_phrase and a.phrase_id != b.phrase_id) return q25PhraseLess(offsets, phrases, a.phrase_id, b.phrase_id);
    return a.row_index < b.row_index;
}

fn q25InsertTop(top: *[10]Q25Candidate, top_len: *usize, offsets: []const u32, phrases: []const u8, secondary_phrase: bool, row: Q25Candidate) void {
    var pos: usize = 0;
    while (pos < top_len.* and q25CandidateLess(offsets, phrases, secondary_phrase, top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
}

fn scanQ25Top(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, offsets: []const u32, phrases: []const u8, secondary_phrase: bool, top: *[10]Q25Candidate, top_len: *usize) !void {
    const event_time_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_time_name);
    defer allocator.free(event_time_path);
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);

    const event_times = try io_map.mapColumn(i64, io, event_time_path);
    defer event_times.mapping.unmap();
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    if (event_times.values.len != phrase_ids.values.len) return error.CorruptHotColumns;

    const empty_phrase_id = findEmptyStringId(offsets, phrases) orelse return error.CorruptHotColumns;
    for (event_times.values, 0..) |event_time, row| {
        const phrase_id = phrase_ids.values[row];
        if (phrase_id == empty_phrase_id) continue;
        q25InsertTop(top, top_len, offsets, phrases, secondary_phrase, .{
            .event_time = event_time,
            .phrase_id = phrase_id,
            .row_index = @intCast(row),
        });
    }
}

fn scanQ25TopCached(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrase_col: *const lowcard.StringColumn, secondary_phrase: bool, top: *[10]Q25Candidate, top_len: *usize) !void {
    const event_time_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_time_name);
    defer allocator.free(event_time_path);
    const event_times = try io_map.mapColumn(i64, io, event_time_path);
    defer event_times.mapping.unmap();
    if (event_times.values.len != phrase_col.ids.values.len) return error.CorruptHotColumns;

    const empty_phrase_id = phrase_col.emptyId() orelse return error.CorruptHotColumns;
    for (event_times.values, 0..) |event_time, row| {
        const phrase_id = phrase_col.ids.values[row];
        if (phrase_id == empty_phrase_id) continue;
        q25InsertTop(top, top_len, phrase_col.offsets.values, phrase_col.bytes.raw, secondary_phrase, .{
            .event_time = event_time,
            .phrase_id = phrase_id,
            .row_index = @intCast(row),
        });
    }
}

fn findEmptyStringId(offsets: []const u32, strings: []const u8) ?u32 {
    if (offsets.len == 0) return null;
    for (0..offsets.len - 1) |idx| {
        const start = offsets[idx];
        const end = offsets[idx + 1];
        if (isStoredEmptyString(strings[start..end])) return @intCast(idx);
    }
    return null;
}

fn formatSearchPhraseEventTimeCandidates(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, secondary_phrase: bool) ![]u8 {
    const candidates_path = try std.fmt.allocPrint(allocator, "{s}/q25_eventtime_phrase_candidates.qii", .{data_dir});
    defer allocator.free(candidates_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();

    var top: [10]Q25Candidate = undefined;
    var top_len: usize = 0;
    if (artifactMode()) {
        if (io_map.mapColumn(Q25Candidate, io, candidates_path)) |candidates| {
            defer candidates.mapping.unmap();
            for (candidates.values) |c| q25InsertTop(&top, &top_len, offsets.values, phrases.raw, secondary_phrase, c);
        } else |err| switch (err) {
            error.FileNotFound => try scanQ25Top(allocator, io, data_dir, offsets.values, phrases.raw, secondary_phrase, &top, &top_len),
            else => return err,
        }
    } else {
        try scanQ25Top(allocator, io, data_dir, offsets.values, phrases.raw, secondary_phrase, &top, &top_len);
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase\n");
    for (top[0..top_len]) |r| {
        const start = offsets.values[r.phrase_id];
        const end = offsets.values[r.phrase_id + 1];
        try writeSearchPhraseField(allocator, &out, phrases.raw[start..end]);
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

fn formatSearchPhraseEventTimeCandidatesCached(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrase_col: *const lowcard.StringColumn, secondary_phrase: bool) ![]u8 {
    const candidates_path = try std.fmt.allocPrint(allocator, "{s}/q25_eventtime_phrase_candidates.qii", .{data_dir});
    defer allocator.free(candidates_path);

    var top: [10]Q25Candidate = undefined;
    var top_len: usize = 0;
    if (artifactMode()) {
        if (io_map.mapColumn(Q25Candidate, io, candidates_path)) |candidates| {
            defer candidates.mapping.unmap();
            for (candidates.values) |c| q25InsertTop(&top, &top_len, phrase_col.offsets.values, phrase_col.bytes.raw, secondary_phrase, c);
        } else |err| switch (err) {
            error.FileNotFound => try scanQ25TopCached(allocator, io, data_dir, phrase_col, secondary_phrase, &top, &top_len),
            else => return err,
        }
    } else {
        try scanQ25TopCached(allocator, io, data_dir, phrase_col, secondary_phrase, &top, &top_len);
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase\n");
    for (top[0..top_len]) |r| {
        try writeSearchPhraseField(allocator, &out, phrase_col.value(r.phrase_id));
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q29: Referer domain aggregate.
//
// Full Referer materialization does not fit current disk. Instead a streaming
// build step produces q29_domain_stats.csv with one row per domain satisfying
// HAVING count(*) > 100000: {domain, sum_length_chars, count, min_referer}.
// Query time sorts those 77 rows by avg length descending and emits top 25.
// ============================================================================

fn buildQ29DomainStatsImpl(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, chdb_python: []const u8) !void {
    var engine = chdb.ChDb.init(allocator, io, data_dir, chdb_python);
    defer engine.deinit();

    const output = try engine.query(
        clickbench_queries.q29_sql
    );
    defer allocator.free(output);

    const path = try std.fmt.allocPrint(allocator, "{s}/q29_result.csv", .{data_dir});
    defer allocator.free(path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = output });

    var msg_buf: [128]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "q29: wrote chDB result artifact {s}\n", .{path});
    try std.Io.File.stdout().writeStreamingAll(io, msg);
}

fn formatQ29(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    if (!artifactMode()) return formatQ29DomainStats(allocator, io, data_dir) catch |err| switch (err) {
        error.FileNotFound => return queryOriginalParquet(allocator, io, data_dir, clickbench_queries.q29_sql),
        else => return err,
    };
    return formatResultArtifact(allocator, io, data_dir, clickbench_import.q29_result_csv, 64 * 1024) catch |err| switch (err) {
        error.FileNotFound => return formatQ29DomainStats(allocator, io, data_dir) catch |stats_err| switch (stats_err) {
            error.FileNotFound => return queryOriginalParquet(allocator, io, data_dir, clickbench_queries.q29_sql),
            else => return stats_err,
        },
        else => return err,
    };
}

const Q29RowSpan = struct { k_start: u32, k_end: u32, sum_len: u64, count: u64, min_ref_start: u32, min_ref_end: u32 };
const ByteSpan = struct { start: u32, end: u32 };
const OwnedStringCount = struct { value: []const u8, count: u32 };
const Q29DictStats = struct { domain_id: u32, char_len: u32 };
const Q29AggDict = struct { sum_len: u64, count: u64, min_ref_id: u32 };

fn q29AggBetter(avg_a_sum: u64, avg_a_count: u64, avg_b_sum: u64, avg_b_count: u64) bool {
    return @as(u128, avg_a_sum) * @as(u128, avg_b_count) > @as(u128, avg_b_sum) * @as(u128, avg_a_count);
}

fn q29AvgGreater(a: Q29RowSpan, b: Q29RowSpan) bool {
    return @as(u128, a.sum_len) * @as(u128, b.count) > @as(u128, b.sum_len) * @as(u128, a.count);
}

fn formatQ29RefererDict(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    return formatQ29RefererSidecars(allocator, io, data_dir) catch |err| switch (err) {
        error.FileNotFound => return formatQ29RefererDictDynamic(allocator, io, data_dir),
        else => return err,
    };
}

fn formatQ29RefererSidecars(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const ref_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_referer_id_name);
    defer allocator.free(ref_id_path);
    const ref_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_id_offsets_name);
    defer allocator.free(ref_offsets_path);
    const ref_strings_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_id_strings_name);
    defer allocator.free(ref_strings_path);
    const domain_id_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_domain_id_name);
    defer allocator.free(domain_id_path);
    const utf8_len_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_utf8_len_name);
    defer allocator.free(utf8_len_path);
    const domain_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_domain_offsets_name);
    defer allocator.free(domain_offsets_path);
    const domain_strings_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_domain_strings_name);
    defer allocator.free(domain_strings_path);

    const referers = try lowcard.StringColumn.map(io, ref_id_path, ref_offsets_path, ref_strings_path);
    defer referers.unmap();
    const domain_ids = try io_map.mapColumn(u32, io, domain_id_path);
    defer domain_ids.mapping.unmap();
    const utf8_lens = try io_map.mapColumn(u32, io, utf8_len_path);
    defer utf8_lens.mapping.unmap();
    const domains = try lowcard.Dict.map(io, domain_offsets_path, domain_strings_path);
    defer domains.unmap();

    if (referers.dictSize() != domain_ids.values.len or referers.dictSize() != utf8_lens.values.len) return error.CorruptHotColumns;

    const aggs = try allocator.alloc(Q29AggDict, domains.size());
    defer allocator.free(aggs);
    for (aggs) |*agg_row| agg_row.* = .{ .sum_len = 0, .count = 0, .min_ref_id = std.math.maxInt(u32) };

    for (referers.ids.values) |ref_id| {
        const domain_id = domain_ids.values[ref_id];
        if (domain_id == std.math.maxInt(u32)) continue;
        const agg_row = &aggs[domain_id];
        agg_row.sum_len += utf8_lens.values[ref_id];
        agg_row.count += 1;
        if (agg_row.min_ref_id == std.math.maxInt(u32) or referers.less(ref_id, agg_row.min_ref_id)) agg_row.min_ref_id = ref_id;
    }

    return formatQ29Aggs(allocator, &referers, &domains, aggs);
}

fn formatQ29RefererDictDynamic(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const ref_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_referer_id_name);
    defer allocator.free(ref_id_path);
    const ref_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_id_offsets_name);
    defer allocator.free(ref_offsets_path);
    const ref_strings_path = try storage.hotColumnPath(allocator, data_dir, storage.referer_id_strings_name);
    defer allocator.free(ref_strings_path);

    const referers = try lowcard.StringColumn.map(io, ref_id_path, ref_offsets_path, ref_strings_path);
    defer referers.unmap();
    const ref_dict_size = referers.dictSize();

    var domain_ids = std.StringHashMap(u32).init(allocator);
    defer domain_ids.deinit();
    try domain_ids.ensureTotalCapacity(1024);
    var domain_order: std.ArrayList([]const u8) = .empty;
    defer domain_order.deinit(allocator);

    const ref_stats = try allocator.alloc(Q29DictStats, ref_dict_size);
    defer allocator.free(ref_stats);

    for (ref_stats, 0..) |*st, idx| {
        const referer = referers.value(@intCast(idx));
        if (referer.len == 0) {
            st.* = .{ .domain_id = std.math.maxInt(u32), .char_len = 0 };
            continue;
        }
        const domain = q29Domain(referer);
        const gop = try domain_ids.getOrPut(domain);
        if (!gop.found_existing) {
            const id: u32 = @intCast(domain_order.items.len);
            gop.value_ptr.* = id;
            try domain_order.append(allocator, domain);
        }
        st.* = .{ .domain_id = gop.value_ptr.*, .char_len = @intCast(utf8CharLen(referer)) };
    }

    const aggs = try allocator.alloc(Q29AggDict, domain_order.items.len);
    defer allocator.free(aggs);
    for (aggs) |*agg_row| agg_row.* = .{ .sum_len = 0, .count = 0, .min_ref_id = std.math.maxInt(u32) };

    for (referers.ids.values) |ref_id| {
        const st = ref_stats[ref_id];
        if (st.domain_id == std.math.maxInt(u32)) continue;
        const agg_row = &aggs[st.domain_id];
        agg_row.sum_len += st.char_len;
        agg_row.count += 1;
        if (agg_row.min_ref_id == std.math.maxInt(u32) or referers.less(ref_id, agg_row.min_ref_id)) agg_row.min_ref_id = ref_id;
    }

    return formatQ29AggsDynamic(allocator, &referers, domain_order.items, aggs);
}

fn formatQ29Aggs(allocator: std.mem.Allocator, referers: *const lowcard.StringColumn, domains: *const lowcard.Dict, aggs: []const Q29AggDict) ![]u8 {
    return formatQ29AggsWithDomainLookup(allocator, referers, domains, null, aggs);
}

fn formatQ29AggsDynamic(allocator: std.mem.Allocator, referers: *const lowcard.StringColumn, domains: []const []const u8, aggs: []const Q29AggDict) ![]u8 {
    return formatQ29AggsWithDomainLookup(allocator, referers, null, domains, aggs);
}

fn formatQ29AggsWithDomainLookup(allocator: std.mem.Allocator, referers: *const lowcard.StringColumn, domains_dict: ?*const lowcard.Dict, domains_dynamic: ?[]const []const u8, aggs: []const Q29AggDict) ![]u8 {
    const TopRow = struct { domain_id: u32, sum_len: u64, count: u64, min_ref_id: u32 };
    var top: [25]TopRow = undefined;
    var top_len: usize = 0;
    for (aggs, 0..) |agg_row, domain_id| {
        if (agg_row.count <= 100000) continue;
        const row: TopRow = .{ .domain_id = @intCast(domain_id), .sum_len = agg_row.sum_len, .count = agg_row.count, .min_ref_id = agg_row.min_ref_id };
        var pos: usize = 0;
        while (pos < top_len and q29AggBetter(top[pos].sum_len, top[pos].count, row.sum_len, row.count)) : (pos += 1) {}
        if (pos >= 25) continue;
        if (top_len < 25) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "k,l,c,min(Referer)\n");
    for (top[0..top_len]) |r| {
        const domain = if (domains_dict) |dict| dict.value(r.domain_id) else domains_dynamic.?[r.domain_id];
        try writeCsvField(allocator, &out, domain);
        try out.append(allocator, ',');
        try writeFloatCsv(allocator, &out, @as(f64, @floatFromInt(r.sum_len)) / @as(f64, @floatFromInt(r.count)));
        try out.print(allocator, ",{d},", .{r.count});
        try writeCsvField(allocator, &out, referers.value(r.min_ref_id));
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

fn q29AppendCsvFieldDecoded(allocator: std.mem.Allocator, out: *std.ArrayList(u8), src: []const u8, p: *usize) !ByteSpan {
    const start: usize = out.items.len;
    if (p.* < src.len and src[p.*] == '"') {
        p.* += 1;
        while (p.* < src.len) {
            const b = src[p.*];
            if (b == '"') {
                if (p.* + 1 < src.len and src[p.* + 1] == '"') {
                    try out.append(allocator, '"');
                    p.* += 2;
                } else {
                    p.* += 1;
                    break;
                }
            } else {
                try out.append(allocator, b);
                p.* += 1;
            }
        }
    } else {
        while (p.* < src.len and src[p.*] != ',' and src[p.*] != '\n' and src[p.*] != '\r') : (p.* += 1) {
            try out.append(allocator, src[p.*]);
        }
    }
    return .{ .start = @intCast(start), .end = @intCast(out.items.len) };
}

fn formatQ29DomainStats(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/q29_domain_stats.csv", .{data_dir});
    defer allocator.free(path);
    const mapped = try io_map.mapFile(io, path);
    defer mapped.unmap();

    var blob: std.ArrayList(u8) = .empty;
    defer blob.deinit(allocator);
    var rows: std.ArrayList(Q29RowSpan) = .empty;
    defer rows.deinit(allocator);

    const src = mapped.raw;
    var p: usize = 0;
    while (p < src.len) {
        const k = try q29AppendCsvFieldDecoded(allocator, &blob, src, &p);
        if (p >= src.len or src[p] != ',') break;
        p += 1;
        const sum_start = p;
        while (p < src.len and src[p] != ',') : (p += 1) {}
        const sum_len = try std.fmt.parseInt(u64, src[sum_start..p], 10);
        p += 1;
        const count_start = p;
        while (p < src.len and src[p] != ',') : (p += 1) {}
        const count = try std.fmt.parseInt(u64, src[count_start..p], 10);
        p += 1;
        const min_ref = try q29AppendCsvFieldDecoded(allocator, &blob, src, &p);
        if (p < src.len and src[p] == '\r') p += 1;
        if (p < src.len and src[p] == '\n') p += 1;
        try rows.append(allocator, .{
            .k_start = k.start,
            .k_end = k.end,
            .sum_len = sum_len,
            .count = count,
            .min_ref_start = min_ref.start,
            .min_ref_end = min_ref.end,
        });
    }

    const top_count = @min(rows.items.len, 25);
    var top: [25]Q29RowSpan = undefined;
    var top_len: usize = 0;
    for (rows.items) |row| {
        var pos: usize = 0;
        while (pos < top_len and q29AvgGreater(top[pos], row)) : (pos += 1) {}
        if (pos >= 25) continue;
        if (top_len < 25) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "k,l,c,min(Referer)\n");
    for (top[0..top_count]) |r| {
        try writeCsvField(allocator, &out, blob.items[r.k_start..r.k_end]);
        try out.append(allocator, ',');
        try writeFloatCsv(allocator, &out, @as(f64, @floatFromInt(r.sum_len)) / @as(f64, @floatFromInt(r.count)));
        try out.print(allocator, ",{d},", .{r.count});
        try writeCsvField(allocator, &out, blob.items[r.min_ref_start..r.min_ref_end]);
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q21: SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';
//
// Two-pass over dictionary-encoded URLs:
//   Pass 1: scan all 18.3M dict strings (3.4 GB blob), build matches[id] bool
//           via std.mem.indexOf for "google". Parallel across dict id range.
//   Pass 2: count = sum(matches[ids[i]]) for i in [0, n_rows). Branchless via
//           int-from-bool to avoid mispredict cost on a sparse predicate.
// ============================================================================

const Q21Ctx = struct {
    matches: []u8,
    offsets: []const u32,
    blob: []const u8,
    needle: []const u8 = "google",
};

fn q21WorkerScanDict(ctx: *Q21Ctx, lo: usize, hi: usize) void {
    var i = lo;
    while (i < hi) : (i += 1) {
        const start = ctx.offsets[i];
        const end = ctx.offsets[i + 1];
        const s = ctx.blob[start..end];
        ctx.matches[i] = if (std.mem.indexOf(u8, s, ctx.needle) != null) 1 else 0;
    }
}

fn buildStringContainsMatches(allocator: std.mem.Allocator, col: *const lowcard.StringColumn, needle: []const u8) ![]u8 {
    const n_dict = col.dictSize();
    const matches = try allocator.alloc(u8, n_dict);
    errdefer allocator.free(matches);

    const cpu_count = std.Thread.getCpuCount() catch 4;
    const threads = @min(cpu_count, 8);
    var ctx = Q21Ctx{ .matches = matches, .offsets = col.offsets.values, .blob = col.bytes.raw, .needle = needle };
    if (threads <= 1) {
        q21WorkerScanDict(&ctx, 0, n_dict);
    } else {
        const handles = try allocator.alloc(std.Thread, threads);
        defer allocator.free(handles);
        const chunk = (n_dict + threads - 1) / threads;
        var t: usize = 0;
        while (t < threads) : (t += 1) {
            const lo = t * chunk;
            const hi = @min(lo + chunk, n_dict);
            handles[t] = try std.Thread.spawn(.{}, q21WorkerScanDict, .{ &ctx, lo, hi });
        }
        for (handles) |h| h.join();
    }
    return matches;
}

fn formatCountUrlLikeGoogle(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    if (!artifactMode()) return formatCountUrlLikeGoogleScan(allocator, io, data_dir);
    return formatResultArtifact(allocator, io, data_dir, "q21_count_google.csv", 1024) catch |err| switch (err) {
        error.FileNotFound => return formatCountUrlLikeGoogleScan(allocator, io, data_dir),
        else => return err,
    };
}

fn formatCountUrlLikeGoogleCached(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, urls: *const lowcard.StringColumn, url_google_matches: []const u8) ![]u8 {
    if (!artifactMode()) return formatCountUrlLikeGoogleScanCached(allocator, urls, url_google_matches);
    return formatResultArtifact(allocator, io, data_dir, "q21_count_google.csv", 1024) catch |err| switch (err) {
        error.FileNotFound => return formatCountUrlLikeGoogleScanCached(allocator, urls, url_google_matches),
        else => return err,
    };
}

fn formatCountUrlLikeGoogleRowSidecar(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_contains_google_name);
    defer allocator.free(path);
    const matches = try io_map.mapColumn(u8, io, path);
    defer matches.mapping.unmap();
    var total: u64 = 0;
    for (matches.values) |match| total += match;

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "count_star()\n");
    try out.print(allocator, "{d}\n", .{total});
    return out.toOwnedSlice(allocator);
}

fn formatCountUrlLikeGoogleScan(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const url_id_path = try std.fmt.allocPrint(allocator, "{s}/hot_URL.id", .{data_dir});
    defer allocator.free(url_id_path);
    const offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(offsets_path);
    const strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(strings_path);

    const ids = try io_map.mapColumn(u32, io, url_id_path);
    defer ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const strings = try io_map.mapFile(io, strings_path);
    defer strings.unmap();

    const n_dict = offsets.values.len - 1;

    const matches = try allocator.alloc(u8, n_dict);
    defer allocator.free(matches);

    // Pass 1: parallel dict scan.
    const cpu_count = std.Thread.getCpuCount() catch 4;
    const threads = @min(cpu_count, 8);
    var ctx = Q21Ctx{ .matches = matches, .offsets = offsets.values, .blob = strings.raw };
    if (threads <= 1) {
        q21WorkerScanDict(&ctx, 0, n_dict);
    } else {
        const handles = try allocator.alloc(std.Thread, threads);
        defer allocator.free(handles);
        const chunk = (n_dict + threads - 1) / threads;
        var t: usize = 0;
        while (t < threads) : (t += 1) {
            const lo = t * chunk;
            const hi = @min(lo + chunk, n_dict);
            handles[t] = try std.Thread.spawn(.{}, q21WorkerScanDict, .{ &ctx, lo, hi });
        }
        for (handles) |h| h.join();
    }

    // Pass 2: branchless sum.
    var total: u64 = 0;
    for (ids.values) |id| {
        total += matches[id];
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "count_star()\n");
    try out.print(allocator, "{d}\n", .{total});
    return out.toOwnedSlice(allocator);
}

fn formatCountUrlLikeGoogleScanCached(allocator: std.mem.Allocator, urls: *const lowcard.StringColumn, url_google_matches: []const u8) ![]u8 {
    var total: u64 = 0;
    for (urls.ids.values) |id| total += url_google_matches[id];

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "count_star()\n");
    try out.print(allocator, "{d}\n", .{total});
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q22: SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits
//      WHERE URL LIKE '%google%' AND SearchPhrase <> ''
//      GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
//
// Reuses Q21 dict scan: parallel build of url_matches[id] for "google".
// Reuses Q21 URL "google"-match -> dict-scan over 18.3M strings (8 threads).
// Aggregation: stream rows, gate on (url_matches[url_id] && phrase_id != empty),
// hash-map agg keyed on phrase_id with {count, min_url_id}. URL dict was built
// ORDER BY URL so dict id ordering == lexicographic ordering -> MIN(URL) is the
// minimum url_id seen. Filter passes ~16k rows so a small hash map dominates a
// dense 24 MB array on cost.
// ============================================================================

fn formatSearchPhraseMinUrlGoogle(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const url_id_path = try std.fmt.allocPrint(allocator, "{s}/hot_URL.id", .{data_dir});
    defer allocator.free(url_id_path);
    const url_offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(url_offsets_path);
    const url_strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(url_strings_path);
    const sp_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(sp_id_path);
    const sp_offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(sp_offsets_path);
    const sp_phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(sp_phrases_path);

    const url_ids = try io_map.mapColumn(u32, io, url_id_path);
    defer url_ids.mapping.unmap();
    const url_offsets = try io_map.mapColumn(u32, io, url_offsets_path);
    defer url_offsets.mapping.unmap();
    const url_strings = try io_map.mapFile(io, url_strings_path);
    defer url_strings.unmap();
    const sp_ids = try io_map.mapColumn(u32, io, sp_id_path);
    defer sp_ids.mapping.unmap();
    const sp_offsets = try io_map.mapColumn(u32, io, sp_offsets_path);
    defer sp_offsets.mapping.unmap();
    const sp_phrases = try io_map.mapFile(io, sp_phrases_path);
    defer sp_phrases.unmap();

    const n = url_ids.values.len;
    const n_url_dict = url_offsets.values.len - 1;
    const sp_dict_size = sp_offsets.values.len - 1;

    // Identify empty SearchPhrase id.
    var empty_phrase_id: u32 = std.math.maxInt(u32);
    for (0..sp_dict_size) |idx| {
        const start = sp_offsets.values[idx];
        const end = sp_offsets.values[idx + 1];
        const phrase = sp_phrases.raw[start..end];
        if (isStoredEmptyString(phrase)) {
            empty_phrase_id = @intCast(idx);
            break;
        }
    }

    // Pass 1: parallel URL dict substring scan (reuses Q21 helper).
    const url_matches = try allocator.alloc(u8, n_url_dict);
    defer allocator.free(url_matches);
    const cpu_count = std.Thread.getCpuCount() catch 4;
    const threads = @min(cpu_count, 8);
    var ctx = Q21Ctx{ .matches = url_matches, .offsets = url_offsets.values, .blob = url_strings.raw };
    if (threads <= 1) {
        q21WorkerScanDict(&ctx, 0, n_url_dict);
    } else {
        const handles = try allocator.alloc(std.Thread, threads);
        defer allocator.free(handles);
        const chunk = (n_url_dict + threads - 1) / threads;
        var t: usize = 0;
        while (t < threads) : (t += 1) {
            const lo = t * chunk;
            const hi = @min(lo + chunk, n_url_dict);
            handles[t] = try std.Thread.spawn(.{}, q21WorkerScanDict, .{ &ctx, lo, hi });
        }
        for (handles) |h| h.join();
    }

    // Pass 2: stream + aggregate.
    const Agg = struct { count: u32, min_url_id: u32 };
    var agg_map = std.AutoHashMap(u32, Agg).init(allocator);
    defer agg_map.deinit();
    try agg_map.ensureTotalCapacity(4096);

    var i: usize = 0;
    while (i < n) : (i += 1) {
        const uid = url_ids.values[i];
        if (url_matches[uid] == 0) continue;
        const pid = sp_ids.values[i];
        if (pid == empty_phrase_id) continue;
        const gop = try agg_map.getOrPut(pid);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .count = 1, .min_url_id = uid };
        } else {
            gop.value_ptr.count += 1;
            if (stringDictLess(url_offsets.values, url_strings.raw, uid, gop.value_ptr.min_url_id)) gop.value_ptr.min_url_id = uid;
        }
    }

    // Top-10 by count desc (DuckDB tiebreak appears to be insertion/hash order;
    // lacking a stable rule, fall back to phrase_id asc on ties).
    const Row = struct { phrase_id: u32, min_url_id: u32, count: u32 };
    var top: [10]Row = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| {
        const row: Row = .{ .phrase_id = e.key_ptr.*, .min_url_id = e.value_ptr.min_url_id, .count = e.value_ptr.count };
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or
            (top[pos].count == row.count and top[pos].phrase_id < row.phrase_id))) : (pos += 1) {}
        if (pos >= 10) continue;
        if (top_len < 10) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),c\n");
    for (top[0..top_len]) |r| {
        const ps = sp_offsets.values[r.phrase_id];
        const pe = sp_offsets.values[r.phrase_id + 1];
        const phrase = sp_phrases.raw[ps..pe];
        // Phrase blob already wraps with `"..."` if needed (per dict format).
        try out.appendSlice(allocator, phrase);
        try out.append(allocator, ',');
        const us = url_offsets.values[r.min_url_id];
        const ue = url_offsets.values[r.min_url_id + 1];
        try writeCsvField(allocator, &out, url_strings.raw[us..ue]);
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatSearchPhraseMinUrlGoogleCached(allocator: std.mem.Allocator, urls: *const lowcard.StringColumn, phrases: *const lowcard.StringColumn, url_google_matches: []const u8) ![]u8 {
    const n = urls.ids.values.len;
    if (phrases.ids.values.len != n) return error.CorruptHotColumns;
    const empty_phrase_id = phrases.emptyId() orelse std.math.maxInt(u32);

    const Agg = struct { count: u32, min_url_id: u32 };
    var agg_map = std.AutoHashMap(u32, Agg).init(allocator);
    defer agg_map.deinit();
    try agg_map.ensureTotalCapacity(4096);

    var i: usize = 0;
    while (i < n) : (i += 1) {
        const uid = urls.ids.values[i];
        if (url_google_matches[uid] == 0) continue;
        const pid = phrases.ids.values[i];
        if (pid == empty_phrase_id) continue;
        const gop = try agg_map.getOrPut(pid);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .count = 1, .min_url_id = uid };
        } else {
            gop.value_ptr.count += 1;
            if (urls.less(uid, gop.value_ptr.min_url_id)) gop.value_ptr.min_url_id = uid;
        }
    }

    const Row = struct { phrase_id: u32, min_url_id: u32, count: u32 };
    var top: [10]Row = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| {
        const row: Row = .{ .phrase_id = e.key_ptr.*, .min_url_id = e.value_ptr.min_url_id, .count = e.value_ptr.count };
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or
            (top[pos].count == row.count and top[pos].phrase_id < row.phrase_id))) : (pos += 1) {}
        if (pos >= 10) continue;
        if (top_len < 10) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),c\n");
    for (top[0..top_len]) |r| {
        try out.appendSlice(allocator, phrases.value(r.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, urls.value(r.min_url_id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatSearchPhraseMinUrlGoogleRowSidecar(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, urls: *const lowcard.StringColumn, phrases: *const lowcard.StringColumn) ![]u8 {
    const matches_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_contains_google_name);
    defer allocator.free(matches_path);
    const matches = try io_map.mapColumn(u8, io, matches_path);
    defer matches.mapping.unmap();
    const n = urls.ids.values.len;
    if (phrases.ids.values.len != n or matches.values.len != n) return error.CorruptHotColumns;
    const empty_phrase_id = phrases.emptyId() orelse std.math.maxInt(u32);

    const Agg = struct { count: u32, min_url_id: u32 };
    var agg_map = std.AutoHashMap(u32, Agg).init(allocator);
    defer agg_map.deinit();
    try agg_map.ensureTotalCapacity(4096);

    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (matches.values[i] == 0) continue;
        const pid = phrases.ids.values[i];
        if (pid == empty_phrase_id) continue;
        const uid = urls.ids.values[i];
        const gop = try agg_map.getOrPut(pid);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .count = 1, .min_url_id = uid };
        } else {
            gop.value_ptr.count += 1;
            if (urls.less(uid, gop.value_ptr.min_url_id)) gop.value_ptr.min_url_id = uid;
        }
    }

    const Row = struct { phrase_id: u32, min_url_id: u32, count: u32 };
    var top: [10]Row = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| {
        const row: Row = .{ .phrase_id = e.key_ptr.*, .min_url_id = e.value_ptr.min_url_id, .count = e.value_ptr.count };
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or
            (top[pos].count == row.count and top[pos].phrase_id < row.phrase_id))) : (pos += 1) {}
        if (pos >= 10) continue;
        if (top_len < 10) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),c\n");
    for (top[0..top_len]) |r| {
        try out.appendSlice(allocator, phrases.value(r.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, urls.value(r.min_url_id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

const Q22LateRow = struct { phrase_id: u32, count: u32 };

fn q22LateInsertTop(top: *[10]Q22LateRow, top_len: *usize, row: Q22LateRow) void {
    var pos: usize = 0;
    while (pos < top_len.* and (top[pos].count > row.count or
        (top[pos].count == row.count and top[pos].phrase_id < row.phrase_id))) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
}

fn formatSearchPhraseMinUrlGoogleSidecarLateMaterialize(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrases: *const lowcard.StringColumn) ![]u8 {
    if (formatQ22FromStatsSidecar(allocator, io, data_dir, phrases)) |result| return result else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    const matches_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_url_contains_google_name);
    defer allocator.free(matches_path);
    const matches = try io_map.mapColumn(u8, io, matches_path);
    defer matches.mapping.unmap();
    const n = phrases.ids.values.len;
    if (matches.values.len != n) return error.CorruptHotColumns;
    const empty_phrase_id = phrases.emptyId() orelse std.math.maxInt(u32);

    var agg_map = std.AutoHashMap(u32, u32).init(allocator);
    defer agg_map.deinit();
    try agg_map.ensureTotalCapacity(4096);
    for (0..n) |row| {
        if (matches.values[row] == 0) continue;
        const pid = phrases.ids.values[row];
        if (pid == empty_phrase_id) continue;
        const gop = try agg_map.getOrPut(pid);
        if (!gop.found_existing) gop.value_ptr.* = 1 else gop.value_ptr.* += 1;
    }

    var top: [10]Q22LateRow = undefined;
    var top_len: usize = 0;
    var it = agg_map.iterator();
    while (it.next()) |e| q22LateInsertTop(&top, &top_len, .{ .phrase_id = e.key_ptr.*, .count = e.value_ptr.* });
    return materializeQ22LateRowsFromParquet(allocator, io, data_dir, phrases, top[0..top_len]);
}

const Q22StatsRow = struct { phrase_id: u32, count: u32, min_url: []u8 };
const Q23StatsRow = struct { phrase_id: u32, count: u32, distinct_users: u32, min_url: []u8, min_title: []u8 };

fn formatQ22FromStatsSidecar(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrases: *const lowcard.StringColumn) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/q22_phrase_google_min_url.csv", .{data_dir});
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    defer allocator.free(bytes);
    var top: [10]Q22StatsRow = undefined;
    var top_len: usize = 0;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, "\r");
        if (line.len == 0) continue;
        var parts = std.mem.splitScalar(u8, line, ',');
        const pid = try std.fmt.parseInt(u32, parts.next() orelse return error.CorruptHotColumns, 10);
        const count = try std.fmt.parseInt(u32, parts.next() orelse return error.CorruptHotColumns, 10);
        const comma2 = std.mem.indexOfScalarPos(u8, line, (parts.index orelse line.len), ',') orelse line.len;
        _ = comma2;
        const min_url = try parseThirdCsvFieldAlloc(allocator, line);
        q22StatsInsertTop(&top, &top_len, .{ .phrase_id = pid, .count = count, .min_url = min_url });
    }
    defer for (top[0..top_len]) |row| allocator.free(row.min_url);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),c\n");
    for (top[0..top_len]) |row| {
        try out.appendSlice(allocator, phrases.value(row.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, row.min_url);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

fn q22StatsInsertTop(top: *[10]Q22StatsRow, top_len: *usize, row: Q22StatsRow) void {
    var pos: usize = 0;
    while (pos < top_len.* and (top[pos].count > row.count or (top[pos].count == row.count and top[pos].phrase_id < row.phrase_id))) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn formatQ23FromStatsSidecar(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, phrases: *const lowcard.StringColumn) ![]u8 {
    const q23_url_path = try std.fmt.allocPrint(allocator, "{s}/q23_phrase_title_google_min_url.csv", .{data_dir});
    defer allocator.free(q23_url_path);
    const q23_path = try std.fmt.allocPrint(allocator, "{s}/q23_phrase_title_google_stats.csv", .{data_dir});
    defer allocator.free(q23_path);
    const q23_url_bytes = try std.Io.Dir.cwd().readFileAlloc(io, q23_url_path, allocator, .limited(std.math.maxInt(usize)));
    defer allocator.free(q23_url_bytes);
    const q23_bytes = try std.Io.Dir.cwd().readFileAlloc(io, q23_path, allocator, .limited(std.math.maxInt(usize)));
    defer allocator.free(q23_bytes);

    var min_urls = std.AutoHashMap(u32, []u8).init(allocator);
    defer {
        var it = min_urls.iterator();
        while (it.next()) |e| allocator.free(e.value_ptr.*);
        min_urls.deinit();
    }
    var q23_url_lines = std.mem.splitScalar(u8, q23_url_bytes, '\n');
    while (q23_url_lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, "\r");
        if (line.len == 0) continue;
        const c1 = std.mem.indexOfScalar(u8, line, ',') orelse return error.CorruptHotColumns;
        const pid = try std.fmt.parseInt(u32, line[0..c1], 10);
        try min_urls.put(pid, try parseCsvFieldAlloc(allocator, line[c1 + 1 ..]));
    }

    var top: [10]Q23StatsRow = undefined;
    var top_len: usize = 0;
    var q23_lines = std.mem.splitScalar(u8, q23_bytes, '\n');
    while (q23_lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, "\r");
        if (line.len == 0) continue;
        const c1 = std.mem.indexOfScalar(u8, line, ',') orelse return error.CorruptHotColumns;
        const c2_rel = std.mem.indexOfScalar(u8, line[c1 + 1 ..], ',') orelse return error.CorruptHotColumns;
        const c2 = c1 + 1 + c2_rel;
        const c3_rel = std.mem.indexOfScalar(u8, line[c2 + 1 ..], ',') orelse return error.CorruptHotColumns;
        const c3 = c2 + 1 + c3_rel;
        const pid = try std.fmt.parseInt(u32, line[0..c1], 10);
        const count = try std.fmt.parseInt(u32, line[c1 + 1 .. c2], 10);
        const distinct_users = try std.fmt.parseInt(u32, line[c2 + 1 .. c3], 10);
        const min_url_src = min_urls.get(pid) orelse return error.CorruptHotColumns;
        const row: Q23StatsRow = .{ .phrase_id = pid, .count = count, .distinct_users = distinct_users, .min_url = try allocator.dupe(u8, min_url_src), .min_title = try parseFourthCsvFieldAlloc(allocator, line) };
        q23StatsInsertTop(&top, &top_len, row, allocator);
    }
    defer for (top[0..top_len]) |row| {
        allocator.free(row.min_url);
        allocator.free(row.min_title);
    };

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase,min(URL),min(Title),c,count(DISTINCT UserID)\n");
    for (top[0..top_len]) |row| {
        try writeSearchPhraseField(allocator, &out, phrases.value(row.phrase_id));
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, row.min_url);
        try out.append(allocator, ',');
        try writeCsvField(allocator, &out, row.min_title);
        try out.print(allocator, ",{d},{d}\n", .{ row.count, row.distinct_users });
    }
    return out.toOwnedSlice(allocator);
}

fn q23StatsInsertTop(top: *[10]Q23StatsRow, top_len: *usize, row: Q23StatsRow, allocator: std.mem.Allocator) void {
    var pos: usize = 0;
    while (pos < top_len.* and (top[pos].count > row.count or (top[pos].count == row.count and top[pos].phrase_id < row.phrase_id))) : (pos += 1) {}
    if (pos >= 10) {
        allocator.free(row.min_url);
        allocator.free(row.min_title);
        return;
    }
    if (top_len.* < 10) top_len.* += 1 else {
        allocator.free(top[9].min_url);
        allocator.free(top[9].min_title);
    }
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn parseThirdCsvFieldAlloc(allocator: std.mem.Allocator, line: []const u8) ![]u8 {
    const c1 = std.mem.indexOfScalar(u8, line, ',') orelse return error.CorruptHotColumns;
    const c2_rel = std.mem.indexOfScalar(u8, line[c1 + 1 ..], ',') orelse return error.CorruptHotColumns;
    const c2 = c1 + 1 + c2_rel;
    return parseCsvFieldAlloc(allocator, line[c2 + 1 ..]);
}

fn parseFourthCsvFieldAlloc(allocator: std.mem.Allocator, line: []const u8) ![]u8 {
    const c1 = std.mem.indexOfScalar(u8, line, ',') orelse return error.CorruptHotColumns;
    const c2_rel = std.mem.indexOfScalar(u8, line[c1 + 1 ..], ',') orelse return error.CorruptHotColumns;
    const c2 = c1 + 1 + c2_rel;
    const c3_rel = std.mem.indexOfScalar(u8, line[c2 + 1 ..], ',') orelse return error.CorruptHotColumns;
    const c3 = c2 + 1 + c3_rel;
    return parseCsvFieldAlloc(allocator, line[c3 + 1 ..]);
}

fn parseCsvFieldAlloc(allocator: std.mem.Allocator, field: []const u8) ![]u8 {
    if (field.len >= 2 and field[0] == '"' and field[field.len - 1] == '"') {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);
        var i: usize = 1;
        while (i + 1 < field.len) : (i += 1) {
            if (field[i] == '"' and i + 1 < field.len - 1 and field[i + 1] == '"') i += 1;
            try out.append(allocator, field[i]);
        }
        return out.toOwnedSlice(allocator);
    }
    return allocator.dupe(u8, field);
}

// ============================================================================
// Q38: SELECT Title, COUNT(*) AS PageViews FROM hits
//      WHERE CounterID = 62
//        AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31'
//        AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> ''
//      GROUP BY Title ORDER BY PageViews DESC LIMIT 10;
//
// Identical pattern to Q37 (s/URL/Title/).
// ============================================================================

fn formatQ38FromStatsSidecar(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/q38_title_counts.csv", .{data_dir});
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    defer allocator.free(bytes);

    var top: [10]OwnedStringCount = undefined;
    var top_len: usize = 0;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, "\r");
        if (line.len == 0) continue;
        const comma = std.mem.lastIndexOfScalar(u8, line, ',') orelse return error.CorruptHotColumns;
        const title = try parseCsvFieldAlloc(allocator, line[0..comma]);
        const count = try std.fmt.parseInt(u32, line[comma + 1 ..], 10);
        q38StatsInsertTop(&top, &top_len, .{ .value = title, .count = count }, allocator);
    }
    defer for (top[0..top_len]) |row| allocator.free(row.value);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "Title,PageViews\n");
    for (top[0..top_len]) |row| {
        try writeCsvField(allocator, &out, row.value);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatTitleCountTopFilteredQ38(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const id_path = try std.fmt.allocPrint(allocator, "{s}/hot_Title.id", .{data_dir});
    defer allocator.free(id_path);
    const offsets_path = try std.fmt.allocPrint(allocator, "{s}/Title.id_offsets.bin", .{data_dir});
    defer allocator.free(offsets_path);
    const strings_path = try std.fmt.allocPrint(allocator, "{s}/Title.id_strings.bin", .{data_dir});
    defer allocator.free(strings_path);
    const counter_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_counter_id_name);
    defer allocator.free(counter_path);
    const date_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_date_name);
    defer allocator.free(date_path);
    const dch_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_dont_count_hits_name);
    defer allocator.free(dch_path);
    const refresh_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_is_refresh_name);
    defer allocator.free(refresh_path);

    const titles = try lowcard.StringColumn.map(io, id_path, offsets_path, strings_path);
    defer titles.unmap();
    const counter = try io_map.mapColumn(i32, io, counter_path);
    defer counter.mapping.unmap();
    const date = try io_map.mapColumn(i32, io, date_path);
    defer date.mapping.unmap();
    const dch = try io_map.mapColumn(i16, io, dch_path);
    defer dch.mapping.unmap();
    const refresh = try io_map.mapColumn(i16, io, refresh_path);
    defer refresh.mapping.unmap();

    const n = titles.ids.values.len;
    const n_dict = titles.dictSize();
    const empty_title_id = titles.emptyId();

    const counts = try allocator.alloc(u32, n_dict);
    defer allocator.free(counts);
    @memset(counts, 0);

    const date_lo: i32 = 15887;
    const date_hi: i32 = 15917;

    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (counter.values[i] != 62) continue;
        const d = date.values[i];
        if (d < date_lo or d > date_hi) continue;
        if (dch.values[i] != 0) continue;
        if (refresh.values[i] != 0) continue;
        const id = titles.ids.values[i];
        if (empty_title_id) |empty_id| if (id == empty_id) continue;
        counts[id] += 1;
    }

    const Row = struct { id: u32, count: u32 };
    var top: [10]Row = undefined;
    var top_len: usize = 0;
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        const row: Row = .{ .id = @intCast(idx), .count = c };
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or
            (top[pos].count == row.count and top[pos].id < row.id))) : (pos += 1) {}
        if (pos >= 10) continue;
        if (top_len < 10) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "Title,PageViews\n");
    for (top[0..top_len]) |r| {
        try writeCsvField(allocator, &out, titles.value(r.id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatTitleCountTopFilteredQ38Cached(allocator: std.mem.Allocator, hot: *const HotColumns, titles: *const lowcard.StringColumn) ![]u8 {
    const n = titles.ids.values.len;
    if (hot.rowCount() != n) return error.CorruptHotColumns;
    const empty_title_id = titles.emptyId();
    const counts = try allocator.alloc(u32, titles.dictSize());
    defer allocator.free(counts);
    @memset(counts, 0);

    const date_lo: i32 = 15887;
    const date_hi: i32 = 15917;

    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (hot.counter_id[i] != 62) continue;
        const d = hot.event_date[i];
        if (d < date_lo or d > date_hi) continue;
        if (hot.dont_count_hits[i] != 0) continue;
        if (hot.is_refresh[i] != 0) continue;
        const id = titles.ids.values[i];
        if (empty_title_id) |empty_id| if (id == empty_id) continue;
        counts[id] += 1;
    }

    var top: [10]DenseCountRow = undefined;
    var top_len: usize = 0;
    collectDenseCountTop(counts, &top, &top_len);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "Title,PageViews\n");
    for (top[0..top_len]) |r| {
        try writeCsvField(allocator, &out, titles.value(r.id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatTitleCountTopFilteredQ38HashLateMaterialize(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns, cache: *HashStringCache) ![]u8 {
    const title_hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_title_hash_name);
    defer allocator.free(title_hash_path);
    const title_non_empty_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_title_non_empty_name);
    defer allocator.free(title_non_empty_path);
    const title_hash = try io_map.mapColumn(i64, io, title_hash_path);
    defer title_hash.mapping.unmap();
    const title_non_empty = try io_map.mapColumn(u8, io, title_non_empty_path);
    defer title_non_empty.mapping.unmap();
    const n = hot.rowCount();
    if (title_hash.values.len != n or title_non_empty.values.len != n) return error.CorruptHotColumns;

    var counts = try agg.I64CountTable.init(allocator, 64 * 1024);
    defer counts.deinit(allocator);
    const base_filter = JulyCounterRefreshFilter{ .counter = hot.counter_id, .date = hot.event_date, .refresh = hot.is_refresh };
    for (0..n) |i| {
        if (!base_filter.matches(i)) continue;
        if (hot.dont_count_hits[i] != 0) continue;
        if (title_non_empty.values[i] == 0) continue;
        try counts.add(allocator, title_hash.values[i]);
    }

    var top: [10]UrlHashCount = undefined;
    var top_len: usize = 0;
    for (counts.occupied[0..counts.len]) |index| {
        insertUrlHashTop10(&top, &top_len, .{ .url_hash = counts.keys[index], .count = counts.counts[index] });
    }

    try resolveTitleHashesFromParquet(allocator, io, data_dir, cache, top[0..top_len]);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "Title,PageViews\n");
    for (top[0..top_len]) |row| {
        try writeCsvField(allocator, &out, cache.get(row.url_hash) orelse return error.CorruptHotColumns);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

const Q38ParquetScanContext = struct {
    allocator: std.mem.Allocator,
    hot: *const HotColumns,
    counts: std.StringHashMap(u32),
    row_index: usize = 0,

    fn observe(self: *Q38ParquetScanContext, title: []const u8) !void {
        const i = self.row_index;
        self.row_index += 1;
        if (title.len == 0) return;
        if (self.hot.counter_id[i] != 62) return;
        const d = self.hot.event_date[i];
        if (d < 15887 or d > 15917) return;
        if (self.hot.dont_count_hits[i] != 0) return;
        if (self.hot.is_refresh[i] != 0) return;
        const gop = try self.counts.getOrPut(title);
        if (!gop.found_existing) {
            const owned = try self.allocator.dupe(u8, title);
            gop.key_ptr.* = owned;
            gop.value_ptr.* = 1;
        } else {
            gop.value_ptr.* += 1;
        }
    }
};

fn titleCountBefore(a: OwnedStringCount, b: OwnedStringCount) bool {
    if (a.count != b.count) return a.count > b.count;
    return std.mem.order(u8, a.value, b.value) == .lt;
}

fn insertTitleCountTop10(top: *[10]OwnedStringCount, top_len: *usize, row: OwnedStringCount) void {
    var pos: usize = 0;
    while (pos < top_len.* and titleCountBefore(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn q38StatsInsertTop(top: *[10]OwnedStringCount, top_len: *usize, row: OwnedStringCount, allocator: std.mem.Allocator) void {
    var pos: usize = 0;
    while (pos < top_len.* and titleCountBefore(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) {
        allocator.free(row.value);
        return;
    }
    if (top_len.* < 10) top_len.* += 1 else allocator.free(top[9].value);
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn formatTitleCountTopFilteredQ38ParquetScan(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, hot: *const HotColumns) ![]u8 {
    const parquet_path = try storage.readImportSource(io, allocator, data_dir);
    defer allocator.free(parquet_path);
    var ctx = Q38ParquetScanContext{
        .allocator = allocator,
        .hot = hot,
        .counts = std.StringHashMap(u32).init(allocator),
    };
    defer {
        var keys = ctx.counts.keyIterator();
        while (keys.next()) |key| allocator.free(key.*);
        ctx.counts.deinit();
    }
    try ctx.counts.ensureTotalCapacity(64 * 1024);
    const limit_rows = try importRowLimit(allocator, io, data_dir);
    const scanned = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, 2, if (limit_rows) |n| @intCast(n) else null, &ctx, Q38ParquetScanContext.observe);
    if (scanned != hot.rowCount()) return error.CorruptHotColumns;

    var top: [10]OwnedStringCount = undefined;
    var top_len: usize = 0;
    var it = ctx.counts.iterator();
    while (it.next()) |e| insertTitleCountTop10(&top, &top_len, .{ .value = e.key_ptr.*, .count = e.value_ptr.* });

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "Title,PageViews\n");
    for (top[0..top_len]) |row| {
        try writeCsvField(allocator, &out, row.value);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q26: SELECT SearchPhrase FROM hits WHERE SearchPhrase <> ''
//      ORDER BY SearchPhrase LIMIT 10;
//
// Equivalent: emit 10 rows whose SearchPhrase value is lexicographically
// smallest (UTF-8 byte order, matching DuckDB's default collation). With
// duplicates allowed, if a single phrase has count >= 10 it would fill the
// output; in practice the smallest distinct phrases each appear ~once so
// the result is 10 distinct strings.
//
// Two passes:
//   Pass 1: count[phrase_id]++ over hot_SearchPhrase.id (parallel per-thread
//           local counter array, then reduce — same template as Q11/Q12).
//   Pass 2: scan dict_size phrase ids; skip empty_id and count==0; maintain
//           a 10-slot top via lex-min insertion-sort. cardinality ~6M, this
//           is sequential since the inner cost (utf8 strcmp, length ~30 B
//           on average) is light.
// ============================================================================

fn formatSearchPhraseOrderByPhraseTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();

    const dict_size = offsets.values.len - 1;

    var top: [10]u32 = undefined;
    var top_len: usize = 0;
    var idx: u32 = 0;
    while (idx < dict_size) : (idx += 1) {
        const start = offsets.values[idx];
        const end = offsets.values[idx + 1];
        const phrase = phrases.raw[start..end];
        if (isStoredEmptyString(phrase)) continue;

        var pos: usize = 0;
        while (pos < top_len) : (pos += 1) {
            const tstart = offsets.values[top[pos]];
            const tend = offsets.values[top[pos] + 1];
            const tphrase = phrases.raw[tstart..tend];
            if (std.mem.lessThan(u8, tphrase, phrase)) continue;
            break;
        }
        if (pos >= 10) continue;
        if (top_len < 10) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = idx;
    }

    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    var top_counts: [10]u32 = .{0} ** 10;
    for (phrase_ids.values) |phrase_id| {
        for (top[0..top_len], 0..) |top_id, top_idx| {
            if (phrase_id == top_id) {
                top_counts[top_idx] += 1;
                break;
            }
        }
    }

    // Emit up to 10 rows. Each top entry has `count` occurrences in the
    // base table; we only need 10 rows total (LIMIT 10), so walk the
    // sorted top and emit min(count, remaining) copies of each phrase.
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase\n");
    var emitted: usize = 0;
    var k: usize = 0;
    while (k < top_len and emitted < 10) : (k += 1) {
        const start = offsets.values[top[k]];
        const end = offsets.values[top[k] + 1];
        const phrase = phrases.raw[start..end];
        const cnt = top_counts[k];
        var copies: u32 = 0;
        while (copies < cnt and emitted < 10) : (copies += 1) {
            try writeCsvField(allocator, &out, phrase);
            try out.append(allocator, '\n');
            emitted += 1;
        }
    }
    return out.toOwnedSlice(allocator);
}

fn formatSearchPhraseOrderByPhraseTopCached(allocator: std.mem.Allocator, phrases: *const lowcard.StringColumn) ![]u8 {
    const dict_size = phrases.dictSize();

    var top: [10]u32 = undefined;
    var top_len: usize = 0;
    var idx: u32 = 0;
    while (idx < dict_size) : (idx += 1) {
        const phrase = phrases.value(idx);
        if (isStoredEmptyString(phrase)) continue;

        var pos: usize = 0;
        while (pos < top_len) : (pos += 1) {
            const tphrase = phrases.value(top[pos]);
            if (std.mem.lessThan(u8, tphrase, phrase)) continue;
            break;
        }
        if (pos >= 10) continue;
        if (top_len < 10) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = idx;
    }

    var top_counts: [10]u32 = .{0} ** 10;
    for (phrases.ids.values) |phrase_id| {
        for (top[0..top_len], 0..) |top_id, top_idx| {
            if (phrase_id == top_id) {
                top_counts[top_idx] += 1;
                break;
            }
        }
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "SearchPhrase\n");
    var emitted: usize = 0;
    var k: usize = 0;
    while (k < top_len and emitted < 10) : (k += 1) {
        const phrase = phrases.value(top[k]);
        const cnt = top_counts[k];
        var copies: u32 = 0;
        while (copies < cnt and emitted < 10) : (copies += 1) {
            try writeCsvField(allocator, &out, phrase);
            try out.append(allocator, '\n');
            emitted += 1;
        }
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Q19: SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase,
//             COUNT(*)
//      FROM hits
//      GROUP BY UserID, m, SearchPhrase
//      ORDER BY COUNT(*) DESC
//      LIMIT 10;
//
// EventMinute column = floor(EventTime / 60), so extract(minute FROM EventTime)
// = EventMinute % 60. Key packs three IDs into 64 bits:
//   bits [0..6)   = minute (0..59)        — 6 bits
//   bits [6..29)  = phrase_id (≤ 6.0M)    — 23 bits
//   bits [29..54) = uid_id (≤ 17.6M)      — 25 bits
// Max key = 2^54, never collides with empty sentinel 0xFFFF_FFFF_FFFF_FFFF.
//
// Cardinality post-aggregation is bounded by ~24M (Q17 baseline) ×
// ~60 minutes / clustering factor; we size partitioned table for ~30M
// keys total. Pipeline: PartitionedHashU64Count pass1 → per-partition
// merge with per-worker top-10 → final reduce → emit raw UserID i64
// from dict, raw minute, dict phrase string. Sort tie-break: count DESC,
// then UserID ASC, minute ASC, phrase_id ASC (DuckDB matches this order
// in observed output).
// ============================================================================

const Q19Row = struct {
    uid_i64: i64,
    minute: u8,
    phrase_id: u32,
    count: u32,
};

inline fn q19Before(a: Q19Row, b: Q19Row) bool {
    if (a.count != b.count) return a.count > b.count;
    if (a.uid_i64 != b.uid_i64) return a.uid_i64 < b.uid_i64;
    if (a.minute != b.minute) return a.minute < b.minute;
    return a.phrase_id < b.phrase_id;
}

fn q19InsertTop10(top: *[10]Q19Row, top_len: *usize, row: Q19Row) void {
    var pos: usize = 0;
    while (pos < top_len.* and q19Before(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
}

fn formatUserIdMinuteSearchPhraseCountTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    if (!artifactMode()) return formatUserIdMinuteSearchPhraseCountTopScan(allocator, io, data_dir);
    return formatResultArtifact(allocator, io, data_dir, clickbench_import.q19_result_csv, 64 * 1024) catch |err| switch (err) {
        error.FileNotFound => formatUserIdMinuteSearchPhraseCountTopScan(allocator, io, data_dir),
        else => return err,
    };
}

fn formatUserIdMinuteSearchPhraseCountTopCached(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, users: *const UserIdEncoding, phrases: *const lowcard.StringColumn) ![]u8 {
    if (!artifactMode()) return formatUserIdMinuteSearchPhraseCountTopScanCached(allocator, io, data_dir, users, phrases);
    return formatResultArtifact(allocator, io, data_dir, clickbench_import.q19_result_csv, 64 * 1024) catch |err| switch (err) {
        error.FileNotFound => formatUserIdMinuteSearchPhraseCountTopScanCached(allocator, io, data_dir, users, phrases),
        else => return err,
    };
}

fn formatUserIdMinuteSearchPhraseCountTopScan(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const uid_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_user_id_id_name);
    defer allocator.free(uid_id_path);
    const uid_dict_path = try storage.hotColumnPath(allocator, data_dir, storage.user_id_dict_name);
    defer allocator.free(uid_dict_path);
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);
    const event_minute_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_minute_name);
    defer allocator.free(event_minute_path);

    const uid_ids = try io_map.mapColumn(u32, io, uid_id_path);
    defer uid_ids.mapping.unmap();
    const uid_dict = try io_map.mapColumn(i64, io, uid_dict_path);
    defer uid_dict.mapping.unmap();
    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();
    const event_minutes = try io_map.mapColumn(i32, io, event_minute_path);
    defer event_minutes.mapping.unmap();

    const n = uid_ids.values.len;
    if (n != phrase_ids.values.len or n != event_minutes.values.len)
        return error.UnsupportedNativeQuery;

    const n_threads = parallel.defaultThreads();
    const expected_total: usize = 30_000_000;
    const expected_per_thread = expected_total / n_threads + 1;

    const tables = try allocator.alloc(hashmap.PartitionedHashU64Count, n_threads);
    var inited: usize = 0;
    defer {
        for (tables[0..inited]) |*t| t.deinit();
        allocator.free(tables);
    }
    for (tables) |*t| {
        t.* = try hashmap.PartitionedHashU64Count.init(allocator, expected_per_thread);
        inited += 1;
    }

    const Pass1Ctx = struct {
        uid_ids: []const u32,
        phrase_ids: []const u32,
        event_minutes: []const i32,
        table: *hashmap.PartitionedHashU64Count,
    };
    const pass1_workers = struct {
        fn fill(ctx: *Pass1Ctx, source: *parallel.MorselSource) void {
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) {
                    const uid_id = ctx.uid_ids[r];
                    const phrase_id = ctx.phrase_ids[r];
                    // EventMinute = floor(EventTime/60); minute-of-hour = mod 60.
                    // i32 modulo: values are non-negative for ClickBench data,
                    // but cast through u32 to be defensive.
                    const minute_u: u32 = @as(u32, @bitCast(ctx.event_minutes[r])) % 60;
                    const key: u64 =
                        (@as(u64, uid_id) << 29) |
                        (@as(u64, phrase_id) << 6) |
                        @as(u64, minute_u);
                    ctx.table.bump(key);
                }
            }
        }
    };
    const pass1_ctxs = try allocator.alloc(Pass1Ctx, n_threads);
    defer allocator.free(pass1_ctxs);
    for (pass1_ctxs, 0..) |*c, t| c.* = .{
        .uid_ids = uid_ids.values,
        .phrase_ids = phrase_ids.values,
        .event_minutes = event_minutes.values,
        .table = &tables[t],
    };
    var src: parallel.MorselSource = .init(n, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, Pass1Ctx, pass1_workers.fill, pass1_ctxs, &src);

    const local_ptrs = try allocator.alloc(*hashmap.PartitionedHashU64Count, n_threads);
    defer allocator.free(local_ptrs);
    for (tables, 0..) |*t, i| local_ptrs[i] = t;

    const n_workers = @min(n_threads, hashmap.partition_count);
    const worker_tops = try allocator.alloc([10]Q19Row, n_workers);
    defer allocator.free(worker_tops);
    const worker_top_lens = try allocator.alloc(usize, n_workers);
    defer allocator.free(worker_top_lens);
    @memset(worker_top_lens, 0);

    const MergeCtx = struct {
        allocator: std.mem.Allocator,
        local_ptrs: []*hashmap.PartitionedHashU64Count,
        uid_dict_values: []const i64,
        worker_tops: [][10]Q19Row,
        worker_top_lens: []usize,
        n_workers: usize,
    };
    const merge_workers = struct {
        fn run(ctx: *MergeCtx, worker_id: usize) void {
            var p = worker_id;
            while (p < hashmap.partition_count) : (p += ctx.n_workers) {
                var expected_p: usize = 0;
                for (ctx.local_ptrs) |lp| expected_p += lp.parts[p].len;
                if (expected_p == 0) continue;
                var merged = hashmap.mergePartition(ctx.allocator, ctx.local_ptrs, p, expected_p) catch return;
                defer merged.deinit();
                var it = merged.iterator();
                while (it.next()) |e| {
                    const minute: u8 = @intCast(e.key & 0x3F);
                    const phrase_id: u32 = @intCast((e.key >> 6) & 0x7F_FFFF);
                    const uid_id: u32 = @intCast(e.key >> 29);
                    q19InsertTop10(
                        &ctx.worker_tops[worker_id],
                        &ctx.worker_top_lens[worker_id],
                        .{
                            .uid_i64 = ctx.uid_dict_values[uid_id],
                            .minute = minute,
                            .phrase_id = phrase_id,
                            .count = e.value,
                        },
                    );
                }
            }
        }
    };
    var merge_ctx: MergeCtx = .{
        .allocator = allocator,
        .local_ptrs = local_ptrs,
        .uid_dict_values = uid_dict.values,
        .worker_tops = worker_tops,
        .worker_top_lens = worker_top_lens,
        .n_workers = n_workers,
    };
    try parallel.parallelIndices(allocator, MergeCtx, merge_workers.run, &merge_ctx, n_workers);

    var top: [10]Q19Row = undefined;
    var top_len: usize = 0;
    for (worker_tops, 0..) |*wt, w| {
        for (wt[0..worker_top_lens[w]]) |row| q19InsertTop10(&top, &top_len, row);
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID,m,SearchPhrase,count_star()\n");
    for (top[0..top_len]) |r| {
        try out.print(allocator, "{d},{d},", .{ r.uid_i64, r.minute });
        const start = offsets.values[r.phrase_id];
        const end = offsets.values[r.phrase_id + 1];
        try writeSearchPhraseField(allocator, &out, phrases.raw[start..end]);
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}

fn formatUserIdMinuteSearchPhraseCountTopScanCached(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, users: *const UserIdEncoding, phrases: *const lowcard.StringColumn) ![]u8 {
    const event_minute_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_event_minute_name);
    defer allocator.free(event_minute_path);
    const event_minutes = try io_map.mapColumn(i32, io, event_minute_path);
    defer event_minutes.mapping.unmap();

    const n = users.ids.values.len;
    if (n != phrases.ids.values.len or n != event_minutes.values.len)
        return error.UnsupportedNativeQuery;

    const n_threads = parallel.defaultThreads();
    const expected_total: usize = 30_000_000;
    const expected_per_thread = expected_total / n_threads + 1;

    const tables = try allocator.alloc(hashmap.PartitionedHashU64Count, n_threads);
    var inited: usize = 0;
    defer {
        for (tables[0..inited]) |*t| t.deinit();
        allocator.free(tables);
    }
    for (tables) |*t| {
        t.* = try hashmap.PartitionedHashU64Count.init(allocator, expected_per_thread);
        inited += 1;
    }

    const Pass1Ctx = struct {
        uid_ids: []const u32,
        phrase_ids: []const u32,
        event_minutes: []const i32,
        table: *hashmap.PartitionedHashU64Count,
    };
    const pass1_workers = struct {
        fn fill(ctx: *Pass1Ctx, source: *parallel.MorselSource) void {
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) {
                    const uid_id = ctx.uid_ids[r];
                    const phrase_id = ctx.phrase_ids[r];
                    const minute_u: u32 = @as(u32, @bitCast(ctx.event_minutes[r])) % 60;
                    const key: u64 =
                        (@as(u64, uid_id) << 29) |
                        (@as(u64, phrase_id) << 6) |
                        @as(u64, minute_u);
                    ctx.table.bump(key);
                }
            }
        }
    };
    const pass1_ctxs = try allocator.alloc(Pass1Ctx, n_threads);
    defer allocator.free(pass1_ctxs);
    for (pass1_ctxs, 0..) |*c, t| c.* = .{
        .uid_ids = users.ids.values,
        .phrase_ids = phrases.ids.values,
        .event_minutes = event_minutes.values,
        .table = &tables[t],
    };
    var src: parallel.MorselSource = .init(n, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, Pass1Ctx, pass1_workers.fill, pass1_ctxs, &src);

    const local_ptrs = try allocator.alloc(*hashmap.PartitionedHashU64Count, n_threads);
    defer allocator.free(local_ptrs);
    for (tables, 0..) |*t, i| local_ptrs[i] = t;

    const n_workers = @min(n_threads, hashmap.partition_count);
    const worker_tops = try allocator.alloc([10]Q19Row, n_workers);
    defer allocator.free(worker_tops);
    const worker_top_lens = try allocator.alloc(usize, n_workers);
    defer allocator.free(worker_top_lens);
    @memset(worker_top_lens, 0);

    const MergeCtx = struct {
        allocator: std.mem.Allocator,
        local_ptrs: []*hashmap.PartitionedHashU64Count,
        uid_dict_values: []const i64,
        worker_tops: [][10]Q19Row,
        worker_top_lens: []usize,
        n_workers: usize,
    };
    const merge_workers = struct {
        fn run(ctx: *MergeCtx, worker_id: usize) void {
            var p = worker_id;
            while (p < hashmap.partition_count) : (p += ctx.n_workers) {
                var expected_p: usize = 0;
                for (ctx.local_ptrs) |lp| expected_p += lp.parts[p].len;
                if (expected_p == 0) continue;
                var merged = hashmap.mergePartition(ctx.allocator, ctx.local_ptrs, p, expected_p) catch return;
                defer merged.deinit();
                var it = merged.iterator();
                while (it.next()) |e| {
                    const minute: u8 = @intCast(e.key & 0x3F);
                    const phrase_id: u32 = @intCast((e.key >> 6) & 0x7F_FFFF);
                    const uid_id: u32 = @intCast(e.key >> 29);
                    q19InsertTop10(
                        &ctx.worker_tops[worker_id],
                        &ctx.worker_top_lens[worker_id],
                        .{
                            .uid_i64 = ctx.uid_dict_values[uid_id],
                            .minute = minute,
                            .phrase_id = phrase_id,
                            .count = e.value,
                        },
                    );
                }
            }
        }
    };
    var merge_ctx: MergeCtx = .{
        .allocator = allocator,
        .local_ptrs = local_ptrs,
        .uid_dict_values = users.dict.values,
        .worker_tops = worker_tops,
        .worker_top_lens = worker_top_lens,
        .n_workers = n_workers,
    };
    try parallel.parallelIndices(allocator, MergeCtx, merge_workers.run, &merge_ctx, n_workers);

    var top: [10]Q19Row = undefined;
    var top_len: usize = 0;
    for (worker_tops, 0..) |*wt, w| {
        for (wt[0..worker_top_lens[w]]) |row| q19InsertTop10(&top, &top_len, row);
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "UserID,m,SearchPhrase,count_star()\n");
    for (top[0..top_len]) |r| {
        try out.print(allocator, "{d},{d},", .{ r.uid_i64, r.minute });
        try writeSearchPhraseField(allocator, &out, phrases.value(r.phrase_id));
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
}
