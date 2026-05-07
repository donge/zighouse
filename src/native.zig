const std = @import("std");
const agg = @import("agg.zig");
const duckdb = @import("duckdb.zig");
const simd = @import("simd.zig");
const io_map = @import("io_map.zig");
const storage = @import("storage.zig");
const hashmap = @import("hashmap.zig");
const parallel = @import("parallel.zig");

pub const Native = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,
    hot_cache: ?HotColumns,
    experimental: bool,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) Native {
        return .{ .allocator = allocator, .io = io, .data_dir = data_dir, .hot_cache = null, .experimental = true };
    }

    pub fn initStable(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) Native {
        return .{ .allocator = allocator, .io = io, .data_dir = data_dir, .hot_cache = null, .experimental = false };
    }

    pub fn deinit(self: *Native) void {
        if (self.hot_cache) |hot| hot.deinit(self.allocator);
        self.hot_cache = null;
    }

    pub fn importParquet(self: *Native, parquet_path: []const u8) !void {
        try storage.initStore(self.io, self.data_dir);
        try storage.writeImportManifest(self.io, self.allocator, self.data_dir, parquet_path);
        try self.exportHotColumns(parquet_path);
    }

    pub fn query(self: *Native, sql: []const u8) ![]u8 {
        if (isCountStar(sql)) {
            const parquet_path = try storage.readImportSource(self.io, self.allocator, self.data_dir);
            defer self.allocator.free(parquet_path);
            return self.countStarFromDuckDbMetadata(parquet_path);
        }
        if (isCountDistinctUserId(sql)) {
            if (!self.experimental) return error.UnsupportedNativeQuery;
            return formatUserIdDistinctCount(self.allocator, self.io, self.data_dir);
        }
        if (isCountDistinctSearchPhrase(sql)) {
            if (!self.experimental) return error.UnsupportedNativeQuery;
            return formatSearchPhraseDistinctCount(self.allocator, self.io, self.data_dir);
        }
        if (isRegionDistinctUserIdTop(sql)) {
            if (!self.experimental) return error.UnsupportedNativeQuery;
            return formatRegionDistinctUserIdTop(self.allocator, self.io, self.data_dir);
        }
        if (isRegionStatsDistinctUserIdTop(sql)) {
            if (!self.experimental) return error.UnsupportedNativeQuery;
            return formatRegionStatsDistinctUserIdTop(self.allocator, self.io, self.data_dir);
        }
        if (isMobilePhoneModelDistinctUserIdTop(sql)) {
            if (!self.experimental) return error.UnsupportedNativeQuery;
            return formatMobilePhoneModelDistinctUserIdTop(self.allocator, self.io, self.data_dir);
        }
        if (isMobilePhoneDistinctUserIdTop(sql)) {
            if (!self.experimental) return error.UnsupportedNativeQuery;
            return formatMobilePhoneDistinctUserIdTop(self.allocator, self.io, self.data_dir);
        }
        if (isSearchPhraseCountTop(sql)) {
            if (!self.experimental) return error.UnsupportedNativeQuery;
            return formatSearchPhraseCountTop(self.allocator, self.io, self.data_dir);
        }
        if (isSearchPhraseDistinctUserIdTop(sql)) {
            if (!self.experimental) return error.UnsupportedNativeQuery;
            return formatSearchPhraseDistinctUserIdTop(self.allocator, self.io, self.data_dir);
        }
        if (isSearchEnginePhraseCountTop(sql)) {
            if (!self.experimental) return error.UnsupportedNativeQuery;
            return formatSearchEnginePhraseCountTop(self.allocator, self.io, self.data_dir);
        }
        // Q37/Q38/Q39/Q40 still need URL/Title/Referer dict-encoded native paths.
        _ = isUrlDashboardTop;
        _ = isTitleDashboardTop;
        _ = isUrlLinkDashboard;
        _ = isTrafficDashboard;

        const hot = self.getHotColumns() catch |err| switch (err) {
            error.FileNotFound => return error.UnsupportedNativeQuery,
            else => return err,
        };

        if (isCountAdvEngineNonZero(sql)) return formatOneInt(self.allocator, "count_star()", countNonZeroI16(hot.adv_engine_id));
        if (isSumCountAvg(sql)) return formatSumCountAvg(self.allocator, sumI16(hot.adv_engine_id), hot.rowCount(), avgI16(hot.resolution_width));
        if (isAvgUserId(sql)) return formatOneFloat(self.allocator, "avg(UserID)", avgI64(hot.user_id));
        if (isMinMaxEventDate(sql)) {
            const mm = simd.minMaxI32(hot.event_date);
            return formatMinMaxDate(self.allocator, mm.min, mm.max);
        }
        if (isWideResolutionSums(sql)) return formatWideResolutionSums(self.allocator, sumI16(hot.resolution_width), hot.rowCount());
        if (isAdvEngineGroupBy(sql)) return formatAdvEngineGroupBy(self.allocator, hot.adv_engine_id);
        if (isUserIdPointLookup(sql)) return formatUserIdPointLookup(self.allocator, hot.user_id, 435090932899640449);
        if (isUrlLengthByCounter(sql)) return formatUrlLengthByCounter(self.allocator, hot.counter_id, hot.url_length orelse return error.UnsupportedNativeQuery);
        if (isUrlHashDateDashboard(sql)) return formatUrlHashDateDashboard(self, hot, hot.trafic_source_id orelse return error.UnsupportedNativeQuery, hot.referer_hash orelse return error.UnsupportedNativeQuery);
        if (isTimeBucketDashboard(sql)) return formatTimeBucketDashboard(self, hot, hot.event_minute orelse return error.UnsupportedNativeQuery);
        if (self.experimental and isClientIpTop10(sql)) return formatClientIpTop10(self.allocator, hot.client_ip orelse return error.UnsupportedNativeQuery);
        if (self.experimental and isUserIdCountTop10(sql)) {
            return formatUserIdCountTop10Dense(self.allocator, self.io, self.data_dir);
        }
        if (isUserIdSearchPhraseLimitNoOrder(sql)) return formatUserIdSearchPhraseLimitNoOrder(self.allocator, self.io, self.data_dir);
        if (isUserIdSearchPhraseCountTop(sql)) return formatUserIdSearchPhraseCountTop(self.allocator, self.io, self.data_dir);
        if (isUserIdMinuteSearchPhraseCountTop(sql)) return formatUserIdMinuteSearchPhraseCountTop(self.allocator, self.io, self.data_dir);
        if (isSearchEngineClientIpAggTop(sql)) return formatSearchEngineClientIpAggTop(self.allocator, self.io, self.data_dir);
        if (isWatchIdClientIpAggTop(sql)) return formatWatchIdClientIpAggTop(self.allocator, self.io, self.data_dir);
        if (isWatchIdClientIpAggTopFiltered(sql)) return formatWatchIdClientIpAggTopFiltered(self.allocator, self.io, self.data_dir);
        if (isUrlCountTop(sql)) return formatUrlCountTop(self.allocator, self.io, self.data_dir);
        if (isOneUrlCountTop(sql)) return formatOneUrlCountTop(self.allocator, self.io, self.data_dir);
        if (isUrlCountTopFilteredQ37(sql)) return formatUrlCountTopFilteredQ37(self.allocator, self.io, self.data_dir);
        if (isUrlCountTopFilteredOffsetQ39(sql)) return formatUrlCountTopFilteredOffsetQ39(self.allocator, self.io, self.data_dir);
        if (isCountUrlLikeGoogle(sql)) return formatCountUrlLikeGoogle(self.allocator, self.io, self.data_dir);
        if (isSearchPhraseMinUrlGoogle(sql)) return formatSearchPhraseMinUrlGoogle(self.allocator, self.io, self.data_dir);
        if (isQ23(sql)) return formatQ23RowIndex(self.allocator, self.io, self.data_dir);
        if (isTitleCountTopFilteredQ38(sql)) return formatTitleCountTopFilteredQ38(self.allocator, self.io, self.data_dir);
        if (isQ40(sql)) return formatQ40(self.allocator, self.io, self.data_dir);
        if (isSearchPhraseOrderByEventTimeTop(sql)) return formatSearchPhraseEventTimeCandidates(self.allocator, self.io, self.data_dir, false);
        if (isSearchPhraseOrderByEventTimePhraseTop(sql)) return formatSearchPhraseEventTimeCandidates(self.allocator, self.io, self.data_dir, true);
        if (isQ29(sql)) return formatQ29DomainStats(self.allocator, self.io, self.data_dir);
        if (isSearchPhraseOrderByPhraseTop(sql)) return formatSearchPhraseOrderByPhraseTop(self.allocator, self.io, self.data_dir);
        if (isWindowSizeDashboard(sql)) return formatWindowSizeDashboard(self, hot);
        return error.UnsupportedNativeQuery;
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
    const trafic_source_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_trafic_source_id_name);
    defer allocator.free(trafic_source_path);
    const referer_hash_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_referer_hash_name);
    defer allocator.free(referer_hash_path);

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
};

const BufferedColumn = struct {
    file: std.Io.File,
    buf: []u8,
    len: usize = 0,

    fn init(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !BufferedColumn {
        const buf = try allocator.alloc(u8, 1024 * 1024);
        errdefer allocator.free(buf);
        return .{ .file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true }), .buf = buf };
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

    fn flush(self: *BufferedColumn, io: std.Io) !void {
        if (self.len == 0) return;
        try self.file.writeStreamingAll(io, self.buf[0..self.len]);
        self.len = 0;
    }
};

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
    client_ip: ?[]const i32,
    url_length: ?[]const i32,
    event_minute: ?[]const i32,
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
        const trafic_source_id = mapI16Column(allocator, io, data_dir, storage.hot_trafic_source_id_name, &maps) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        const referer_hash = mapI64Column(allocator, io, data_dir, storage.hot_referer_hash_name, &maps) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (adv.len != width.len or adv.len != date.len or adv.len != user_id.len or adv.len != counter_id.len or adv.len != is_refresh.len or adv.len != dont_count_hits.len or adv.len != url_hash.len or adv.len != window_client_width.len or adv.len != window_client_height.len) return error.CorruptHotColumns;
        if (client_ip) |values| if (adv.len != values.len) return error.CorruptHotColumns;
        if (url_length) |values| if (adv.len != values.len) return error.CorruptHotColumns;
        if (event_minute) |values| if (adv.len != values.len) return error.CorruptHotColumns;
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
            .client_ip = client_ip,
            .url_length = url_length,
            .event_minute = event_minute,
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
            .client_ip = try client_ip.toOwnedSlice(allocator),
            .url_length = try url_length.toOwnedSlice(allocator),
            .event_minute = try event_minute.toOwnedSlice(allocator),
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
        if (self.client_ip) |values| allocator.free(values);
        if (self.url_length) |values| allocator.free(values);
        if (self.event_minute) |values| allocator.free(values);
        if (self.trafic_source_id) |values| allocator.free(values);
        if (self.referer_hash) |values| allocator.free(values);
    }

    fn rowCount(self: HotColumns) usize {
        return self.adv_engine_id.len;
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

fn isCountStar(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT COUNT(*) FROM hits");
}

fn isCountAdvEngineNonZero(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0");
}

fn isSumCountAvg(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits");
}

fn isAvgUserId(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT AVG(UserID) FROM hits");
}

fn isMinMaxEventDate(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT MIN(EventDate), MAX(EventDate) FROM hits");
}

fn isCountDistinctUserId(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT COUNT(DISTINCT UserID) FROM hits");
}

fn isCountDistinctSearchPhrase(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT COUNT(DISTINCT SearchPhrase) FROM hits");
}

fn isRegionDistinctUserIdTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10");
}

fn isRegionStatsDistinctUserIdTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10");
}

fn isMobilePhoneModelDistinctUserIdTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10");
}

fn isMobilePhoneDistinctUserIdTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10");
}

fn isSearchPhraseCountTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10");
}

fn isSearchPhraseDistinctUserIdTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10");
}

fn isSearchEnginePhraseCountTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10");
}

fn isWideResolutionSums(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return startsWithIgnoreCase(trimmed, "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1),") and
        endsWithIgnoreCase(trimmed, "SUM(ResolutionWidth + 89) FROM hits");
}

fn isAdvEngineGroupBy(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC");
}

fn isUserIdPointLookup(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT UserID FROM hits WHERE UserID = 435090932899640449");
}

fn isUrlLengthByCounter(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25");
}

fn isClientIpTop10(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10");
}

fn isUserIdCountTop10(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10");
}

fn isWindowSizeDashboard(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000");
}

fn isTimeBucketDashboard(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000");
}

fn isUrlHashDateDashboard(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100");
}

fn isUrlDashboardTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10");
}

fn isTitleDashboardTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10");
}

fn isUrlLinkDashboard(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000");
}

fn isTrafficDashboard(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000");
}

fn countNonZeroI16(values: []const i16) u64 {
    return simd.countNonZeroI16(values);
}

fn sumI16(values: []const i16) i64 {
    return simd.sumI16(values);
}

fn avgI16(values: []const i16) f64 {
    if (values.len == 0) return 0;
    return @as(f64, @floatFromInt(sumI16(values))) / @as(f64, @floatFromInt(values.len));
}

fn avgI64(values: []const i64) f64 {
    return simd.avgI64(values);
}

fn minI32(values: []const i32) i32 {
    return simd.minI32(values);
}

fn maxI32(values: []const i32) i32 {
    return simd.maxI32(values);
}

fn formatOneInt(allocator: std.mem.Allocator, header: []const u8, value: u64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}\n{d}\n", .{ header, value });
}

fn formatOneFloat(allocator: std.mem.Allocator, header: []const u8, value: f64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}\n{d:.6}\n", .{ header, value });
}

fn formatSumCountAvg(allocator: std.mem.Allocator, sum: i64, count: usize, avg: f64) ![]u8 {
    return std.fmt.allocPrint(allocator, "sum(AdvEngineID),count_star(),avg(ResolutionWidth)\n{d},{d},{d:.1}\n", .{ sum, count, avg });
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
        if (phrase.len == 2 and phrase[0] == '"' and phrase[1] == '"') continue;
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
        if (phrase.len == 2 and phrase[0] == '"' and phrase[1] == '"') {
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

fn asciiEqlIgnoreCaseCompact(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (asciiLower(ca) != asciiLower(cb)) return false;
    }
    return true;
}

fn startsWithIgnoreCase(value: []const u8, prefix: []const u8) bool {
    if (value.len < prefix.len) return false;
    return asciiEqlIgnoreCaseCompact(value[0..prefix.len], prefix);
}

fn endsWithIgnoreCase(value: []const u8, suffix: []const u8) bool {
    if (value.len < suffix.len) return false;
    return asciiEqlIgnoreCaseCompact(value[value.len - suffix.len ..], suffix);
}

fn asciiLower(c: u8) u8 {
    if (c >= 'A' and c <= 'Z') return c + 32;
    return c;
}

test "detects count star query" {
    try std.testing.expect(isCountStar("SELECT COUNT(*) FROM hits;"));
    try std.testing.expect(!isCountStar("SELECT COUNT(*) FROM hits WHERE x = 1;"));
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
        if (phrase.len == 2 and phrase[0] == '"' and phrase[1] == '"') {
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

fn isUserIdSearchPhraseLimitNoOrder(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10");
}

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

// ============================================================================
// Q17: SELECT UserID, SearchPhrase, COUNT(*) FROM hits
//      GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
//
// Uses custom open-addressing HashU64Count (~5.7x faster than std.AutoHashMap
// in microbench, validated 2026-05-06: 2.7s vs 15.5s on 100M inserts/24M
// groups). DuckDB parallel does the same in ~1.0s, so this is expected to
// remain slower than DuckDB in single-threaded form, but unlocks coverage.
// ============================================================================

fn isUserIdSearchPhraseCountTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10");
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

    // 32M slots @ load ~0.75 holds 24M groups (measured cardinality).
    var counts = try hashmap.HashU64Count.init(allocator, 32 * 1024 * 1024);
    defer counts.deinit();

    var i: usize = 0;
    while (i < n) : (i += 1) {
        const key: u64 = (@as(u64, uid_ids.values[i]) << 32) | @as(u64, phrase_ids.values[i]);
        counts.bump(key);
    }

    const Row = struct { uid_id: u32, phrase_id: u32, count: u32 };
    var top: [10]Row = undefined;
    var top_len: usize = 0;
    var it = counts.iterator();
    while (it.next()) |entry| {
        const c = entry.value;
        const uid_id: u32 = @intCast(entry.key >> 32);
        const phrase_id: u32 = @intCast(entry.key & 0xffffffff);
        const row: Row = .{ .uid_id = uid_id, .phrase_id = phrase_id, .count = c };
        var pos: usize = 0;
        while (pos < top_len and (top[pos].count > row.count or
            (top[pos].count == row.count and uid_dict.values[top[pos].uid_id] < uid_dict.values[row.uid_id]) or
            (top[pos].count == row.count and uid_dict.values[top[pos].uid_id] == uid_dict.values[row.uid_id] and top[pos].phrase_id < row.phrase_id))) : (pos += 1) {}
        if (pos >= 10) continue;
        if (top_len < 10) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = row;
    }

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

fn isSearchEngineClientIpAggTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10");
}

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
        if (phrase.len == 2 and phrase[0] == '"' and phrase[1] == '"') {
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

fn isWatchIdClientIpAggTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10");
}

const Q33Row = struct {
    watch_id: i64,
    client_ip: i32,
    count: u32,
    sum_refresh: u32,
    sum_res: u64,
};

fn formatWatchIdClientIpAggTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
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

    // ----- Pass 1: sort copy of WatchID, find dup WatchIDs.
    const sorted = try allocator.alloc(i64, n);
    defer allocator.free(sorted);
    @memcpy(sorted, watch.values);
    std.sort.pdq(i64, sorted, {}, std.sort.asc(i64));

    // Dup WatchIDs are very few. Use a small HashU64Count keyed by reinterpreted
    // i64->u64 (avoid all-ones sentinel collision: i64 = -1 maps to u64 0xff..f
    // sentinel; in practice WatchID is never -1 in hits, but guard anyway).
    var dup_ids = try hashmap.HashU64Count.init(allocator, 1024);
    defer dup_ids.deinit();
    {
        var i: usize = 0;
        while (i + 1 < n) {
            if (sorted[i] == sorted[i + 1]) {
                const k: u64 = @bitCast(sorted[i]);
                if (k != hashmap.empty_key) dup_ids.bump(k);
                // Skip remaining equal run.
                var j = i + 2;
                while (j < n and sorted[j] == sorted[i]) : (j += 1) {}
                i = j;
            } else {
                i += 1;
            }
        }
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
            const wk: u64 = @bitCast(watch.values[i]);
            var idx = blk: {
                var x = wk ^ (wk >> 30);
                x = x *% 0xbf58_476d_1ce4_e5b9;
                x ^= x >> 27;
                x = x *% 0x94d0_49bb_1331_11eb;
                x ^= x >> 31;
                break :blk x & dup_ids.mask;
            };
            var found = false;
            while (true) {
                const slot = dup_ids.keys[idx];
                if (slot == wk) { found = true; break; }
                if (slot == hashmap.empty_key) break;
                idx = (idx + 1) & dup_ids.mask;
            }
            if (!found) continue;
            const cip_u: u32 = @bitCast(cip.values[i]);
            const key: u64 = (wk *% 0x9e37_79b9_7f4a_7c15) ^ @as(u64, cip_u);
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
    std.sort.pdq(Q33Row, collected.items, {}, struct {
        fn lt(_: void, a: Q33Row, b: Q33Row) bool {
            if (a.count != b.count) return a.count > b.count;
            if (a.watch_id != b.watch_id) return a.watch_id < b.watch_id;
            return a.client_ip < b.client_ip;
        }
    }.lt);

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
            const wk: u64 = @bitCast(watch.values[i]);
            // Skip if already a dup WatchID (those rows were aggregated).
            var idx = blk: {
                var x = wk ^ (wk >> 30);
                x = x *% 0xbf58_476d_1ce4_e5b9;
                x ^= x >> 27;
                x = x *% 0x94d0_49bb_1331_11eb;
                x ^= x >> 31;
                break :blk x & dup_ids.mask;
            };
            var is_dup = false;
            while (true) {
                const slot = dup_ids.keys[idx];
                if (slot == wk) { is_dup = true; break; }
                if (slot == hashmap.empty_key) break;
                idx = (idx + 1) & dup_ids.mask;
            }
            if (is_dup) continue;
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

fn isWatchIdClientIpAggTopFiltered(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10");
}

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
        if (phrase.len == 2 and phrase[0] == '"' and phrase[1] == '"') {
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

fn isUrlCountTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10");
}

/// Append `s` to `out` with RFC4180 quoting matching DuckDB's CSV output:
/// the existing `writeCsvField` (above) handles ASCII control chars, comma,
/// quote, and any high-bit byte (which causes DuckDB to quote Cyrillic etc).
fn formatUrlCountTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const id_path = try std.fmt.allocPrint(allocator, "{s}/hot_URL.id", .{data_dir});
    defer allocator.free(id_path);
    const offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(offsets_path);
    const strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(strings_path);

    const ids = try io_map.mapColumn(u32, io, id_path);
    defer ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const strings = try io_map.mapFile(io, strings_path);
    defer strings.unmap();

    const n_dict = offsets.values.len - 1;

    // Dense per-id counter array.
    const counts = try allocator.alloc(u32, n_dict);
    defer allocator.free(counts);
    @memset(counts, 0);

    for (ids.values) |id| {
        counts[id] += 1;
    }

    // Top-10 by count desc; tiebreak by id ascending (matches no-tiebreak SQL).
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
    try out.appendSlice(allocator, "URL,c\n");
    for (top[0..top_len]) |r| {
        const start = offsets.values[r.id];
        const end = offsets.values[r.id + 1];
        try writeCsvField(allocator, &out, strings.raw[start..end]);
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

fn isOneUrlCountTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10");
}

fn formatOneUrlCountTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const id_path = try std.fmt.allocPrint(allocator, "{s}/hot_URL.id", .{data_dir});
    defer allocator.free(id_path);
    const offsets_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_offsets.bin", .{data_dir});
    defer allocator.free(offsets_path);
    const strings_path = try std.fmt.allocPrint(allocator, "{s}/URL.id_strings.bin", .{data_dir});
    defer allocator.free(strings_path);

    const ids = try io_map.mapColumn(u32, io, id_path);
    defer ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const strings = try io_map.mapFile(io, strings_path);
    defer strings.unmap();

    const n_dict = offsets.values.len - 1;

    const counts = try allocator.alloc(u32, n_dict);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (ids.values) |id| counts[id] += 1;

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
    try out.appendSlice(allocator, "1,URL,c\n");
    for (top[0..top_len]) |r| {
        const start = offsets.values[r.id];
        const end = offsets.values[r.id + 1];
        try out.appendSlice(allocator, "1,");
        try writeCsvField(allocator, &out, strings.raw[start..end]);
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

fn isUrlCountTopFilteredQ37(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10");
}

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
    const dch = try io_map.mapColumn(i16, io, dch_path);
    defer dch.mapping.unmap();
    const refresh = try io_map.mapColumn(i16, io, refresh_path);
    defer refresh.mapping.unmap();

    const n = ids.values.len;
    const n_dict = offsets.values.len - 1;

    const counts = try allocator.alloc(u32, n_dict);
    defer allocator.free(counts);
    @memset(counts, 0);

    const date_lo: i32 = 15887; // 2013-07-01
    const date_hi: i32 = 15917; // 2013-07-31

    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (counter.values[i] != 62) continue;
        const d = date.values[i];
        if (d < date_lo or d > date_hi) continue;
        if (dch.values[i] != 0) continue;
        if (refresh.values[i] != 0) continue;
        const id = ids.values[i];
        if (id == 0) continue; // empty URL
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
    try out.appendSlice(allocator, "URL,PageViews\n");
    for (top[0..top_len]) |r| {
        const start = offsets.values[r.id];
        const end = offsets.values[r.id + 1];
        try writeCsvField(allocator, &out, strings.raw[start..end]);
        try out.print(allocator, ",{d}\n", .{r.count});
    }
    return out.toOwnedSlice(allocator);
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

fn isUrlCountTopFilteredOffsetQ39(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000");
}

const Q39Row = struct { id: u32, count: u32 };

fn q39InsertTop(top: []Q39Row, top_len: *usize, row: Q39Row) void {
    var pos: usize = 0;
    while (pos < top_len.* and (top[pos].count > row.count or
        (top[pos].count == row.count and top[pos].id < row.id))) : (pos += 1) {}
    if (pos >= top.len) return;
    if (top_len.* < top.len) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
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

    const date_lo: i32 = 15887;
    const date_hi: i32 = 15917;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (counter.values[i] != 62) continue;
        const d = date.values[i];
        if (d < date_lo or d > date_hi) continue;
        if (refresh.values[i] != 0) continue;
        if (is_link.values[i] == 0) continue;
        if (is_download.values[i] != 0) continue;
        counts[ids.values[i]] += 1;
    }

    var top: [1010]Q39Row = undefined;
    var top_len: usize = 0;
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        q39InsertTop(&top, &top_len, .{ .id = @intCast(idx), .count = c });
    }

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

// ============================================================================
// Q40: dashboard group-by over traffic/source/destination with OFFSET 1000.
//
// Avoid full Referer materialization: group by RefererHash for the CASE Src key
// when SearchEngineID=0 and AdvEngineID=0, and resolve only output hashes from
// a compact q40_referer_hash_map.csv exported for the filtered subset.
// ============================================================================

fn isQ40(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000");
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
    url_id: u32,
};

const Q40TopRow = struct { key: Q40Key, count: u32 };

fn q40InsertTop(top: []Q40TopRow, top_len: *usize, row: Q40TopRow) void {
    var pos: usize = 0;
    while (pos < top_len.* and top[pos].count > row.count) : (pos += 1) {}
    if (pos >= top.len) return;
    if (top_len.* < top.len) top_len.* += 1;
    var j = top_len.* - 1;
    while (j > pos) : (j -= 1) top[j] = top[j - 1];
    top[pos] = row;
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
        const key: Q40Key = .{ .trafic = trafic.values[i], .search = se, .adv = ae, .src_hash = src_hash, .url_id = url_ids.values[i] };
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
        const us = url_offsets.values[r.key.url_id];
        const ue = url_offsets.values[r.key.url_id + 1];
        try writeCsvField(allocator, &out, url_strings.raw[us..ue]);
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

fn isQ23(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10");
}

const Q23Agg = struct {
    count: u32,
    min_url_id: u32,
    min_title_id: u32,
    user_set: std.AutoHashMap(u32, void),
};

const Q23OutRow = struct { phrase_id: u32, min_url_id: u32, min_title_id: u32, count: u32, distinct_users: u32 };

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

    const candidates = try io_map.mapColumn(Q23Candidate, io, candidates_path);
    defer candidates.mapping.unmap();
    const url_offsets = try io_map.mapColumn(u32, io, url_offsets_path);
    defer url_offsets.mapping.unmap();
    const url_strings = try io_map.mapFile(io, url_strings_path);
    defer url_strings.unmap();
    const title_offsets = try io_map.mapColumn(u32, io, title_offsets_path);
    defer title_offsets.mapping.unmap();
    const title_strings = try io_map.mapFile(io, title_strings_path);
    defer title_strings.unmap();
    const sp_offsets = try io_map.mapColumn(u32, io, sp_offsets_path);
    defer sp_offsets.mapping.unmap();
    const sp_phrases = try io_map.mapFile(io, sp_phrases_path);
    defer sp_phrases.unmap();

    var empty_phrase_id: u32 = std.math.maxInt(u32);
    for (0..sp_offsets.values.len - 1) |idx| {
        const start = sp_offsets.values[idx];
        const end = sp_offsets.values[idx + 1];
        const phrase = sp_phrases.raw[start..end];
        if (phrase.len == 2 and phrase[0] == '"' and phrase[1] == '"') {
            empty_phrase_id = @intCast(idx);
            break;
        }
    }

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

    for (candidates.values) |cand| {
        const pid = cand.phrase_id;
        if (pid == empty_phrase_id) continue;
        const uid = cand.url_id;
        const url_gop = try url_excludes_cache.getOrPut(uid);
        if (!url_gop.found_existing) {
            const us = url_offsets.values[uid];
            const ue = url_offsets.values[uid + 1];
            url_gop.value_ptr.* = std.mem.indexOf(u8, url_strings.raw[us..ue], ".google.") != null;
        }
        if (url_gop.value_ptr.*) continue;
        const tid = cand.title_id;
        const user_id = cand.user_id;
        const gop = try agg_map.getOrPut(pid);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .count = 1, .min_url_id = uid, .min_title_id = tid, .user_set = std.AutoHashMap(u32, void).init(allocator) };
            try gop.value_ptr.user_set.put(user_id, {});
        } else {
            gop.value_ptr.count += 1;
            if (uid < gop.value_ptr.min_url_id) gop.value_ptr.min_url_id = uid;
            if (tid < gop.value_ptr.min_title_id) gop.value_ptr.min_title_id = tid;
            try gop.value_ptr.user_set.put(user_id, {});
        }
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
        const ps = sp_offsets.values[r.phrase_id];
        const pe = sp_offsets.values[r.phrase_id + 1];
        try out.appendSlice(allocator, sp_phrases.raw[ps..pe]);
        try out.append(allocator, ',');
        const us = url_offsets.values[r.min_url_id];
        const ue = url_offsets.values[r.min_url_id + 1];
        try writeCsvField(allocator, &out, url_strings.raw[us..ue]);
        try out.append(allocator, ',');
        const ts = title_offsets.values[r.min_title_id];
        const te = title_offsets.values[r.min_title_id + 1];
        try writeCsvField(allocator, &out, title_strings.raw[ts..te]);
        try out.print(allocator, ",{d},{d}\n", .{ r.count, r.distinct_users });
    }
    return out.toOwnedSlice(allocator);
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

fn isSearchPhraseOrderByEventTimeTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10");
}

fn isSearchPhraseOrderByEventTimePhraseTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10");
}

const Q25Candidate = extern struct { event_time: i64, phrase_id: u32, row_index: u32 };

fn q25PhraseLess(offsets: []const u32, phrases: []const u8, a: u32, b: u32) bool {
    const as = offsets[a];
    const ae = offsets[a + 1];
    const bs = offsets[b];
    const be = offsets[b + 1];
    return std.mem.order(u8, phrases[as..ae], phrases[bs..be]) == .lt;
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

fn formatSearchPhraseEventTimeCandidates(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, secondary_phrase: bool) ![]u8 {
    const candidates_path = try std.fmt.allocPrint(allocator, "{s}/q25_eventtime_phrase_candidates.qii", .{data_dir});
    defer allocator.free(candidates_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const candidates = try io_map.mapColumn(Q25Candidate, io, candidates_path);
    defer candidates.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();

    var top: [10]Q25Candidate = undefined;
    var top_len: usize = 0;
    for (candidates.values) |c| q25InsertTop(&top, &top_len, offsets.values, phrases.raw, secondary_phrase, c);

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

// ============================================================================
// Q29: Referer domain aggregate.
//
// Full Referer materialization does not fit current disk. Instead a streaming
// build step produces q29_domain_stats.csv with one row per domain satisfying
// HAVING count(*) > 100000: {domain, sum_length_chars, count, min_referer}.
// Query time sorts those 77 rows by avg length descending and emits top 25.
// ============================================================================

fn isQ29(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25");
}

const Q29Row = struct { k: []const u8, sum_len: u64, count: u64, min_ref: []const u8 };

fn q29AvgGreater(a: Q29Row, b: Q29Row) bool {
    return @as(u128, a.sum_len) * @as(u128, b.count) > @as(u128, b.sum_len) * @as(u128, a.count);
}

fn q29AppendCsvFieldDecoded(allocator: std.mem.Allocator, out: *std.ArrayList(u8), src: []const u8, p: *usize) ![]const u8 {
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
    return out.items[start..out.items.len];
}

fn formatQ29DomainStats(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/q29_domain_stats.csv", .{data_dir});
    defer allocator.free(path);
    const mapped = try io_map.mapFile(io, path);
    defer mapped.unmap();

    var blob: std.ArrayList(u8) = .empty;
    defer blob.deinit(allocator);
    var rows: std.ArrayList(Q29Row) = .empty;
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
        try rows.append(allocator, .{ .k = k, .sum_len = sum_len, .count = count, .min_ref = min_ref });
    }

    const top_count = @min(rows.items.len, 25);
    var top: [25]Q29Row = undefined;
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
        try writeCsvField(allocator, &out, r.k);
        try out.append(allocator, ',');
        try writeFloatCsv(allocator, &out, @as(f64, @floatFromInt(r.sum_len)) / @as(f64, @floatFromInt(r.count)));
        try out.print(allocator, ",{d},", .{r.count});
        try writeCsvField(allocator, &out, r.min_ref);
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

fn isCountUrlLikeGoogle(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'");
}

const Q21Ctx = struct {
    matches: []u8,
    offsets: []const u32,
    blob: []const u8,
};

fn q21WorkerScanDict(ctx: *Q21Ctx, lo: usize, hi: usize) void {
    var i = lo;
    while (i < hi) : (i += 1) {
        const start = ctx.offsets[i];
        const end = ctx.offsets[i + 1];
        const s = ctx.blob[start..end];
        ctx.matches[i] = if (std.mem.indexOf(u8, s, "google") != null) 1 else 0;
    }
}

fn formatCountUrlLikeGoogle(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
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

fn isSearchPhraseMinUrlGoogle(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10");
}

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

    // Identify empty SearchPhrase id (encoded as `""` 2-char in dict).
    var empty_phrase_id: u32 = std.math.maxInt(u32);
    for (0..sp_dict_size) |idx| {
        const start = sp_offsets.values[idx];
        const end = sp_offsets.values[idx + 1];
        const phrase = sp_phrases.raw[start..end];
        if (phrase.len == 2 and phrase[0] == '"' and phrase[1] == '"') {
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
            if (uid < gop.value_ptr.min_url_id) gop.value_ptr.min_url_id = uid;
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

// ============================================================================
// Q38: SELECT Title, COUNT(*) AS PageViews FROM hits
//      WHERE CounterID = 62
//        AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31'
//        AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> ''
//      GROUP BY Title ORDER BY PageViews DESC LIMIT 10;
//
// Identical pattern to Q37 (s/URL/Title/). Title id 0 = empty Title (dict was
// built ORDER BY Title, empty string sorts first).
// ============================================================================

fn isTitleCountTopFilteredQ38(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10");
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

    const ids = try io_map.mapColumn(u32, io, id_path);
    defer ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const strings = try io_map.mapFile(io, strings_path);
    defer strings.unmap();
    const counter = try io_map.mapColumn(i32, io, counter_path);
    defer counter.mapping.unmap();
    const date = try io_map.mapColumn(i32, io, date_path);
    defer date.mapping.unmap();
    const dch = try io_map.mapColumn(i16, io, dch_path);
    defer dch.mapping.unmap();
    const refresh = try io_map.mapColumn(i16, io, refresh_path);
    defer refresh.mapping.unmap();

    const n = ids.values.len;
    const n_dict = offsets.values.len - 1;

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
        const id = ids.values[i];
        if (id == 0) continue;
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
        const start = offsets.values[r.id];
        const end = offsets.values[r.id + 1];
        try writeCsvField(allocator, &out, strings.raw[start..end]);
        try out.print(allocator, ",{d}\n", .{r.count});
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

fn isSearchPhraseOrderByPhraseTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10");
}

fn formatSearchPhraseOrderByPhraseTop(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) ![]u8 {
    const phrase_id_path = try storage.hotColumnPath(allocator, data_dir, storage.hot_search_phrase_id_name);
    defer allocator.free(phrase_id_path);
    const offsets_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_offsets_name);
    defer allocator.free(offsets_path);
    const phrases_path = try storage.hotColumnPath(allocator, data_dir, storage.search_phrase_id_phrases_name);
    defer allocator.free(phrases_path);

    const phrase_ids = try io_map.mapColumn(u32, io, phrase_id_path);
    defer phrase_ids.mapping.unmap();
    const offsets = try io_map.mapColumn(u32, io, offsets_path);
    defer offsets.mapping.unmap();
    const phrases = try io_map.mapFile(io, phrases_path);
    defer phrases.unmap();

    const n = phrase_ids.values.len;
    const dict_size = offsets.values.len - 1;

    // Identify empty phrase id: stored as the literal 2-byte sequence `""`.
    var empty_phrase_id: u32 = std.math.maxInt(u32);
    for (0..dict_size) |idx| {
        const start = offsets.values[idx];
        const end = offsets.values[idx + 1];
        const blob = phrases.raw[start..end];
        if (blob.len == 2 and blob[0] == '"' and blob[1] == '"') {
            empty_phrase_id = @intCast(idx);
            break;
        }
    }

    // Pass 1 parallel: per-thread local count array of dict_size u32, then
    // reduce. dict_size is ~6M -> 24 MB per thread; fits within budget.
    const n_threads = parallel.defaultThreads();
    const local_counts = try allocator.alloc(u32, n_threads * dict_size);
    defer allocator.free(local_counts);
    @memset(local_counts, 0);

    const Pass1Ctx = struct {
        phrase_ids: []const u32,
        local: []u32,
    };
    const pass1_workers = struct {
        fn fill(ctx: *Pass1Ctx, source: *parallel.MorselSource) void {
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) ctx.local[ctx.phrase_ids[r]] += 1;
            }
        }
    };
    const pass1_ctxs = try allocator.alloc(Pass1Ctx, n_threads);
    defer allocator.free(pass1_ctxs);
    for (pass1_ctxs, 0..) |*c, t| {
        c.* = .{
            .phrase_ids = phrase_ids.values,
            .local = local_counts[t * dict_size .. (t + 1) * dict_size],
        };
    }
    var pass1_src: parallel.MorselSource = .init(n, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, Pass1Ctx, pass1_workers.fill, pass1_ctxs, &pass1_src);

    const counts = try allocator.alloc(u32, dict_size);
    defer allocator.free(counts);
    // Reduce. Parallel across the dict_size dimension since it's ~6M entries
    // and each thread reads n_threads cache lines per output -> bandwidth
    // bound. Keep sequential — overhead would dominate for the inner work.
    for (0..dict_size) |idx| {
        var sum: u32 = 0;
        var t: usize = 0;
        while (t < n_threads) : (t += 1) sum += local_counts[t * dict_size + idx];
        counts[idx] = sum;
    }

    // Pass 2: lex-min top-10 over phrase_ids with count > 0 and not empty_id.
    const Top = struct { phrase_id: u32, count: u32 };
    var top: [10]Top = undefined;
    var top_len: usize = 0;

    var idx: u32 = 0;
    while (idx < dict_size) : (idx += 1) {
        if (counts[idx] == 0) continue;
        if (idx == empty_phrase_id) continue;
        const start = offsets.values[idx];
        const end = offsets.values[idx + 1];
        const phrase = phrases.raw[start..end];

        // Find insertion position via lex compare against current top.
        var pos: usize = 0;
        while (pos < top_len) : (pos += 1) {
            const tstart = offsets.values[top[pos].phrase_id];
            const tend = offsets.values[top[pos].phrase_id + 1];
            const tphrase = phrases.raw[tstart..tend];
            if (std.mem.lessThan(u8, tphrase, phrase)) continue;
            break;
        }
        if (pos >= 10) continue;
        if (top_len < 10) top_len += 1;
        var j = top_len - 1;
        while (j > pos) : (j -= 1) top[j] = top[j - 1];
        top[pos] = .{ .phrase_id = idx, .count = counts[idx] };
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
        const start = offsets.values[top[k].phrase_id];
        const end = offsets.values[top[k].phrase_id + 1];
        const phrase = phrases.raw[start..end];
        const cnt = top[k].count;
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

fn isUserIdMinuteSearchPhraseCountTop(sql: []const u8) bool {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    return asciiEqlIgnoreCaseCompact(trimmed, "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10");
}

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
