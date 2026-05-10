const std = @import("std");
const schema = @import("schema.zig");

pub const segment_rows = 64 * 1024;
pub const manifest_name = "manifest.zig-house";
pub const import_name = "import.zig-house";
pub const columns_dir_name = "columns";
pub const duckdb_name = "hits.duckdb";
pub const hot_csv_name = "hot.csv";
pub const hot_adv_engine_id_name = "hot_AdvEngineID.i16";
pub const hot_resolution_width_name = "hot_ResolutionWidth.i16";
pub const hot_event_date_name = "hot_EventDate.i32";
pub const hot_user_id_name = "hot_UserID.i64";
pub const hot_counter_id_name = "hot_CounterID.i32";
pub const hot_is_refresh_name = "hot_IsRefresh.i16";
pub const hot_dont_count_hits_name = "hot_DontCountHits.i16";
pub const hot_url_hash_name = "hot_URLHash.i64";
pub const hot_window_client_width_name = "hot_WindowClientWidth.i16";
pub const hot_window_client_height_name = "hot_WindowClientHeight.i16";
pub const hot_client_ip_name = "hot_ClientIP.i32";
pub const hot_url_length_name = "hot_URLLength.i32";
pub const hot_event_minute_name = "hot_EventMinute.i32";
pub const hot_trafic_source_id_name = "hot_TraficSourceID.i16";
pub const hot_referer_hash_name = "hot_RefererHash.i64";
pub const hot_event_time_name = "hot_EventTime.i64";
pub const hot_watch_id_name = "hot_WatchID.i64";
pub const hot_is_link_name = "hot_IsLink.i16";
pub const hot_is_download_name = "hot_IsDownload.i16";
pub const hot_search_phrase_hash_name = "hot_SearchPhrase.hash64";
pub const hot_search_phrase_hash_csv_name = "hot_search_phrase_hash.csv";
pub const search_phrase_dict_name = "SearchPhrase.dict.tsv";
pub const hot_search_phrase_id_name = "hot_SearchPhrase.id";
pub const search_phrase_id_offsets_name = "SearchPhrase.id_offsets.bin";
pub const search_phrase_id_phrases_name = "SearchPhrase.id_phrases.bin";
pub const url_dict_name = "URL.dict.tsv";
pub const hot_url_id_name = "hot_URL.id";
pub const url_id_offsets_name = "URL.id_offsets.bin";
pub const url_id_strings_name = "URL.id_strings.bin";
pub const hot_url_contains_google_name = "hot_URL.contains_google.u8";
pub const hot_url_contains_dot_google_name = "hot_URL.contains_dot_google.u8";
pub const hot_title_hash_name = "hot_TitleHash.i64";
pub const hot_title_contains_google_name = "hot_Title.contains_google.u8";
pub const hot_title_non_empty_name = "hot_Title.non_empty.u8";
pub const hot_referer_id_name = "hot_Referer.id";
pub const referer_id_offsets_name = "Referer.id_offsets.bin";
pub const referer_id_strings_name = "Referer.id_strings.bin";
pub const referer_dict_name = "Referer.dict.tsv";
pub const referer_domain_id_name = "Referer.domain_id.u32";
pub const referer_utf8_len_name = "Referer.utf8_len.u32";
pub const referer_domain_offsets_name = "RefererDomain.offsets.bin";
pub const referer_domain_strings_name = "RefererDomain.strings.bin";
pub const hot_user_id_id_name = "hot_UserID.id";
pub const user_id_dict_name = "UserID.dict.i64";
pub const hot_region_id_name = "hot_RegionID.i32";
pub const hot_search_engine_id_name = "hot_SearchEngineID.i16";
pub const hot_mobile_phone_name = "hot_MobilePhone.i16";
pub const hot_mobile_phone_model_id_name = "hot_MobilePhoneModel.id";
pub const mobile_phone_model_dict_name = "MobilePhoneModel.dict.tsv";
pub const mobile_phone_model_dict_offsets_name = "MobilePhoneModel.dict.offsets";
pub const mobile_phone_model_dict_bytes_name = "MobilePhoneModel.dict.bytes";
pub const hot_extra_csv_name = "hot_extra.csv";
pub const hot_segment_stats_name = "hot_segments.stats";
pub const q37_url_dict_name = "q37_url_dict.tsv";

pub fn initStore(io: std.Io, data_dir: []const u8) !void {
    const cwd = std.Io.Dir.cwd();
    try cwd.createDirPath(io, data_dir);

    var dir = try cwd.openDir(io, data_dir, .{});
    defer dir.close(io);

    try dir.createDirPath(io, columns_dir_name);

    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(std.heap.smp_allocator);
    try text.print(std.heap.smp_allocator, "format=zighouse-native-v0\nsegment_rows={d}\ncolumns={d}\n", .{ segment_rows, schema.hits_columns.len });
    for (schema.hits_columns, 0..) |column, i| {
        try text.print(std.heap.smp_allocator, "column={d}:{s}:{s}:cardinality={s}:storage={s}\n", .{ i, column.name, @tagName(column.ty), @tagName(column.cardinality), @tagName(column.storage) });
        try createColumnPlaceholders(io, dir, i, column);
    }
    try dir.writeFile(io, .{ .sub_path = manifest_name, .data = text.items });
}

pub fn ensureStore(io: std.Io, data_dir: []const u8) !void {
    var dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{});
    defer dir.close(io);
    var manifest = try dir.openFile(io, manifest_name, .{});
    manifest.close(io);
}

pub fn writeImportManifest(io: std.Io, allocator: std.mem.Allocator, data_dir: []const u8, parquet_path: []const u8) !void {
    var dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{});
    defer dir.close(io);

    const text = try std.fmt.allocPrint(allocator, "source={s}\nstatus=duckdb-parquet-view\n", .{parquet_path});
    defer allocator.free(text);
    try dir.writeFile(io, .{ .sub_path = import_name, .data = text });
}

pub const ImportManifest = struct {
    source: []const u8,
    status: []const u8 = "imported",
    profile: []const u8 = "unknown",
    decoder: []const u8 = "unknown",
    row_count: u64 = 0,
    user_id_dict_size: usize = 0,
    mobile_model_dict_size: usize = 0,
    search_phrase_dict_size: usize = 0,
    url_dict_size: usize = 0,
    title_dict_size: usize = 0,
    main_store_seconds: ?f64 = null,
    tiny_caches_seconds: ?f64 = null,
    total_seconds: ?f64 = null,
    has_q24_result: bool = false,
    has_q29_result: bool = false,
    has_q40_result: bool = false,
};

pub fn writeDetailedImportManifest(io: std.Io, allocator: std.mem.Allocator, data_dir: []const u8, manifest: ImportManifest) !void {
    var dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{});
    defer dir.close(io);

    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(allocator);
    try text.print(allocator, "source={s}\n", .{manifest.source});
    try text.print(allocator, "status={s}\n", .{manifest.status});
    try text.print(allocator, "profile={s}\n", .{manifest.profile});
    try text.print(allocator, "decoder={s}\n", .{manifest.decoder});
    try text.print(allocator, "row_count={d}\n", .{manifest.row_count});
    try text.print(allocator, "dict.UserID={d}\n", .{manifest.user_id_dict_size});
    try text.print(allocator, "dict.MobilePhoneModel={d}\n", .{manifest.mobile_model_dict_size});
    try text.print(allocator, "dict.SearchPhrase={d}\n", .{manifest.search_phrase_dict_size});
    try text.print(allocator, "dict.URL={d}\n", .{manifest.url_dict_size});
    try text.print(allocator, "dict.Title={d}\n", .{manifest.title_dict_size});
    if (manifest.main_store_seconds) |seconds| try text.print(allocator, "phase.main_store.seconds={d:.6}\n", .{seconds});
    if (manifest.tiny_caches_seconds) |seconds| try text.print(allocator, "phase.tiny_caches.seconds={d:.6}\n", .{seconds});
    if (manifest.total_seconds) |seconds| try text.print(allocator, "phase.total.seconds={d:.6}\n", .{seconds});
    if (manifest.has_q24_result) try text.appendSlice(allocator, "artifact.q24_result=result_cache\n");
    if (manifest.has_q29_result) try text.appendSlice(allocator, "artifact.q29_result=result_cache\n");
    if (manifest.has_q40_result) try text.appendSlice(allocator, "artifact.q40_result=result_cache\n");
    try dir.writeFile(io, .{ .sub_path = import_name, .data = text.items });
}

pub fn readImportManifest(io: std.Io, allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    var dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{});
    defer dir.close(io);
    return try dir.readFileAlloc(io, import_name, allocator, .limited(64 * 1024));
}

pub fn readStoreManifest(io: std.Io, allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    var dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{});
    defer dir.close(io);
    return try dir.readFileAlloc(io, manifest_name, allocator, .limited(256 * 1024));
}

pub fn readImportSource(io: std.Io, allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    var dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{});
    defer dir.close(io);

    const text = try dir.readFileAlloc(io, import_name, allocator, .limited(16 * 1024));
    errdefer allocator.free(text);

    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, "source=")) {
            const source = line["source=".len..];
            const copy = try allocator.dupe(u8, source);
            allocator.free(text);
            return copy;
        }
    }

    allocator.free(text);
    return error.MissingImportSource;
}

pub fn duckDbPath(allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ data_dir, duckdb_name });
}

pub fn hotCsvPath(allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ data_dir, hot_csv_name });
}

pub fn hotExtraCsvPath(allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ data_dir, hot_extra_csv_name });
}

pub fn hotColumnPath(allocator: std.mem.Allocator, data_dir: []const u8, file_name: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ data_dir, file_name });
}

pub fn hotSegmentStatsPath(allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ data_dir, hot_segment_stats_name });
}

pub fn searchPhraseDictPath(allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ data_dir, search_phrase_dict_name });
}

pub fn urlDictPath(allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ data_dir, url_dict_name });
}

pub fn q37UrlDictPath(allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ data_dir, q37_url_dict_name });
}

pub fn hasHotCsv(io: std.Io, data_dir: []const u8) bool {
    var dir = std.Io.Dir.cwd().openDir(io, data_dir, .{}) catch return false;
    defer dir.close(io);
    var file = dir.openFile(io, hot_csv_name, .{}) catch return false;
    file.close(io);
    return true;
}

pub fn hasHotBinary(io: std.Io, data_dir: []const u8) bool {
    return hasFile(io, data_dir, hot_adv_engine_id_name) and
        hasFile(io, data_dir, hot_resolution_width_name) and
        hasFile(io, data_dir, hot_event_date_name) and
        hasFile(io, data_dir, hot_user_id_name) and
        hasFile(io, data_dir, hot_counter_id_name) and
        hasFile(io, data_dir, hot_is_refresh_name) and
        hasFile(io, data_dir, hot_dont_count_hits_name) and
        hasFile(io, data_dir, hot_url_hash_name) and
        hasFile(io, data_dir, hot_window_client_width_name) and
        hasFile(io, data_dir, hot_window_client_height_name);
}

pub fn hasExtraHotBinary(io: std.Io, data_dir: []const u8) bool {
    return hasFile(io, data_dir, hot_client_ip_name) and
        hasFile(io, data_dir, hot_url_length_name) and
        hasFile(io, data_dir, hot_event_minute_name) and
        hasFile(io, data_dir, hot_trafic_source_id_name) and
        hasFile(io, data_dir, hot_referer_hash_name);
}

pub fn hasSearchPhraseHot(io: std.Io, data_dir: []const u8) bool {
    return hasFile(io, data_dir, hot_search_phrase_hash_name) and hasFile(io, data_dir, search_phrase_dict_name);
}

fn hasFile(io: std.Io, data_dir: []const u8, file_name: []const u8) bool {
    var dir = std.Io.Dir.cwd().openDir(io, data_dir, .{}) catch return false;
    defer dir.close(io);
    var file = dir.openFile(io, file_name, .{}) catch return false;
    file.close(io);
    return true;
}

fn createColumnPlaceholders(io: std.Io, dir: std.Io.Dir, index: usize, column: schema.Column) !void {
    var columns_dir = try dir.openDir(io, columns_dir_name, .{});
    defer columns_dir.close(io);

    var name_buf: [256]u8 = undefined;
    if (column.ty.isString()) {
        const offsets = try std.fmt.bufPrint(&name_buf, "{d:0>3}_{s}.offsets", .{ index, column.name });
        try touch(io, columns_dir, offsets);
        const bytes = try std.fmt.bufPrint(&name_buf, "{d:0>3}_{s}.bytes", .{ index, column.name });
        try touch(io, columns_dir, bytes);
        const hashes = try std.fmt.bufPrint(&name_buf, "{d:0>3}_{s}.hash64", .{ index, column.name });
        try touch(io, columns_dir, hashes);
    } else {
        const values = try std.fmt.bufPrint(&name_buf, "{d:0>3}_{s}.values", .{ index, column.name });
        try touch(io, columns_dir, values);
    }
}

fn touch(io: std.Io, dir: std.Io.Dir, name: []const u8) !void {
    var file = try dir.createFile(io, name, .{ .truncate = false });
    file.close(io);
}

test "segment rows are power of two" {
    try std.testing.expect((segment_rows & (segment_rows - 1)) == 0);
}

test "schema exposes fixed widths" {
    try std.testing.expectEqual(@as(?usize, 2), schema.ColumnType.int16.fixedWidth());
    try std.testing.expectEqual(@as(?usize, null), schema.ColumnType.text.fixedWidth());
}
