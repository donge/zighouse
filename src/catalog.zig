/// Catalog manifest helpers.
///
/// The store layout is selected from on-disk files by `store_query.zig`; the
/// only catalog metadata still written is the small `catalog.zig-house`
/// manifest inside a part directory.
const std = @import("std");

pub const catalog_manifest_name = "catalog.zig-house";

/// Part storage format tag.
pub const PartFormat = enum {
    /// ZigHouse generic store: Store V2 block files with LZ4/raw payloads.
    generic,
    /// ClickHouse MergeTree-compatible: LZ4 compressed .bin, .mrk2, checksums.txt.
    ch_mergetree,
};

/// Write the catalog manifest for a part into
///   <store_dir>/<table_name>/parts/all_1_1_0/catalog.zig-house
pub fn writeManifest(
    io: std.Io,
    allocator: std.mem.Allocator,
    store_dir: []const u8,
    table_name: []const u8,
    parquet_path: []const u8,
    part_format: PartFormat,
) !void {
    const manifest_path = try std.fmt.allocPrint(
        allocator,
        "{s}/{s}/parts/all_1_1_0/{s}",
        .{ store_dir, table_name, catalog_manifest_name },
    );
    defer allocator.free(manifest_path);

    const fmt_str: []const u8 = switch (part_format) {
        .generic => "generic",
        .ch_mergetree => "ch_mergetree",
    };
    const content = try std.fmt.allocPrint(
        allocator,
        "table={s}\nparquet={s}\npart_format={s}\n",
        .{ table_name, parquet_path, fmt_str },
    );
    defer allocator.free(content);

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = manifest_path, .data = content });
}
