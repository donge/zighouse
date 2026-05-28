/// Materialized view metadata persistence.
///
/// Stores materialized view definitions at:
///   <data_dir>/metadata/<db>/<mv_name>.sql
///
/// The file content is the full original CREATE MATERIALIZED VIEW SQL.
/// This follows ClickHouse's convention of storing DDL SQL in metadata/.
///
/// Usage:
///   try mv_persist.save(io, allocator, data_dir, &entry);
///   const mvs = try mv_persist.loadAll(allocator, io, data_dir);
///   defer { for (mvs) |*mv| mv.deinit(); allocator.free(mvs); }

const std = @import("std");
const mv_parse = @import("mv_parse");

/// Write a materialized view definition to disk.
/// Creates <data_dir>/metadata/<db>/<mv_name>.sql
pub fn save(
    io: std.Io,
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    entry: *const mv_parse.MatViewEntry,
) !void {
    const dir_path = try std.fmt.allocPrint(allocator, "{s}/metadata/{s}", .{ data_dir, entry.db });
    defer allocator.free(dir_path);

    try std.Io.Dir.cwd().createDirPath(io, dir_path);

    const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}.sql", .{ dir_path, entry.mv_name });
    defer allocator.free(file_path);

    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = file_path,
        .data     = entry.raw_sql,
    });
}

/// Delete a materialized view's metadata file.
pub fn delete(
    io: std.Io,
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    db: []const u8,
    mv_name: []const u8,
) !void {
    const file_path = try std.fmt.allocPrint(allocator, "{s}/metadata/{s}/{s}.sql", .{ data_dir, db, mv_name });
    defer allocator.free(file_path);
    std.Io.Dir.cwd().deleteFile(io, file_path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}

/// Scan <data_dir>/metadata/<db>/*.sql and parse each as a MatViewEntry.
/// Returns a heap-allocated slice; caller must:
///   defer { for (result) |*mv| mv.deinit(); allocator.free(result); }
pub fn loadAll(
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,
) ![]mv_parse.MatViewEntry {
    var results: std.ArrayListUnmanaged(mv_parse.MatViewEntry) = .empty;
    errdefer {
        for (results.items) |*e| e.deinit();
        results.deinit(allocator);
    }

    const meta_dir_path = try std.fmt.allocPrint(allocator, "{s}/metadata", .{data_dir});
    defer allocator.free(meta_dir_path);

    var meta_dir = std.Io.Dir.cwd().openDir(io, meta_dir_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return results.toOwnedSlice(allocator),
        else => return err,
    };
    defer meta_dir.close(io);

    var db_iter = meta_dir.iterate();
    while (try db_iter.next(io)) |db_entry| {
        if (db_entry.kind != .directory) continue;
        const db = db_entry.name;

        const db_path = try std.fmt.allocPrint(allocator, "{s}/metadata/{s}", .{ data_dir, db });
        defer allocator.free(db_path);

        var db_dir = std.Io.Dir.cwd().openDir(io, db_path, .{ .iterate = true }) catch continue;
        defer db_dir.close(io);

        var file_iter = db_dir.iterate();
        while (try file_iter.next(io)) |file_entry| {
            if (file_entry.kind != .file) continue;
            const name = file_entry.name;
            if (!std.mem.endsWith(u8, name, ".sql")) continue;

            const file_path = try std.fmt.allocPrint(allocator, "{s}/metadata/{s}/{s}", .{ data_dir, db, name });
            defer allocator.free(file_path);

            const sql = std.Io.Dir.cwd().readFileAlloc(io, file_path, allocator, .limited(1 * 1024 * 1024)) catch continue;
            defer allocator.free(sql);

            const trimmed = std.mem.trim(u8, sql, " \t\r\n");
            const entry = mv_parse.parse(allocator, trimmed) catch continue; // skip malformed files
            try results.append(allocator, entry);
        }
    }

    return results.toOwnedSlice(allocator);
}
