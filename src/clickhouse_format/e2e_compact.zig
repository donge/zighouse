//! E2E test: write a CompactPart from zighouse, ATTACH it to CH server, query it.
//!
//! Usage:  zig run src/clickhouse_format/e2e_compact.zig \
//!           --dep schema --dep types \
//!           -Mschema=src/schema.zig \
//!           -Mtypes=src/clickhouse_format/types.zig \
//!           -Mroot=src/clickhouse_format/e2e_compact.zig \
//!           -I/opt/homebrew/opt/lz4/include -L/opt/homebrew/opt/lz4/lib -llz4 \
//!           -I/opt/homebrew/opt/zstd/include -L/opt/homebrew/opt/zstd/lib -lzstd -lc
const std = @import("std");
const part_mod = @import("part.zig");
const schema = @import("schema");
const types = @import("types");

_ = types;

const CH_STORE = "/tmp/ch-srv/data/store/4d0/4d02ac62-2539-4cf7-97dc-28415c3acc30";
const DETACHED  = CH_STORE ++ "/detached";
const PART_NAME = "all_2_2_0";

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = std.io;

    const cols = [_]schema.Column{
        .{ .name = "event_date",  .ty = .date },
        .{ .name = "event_time",  .ty = .int32 },
        .{ .name = "user_id",     .ty = .int32 },
        .{ .name = "page_id",     .ty = .int32 },
        .{ .name = "duration",    .ty = .int64 },
        .{ .name = "url",         .ty = .text },
    };
    const table = schema.Table{ .name = "events", .columns = &cols };

    const part_dir = DETACHED ++ "/" ++ PART_NAME;

    // Remove existing part if present
    std.fs.deleteTreeAbsolute(part_dir) catch {};

    std.debug.print("Writing compact part to {s}\n", .{part_dir});

    var cp = try part_mod.CompactPart.open(io, allocator, part_dir, table, 0x82);
    defer cp.deinit();

    // Same 5 rows as in CH (ordered by event_date, user_id)
    // event_date: days since epoch; 2024-01-01 = 19723, 2024-01-02 = 19724
    const dates   = [_]i64{ 19723, 19723, 19723, 19724, 19724 };
    const times   = [_]i64{ 1704099600, 1704099720, 1704099660, 1704186000, 1704186300 };
    const users   = [_]i64{ 1, 1, 2, 3, 4 };
    const pages   = [_]i64{ 100, 102, 101, 100, 103 };
    const durs    = [_]i64{ 3500, 8900, 1200, 4200, 600 };
    const urls    = [_][]const u8{
        "https://example.com/home",
        "https://example.com/products",
        "https://example.com/about",
        "https://example.com/home",
        "https://example.com/contact",
    };

    try cp.appendFixedBatch(0, &dates);
    try cp.appendFixedBatch(1, &times);
    try cp.appendFixedBatch(2, &users);
    try cp.appendFixedBatch(3, &pages);
    try cp.appendFixedBatch(4, &durs);
    for (urls) |u| try cp.appendString(5, u);
    cp.row_count = 5; // appendString doesn't increment row_count; fix manually

    try cp.finish();

    std.debug.print("Part written. Files:\n", .{});
    var dir = try std.fs.openDirAbsolute(part_dir, .{ .iterate = true });
    defer dir.close();
    var it = dir.iterate();
    while (try it.next()) |entry| {
        std.debug.print("  {s}\n", .{entry.name});
    }

    std.debug.print("\nTo attach, run:\n", .{});
    std.debug.print("  clickhouse client --port 19000 --query \"ALTER TABLE default.events ATTACH PART '{s}'\"\n", .{PART_NAME});
}
