/// Owned chunk stream source. Used by the new Sink-based execution path so a
/// breaker can expose its output as chunks instead of a `row buffer`.
const std = @import("std");
const chunk = @import("../chunk.zig");
const result = @import("../result.zig");
const pipeline = @import("../exec/pipeline.zig");

pub const DataChunk = chunk.DataChunk;
pub const ColMeta = result.ColMeta;
pub const SourceIface = pipeline.SourceIface;
pub const QueryContext = pipeline.QueryContext;

pub const ChunkStream = struct {
    alloc: std.mem.Allocator,
    chunks: []DataChunk,
    metas: []const ColMeta,
    cursor: usize = 0,
    rows: u64,

    pub fn init(alloc: std.mem.Allocator, chunks: []DataChunk, metas: []const ColMeta) ChunkStream {
        var rows: u64 = 0;
        for (chunks) |c| rows += c.num_rows;
        return .{ .alloc = alloc, .chunks = chunks, .metas = metas, .rows = rows };
    }

    pub fn source(self: *ChunkStream) SourceIface {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn deinit(self: *ChunkStream) void {
        for (self.chunks) |*c| c.deinit();
        self.alloc.free(self.chunks);
        self.chunks = &.{};
    }

    fn nextChunkFn(ptr: *anyopaque, out: *DataChunk, _: *QueryContext) !bool {
        const self: *ChunkStream = @ptrCast(@alignCast(ptr));
        if (self.cursor >= self.chunks.len) return false;
        out.* = self.chunks[self.cursor];
        self.cursor += 1;
        return true;
    }

    fn resetFn(ptr: *anyopaque) void {
        const self: *ChunkStream = @ptrCast(@alignCast(ptr));
        self.cursor = 0;
    }

    fn schemaFn(ptr: *anyopaque) []const ColMeta {
        const self: *ChunkStream = @ptrCast(@alignCast(ptr));
        return self.metas;
    }

    fn rowCountFn(ptr: *anyopaque) u64 {
        const self: *ChunkStream = @ptrCast(@alignCast(ptr));
        return self.rows;
    }

    const vtable = SourceIface.VTable{
        .nextChunk = nextChunkFn,
        .reset = resetFn,
        .schema = schemaFn,
        .rowCount = rowCountFn,
    };
};

test "ChunkStream iterates owned chunks" {
    const alloc = std.testing.allocator;
    const chunk_sink = @import("../exec/chunk_sink.zig");
    const metas = [_]ColMeta{
        .{ .name = "id", .col_type = .int64, .ch_type = "Int64" },
    };
    var sink = try chunk_sink.ChunkSink.init(alloc, &metas);
    defer sink.deinit();
    var i: i64 = 0;
    while (i < 3000) : (i += 1) {
        try sink.append(&.{.{ .int64 = i }});
    }
    const chunks = try sink.finish();
    var stream = ChunkStream.init(alloc, chunks, &metas);
    defer stream.deinit();
    var ctx = QueryContext.init(alloc, stream.source());
    defer ctx.deinit();

    var out: DataChunk = undefined;
    try std.testing.expect(try stream.source().nextChunk(&out, &ctx));
    try std.testing.expectEqual(@as(usize, 2048), out.num_rows);
    try std.testing.expect(try stream.source().nextChunk(&out, &ctx));
    try std.testing.expectEqual(@as(usize, 952), out.num_rows);
    try std.testing.expect(!try stream.source().nextChunk(&out, &ctx));
}
