/// MemTable source — an in-memory table for ZigDB.
///
/// Wraps a slice of DataChunks (or a single pre-built chunk) and exposes
/// it as a SourceIface for the pipeline engine.
const std    = @import("std");
const chunk  = @import("../chunk.zig");
const result = @import("../result.zig");
const pipeline = @import("../exec/pipeline.zig");

pub const DataChunk  = chunk.DataChunk;
pub const SourceIface = pipeline.SourceIface;
pub const QueryContext = pipeline.QueryContext;

/// An in-memory table consisting of a list of DataChunks.
/// Ownership: MemTable does NOT own the chunks; caller must keep them alive.
pub const MemTable = struct {
    chunks:  []DataChunk,
    schema:  []result.ColMeta,
    cursor:  usize = 0,

    pub fn init(chunks: []DataChunk, schema: []result.ColMeta) MemTable {
        return .{ .chunks = chunks, .schema = schema };
    }

    /// Return a SourceIface that wraps this MemTable.
    pub fn source(self: *MemTable) SourceIface {
        return .{
            .ptr    = self,
            .vtable = &vtable,
        };
    }

    fn nextChunkFn(ptr: *anyopaque, out: *DataChunk, _: *QueryContext) !bool {
        const self: *MemTable = @ptrCast(@alignCast(ptr));
        if (self.cursor >= self.chunks.len) return false;
        out.* = self.chunks[self.cursor];
        self.cursor += 1;
        return true;
    }

    fn resetFn(ptr: *anyopaque) void {
        const self: *MemTable = @ptrCast(@alignCast(ptr));
        self.cursor = 0;
    }

    fn schemaFn(ptr: *anyopaque) []const result.ColMeta {
        const self: *MemTable = @ptrCast(@alignCast(ptr));
        return self.schema;
    }

    const vtable = SourceIface.VTable{
        .nextChunk = nextChunkFn,
        .reset     = resetFn,
        .schema    = schemaFn,
    };
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "MemTable iterates chunks" {
    const alloc = std.testing.allocator;

    // Build two chunks.
    var b1 = chunk.ChunkBuilder.init(alloc, 2);
    defer b1.chunk.deinit();
    const c1 = try b1.addColumn("x", .int64);
    c1.data.int64[0] = 1; c1.data.int64[1] = 2;

    var b2 = chunk.ChunkBuilder.init(alloc, 1);
    defer b2.chunk.deinit();
    const c2 = try b2.addColumn("x", .int64);
    c2.data.int64[0] = 3;

    var chunks = [_]DataChunk{ b1.finish(), b2.finish() };
    var schema = [_]result.ColMeta{.{ .name = "x", .col_type = .int64 }};
    var mt = MemTable.init(&chunks, &schema);
    var src = mt.source();

    var ctx = QueryContext.init(alloc, src);
    defer ctx.deinit();

    var out: DataChunk = undefined;
    try std.testing.expect(try src.nextChunk(&out, &ctx));
    try std.testing.expectEqual(@as(usize, 2), out.num_rows);
    try std.testing.expect(try src.nextChunk(&out, &ctx));
    try std.testing.expectEqual(@as(usize, 1), out.num_rows);
    try std.testing.expect(!try src.nextChunk(&out, &ctx));
}
