/// Row -> chunk sink used while migrating breakers off `row buffer`.
///
/// Operators still produce logical rows internally for a while. The sink packs
/// them into 2048-row `DataChunk`s so downstream code can consume columnar
/// chunks without a `[]?Value` boundary.
const std = @import("std");
const chunk = @import("../chunk.zig");
const result = @import("../result.zig");

pub const DataChunk = chunk.DataChunk;
pub const ColMeta = result.ColMeta;
pub const Value = chunk.Value;

pub const ChunkSink = struct {
    alloc: std.mem.Allocator,
    metas: []ColMeta,
    chunks: std.ArrayListUnmanaged(DataChunk) = .empty,
    builder: chunk.ChunkBuilder,
    rows: usize = 0,

    pub fn init(alloc: std.mem.Allocator, metas: []const ColMeta) !ChunkSink {
        var self = ChunkSink{
            .alloc = alloc,
            .metas = try alloc.dupe(ColMeta, metas),
            .builder = chunk.ChunkBuilder.init(alloc, chunk.CHUNK_SIZE),
        };
        errdefer self.deinit();
        try self.addColumns();
        return self;
    }

    fn addColumns(self: *ChunkSink) !void {
        for (self.metas) |meta| {
            _ = try self.builder.addColumn(meta.name, meta.col_type);
        }
    }

    pub fn append(self: *ChunkSink, row: []const ?Value) !void {
        if (self.rows == chunk.CHUNK_SIZE) try self.flush();
        if (row.len != self.metas.len) return error.ColumnCountMismatch;
        for (self.metas, 0..) |_, ci| {
            const col = &self.builder.chunk.columns[ci];
            if (row[ci]) |v| {
                col.data.set(self.rows, v);
            } else {
                chunk.setNull(col.null_mask, self.rows);
            }
        }
        self.rows += 1;
    }

    /// Take ownership of an existing chunk (zero-copy scan/operator output).
    pub fn consumeChunk(self: *ChunkSink, c: DataChunk) !void {
        if (self.rows > 0) try self.flush();
        try self.chunks.append(self.alloc, c);
    }

    fn flush(self: *ChunkSink) !void {
        if (self.rows == 0) return;
        for (self.builder.chunk.columns) |*col| {
            col.data = col.data.truncate(self.rows);
            col.len = self.rows;
        }
        self.builder.chunk.num_rows = self.rows;
        try self.chunks.append(self.alloc, self.builder.finish());
        self.builder = chunk.ChunkBuilder.init(self.alloc, chunk.CHUNK_SIZE);
        try self.addColumns();
        self.rows = 0;
    }

    pub const Result = struct {
        chunks: []DataChunk,
        metas: []ColMeta,

        pub fn deinit(self: *Result, alloc: std.mem.Allocator) void {
            for (self.chunks) |*c| c.deinit();
            alloc.free(self.chunks);
            alloc.free(self.metas);
            self.* = undefined;
        }
    };

    /// Finish and transfer both chunks and metadata.
    pub fn finishResult(self: *ChunkSink) !Result {
        const chunks = try self.finish();
        const metas = self.metas;
        self.metas = &.{};
        return .{ .chunks = chunks, .metas = metas };
    }

    /// Flush and transfer the chunk list to the caller. The sink is empty
    /// afterwards; call `deinit` to release its metadata.
    pub fn finish(self: *ChunkSink) ![]DataChunk {
        try self.flush();
        const out = try self.chunks.toOwnedSlice(self.alloc);
        self.chunks = .empty;
        return out;
    }

    pub fn deinit(self: *ChunkSink) void {
        for (self.chunks.items) |*c| c.deinit();
        self.chunks.deinit(self.alloc);
        self.builder.chunk.deinit();
        self.alloc.free(self.metas);
    }
};

test "ChunkSink packs rows into 2048-row chunks" {
    const alloc = std.testing.allocator;
    const metas = [_]ColMeta{
        .{ .name = "id", .col_type = .int64, .ch_type = "Int64" },
        .{ .name = "s", .col_type = .string, .ch_type = "String" },
    };
    var sink = try ChunkSink.init(alloc, &metas);
    defer sink.deinit();
    var i: i64 = 0;
    while (i < 3000) : (i += 1) {
        const s: []const u8 = if (i % 2 == 0) "even" else "odd";
        try sink.append(&.{ .{ .int64 = i }, .{ .string = s } });
    }
    const chunks = try sink.finish();
    defer {
        for (chunks) |*c| c.deinit();
        alloc.free(chunks);
    }
    try std.testing.expectEqual(@as(usize, 2), chunks.len);
    try std.testing.expectEqual(@as(usize, 2048), chunks[0].num_rows);
    try std.testing.expectEqual(@as(usize, 952), chunks[1].num_rows);
    try std.testing.expectEqual(@as(i64, 2048), chunks[1].columns[0].data.int64[0]);
}
