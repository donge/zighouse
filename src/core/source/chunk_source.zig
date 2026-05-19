/// ChunkSource — wraps an already-computed ResultSet as a pipeline source.
///
/// Used for subqueries: the inner query produces a ResultSet, and the outer
/// query uses ChunkSource to feed that ResultSet back into the pipeline
/// as a SourceIface. This replaces the old CSV-roundtrip approach.
const std    = @import("std");
const chunk  = @import("../chunk.zig");
const result = @import("../result.zig");
const pipeline = @import("../exec/pipeline.zig");

pub const DataChunk   = chunk.DataChunk;
pub const ResultSet   = result.ResultSet;
pub const SourceIface = pipeline.SourceIface;
pub const QueryContext = pipeline.QueryContext;

/// A source that yields rows from a ResultSet in DataChunk-sized batches.
/// Does NOT own the ResultSet — caller must keep rs alive for the scan duration.
pub const ChunkSource = struct {
    rs:        *const ResultSet,
    row_cursor: usize = 0,
    alloc:     std.mem.Allocator,

    pub fn init(rs: *const ResultSet, alloc: std.mem.Allocator) ChunkSource {
        return .{ .rs = rs, .row_cursor = 0, .alloc = alloc };
    }

    pub fn source(self: *ChunkSource) SourceIface {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn nextChunkFn(ptr: *anyopaque, out: *DataChunk, ctx: *QueryContext) !bool {
        const self: *ChunkSource = @ptrCast(@alignCast(ptr));
        if (self.row_cursor >= self.rs.num_rows) return false;

        const take = @min(chunk.CHUNK_SIZE, self.rs.num_rows - self.row_cursor);
        const arena_alloc = ctx.allocator();

        // Build a new DataChunk backed by ctx's arena.
        var b = chunk.ChunkBuilder.init(arena_alloc, take);
        for (self.rs.columns, 0..) |src_col, ci| {
            _ = ci;
            const out_col = try b.addColumn(src_col.name, @as(chunk.ColumnType, src_col.data));
            for (0..take) |r| {
                const src_r = self.row_cursor + r;
                if (chunk.isNull(src_col.null_mask, src_r)) {
                    chunk.setNull(out_col.null_mask, r);
                    chunk.ColumnData.set(&out_col.data, r, zeroVal(src_col.data));
                } else {
                    chunk.ColumnData.set(&out_col.data, r, src_col.data.get(src_r));
                }
            }
        }

        self.row_cursor += take;
        out.* = b.finish();
        return true;
    }

    fn resetFn(ptr: *anyopaque) void {
        const self: *ChunkSource = @ptrCast(@alignCast(ptr));
        self.row_cursor = 0;
    }

    fn schemaFn(ptr: *anyopaque) []const result.ColMeta {
        const self: *ChunkSource = @ptrCast(@alignCast(ptr));
        return self.rs.metas;
    }

    const vtable = SourceIface.VTable{
        .nextChunk = nextChunkFn,
        .reset     = resetFn,
        .schema    = schemaFn,
    };
};

fn zeroVal(data: chunk.ColumnData) chunk.Value {
    return switch (data) {
        .bool_u8       => .{ .bool_u8       = 0 },
        .int64         => .{ .int64         = 0 },
        .uint64        => .{ .uint64        = 0 },
        .float64       => .{ .float64       = 0.0 },
        .date_u16      => .{ .date_u16      = 0 },
        .datetime64_ms => .{ .datetime64_ms = 0 },
        .string        => .{ .string        = "" },
        .array_string  => .{ .array_string  = &.{} },
    };
}
