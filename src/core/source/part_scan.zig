/// PartScanSource — scans persisted ClickHouse-format part files (ZigHouse).
///
/// This is a stub/interface definition. The actual reading logic is in
/// src/ingest/part_scanner.zig and src/clickhouse_format/part.zig.
/// PartScanSource bridges those into the SourceIface expected by the pipeline.
const std    = @import("std");
const chunk  = @import("../chunk.zig");
const result = @import("../result.zig");
const pipeline = @import("../exec/pipeline.zig");

pub const DataChunk   = chunk.DataChunk;
pub const SourceIface = pipeline.SourceIface;
pub const QueryContext = pipeline.QueryContext;

/// Minimal interface that the storage layer must satisfy.
/// Concrete implementation injected at startup by ZigHouse's main.zig.
pub const PartReaderVTable = struct {
    /// Open a scan over the given part directories for the named columns.
    open: *const fn (
        self: *anyopaque,
        db: []const u8,
        table: []const u8,
        columns: []const []const u8,
        alloc: std.mem.Allocator,
    ) anyerror!*PartReaderState,

    /// Read the next chunk. Returns false when all parts are exhausted.
    nextChunk: *const fn (state: *PartReaderState, out: *DataChunk) anyerror!bool,

    /// Close and free the scan state.
    close: *const fn (state: *PartReaderState) void,
};

/// Opaque scan state returned by PartReaderVTable.open.
pub const PartReaderState = opaque {};

/// SourceIface wrapper for a part scan.
pub const PartScanSource = struct {
    reader:  *const PartReaderVTable,
    reader_self: *anyopaque,
    state:   ?*PartReaderState = null,
    db:      []const u8,
    table:   []const u8,
    columns: []const []const u8,
    schema_metas: []result.ColMeta,
    alloc:   std.mem.Allocator,

    pub fn source(self: *PartScanSource) SourceIface {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn nextChunkFn(ptr: *anyopaque, out: *DataChunk, ctx: *QueryContext) !bool {
        const self: *PartScanSource = @ptrCast(@alignCast(ptr));
        if (self.state == null) {
            // Lazy open on first call.
            self.state = try self.reader.open(
                self.reader_self,
                self.db,
                self.table,
                self.columns,
                ctx.allocator(),
            );
        }
        return self.reader.nextChunk(self.state.?, out);
    }

    fn resetFn(ptr: *anyopaque) void {
        const self: *PartScanSource = @ptrCast(@alignCast(ptr));
        if (self.state) |s| {
            self.reader.close(s);
            self.state = null;
        }
    }

    fn schemaFn(ptr: *anyopaque) []const result.ColMeta {
        const self: *PartScanSource = @ptrCast(@alignCast(ptr));
        return self.schema_metas;
    }

    fn rowCountFn(_: *anyopaque) u64 { return 0; }

    const vtable = SourceIface.VTable{
        .nextChunk = nextChunkFn,
        .reset     = resetFn,
        .schema    = schemaFn,
        .rowCount  = rowCountFn,
    };
};
