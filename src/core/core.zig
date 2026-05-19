/// ZigHouse/ZigDB core execution engine.
///
/// Public re-exports for consumers:
///
///   const core = @import("core");
///   core.types.Value
///   core.chunk.DataChunk
///   core.result.ResultSet
///   core.exec.pipeline.runLinearPipeline
///   core.source.mem_table.MemTable
///   core.source.chunk_source.ChunkSource
///   core.source.part_scan.PartScanSource

pub const types   = @import("types.zig");
pub const chunk   = @import("chunk.zig");
pub const result  = @import("result.zig");

pub const exec = struct {
    pub const plan      = @import("exec/plan.zig");
    pub const kernels   = @import("exec/kernels.zig");
    pub const hash_table = @import("exec/hash_table.zig");
    pub const pipeline  = @import("exec/pipeline.zig");
};

pub const source = struct {
    pub const mem_table   = @import("source/mem_table.zig");
    pub const chunk_source = @import("source/chunk_source.zig");
    pub const part_scan   = @import("source/part_scan.zig");
};

// Convenience re-exports of the most commonly used types.
pub const Value       = types.Value;
pub const ColumnType  = types.ColumnType;
pub const DataChunk   = chunk.DataChunk;
pub const Column      = chunk.Column;
pub const ChunkBuilder = chunk.ChunkBuilder;
pub const ResultSet   = result.ResultSet;
pub const ResultSink  = result.ResultSink;
pub const ColMeta     = result.ColMeta;

// Pull all sub-module tests into the core test binary.
test {
    const std = @import("std");
    std.testing.refAllDecls(@This());
    _ = types;
    _ = chunk;
    _ = result;
    _ = exec;
    _ = source;
}
