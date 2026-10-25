//! Query context and the type-erased source interface.
const std = @import("std");
const chunk = @import("../chunk.zig");
const result = @import("../result.zig");
const profile = @import("profile.zig");

pub const QueryProfile = profile.QueryProfile;
pub const DataChunk = chunk.DataChunk;

/// Per-query execution context. Holds the arena for all intermediate
/// allocations during one query's lifetime.
pub const QueryContext = struct {
    /// Allocator that owns final ResultSet arenas. It must outlive the query
    /// context so callers can serialize and then deinit returned results.
    parent_alloc: std.mem.Allocator,
    /// All transient allocations (intermediate chunks, hash tables, etc.)
    /// are made from this arena. Freed when the query finishes.
    arena: std.heap.ArenaAllocator,
    /// Injected source implementations (set before executing a plan).
    source: SourceIface,
    /// Optional profile counters, enabled by ZIGHOUSE_PROFILE=1.
    profile: QueryProfile,

    pub fn init(parent_alloc: std.mem.Allocator, source: SourceIface) QueryContext {
        return .{
            .parent_alloc = parent_alloc,
            .arena = std.heap.ArenaAllocator.init(parent_alloc),
            .source = source,
            .profile = QueryProfile.init(),
        };
    }

    pub fn deinit(self: *QueryContext) void {
        if (self.profile.enabled) {
            const source_stats = self.source.profileStats();
            self.profile.open_ns = source_stats.open_ns;
            self.profile.mmap_ns = source_stats.mmap_ns;
            self.profile.bytes_read = source_stats.bytes_read;
            self.profile.bytes_mapped = source_stats.bytes_mapped;
        }
        self.profile.emit();
        self.arena.deinit();
    }

    pub fn allocator(self: *QueryContext) std.mem.Allocator {
        return self.arena.allocator();
    }

    pub fn resultAllocator(self: *QueryContext) std.mem.Allocator {
        return self.parent_alloc;
    }

    pub fn setProfileLabel(self: *QueryContext, label: []const u8) void {
        self.profile.label = label;
    }

    pub fn profileReason(self: *QueryContext, reason: []const u8) void {
        self.profile.recordReason(reason);
    }
};

// ── Source interface ──────────────────────────────────────────────────────────

/// A type-erased source. Concrete implementations live in src/core/source/.
/// Using a vtable (function pointers) here rather than comptime generics
/// because the source type is selected at runtime (config / query routing).
pub const SourceIface = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const RowRange = struct {
        lo: u64,
        hi: u64,
    };

    pub const ProfileStats = struct {
        open_ns: u64 = 0,
        mmap_ns: u64 = 0,
        bytes_read: u64 = 0,
        bytes_mapped: u64 = 0,
    };

    pub const VTable = struct {
        /// Fill `out` with the next chunk of rows. Returns false when exhausted.
        nextChunk: *const fn (ptr: *anyopaque, out: *DataChunk, ctx: *QueryContext) anyerror!bool,
        /// Reset the source to re-scan from the beginning.
        reset: *const fn (ptr: *anyopaque) void,
        /// Return column metadata for this source's schema.
        schema: *const fn (ptr: *anyopaque) []const result.ColMeta,
        /// Return an upper-bound row count estimate (0 = unknown).
        rowCount: *const fn (ptr: *anyopaque) u64,
        /// Optional: fetch a specific row range [start, start+n) into `out`.
        /// If null, the source does not support random-access range reads.
        /// `alloc` is used for the chunk's column buffers.
        fetchRange: ?*const fn (ptr: *anyopaque, start: u64, n: usize, out: *DataChunk, alloc: std.mem.Allocator) anyerror!void = null,
        /// Optional: restrict which columns are decoded during nextChunk / fetchRange.
        /// Pass null to restore all columns. col_names is borrowed (caller owns).
        setNeededCols: ?*const fn (ptr: *anyopaque, col_names: ?[]const []const u8) void = null,
        /// Optional: mark a string column to be decoded as bool_u8 (1=non-empty, 0=empty)
        /// instead of full string slices. Use for columns only needed for `!= ''` checks.
        /// Pass null col_name to clear all such marks.
        setStringNonEmptyBool: ?*const fn (ptr: *anyopaque, col_name: ?[]const u8) void = null,
        /// Optional: return raw int16 slice for a column (bypasses fetchRange overhead).
        getRawInt16Col: ?*const fn (ptr: *anyopaque, col_name: []const u8) ?[]const i16 = null,
        /// Optional: return raw int32 slice for a column.
        getRawInt32Col: ?*const fn (ptr: *anyopaque, col_name: []const u8) ?[]const i32 = null,
        /// Optional: return raw int64 slice for a column.
        getRawInt64Col: ?*const fn (ptr: *anyopaque, col_name: []const u8) ?[]const i64 = null,
        /// Optional: return [min, max] from block statistics without decoding.
        getIntMinMax: ?*const fn (ptr: *anyopaque, col_name: []const u8) ?[2]i64 = null,
        /// Optional: return raw string offsets for a column.
        getRawStrOffsets: ?*const fn (ptr: *anyopaque, col_name: []const u8) ?[]const u64 = null,
        /// Optional: return raw string bytes for a column.
        getRawStrBytes: ?*const fn (ptr: *anyopaque, col_name: []const u8) ?[]const u8 = null,
        /// Optional: restrict scan to rows [lo, hi).  rowCount() and fetchRange()
        /// will reflect the restricted window until setRowRange(0, row_count) is called.
        /// reset() does NOT clear this; the caller must explicitly restore.
        setRowRange: ?*const fn (ptr: *anyopaque, lo: u64, hi: u64) void = null,
        /// Optional: return the table's declared sort-key column names.
        getSortKeys: ?*const fn (ptr: *anyopaque) []const []const u8 = null,
        /// Optional: find the exact row range for an equality on a sorted int key.
        findIntRange: ?*const fn (ptr: *anyopaque, col_name: []const u8, value: i64) ?RowRange = null,
        /// Optional: return cumulative source setup and I/O counters.
        profileStats: ?*const fn (ptr: *anyopaque) ProfileStats = null,
    };

    pub fn nextChunk(self: SourceIface, out: *DataChunk, ctx: *QueryContext) !bool {
        const ok = try self.vtable.nextChunk(self.ptr, out, ctx);
        if (ok and ctx.profile.enabled) {
            ctx.profile.next_chunks += 1;
            ctx.profile.next_rows += out.num_rows;
        }
        return ok;
    }

    pub fn reset(self: SourceIface) void {
        self.vtable.reset(self.ptr);
    }

    pub fn schema(self: SourceIface) []const result.ColMeta {
        return self.vtable.schema(self.ptr);
    }

    pub fn rowCount(self: SourceIface) u64 {
        return self.vtable.rowCount(self.ptr);
    }

    /// Returns true if this source supports parallel range reads.
    pub fn supportsRange(self: SourceIface) bool {
        return self.vtable.fetchRange != null;
    }

    pub fn fetchRange(self: SourceIface, start: u64, n: usize, out: *DataChunk, alloc: std.mem.Allocator) !void {
        return self.vtable.fetchRange.?(self.ptr, start, n, out, alloc);
    }

    /// Temporarily restrict which columns are decoded during nextChunk / fetchRange.
    /// Pass null to restore all columns. Only has effect if source supports it.
    pub fn setNeededCols(self: SourceIface, col_names: ?[]const []const u8) void {
        if (self.vtable.setNeededCols) |f| f(self.ptr, col_names);
    }

    /// Block-level min/max for an integer column, when the source has stats.
    pub fn getIntMinMax(self: SourceIface, col_name: []const u8) ?[2]i64 {
        if (self.vtable.getIntMinMax) |f| return f(self.ptr, col_name);
        return null;
    }

    /// Mark a string column for lightweight bool_u8 decoding (1=non-empty, 0=empty).
    /// Use when the column is only needed for a `!= ''` filter (not projected/keyed).
    /// Pass null col_name to clear all marks. No-op if source doesn't support it.
    pub fn setStringNonEmptyBool(self: SourceIface, col_name: ?[]const u8) void {
        if (self.vtable.setStringNonEmptyBool) |f| f(self.ptr, col_name);
    }

    /// Return the raw per-row string offsets for `col_name` (row_count+1 entries).
    /// Returns null if not supported or column not found.
    pub fn getRawInt16Col(self: SourceIface, col_name: []const u8) ?[]const i16 {
        const f = self.vtable.getRawInt16Col orelse return null;
        return f(self.ptr, col_name);
    }

    pub fn getRawStrOffsets(self: SourceIface, col_name: []const u8) ?[]const u64 {
        const f = self.vtable.getRawStrOffsets orelse return null;
        return f(self.ptr, col_name);
    }

    /// Return the raw string byte blob for `col_name`.
    /// Returns null if not supported or column not found.
    pub fn getRawStrBytes(self: SourceIface, col_name: []const u8) ?[]const u8 {
        const f = self.vtable.getRawStrBytes orelse return null;
        return f(self.ptr, col_name);
    }

    pub fn getRawInt32Col(self: SourceIface, col_name: []const u8) ?[]const i32 {
        const f = self.vtable.getRawInt32Col orelse return null;
        return f(self.ptr, col_name);
    }

    pub fn getRawInt64Col(self: SourceIface, col_name: []const u8) ?[]const i64 {
        const f = self.vtable.getRawInt64Col orelse return null;
        return f(self.ptr, col_name);
    }

    /// Restrict subsequent scans to rows [lo, hi).  Call setRowRange(0, rowCount())
    /// to restore the full range.  No-op if the source doesn't support it.
    pub fn setRowRange(self: SourceIface, lo: u64, hi: u64) void {
        if (self.vtable.setRowRange) |f| f(self.ptr, lo, hi);
    }

    /// Return the table's declared sort-key column names, or &.{} if unknown.
    pub fn getSortKeys(self: SourceIface) []const []const u8 {
        const f = self.vtable.getSortKeys orelse return &.{};
        return f(self.ptr);
    }

    pub fn findIntRange(self: SourceIface, col_name: []const u8, value: i64) ?RowRange {
        const f = self.vtable.findIntRange orelse return null;
        return f(self.ptr, col_name, value);
    }

    pub fn profileStats(self: SourceIface) ProfileStats {
        const f = self.vtable.profileStats orelse return .{};
        return f(self.ptr);
    }
};
