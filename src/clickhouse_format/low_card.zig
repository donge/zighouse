/// LowCardinality(T) stream encoder/decoder for ClickHouse MergeTree parts.
///
/// ClickHouse on-disk format (Compact part, per-granule):
///
/// Dict stream  ({col}.dict.bin):
///   uint64 LE  version  = 1
///   uint64 LE  num_entries
///   <inner-type values × num_entries>   (String = LEB128 len + bytes; fixed = raw LE)
///
/// Index stream ({col}.bin), one block per granule:
///   uint64 LE  flags  (see FLAG_* constants)
///   uint64 LE  0      (reserved / secondary dict size)
///   uint64 LE  num_rows
///   <key per row>  (u8/u16/u32/u64 LE depending on key_bits in flags)
///
/// For the global-dict strategy (one dict per part, written at finish):
///   - First granule flags: NeedUpdateDictionary | HasAdditionalKeys | NeedGlobalDict | key_type
///   - Subsequent granule flags: HasAdditionalKeys | NeedGlobalDict | key_type
///
/// key_type field (bits 0-2 of flags):
///   0 = UInt8, 1 = UInt16, 2 = UInt32, 3 = UInt64

const std = @import("std");
const schema = @import("schema");
const primary_idx = @import("primary_idx.zig"); // for writeVarint / readVarint

// ── Flag constants ─────────────────────────────────────────────────────────────
pub const FLAG_NEED_UPDATE_DICT: u64 = 0x400;
pub const FLAG_HAS_ADDITIONAL_KEYS: u64 = 0x200;
pub const FLAG_NEED_GLOBAL_DICT: u64 = 0x100;

pub const KEY_TYPE_U8: u64 = 0;
pub const KEY_TYPE_U16: u64 = 1;
pub const KEY_TYPE_U32: u64 = 2;
pub const KEY_TYPE_U64: u64 = 3;

// ── DictBuilder ───────────────────────────────────────────────────────────────

/// Accumulates LowCardinality(String) values, builds a global dictionary,
/// then serializes dict stream + index stream in CH format.
pub const DictBuilder = struct {
    allocator: std.mem.Allocator,
    /// Ordered list of unique strings (dict entries).
    entries: std.ArrayListUnmanaged([]u8),
    /// Map from string → index in `entries`.
    map: std.StringHashMapUnmanaged(u32),
    /// Per-row indices (into `entries`).
    keys: std.ArrayListUnmanaged(u32),
    /// Granule boundary row indices (one per granule start, inclusive).
    granule_starts: std.ArrayListUnmanaged(u64),
    /// Total rows appended.
    row_count: u64,

    pub fn init(allocator: std.mem.Allocator) DictBuilder {
        return .{
            .allocator = allocator,
            .entries = .empty,
            .map = .empty,
            .keys = .empty,
            .granule_starts = .empty,
            .row_count = 0,
        };
    }

    pub fn deinit(self: *DictBuilder) void {
        for (self.entries.items) |e| self.allocator.free(e);
        self.entries.deinit(self.allocator);
        self.map.deinit(self.allocator);
        self.keys.deinit(self.allocator);
        self.granule_starts.deinit(self.allocator);
    }

    /// Append a string value. Call markGranule() at each GRANULE_SIZE boundary.
    pub fn append(self: *DictBuilder, s: []const u8) !void {
        const gop = try self.map.getOrPut(self.allocator, s);
        if (!gop.found_existing) {
            const owned = try self.allocator.dupe(u8, s);
            errdefer self.allocator.free(owned);
            const idx: u32 = @intCast(self.entries.items.len);
            try self.entries.append(self.allocator, owned);
            gop.key_ptr.* = owned;
            gop.value_ptr.* = idx;
        }
        try self.keys.append(self.allocator, gop.value_ptr.*);
        self.row_count += 1;
    }

    /// Record the start of a new granule (call before the first row of each granule).
    pub fn markGranule(self: *DictBuilder) !void {
        try self.granule_starts.append(self.allocator, self.row_count);
    }

    /// Determine minimal key type based on dict size.
    pub fn keyType(self: *const DictBuilder) u64 {
        const n = self.entries.items.len;
        if (n <= 256) return KEY_TYPE_U8;
        if (n <= 65536) return KEY_TYPE_U16;
        if (n <= 0x100000000) return KEY_TYPE_U32;
        return KEY_TYPE_U64;
    }

    /// Number of bytes per key.
    pub fn keyWidth(self: *const DictBuilder) usize {
        return switch (self.keyType()) {
            KEY_TYPE_U8 => 1,
            KEY_TYPE_U16 => 2,
            KEY_TYPE_U32 => 4,
            else => 8,
        };
    }

    /// Serialize the dict stream to `writer`.
    /// Format: version(u64) + num_entries(u64) + <varint_len + bytes per entry>
    pub fn serializeDict(self: *const DictBuilder, writer: *std.Io.Writer) !void {
        // version = 1
        var buf8: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf8, 1, .little);
        try writer.writeAll(&buf8);
        // num_entries
        std.mem.writeInt(u64, &buf8, @intCast(self.entries.items.len), .little);
        try writer.writeAll(&buf8);
        // entries: varint length + raw bytes (String inner type)
        for (self.entries.items) |e| {
            try writeVarUint(writer, e.len);
            try writer.writeAll(e);
        }
    }

    /// Serialize the index stream to `writer`.
    /// All granules' data are written sequentially (one CH block per granule is
    /// handled by the caller wrapping each granule in a block; here we just emit
    /// the raw payload for the entire column as one sequential byte stream which
    /// the caller will split at granule boundaries).
    ///
    /// If `single_block` is true, all granules are written as one payload
    /// (compact part style: per-column per-granule block is split by the caller).
    /// Pass false for wide parts.
    pub fn serializeIndexAllGranules(self: *const DictBuilder, allocator: std.mem.Allocator) ![][]u8 {
        const n_gran = if (self.granule_starts.items.len > 0) self.granule_starts.items.len else 1;
        const ktype = self.keyType();
        const kw = self.keyWidth();

        const granule_bufs = try allocator.alloc([]u8, n_gran);
        errdefer {
            for (granule_bufs[0..n_gran]) |g| allocator.free(g);
            allocator.free(granule_bufs);
        }

        for (0..n_gran) |gi| {
            const row_start: u64 = self.granule_starts.items[gi];
            const row_end: u64 = if (gi + 1 < self.granule_starts.items.len)
                self.granule_starts.items[gi + 1]
            else
                self.row_count;
            const n_rows = row_end - row_start;

            // flags
            var flags: u64 = FLAG_HAS_ADDITIONAL_KEYS | FLAG_NEED_GLOBAL_DICT | ktype;
            if (gi == 0) flags |= FLAG_NEED_UPDATE_DICT;

            // payload: 8 (flags) + 8 (reserved=0) + 8 (n_rows) + n_rows * kw
            const payload_len: usize = 24 + n_rows * kw;
            const buf = try allocator.alloc(u8, payload_len);
            errdefer allocator.free(buf);

            var pos: usize = 0;
            std.mem.writeInt(u64, buf[pos..][0..8], flags, .little); pos += 8;
            std.mem.writeInt(u64, buf[pos..][0..8], 0, .little); pos += 8;
            std.mem.writeInt(u64, buf[pos..][0..8], n_rows, .little); pos += 8;

            for (0..n_rows) |ri| {
                const key = self.keys.items[@intCast(row_start + ri)];
                switch (kw) {
                    1 => { buf[pos] = @intCast(key); pos += 1; },
                    2 => { std.mem.writeInt(u16, buf[pos..][0..2], @intCast(key), .little); pos += 2; },
                    4 => { std.mem.writeInt(u32, buf[pos..][0..4], key, .little); pos += 4; },
                    else => { std.mem.writeInt(u64, buf[pos..][0..8], key, .little); pos += 8; },
                }
            }

            granule_bufs[gi] = buf;
        }

        return granule_bufs;
    }
};

// ── Deserializer ──────────────────────────────────────────────────────────────

/// Deserialize a LowCardinality(String) column from dict+index raw bytes into
/// the standard ColumnReader format: size_data (u64 LE per row) + data (raw bytes).
///
/// `dict_raw`: full decompressed content of the dict stream.
/// `index_raw`: full decompressed content of all index granule payloads (concatenated).
/// `row_count`: total number of rows in the column.
/// Returns: .data (raw string bytes) and .size_data (u64 LE lengths), both owned.
pub const DeserializedLC = struct {
    data: []u8,
    size_data: []u8,
};

pub fn deserializeToStringBuf(
    allocator: std.mem.Allocator,
    dict_raw: []const u8,
    index_raw: []const u8,
    row_count: u64,
) !DeserializedLC {
    // ── Parse dict stream ──────────────────────────────────────────────────
    if (dict_raw.len < 16) return error.InvalidDictStream;
    // const version = std.mem.readInt(u64, dict_raw[0..8], .little);  // must be 1
    const num_entries = std.mem.readInt(u64, dict_raw[8..16], .little);

    var dict: std.ArrayListUnmanaged([]const u8) = .empty;
    defer dict.deinit(allocator);

    var dp: usize = 16;
    for (0..num_entries) |_| {
        const slen = try readVarUint(dict_raw, &dp);
        if (dp + slen > dict_raw.len) return error.InvalidDictStream;
        try dict.append(allocator, dict_raw[dp .. dp + slen]);
        dp += slen;
    }

    // ── Parse index stream ─────────────────────────────────────────────────
    var size_buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer size_buf.deinit(allocator);
    var data_buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer data_buf.deinit(allocator);

    var ip: usize = 0;
    var rows_done: u64 = 0;

    while (rows_done < row_count) {
        if (ip + 24 > index_raw.len) return error.InvalidIndexStream;
        const flags = std.mem.readInt(u64, index_raw[ip..][0..8], .little); ip += 8;
        ip += 8; // reserved
        const n_rows = std.mem.readInt(u64, index_raw[ip..][0..8], .little); ip += 8;

        const ktype = flags & 0x3;
        const kw: usize = switch (ktype) {
            KEY_TYPE_U8 => 1,
            KEY_TYPE_U16 => 2,
            KEY_TYPE_U32 => 4,
            else => 8,
        };

        for (0..n_rows) |_| {
            if (ip + kw > index_raw.len) return error.InvalidIndexStream;
            const key: u64 = switch (kw) {
                1 => index_raw[ip],
                2 => std.mem.readInt(u16, index_raw[ip..][0..2], .little),
                4 => std.mem.readInt(u32, index_raw[ip..][0..4], .little),
                else => std.mem.readInt(u64, index_raw[ip..][0..8], .little),
            };
            ip += kw;

            const s: []const u8 = if (key < dict.items.len) dict.items[key] else "";
            // Append u64 LE length to size_buf
            var sz_bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &sz_bytes, s.len, .little);
            try size_buf.appendSlice(allocator, &sz_bytes);
            try data_buf.appendSlice(allocator, s);
        }
        rows_done += n_rows;
    }

    return .{
        .data = try data_buf.toOwnedSlice(allocator),
        .size_data = try size_buf.toOwnedSlice(allocator),
    };
}

// ── varint helpers ────────────────────────────────────────────────────────────

fn writeVarUint(writer: *std.Io.Writer, v: usize) !void {
    var x = v;
    while (x >= 0x80) {
        try writer.writeByte(@as(u8, @intCast((x & 0x7f) | 0x80)));
        x >>= 7;
    }
    try writer.writeByte(@as(u8, @intCast(x)));
}

fn readVarUint(buf: []const u8, pos: *usize) !usize {
    var result: usize = 0;
    var shift: u6 = 0;
    while (pos.* < buf.len) {
        const b = buf[pos.*];
        pos.* += 1;
        result |= @as(usize, b & 0x7f) << shift;
        if ((b & 0x80) == 0) return result;
        shift += 7;
        if (shift >= 63) return error.VarUintOverflow;
    }
    return error.UnexpectedEof;
}
