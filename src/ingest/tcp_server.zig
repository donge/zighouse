/// ClickHouse Native TCP protocol server (port 9000).
///
/// This module is @import'd by server.zig and compiled inside ingest_server_mod,
/// so it shares all the same module imports (schema_config, row_binary_decoder, …).
///
/// Protocol revision advertised: 54460 (clickhouse-go v2.45 DBMS_TCP_PROTOCOL_VERSION).

const std = @import("std");
const schema = @import("schema");
const schema_config = @import("schema_config");
const row_binary_decoder = @import("row_binary_decoder");
const part_writer_session = @import("part_writer_session");
const ddl_parser = @import("ddl_parser");
const schema_persist = @import("schema_persist");
const generic_sql = @import("generic_sql");
const ir_planner = @import("ir_planner");
const core = @import("core");
const part_scan_bridge = @import("part_scan_bridge");
const serializer = @import("serializer");
const part_scanner = @import("part_scanner");

const Io = std.Io;
const net = std.Io.net;

// ── Shared state passed from Server ────────────────────────────────────────

pub const ServerCtx = struct {
    allocator: std.mem.Allocator,
    io: Io,
    data_dir: []const u8,
    schemas: *schema_config.SchemaConfig,
    seq: *std.atomic.Value(u64),
};

// ── Protocol constants ──────────────────────────────────────────────────────

const REVISION: u64 = 54460;
const DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION: u64 = 54454;
const DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM: u64 = 54458;
const DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS: u64 = 54459;

const CLIENT_HELLO: u8 = 0;
const CLIENT_QUERY: u8 = 1;
const CLIENT_DATA: u8 = 2;
const CLIENT_CANCEL: u8 = 3;
const CLIENT_PING: u8 = 4;

const SERVER_HELLO: u8 = 0;
const SERVER_DATA: u8 = 1;
const SERVER_EXCEPTION: u8 = 2;
const SERVER_PONG: u8 = 4;
const SERVER_END_OF_STREAM: u8 = 5;
const SERVER_PROFILE_INFO: u8 = 6;

// ── Wire primitives ─────────────────────────────────────────────────────────

const TcpReader = struct {
    r: *Io.Reader,

    fn readByte(self: TcpReader) !u8 {
        var buf: [1]u8 = undefined;
        try self.r.readSliceAll(&buf);
        return buf[0];
    }

    fn readNoEof(self: TcpReader, buf: []u8) !void {
        try self.r.readSliceAll(buf);
    }

    fn readInt(self: TcpReader, comptime T: type, _: std.builtin.Endian) !T {
        var buf: [@sizeOf(T)]u8 = undefined;
        try self.r.readSliceAll(&buf);
        return std.mem.readInt(T, &buf, .little);
    }

    fn skipBytes(self: TcpReader, n: u64, _: anytype) !void {
        var remaining = n;
        var tmp: [256]u8 = undefined;
        while (remaining > 0) {
            const chunk = @min(remaining, tmp.len);
            try self.r.readSliceAll(tmp[0..chunk]);
            remaining -= chunk;
        }
    }

    fn readUVarInt(self: TcpReader) !u64 {
        var result: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            const b = try self.readByte();
            result |= @as(u64, b & 0x7F) << shift;
            if (b & 0x80 == 0) break;
            shift += 7;
            if (shift >= 64) return error.UVarIntOverflow;
        }
        return result;
    }

    fn readString(self: TcpReader, a: std.mem.Allocator) ![]u8 {
        const len = try self.readUVarInt();
        if (len > 1024 * 1024) return error.StringTooLong;
        const buf = try a.alloc(u8, len);
        errdefer a.free(buf);
        try self.readNoEof(buf);
        return buf;
    }

    fn skipString(self: TcpReader) !void {
        const len = try self.readUVarInt();
        if (len > 16 * 1024 * 1024) return error.StringTooLong;
        try self.skipBytes(len, .{});
    }
};

// ── Write helpers ───────────────────────────────────────────────────────────

fn wuv(buf: *std.ArrayListUnmanaged(u8), a: std.mem.Allocator, v: u64) !void {
    var x = v;
    while (x >= 0x80) {
        try buf.append(a, @as(u8, @intCast((x & 0x7F) | 0x80)));
        x >>= 7;
    }
    try buf.append(a, @as(u8, @intCast(x)));
}

fn wstr(buf: *std.ArrayListUnmanaged(u8), a: std.mem.Allocator, s: []const u8) !void {
    try wuv(buf, a, s.len);
    try buf.appendSlice(a, s);
}

fn writeBlockInfo(buf: *std.ArrayListUnmanaged(u8), a: std.mem.Allocator) !void {
    try wuv(buf, a, 1); try buf.append(a, 0);
    try wuv(buf, a, 2); try buf.appendSlice(a, &[4]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
    try wuv(buf, a, 0);
}

fn flush(w: *Io.Writer, a: std.mem.Allocator, buf: *std.ArrayListUnmanaged(u8)) !void {
    try w.writeAll(buf.items);
    try w.flush();
    buf.clearRetainingCapacity();
    _ = a;
}

// ── Send packets ────────────────────────────────────────────────────────────

fn sendHello(a: std.mem.Allocator, w: *Io.Writer) !void {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(a);
    try buf.append(a, SERVER_HELLO);
    try wstr(&buf, a, "ZigHouse");
    try wuv(&buf, a, 24); try wuv(&buf, a, 3); try wuv(&buf, a, REVISION);
    try wstr(&buf, a, "UTC");
    try wstr(&buf, a, "ZigHouse");
    try wuv(&buf, a, 0);
    try w.writeAll(buf.items);
    try w.flush();
}

fn sendData(a: std.mem.Allocator, w: *Io.Writer, block: []const u8) !void {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(a);
    try buf.append(a, SERVER_DATA);
    try wstr(&buf, a, "");
    try buf.appendSlice(a, block);
    try w.writeAll(buf.items);
    try w.flush();
}

fn sendProfileInfo(a: std.mem.Allocator, w: *Io.Writer) !void {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(a);
    try buf.append(a, SERVER_PROFILE_INFO);
    try wuv(&buf, a, 0); try wuv(&buf, a, 0); try wuv(&buf, a, 0);
    try buf.append(a, 0); try wuv(&buf, a, 0); try buf.append(a, 0);
    try w.writeAll(buf.items);
    try w.flush();
}

fn sendEos(w: *Io.Writer) !void {
    try w.writeAll(&[_]u8{SERVER_END_OF_STREAM});
    try w.flush();
}

fn sendPong(w: *Io.Writer) !void {
    try w.writeAll(&[_]u8{SERVER_PONG});
    try w.flush();
}

fn sendException(a: std.mem.Allocator, w: *Io.Writer, code: i32, msg: []const u8) !void {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(a);
    try buf.append(a, SERVER_EXCEPTION);
    var cb: [4]u8 = undefined;
    std.mem.writeInt(i32, &cb, code, .little);
    try buf.appendSlice(a, &cb);
    try wstr(&buf, a, "DB::Exception");
    try wstr(&buf, a, msg);
    try wstr(&buf, a, "");
    try buf.append(a, 0);
    try w.writeAll(buf.items);
    try w.flush();
}

// ── Schema block (0 rows, column definitions) ───────────────────────────────

fn buildSchemaBlock(a: std.mem.Allocator, entry: *const schema_config.TableEntry) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(a);
    const cols = entry.table.columns;
    try writeBlockInfo(&buf, a);
    try wuv(&buf, a, cols.len);
    try wuv(&buf, a, 0);
    for (cols) |col| {
        try wstr(&buf, a, col.name);
        try wstr(&buf, a, chTypeName(col));
        try buf.append(a, 0); // custom_serialization = false
    }
    return buf.toOwnedSlice(a);
}

fn chTypeName(col: schema.Column) []const u8 {
    if (col.ch_type) |ct| {
        // Normalize DateTime(tz) variants to plain "DateTime" for wire protocol
        if (std.ascii.startsWithIgnoreCase(ct, "DateTime(") and
            !std.ascii.startsWithIgnoreCase(ct, "DateTime64("))
            return "DateTime";
        return ct;
    }
    return switch (col.ty) {
        .int8    => "Int8",
        .int16   => "Int16",
        .int32   => "Int32",
        .int64   => "Int64",
        .float32 => "Float32",
        .float64 => "Float64",
        .text, .char, .low_card => "String",
        .date    => "Date",
        .timestamp => "DateTime",
    };
}

// ── Read ClientQuery packet (packet byte already consumed) ──────────────────

fn readClientQuery(a: std.mem.Allocator, rd: TcpReader, client_rev: u64) ![]u8 {
    try rd.skipString(); // query_id

    // client_info
    _ = try rd.readByte(); // initial_type
    try rd.skipString(); // initial_user
    try rd.skipString(); // initial_query_id
    try rd.skipString(); // initial_address
    if (client_rev >= 54449) _ = try rd.readInt(i64, .little); // initial_query_start_time_us
    _ = try rd.readByte(); // interface
    try rd.skipString(); // os_user
    try rd.skipString(); // client_hostname
    try rd.skipString(); // client_name
    _ = try rd.readUVarInt(); // version_major
    _ = try rd.readUVarInt(); // version_minor
    _ = try rd.readUVarInt(); // tcp_protocol_version
    if (client_rev >= 54060) try rd.skipString(); // quota_key in client_info
    if (client_rev >= 54448) _ = try rd.readUVarInt(); // distributed_depth
    if (client_rev >= 54401) _ = try rd.readUVarInt(); // version_patch
    if (client_rev >= 54442) {
        const has_trace = try rd.readByte();
        if (has_trace != 0) {
            try rd.skipBytes(16 + 8, .{}); // trace_id + span_id
            try rd.skipString(); // trace_state
            _ = try rd.readByte(); // trace_flags
        }
    }
    if (client_rev >= 54453) {
        _ = try rd.readUVarInt(); _ = try rd.readUVarInt(); _ = try rd.readUVarInt();
    }

    // settings: key-value terminated by empty key
    while (true) {
        const key = try rd.readString(a);
        defer a.free(key);
        if (key.len == 0) break;
        _ = try rd.readByte(); // flags
        try rd.skipString();
    }

    if (client_rev >= 54441) try rd.skipString(); // interserver secret

    _ = try rd.readByte(); // state
    _ = try rd.readByte(); // compression
    const sql = try rd.readString(a);

    if (client_rev >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS) {
        while (true) {
            const pk = try rd.readString(a);
            defer a.free(pk);
            if (pk.len == 0) break;
            try rd.skipString();
        }
    }

    return sql;
}

// ── Skip block info ─────────────────────────────────────────────────────────

fn skipBlockInfo(rd: TcpReader) !void {
    while (true) {
        const field = try rd.readUVarInt();
        if (field == 0) break;
        switch (field) {
            1 => _ = try rd.readByte(),
            2 => _ = try rd.readInt(i32, .little),
            else => break,
        }
    }
}

// ── Wire type classification ─────────────────────────────────────────────────

const WireKind = enum { string, fixed1, fixed2, fixed4, fixed8, low_card, array_str, array_lc_str, array_fixed1, array_fixed4, array_fixed8 };

fn wireKind(ch_type: []const u8) WireKind {
    // SimpleAggregateFunction(func, InnerType) → unwrap to InnerType
    if (std.ascii.startsWithIgnoreCase(ch_type, "SimpleAggregateFunction(")) {
        // Find the comma separating func name from inner type (may be nested parens)
        const inner_start = "SimpleAggregateFunction(".len;
        const body = ch_type[inner_start .. ch_type.len - 1]; // strip trailing ')'
        var depth: usize = 0;
        var i: usize = 0;
        while (i < body.len) : (i += 1) {
            if (body[i] == '(') { depth += 1; continue; }
            if (body[i] == ')') { if (depth > 0) depth -= 1; continue; }
            if (body[i] == ',' and depth == 0) {
                const inner_type = std.mem.trim(u8, body[i + 1 ..], " \t");
                return wireKind(inner_type);
            }
        }
    }
    // Nullable(T) → unwrap T
    if (std.ascii.startsWithIgnoreCase(ch_type, "Nullable(")) {
        const inner_type = std.mem.trim(u8, ch_type[9 .. ch_type.len - 1], " \t");
        return wireKind(inner_type);
    }
    if (std.ascii.eqlIgnoreCase(ch_type, "Int8") or std.ascii.eqlIgnoreCase(ch_type, "UInt8")) return .fixed1;
    if (std.ascii.eqlIgnoreCase(ch_type, "Int16") or std.ascii.eqlIgnoreCase(ch_type, "UInt16")) return .fixed2;
    if (std.ascii.eqlIgnoreCase(ch_type, "Int32") or std.ascii.eqlIgnoreCase(ch_type, "UInt32") or
        std.ascii.eqlIgnoreCase(ch_type, "Float32") or std.ascii.eqlIgnoreCase(ch_type, "Date")) return .fixed4;
    if (std.ascii.eqlIgnoreCase(ch_type, "Int64") or std.ascii.eqlIgnoreCase(ch_type, "UInt64") or
        std.ascii.eqlIgnoreCase(ch_type, "Float64")) return .fixed8;
    if (std.ascii.startsWithIgnoreCase(ch_type, "DateTime64")) return .fixed8;
    if (std.ascii.startsWithIgnoreCase(ch_type, "DateTime")) return .fixed4;
    if (std.ascii.startsWithIgnoreCase(ch_type, "LowCardinality(")) return .low_card;
    // Array types
    if (std.ascii.startsWithIgnoreCase(ch_type, "Array(")) {
        const inner = ch_type[6 .. ch_type.len - 1]; // strip "Array(" and ")"
        if (std.ascii.startsWithIgnoreCase(inner, "LowCardinality(")) return .array_lc_str;
        if (std.ascii.eqlIgnoreCase(inner, "String") or std.ascii.startsWithIgnoreCase(inner, "FixedString(") or
            std.ascii.startsWithIgnoreCase(inner, "Nullable(")) return .array_str;
        if (std.ascii.eqlIgnoreCase(inner, "Int8") or std.ascii.eqlIgnoreCase(inner, "UInt8")) return .array_fixed1;
        if (std.ascii.eqlIgnoreCase(inner, "Int32") or std.ascii.eqlIgnoreCase(inner, "UInt32") or
            std.ascii.eqlIgnoreCase(inner, "Float32")) return .array_fixed4;
        if (std.ascii.eqlIgnoreCase(inner, "Int64") or std.ascii.eqlIgnoreCase(inner, "UInt64") or
            std.ascii.eqlIgnoreCase(inner, "Float64")) return .array_fixed8;
        return .array_str; // fallback
    }
    return .string;
}

// ── Read one ClientData block ───────────────────────────────────────────────

fn readClientDataBlock(
    a: std.mem.Allocator,
    rd: TcpReader,
    entry: ?*const schema_config.TableEntry,
    col_bufs: ?[]row_binary_decoder.ColumnBuffer,
) !bool {
    try rd.skipString(); // block name
    try skipBlockInfo(rd);

    const num_cols = try rd.readUVarInt();
    const num_rows = try rd.readUVarInt();

    for (0..num_cols) |_| {
        const col_name = try rd.readString(a);
        defer a.free(col_name);
        const col_type_str = try rd.readString(a);
        defer a.free(col_type_str);
        _ = try rd.readByte(); // custom_serialization

        var buf_idx: ?usize = null;
        if (entry) |e| {
            for (e.table.columns, 0..) |col, i| {
                if (std.ascii.eqlIgnoreCase(col.name, col_name)) {
                    buf_idx = i;
                    break;
                }
            }
        }

        const kind = wireKind(col_type_str);

        // LowCardinality: block-level encoding (state prefix + dict + key indices)
        if (kind == .low_card) {
            // State prefix: uint64 (= 1 for sharedDictionariesWithAdditionalKeys)
            _ = try rd.readInt(u64, .little);

            // flags: uint64 (bits 0-1 = key_type, bits 9-10 = updateAll)
            const flags = try rd.readInt(u64, .little);
            const key_type: u2 = @truncate(flags & 0xFF);

            // dict_count: int64
            const dict_count = try rd.readInt(i64, .little);
            if (dict_count < 0) return error.InvalidLCData;

            // Read dictionary strings
            const dict = try a.alloc([]const u8, @intCast(dict_count));
            defer a.free(dict);
            var dict_owned: std.ArrayListUnmanaged(u8) = .empty;
            defer dict_owned.deinit(a);
            for (0..@intCast(dict_count)) |di| {
                const s = try rd.readString(a);
                const offset = dict_owned.items.len;
                try dict_owned.appendSlice(a, s);
                a.free(s);
                dict[di] = dict_owned.items[offset..];
            }
            // Fix up dict slices after appendSlice (potential realloc)
            var dict_pos: usize = 0;
            for (0..@intCast(dict_count)) |di| {
                const s_len = dict[di].len;
                dict[di] = dict_owned.items[dict_pos .. dict_pos + s_len];
                dict_pos += s_len;
            }

            // key_count: int64
            const key_count = try rd.readInt(i64, .little);
            if (key_count < 0 or key_count != @as(i64, @intCast(num_rows))) return error.InvalidLCData;

            // Read indices and expand to strings
            const key_width: usize = switch (key_type) {
                0 => 1, 1 => 2, 2 => 4, 3 => 8,
            };
            for (0..@intCast(key_count)) |_| {
                const key: u64 = switch (key_width) {
                    1 => try rd.readInt(u8, .little),
                    2 => try rd.readInt(u16, .little),
                    4 => try rd.readInt(u32, .little),
                    else => try rd.readInt(u64, .little),
                };
                const s: []const u8 = if (key < dict.len) dict[key] else "";
                if (col_bufs) |bs| if (buf_idx) |idx| {
                    const offset = bs[idx].str_bytes.items.len;
                    try bs[idx].str_bytes.appendSlice(a, s);
                    try bs[idx].str_vals.append(a, bs[idx].str_bytes.items[offset .. offset + s.len]);
                };
            }
            continue;
        }

        // Array types: read offsets then element data, store as \x01-prefixed blob
        if (kind == .array_str or kind == .array_lc_str or
            kind == .array_fixed1 or kind == .array_fixed4 or kind == .array_fixed8)
        {
            // Array(LC(String)) wire layout (from clickhouse-go block.go):
            //   WriteStatePrefix: LC state prefix (8 bytes)  [before Encode]
            //   Encode: offsets (num_rows × uint64) + LC block (flags+dict+keys)
            // Other Array types:
            //   Encode: offsets (num_rows × uint64) + element data
            if (kind == .array_lc_str) {
                _ = try rd.readInt(u64, .little); // LC state prefix (from WriteStatePrefix)
            }

            // Wire format: num_rows cumulative end-offsets (uint64), then element data.
            // offsets[r] = cumulative count of elements through row r (0-indexed).
            // Row r's elements span [offsets[r-1]..offsets[r]] (with offsets[-1]=0).
            const offsets = try a.alloc(u64, num_rows);
            defer a.free(offsets);
            for (0..num_rows) |oi| {
                offsets[oi] = try rd.readInt(u64, .little);
            }

            // Total element count = last offset value
            const total_elems: u64 = if (num_rows > 0) offsets[num_rows - 1] else 0;

            if (kind == .array_lc_str) {
                // LC block: flags(8) + dict_count(8) + dict strings + key_count(8) + key_indices
                const flags = try rd.readInt(u64, .little);
                const key_type_lc: u2 = @truncate(flags & 0xFF);
                const dict_count_lc = try rd.readInt(i64, .little);
                if (dict_count_lc < 0) return error.InvalidLCData;

                // Read dict strings, tracking per-entry lengths
                var dict_lens = try a.alloc(usize, @intCast(dict_count_lc));
                defer a.free(dict_lens);
                var lc_dict_bytes: std.ArrayListUnmanaged(u8) = .empty;
                defer lc_dict_bytes.deinit(a);
                for (0..@intCast(dict_count_lc)) |di| {
                    const ds = try rd.readString(a);
                    dict_lens[di] = ds.len;
                    try lc_dict_bytes.appendSlice(a, ds);
                    a.free(ds);
                }
                // Build slice refs into contiguous buffer
                var lc_dict = try a.alloc([]const u8, @intCast(dict_count_lc));
                defer a.free(lc_dict);
                {
                    var dp: usize = 0;
                    for (0..@intCast(dict_count_lc)) |di| {
                        lc_dict[di] = lc_dict_bytes.items[dp .. dp + dict_lens[di]];
                        dp += dict_lens[di];
                    }
                }

                const key_count_lc = try rd.readInt(i64, .little);
                if (key_count_lc < 0) return error.InvalidLCData;
                const kw_lc: usize = switch (key_type_lc) { 0 => 1, 1 => 2, 2 => 4, 3 => 8 };

                // Read all key indices and resolve to strings
                const all_elems = try a.alloc([]const u8, @intCast(key_count_lc));
                defer a.free(all_elems);
                for (0..@intCast(key_count_lc)) |ei| {
                    const key_lc: u64 = switch (kw_lc) {
                        1 => try rd.readInt(u8, .little),
                        2 => try rd.readInt(u16, .little),
                        4 => try rd.readInt(u32, .little),
                        else => try rd.readInt(u64, .little),
                    };
                    all_elems[ei] = if (key_lc < lc_dict.len) lc_dict[key_lc] else "";
                }

                // Per-row: build \x01-prefixed blob
                for (0..num_rows) |r| {
                    const elem_start: usize = if (r == 0) 0 else @intCast(offsets[r - 1]);
                    const elem_end: usize = @intCast(offsets[r]);
                    if (col_bufs) |bs| if (buf_idx) |idx| {
                        var blob: std.ArrayListUnmanaged(u8) = .empty;
                        defer blob.deinit(a);
                        try blob.append(a, 0x01);
                        for (elem_start..elem_end) |ei| {
                            if (ei > elem_start) try blob.append(a, 0x0c);
                            try blob.appendSlice(a, all_elems[ei]);
                        }
                        const off3 = bs[idx].str_bytes.items.len;
                        try bs[idx].str_bytes.appendSlice(a, blob.items);
                        try bs[idx].str_vals.append(a, bs[idx].str_bytes.items[off3 .. off3 + blob.items.len]);
                    };
                }
            } else if (kind == .array_str) {
                // Array(String): varuint_len + bytes per element
                // Collect all elements then build per-row blobs
                var all_strs_list: std.ArrayListUnmanaged([]const u8) = .empty;
                defer all_strs_list.deinit(a);
                var str_bytes_buf: std.ArrayListUnmanaged(u8) = .empty;
                defer str_bytes_buf.deinit(a);
                var str_lens_list: std.ArrayListUnmanaged(usize) = .empty;
                defer str_lens_list.deinit(a);

                for (0..@intCast(total_elems)) |_| {
                    const es = try rd.readString(a);
                    const slen = es.len;
                    try str_bytes_buf.appendSlice(a, es);
                    a.free(es);
                    try str_lens_list.append(a, slen);
                }
                // Build slice refs from buffer + lengths
                var spos2: usize = 0;
                for (str_lens_list.items) |slen2| {
                    try all_strs_list.append(a, str_bytes_buf.items[spos2 .. spos2 + slen2]);
                    spos2 += slen2;
                }

                for (0..num_rows) |r| {
                    const elem_start: usize = if (r == 0) 0 else @intCast(offsets[r - 1]);
                    const elem_end: usize = @intCast(offsets[r]);
                    if (col_bufs) |bs| if (buf_idx) |idx| {
                        var blob: std.ArrayListUnmanaged(u8) = .empty;
                        defer blob.deinit(a);
                        try blob.append(a, 0x01);
                        for (elem_start..elem_end) |ei| {
                            if (ei > elem_start) try blob.append(a, 0x0c);
                            try blob.appendSlice(a, all_strs_list.items[ei]);
                        }
                        const off5 = bs[idx].str_bytes.items.len;
                        try bs[idx].str_bytes.appendSlice(a, blob.items);
                        try bs[idx].str_vals.append(a, bs[idx].str_bytes.items[off5 .. off5 + blob.items.len]);
                    };
                }
            } else {
                // Array(fixed-width): skip element data
                const elem_width: usize = switch (kind) { .array_fixed1 => 1, .array_fixed4 => 4, .array_fixed8 => 8, else => 1 };
                for (0..@intCast(total_elems)) |_| {
                    var tmp: [8]u8 = undefined;
                    try rd.readNoEof(tmp[0..elem_width]);
                }
                for (0..num_rows) |_| {
                    if (col_bufs) |bs| if (buf_idx) |idx| {
                        const blob = "\x01";
                        const off6 = bs[idx].str_bytes.items.len;
                        try bs[idx].str_bytes.appendSlice(a, blob);
                        try bs[idx].str_vals.append(a, bs[idx].str_bytes.items[off6 .. off6 + blob.len]);
                    };
                }
            }
            continue;
        }

        for (0..num_rows) |_| {
            switch (kind) {
                .fixed1 => {
                    const v = try rd.readInt(i8, .little);
                    if (col_bufs) |bs| if (buf_idx) |idx| try bs[idx].fixed_vals.append(a, @intCast(v));
                },
                .fixed2 => {
                    const v = try rd.readInt(i16, .little);
                    if (col_bufs) |bs| if (buf_idx) |idx| try bs[idx].fixed_vals.append(a, @intCast(v));
                },
                .fixed4 => {
                    const v = try rd.readInt(u32, .little);
                    if (col_bufs) |bs| if (buf_idx) |idx| try bs[idx].fixed_vals.append(a, @intCast(v));
                },
                .fixed8 => {
                    const v = try rd.readInt(i64, .little);
                    if (col_bufs) |bs| if (buf_idx) |idx| try bs[idx].fixed_vals.append(a, v);
                },
                .string => {
                    const s = try rd.readString(a);
                    if (col_bufs) |bs| if (buf_idx) |idx| {
                        const offset = bs[idx].str_bytes.items.len;
                        try bs[idx].str_bytes.appendSlice(a, s);
                        try bs[idx].str_vals.append(a, bs[idx].str_bytes.items[offset .. offset + s.len]);
                        a.free(s);
                    } else {
                        a.free(s);
                    } else {
                        a.free(s);
                    }
                },
                .low_card => unreachable,
                // Array types: handled below per-column (not per-row)
                .array_str, .array_lc_str, .array_fixed1, .array_fixed4, .array_fixed8 => unreachable,
            }
        }
    }

    return num_rows > 0;
}

// ── Connection handler ──────────────────────────────────────────────────────

pub fn handleConnection(ctx: *ServerCtx, stream: net.Stream) void {
    handleConn(ctx, stream) catch |err| {
        std.debug.print("tcp: connection error: {s}\n", .{@errorName(err)});
    };
    stream.close(ctx.io);
}

fn handleConn(ctx: *ServerCtx, stream: net.Stream) !void {
    const a = ctx.allocator;
    var read_buf: [64 * 1024]u8 = undefined;
    var write_buf: [64 * 1024]u8 = undefined;
    var net_reader = stream.reader(ctx.io, &read_buf);
    var net_writer = stream.writer(ctx.io, &write_buf);
    const rd = TcpReader{ .r = &net_reader.interface };
    const w = &net_writer.interface;

    // ── ClientHello ───────────────────────────────────────────────────────
    const pkt0 = try rd.readByte();
    if (pkt0 != CLIENT_HELLO) return error.UnexpectedPacket;
    try rd.skipString(); // client_name
    _ = try rd.readUVarInt(); // version_major
    _ = try rd.readUVarInt(); // version_minor
    const client_rev = try rd.readUVarInt();
    try rd.skipString(); // database
    try rd.skipString(); // user
    try rd.skipString(); // password

    try sendHello(a, w);

    // Addendum
    if (client_rev >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM) {
        try rd.skipString(); // quota_key
    }

    // ── Main request loop ─────────────────────────────────────────────────
    while (true) {
        const pkt = rd.readByte() catch |err| {
            if (err == error.EndOfStream) break;
            return err;
        };
        switch (pkt) {
            CLIENT_PING => try sendPong(w),
            CLIENT_QUERY => {
                const sql = try readClientQuery(a, rd, client_rev);
                defer a.free(sql);
                // Consume trailing empty ClientData
                const trailing = rd.readByte() catch 0xFF;
                if (trailing == CLIENT_DATA) {
                    _ = try readClientDataBlock(a, rd, null, null);
                } else if (trailing != 0xFF) {
                    // Unexpected byte — put it back by re-reading won't work,
                    // log and continue (may cause stream desync)
                    std.debug.print("tcp: WARNING unexpected trailing byte 0x{x:02}, stream may desync\n", .{trailing});
                }
                try dispatchQuery(ctx, a, w, sql, rd);
            },
            CLIENT_CANCEL => break,
            else => {
                std.debug.print("tcp: unknown packet 0x{x:02}\n", .{pkt});
                break;
            },
        }
    }
}

fn dispatchQuery(
    ctx: *ServerCtx,
    a: std.mem.Allocator,
    w: *Io.Writer,
    sql: []const u8,
    rd: TcpReader,
) !void {
    var i: usize = 0;
    while (i < sql.len and (sql[i] == ' ' or sql[i] == '\t' or sql[i] == '\r' or sql[i] == '\n')) i += 1;
    const s = sql[i..];
    if (std.ascii.startsWithIgnoreCase(s, "INSERT")) {
        try handleInsert(ctx, a, w, s, rd);
    } else if (std.ascii.startsWithIgnoreCase(s, "SELECT") or
               std.ascii.startsWithIgnoreCase(s, "SHOW") or
               std.ascii.startsWithIgnoreCase(s, "DESC")) {
        // Normalize: strip FINAL keyword and SETTINGS clause
        const normalized = try normalizeSql(a, s);
        defer a.free(normalized);
        try handleSelect(ctx, a, w, normalized);
    } else if (std.ascii.startsWithIgnoreCase(s, "CREATE TABLE") or
               std.ascii.startsWithIgnoreCase(s, "CREATE OR REPLACE TABLE")) {
        try handleCreateTable(ctx, a, w, s);
    } else if (std.ascii.startsWithIgnoreCase(s, "DROP TABLE")) {
        try handleDropTable(ctx, a, w, s);
    } else if (std.ascii.startsWithIgnoreCase(s, "ALTER TABLE")) {
        try handleAlterTable(ctx, a, w, s);
    } else {
        // Other DDL — acknowledge with empty result
        try sendEos(w);
    }
}

/// Strip FINAL keyword and SETTINGS clause from SQL.
fn normalizeSql(a: std.mem.Allocator, sql: []const u8) ![]u8 {
    // Step 1: Remove SETTINGS ... at end (top-level, after query body)
    const after_settings = stripSettingsClause(sql);
    // Step 2: Remove FINAL keyword
    const result = try removeFinalKeyword(a, after_settings);
    return result;
}

fn stripSettingsClause(sql: []const u8) []const u8 {
    // Find top-level SETTINGS keyword (not inside parentheses or quotes)
    var depth: usize = 0;
    var in_quote: bool = false;
    var quote_char: u8 = 0;
    var i: usize = 0;
    while (i < sql.len) {
        const c = sql[i];
        if (in_quote) {
            if (c == quote_char) in_quote = false;
            i += 1;
            continue;
        }
        if (c == '\'' or c == '"' or c == '`') {
            in_quote = true; quote_char = c; i += 1; continue;
        }
        if (c == '(') { depth += 1; i += 1; continue; }
        if (c == ')') { if (depth > 0) depth -= 1; i += 1; continue; }
        if (depth == 0) {
            const kw = "SETTINGS";
            if (i + kw.len <= sql.len and std.ascii.eqlIgnoreCase(sql[i..i + kw.len], kw)) {
                const before = i == 0 or !std.ascii.isAlphanumeric(sql[i - 1]);
                const after_pos = i + kw.len;
                const after = after_pos >= sql.len or !std.ascii.isAlphanumeric(sql[after_pos]);
                if (before and after) {
                    // Strip from here to end
                    var end = i;
                    while (end > 0 and (sql[end-1] == ' ' or sql[end-1] == '\t' or sql[end-1] == '\r' or sql[end-1] == '\n')) end -= 1;
                    return sql[0..end];
                }
            }
        }
        i += 1;
    }
    return sql;
}

fn removeFinalKeyword(a: std.mem.Allocator, sql: []const u8) ![]u8 {
    var result: std.ArrayListUnmanaged(u8) = .empty;
    errdefer result.deinit(a);
    var i: usize = 0;
    while (i < sql.len) {
        const kw = "FINAL";
        if (i + kw.len <= sql.len and std.ascii.eqlIgnoreCase(sql[i..i + kw.len], kw)) {
            const before_ok = i == 0 or (!std.ascii.isAlphanumeric(sql[i-1]) and sql[i-1] != '_');
            const after_pos = i + kw.len;
            const after_ok = after_pos >= sql.len or (!std.ascii.isAlphanumeric(sql[after_pos]) and sql[after_pos] != '_');
            if (before_ok and after_ok) {
                i = after_pos;
                // Remove one preceding space if present
                if (result.items.len > 0 and result.items[result.items.len - 1] == ' ') {
                    result.items.len -= 1;
                }
                continue;
            }
        }
        try result.append(a, sql[i]);
        i += 1;
    }
    return result.toOwnedSlice(a);
}

/// Extract table name from `WHERE table = 'X'` or `WHERE table = "X"`.
/// Returns slice of sql (no allocation).
fn extractWhereTableName(sql: []const u8) ?[]const u8 {
    return extractWhereNameField(sql, "table");
}

/// Extract value from `WHERE <field> = 'X'` pattern in SQL.
fn extractWhereNameField(sql: []const u8, field: []const u8) ?[]const u8 {
    var pos: usize = 0;
    while (pos < sql.len) {
        const idx = std.ascii.indexOfIgnoreCase(sql[pos..], field) orelse break;
        pos += idx + field.len;
        // Must be followed by whitespace or '='
        if (pos < sql.len and std.ascii.isAlphanumeric(sql[pos])) continue; // part of longer word
        // Skip whitespace and '='
        while (pos < sql.len and (sql[pos] == ' ' or sql[pos] == '\t')) pos += 1;
        if (pos >= sql.len or sql[pos] != '=') continue;
        pos += 1;
        while (pos < sql.len and (sql[pos] == ' ' or sql[pos] == '\t')) pos += 1;
        if (pos >= sql.len) break;
        const q = sql[pos];
        if (q != '\'' and q != '"') continue;
        pos += 1;
        const start = pos;
        while (pos < sql.len and sql[pos] != q) pos += 1;
        if (pos >= sql.len) break;
        return sql[start..pos];
    }
    return null;
}

/// Send a single-column, single-row result with a String value.
fn sendScalarString(a: std.mem.Allocator, w: *Io.Writer, col_name: []const u8, value: []const u8) !void {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(a);
    try writeBlockInfo(&buf, a);
    try wuv(&buf, a, 1); // 1 column
    try wuv(&buf, a, 1); // 1 row
    try wstr(&buf, a, col_name); try wstr(&buf, a, "String"); try buf.append(a, 0);
    try wstr(&buf, a, value);
    try sendData(a, w, buf.items);
    try sendProfileInfo(a, w);
    try sendEos(w);
}

/// Send a single-column, single-row result with a UInt64 value.
fn sendScalarUInt64(a: std.mem.Allocator, w: *Io.Writer, col_name: []const u8, value: u64) !void {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(a);
    try writeBlockInfo(&buf, a);
    try wuv(&buf, a, 1); // 1 column
    try wuv(&buf, a, 1); // 1 row
    try wstr(&buf, a, col_name); try wstr(&buf, a, "UInt64"); try buf.append(a, 0);
    var v_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &v_bytes, value, .little);
    try buf.appendSlice(a, &v_bytes);
    try sendData(a, w, buf.items);
    try sendProfileInfo(a, w);
    try sendEos(w);
}

fn handleSelect(ctx: *ServerCtx, a: std.mem.Allocator, w: *Io.Writer, sql: []const u8) !void {
    // ── SHOW TABLES ───────────────────────────────────────────────────────
    if (std.ascii.startsWithIgnoreCase(sql, "SHOW TABLES") or
        std.ascii.startsWithIgnoreCase(sql, "SHOW FULL TABLES")) {
        const list = if (ctx.schemas.dynamic_tables.items.len > 0)
            ctx.schemas.dynamic_tables.items else ctx.schemas.tables;
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(a);
        try writeBlockInfo(&buf, a);
        try wuv(&buf, a, 1);
        try wuv(&buf, a, @intCast(list.len));
        try wstr(&buf, a, "name"); try wstr(&buf, a, "String"); try buf.append(a, 0);
        for (list) |entry| try wstr(&buf, a, entry.name);
        try sendData(a, w, buf.items);
        try sendProfileInfo(a, w);
        try sendEos(w);
        return;
    }

    // ── SELECT version() ─────────────────────────────────────────────────
    if (std.ascii.indexOfIgnoreCase(sql, "version()") != null) {
        try sendScalarString(a, w, "version()", "24.3.0.1-ZigHouse");
        return;
    }

    // ── SELECT currentDatabase() ──────────────────────────────────────────
    if (std.ascii.indexOfIgnoreCase(sql, "currentDatabase()") != null or
        std.ascii.indexOfIgnoreCase(sql, "current_database()") != null) {
        try sendScalarString(a, w, "currentDatabase()", "default");
        return;
    }

    // ── SELECT count(*) FROM system.tables WHERE ... ─────────────────────
    if (std.ascii.indexOfIgnoreCase(sql, "system.tables") != null and
        std.ascii.indexOfIgnoreCase(sql, "count(") != null) {
        const table_filter = extractWhereNameField(sql, "name");
        var count: u64 = 0;
        if (table_filter) |tf| {
            if (ctx.schemas.find("default", tf) != null) count = 1;
        }
        try sendScalarUInt64(a, w, "count(*)", count);
        return;
    }

    // ── SELECT count(*) FROM system.columns WHERE ... ─────────────────────
    if (std.ascii.indexOfIgnoreCase(sql, "system.columns") != null and
        std.ascii.indexOfIgnoreCase(sql, "count(") != null) {
        const table_filter = extractWhereTableName(sql);
        const col_filter = extractWhereNameField(sql, "name");
        var count: u64 = 0;
        if (table_filter) |tf| {
            if (ctx.schemas.find("default", tf)) |entry| {
                if (col_filter) |cf| {
                    for (entry.table.columns) |col| {
                        if (std.ascii.eqlIgnoreCase(col.name, cf)) { count = 1; break; }
                    }
                } else {
                    count = @intCast(entry.table.columns.len);
                }
            }
        }
        try sendScalarUInt64(a, w, "count(*)", count);
        return;
    }

    // ── SELECT engine_full FROM system.tables WHERE ... ──────────────────
    if (std.ascii.indexOfIgnoreCase(sql, "system.tables") != null) {
        const table_filter = extractWhereTableName(sql);
        var block_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer block_buf.deinit(a);

        try writeBlockInfo(&block_buf, a);
        try wuv(&block_buf, a, 1); // 1 column: engine_full

        // Count matching tables
        var matching: u64 = 0;
        if (table_filter) |tf| {
            if (ctx.schemas.find("default", tf) != null) matching = 1;
        } else {
            const list = if (ctx.schemas.dynamic_tables.items.len > 0)
                ctx.schemas.dynamic_tables.items else ctx.schemas.tables;
            matching = @intCast(list.len);
        }
        try wuv(&block_buf, a, matching);
        try wstr(&block_buf, a, "engine_full");
        try wstr(&block_buf, a, "String");
        try block_buf.append(a, 0); // custom_serialization
        // Emit rows
        for (0..matching) |_| {
            try wstr(&block_buf, a, "MergeTree() ORDER BY tuple()");
        }
        try sendData(a, w, block_buf.items);
        try sendProfileInfo(a, w);
        try sendEos(w);
        return;
    }

    // ── SELECT name FROM system.columns WHERE ... ─────────────────────────
    // ── SELECT name, type, ... FROM system.columns WHERE ... ──────────────
    if (std.ascii.indexOfIgnoreCase(sql, "system.columns") != null) {
        const table_filter = extractWhereTableName(sql);
        var block_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer block_buf.deinit(a);

        var entry_opt: ?*const schema_config.TableEntry = null;
        if (table_filter) |tf| {
            entry_opt = ctx.schemas.find("default", tf);
        }

        const num_rows: u64 = if (entry_opt) |e| @intCast(e.table.columns.len) else 0;

        try writeBlockInfo(&block_buf, a);
        try wuv(&block_buf, a, 2); // 2 columns: name, type
        try wuv(&block_buf, a, num_rows);
        // Column 1: name (meta + data)
        try wstr(&block_buf, a, "name"); try wstr(&block_buf, a, "String"); try block_buf.append(a, 0);
        if (entry_opt) |e| {
            for (e.table.columns) |col| try wstr(&block_buf, a, col.name);
        }
        // Column 2: type (meta + data)
        try wstr(&block_buf, a, "type"); try wstr(&block_buf, a, "String"); try block_buf.append(a, 0);
        if (entry_opt) |e| {
            for (e.table.columns) |col| try wstr(&block_buf, a, chTypeName(col));
        }
        try sendData(a, w, block_buf.items);
        try sendProfileInfo(a, w);
        try sendEos(w);
        return;
    }

    // ── Default: execute against real table data via IR pipeline ─────────────
    const plan_opt = try generic_sql.parse(a, sql);
    if (plan_opt == null) {
        // Unknown SQL — return empty result
        var block_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer block_buf.deinit(a);
        try writeBlockInfo(&block_buf, a);
        try wuv(&block_buf, a, 0); try wuv(&block_buf, a, 0);
        try sendData(a, w, block_buf.items);
        try sendProfileInfo(a, w);
        try sendEos(w);
        return;
    }
    const plan = plan_opt.?;
    defer generic_sql.deinit(a, plan);

    // ── Subquery / UNION ALL: not yet supported — return empty result ─────────
    if (plan.subquery_source != null or plan.union_other != null) {
        var block_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer block_buf.deinit(a);
        try writeBlockInfo(&block_buf, a); try wuv(&block_buf, a, 0); try wuv(&block_buf, a, 0);
        try sendData(a, w, block_buf.items); try sendProfileInfo(a, w); try sendEos(w);
        return;
    }

    const db_table = splitDbTable(plan.table);
    const entry_opt = ctx.schemas.find(db_table.db, db_table.table);
    const table: *const schema.Table = if (entry_opt) |e| &e.table else
        return sendEmptyBlock(a, w);

    var parts = try part_scanner.scan(a, ctx.io, ctx.data_dir, db_table.db, db_table.table);
    defer parts.deinit();

    // ── IR execution path ─────────────────────────────────────────────────────
    const ir_result = ir_exec: {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        var pctx = ir_planner.PlannerCtx.init(arena.allocator(), table.*);
        const node = ir_planner.plan_query(&pctx, plan) catch |err| {
            std.log.warn("tcp ir_planner error: {}", .{err});
            arena.deinit();
            break :ir_exec null;
        };
        if (node == null) {
            arena.deinit();
            break :ir_exec null;
        }
        const pruned_cols = ir_planner.findPrunedCols(node.?);
        var bridge = part_scan_bridge.PartScanBridge.init(
            a, ctx.io, table.*, parts.dirs(), pruned_cols,
        ) catch |err| {
            std.log.warn("tcp part_scan_bridge error: {}", .{err});
            arena.deinit();
            break :ir_exec null;
        };
        defer bridge.deinit();
        var qctx = core.exec.pipeline.QueryContext.init(a, bridge.source());
        defer qctx.deinit();
        const rs = core.exec.pipeline.executePlan(node.?, &qctx) catch |err| {
            std.log.warn("tcp pipeline error: {}", .{err});
            arena.deinit();
            break :ir_exec null;
        };
        arena.deinit();
        break :ir_exec rs;
    };

    if (ir_result) |rs_val| {
        var rs = rs_val;
        defer rs.deinit();
        const nb = try serializer.toNativeBlock(a, rs);
        defer a.free(nb);
        try sendData(a, w, nb);
        try sendProfileInfo(a, w);
        try sendEos(w);
        return;
    }

    // IR returned null — unsupported shape, return empty result.
    try sendEmptyBlock(a, w);
}

fn sendEmptyBlock(a: std.mem.Allocator, w: *Io.Writer) !void {
    var block_buf: std.ArrayListUnmanaged(u8) = .empty;
    defer block_buf.deinit(a);
    try writeBlockInfo(&block_buf, a);
    try wuv(&block_buf, a, 0); try wuv(&block_buf, a, 0);
    try sendData(a, w, block_buf.items);
    try sendProfileInfo(a, w);
    try sendEos(w);
}

// Helper: split "db.table" or just "table" into {db, table}.
const DbTable = struct { db: []const u8, table: []const u8 };
fn splitDbTable(name: []const u8) DbTable {
    if (std.mem.indexOfScalar(u8, name, '.')) |dot| {
        return .{ .db = name[0..dot], .table = name[dot+1..] };
    }
    return .{ .db = "default", .table = name };
}

fn handleCreateTable(ctx: *ServerCtx, a: std.mem.Allocator, w: *Io.Writer, sql: []const u8) !void {
    var parsed = ddl_parser.parse(a, sql) catch {
        // DDL parse failure — still ack with EOS so client doesn't hang
        try sendEos(w);
        return;
    };
    defer parsed.deinit();

    if (ctx.schemas.find(parsed.entry.db, parsed.entry.name) == null) {
        try ctx.schemas.addEntry(a, parsed.entry);
        if (ctx.schemas.find(parsed.entry.db, parsed.entry.name)) |stored| {
            schema_persist.save(ctx.io, a, ctx.data_dir, stored.db, stored) catch |err| {
                std.debug.print("tcp: schema_persist.save warning: {s}\n", .{@errorName(err)});
            };
        }
    }
    try sendEos(w);
}

fn handleDropTable(ctx: *ServerCtx, a: std.mem.Allocator, w: *Io.Writer, sql: []const u8) !void {
    // Parse: DROP TABLE [IF EXISTS] [[db.]table]
    var rest = std.mem.trim(u8, sql[("DROP TABLE").len..], " \t");
    const if_exists = std.ascii.startsWithIgnoreCase(rest, "IF EXISTS");
    if (if_exists) rest = std.mem.trim(u8, rest[("IF EXISTS").len..], " \t");

    // Strip trailing semicolon / whitespace
    rest = std.mem.trim(u8, rest, " \t;\n\r");

    if (rest.len == 0) {
        try sendEos(w);
        return;
    }

    // Split db.table or just table
    var db: []const u8 = "default";
    var table_name: []const u8 = rest;
    if (std.mem.indexOfScalar(u8, rest, '.')) |dot| {
        db = rest[0..dot];
        table_name = rest[dot + 1 ..];
    }

    if (ctx.schemas.find(db, table_name) == null) {
        if (if_exists) {
            try sendEos(w);
        } else {
            try sendException(a, w, 60, "Table not found");
        }
        return;
    }

    ctx.schemas.removeEntry(db, table_name);

    // Delete on-disk directory <data_dir>/<db>/<table>
    const table_dir = try std.fmt.allocPrint(a, "{s}/{s}/{s}", .{ ctx.data_dir, db, table_name });
    defer a.free(table_dir);
    std.Io.Dir.cwd().deleteTree(ctx.io, table_dir) catch |err| {
        std.debug.print("tcp: DROP TABLE deleteTree warning: {s}\n", .{@errorName(err)});
    };

    try sendEos(w);
}

/// ALTER TABLE [db.]table ADD COLUMN col_name Type
/// ALTER TABLE [db.]table DROP COLUMN col_name
fn handleAlterTable(ctx: *ServerCtx, a: std.mem.Allocator, w: *Io.Writer, sql: []const u8) !void {
    // Tokenize: ALTER TABLE <table> ADD|DROP COLUMN <col> [<type>]
    var it = std.mem.tokenizeAny(u8, sql, " \t\r\n");
    _ = it.next(); // ALTER
    _ = it.next(); // TABLE
    const tbl_tok = it.next() orelse { try sendEos(w); return; };
    const tbl = std.mem.trim(u8, tbl_tok, ";");

    var db: []const u8 = "default";
    var table_name: []const u8 = tbl;
    if (std.mem.indexOfScalar(u8, tbl, '.')) |dot| {
        db = tbl[0..dot];
        table_name = tbl[dot + 1 ..];
    }

    const existing = ctx.schemas.find(db, table_name) orelse {
        try sendEos(w); // unknown table — no-op
        return;
    };

    const action_tok = it.next() orelse { try sendEos(w); return; };

    if (std.ascii.eqlIgnoreCase(action_tok, "ADD")) {
        // Consume optional COLUMN keyword
        const maybe_col = it.next() orelse { try sendEos(w); return; };
        const col_name_raw = if (std.ascii.eqlIgnoreCase(maybe_col, "COLUMN"))
            it.next() orelse { try sendEos(w); return; }
        else maybe_col;
        const col_name = std.mem.trim(u8, col_name_raw, "`\"");
        const type_tok = it.next() orelse "String";
        const ch_type = std.mem.trim(u8, type_tok, " \t;");

        // Map CH type → schema.ColumnType
        const col_ty = ddl_parser.parseColumnTypePublic(ch_type) orelse schema.ColumnType.text;

        // Build updated columns list: existing + new col
        const old_cols = existing.table.columns;
        const new_cols = try a.alloc(schema.Column, old_cols.len + 1);
        defer a.free(new_cols);
        @memcpy(new_cols[0..old_cols.len], old_cols);
        new_cols[old_cols.len] = .{
            .name = col_name,
            .ty   = col_ty,
            .ch_type = ch_type,
        };

        var updated = existing.*;
        updated.table.columns = new_cols;
        try ctx.schemas.addEntry(a, updated);

    } else if (std.ascii.eqlIgnoreCase(action_tok, "DROP")) {
        // Consume optional COLUMN keyword
        const maybe_col = it.next() orelse { try sendEos(w); return; };
        const col_name_raw = if (std.ascii.eqlIgnoreCase(maybe_col, "COLUMN"))
            it.next() orelse { try sendEos(w); return; }
        else maybe_col;
        const col_name = std.mem.trim(u8, col_name_raw, "`\";");

        // Build updated columns list: existing minus dropped col
        const old_cols = existing.table.columns;
        var new_cols = try a.alloc(schema.Column, old_cols.len);
        defer a.free(new_cols);
        var n: usize = 0;
        for (old_cols) |col| {
            if (!std.ascii.eqlIgnoreCase(col.name, col_name)) {
                new_cols[n] = col;
                n += 1;
            }
        }
        new_cols = new_cols[0..n];

        var updated = existing.*;
        updated.table.columns = new_cols;
        try ctx.schemas.addEntry(a, updated);
    }
    // Other ALTER operations (MODIFY, RENAME, etc.) — no-op

    // Persist updated schema
    if (ctx.schemas.find(db, table_name)) |stored| {
        schema_persist.save(ctx.io, a, ctx.data_dir, stored.db, stored) catch |err| {
            std.debug.print("tcp: ALTER TABLE schema_persist.save warning: {s}\n", .{@errorName(err)});
        };
    }

    try sendEos(w);
}

fn handleInsert(
    ctx: *ServerCtx,
    a: std.mem.Allocator,
    w: *Io.Writer,
    sql: []const u8,
    rd: TcpReader,
) !void {
    const target = parseInsertTarget(sql) orelse {
        try sendException(a, w, 1, "Cannot parse INSERT table name");
        return;
    };

    const entry = ctx.schemas.find(target.db, target.table) orelse {
        try sendException(a, w, 60, "Table not found");
        return;
    };

    const schema_block = try buildSchemaBlock(a, entry);
    defer a.free(schema_block);
    try sendData(a, w, schema_block);

    var col_bufs = try row_binary_decoder.ColumnBuffer.initAll(a, entry.table);
    defer row_binary_decoder.ColumnBuffer.deinitAll(a, col_bufs);

    while (true) {
        const pkt = rd.readByte() catch break;
        if (pkt != CLIENT_DATA) break;
        const had_rows = try readClientDataBlock(a, rd, entry, col_bufs);
        if (!had_rows) break;
    }

    const total_rows = if (col_bufs.len > 0) col_bufs[0].rowCount() else 0;
    if (total_rows > 0) {
        const seq = ctx.seq.fetchAdd(1, .monotonic);
        var sess = try part_writer_session.CompactPartWriterSession.open(
            a,
            ctx.io,
            ctx.data_dir,
            target.db,
            target.table,
            entry.table,
            seq,
            0x82,
        );
        defer sess.deinit();
        try sess.writeColumns(col_bufs);
        try sess.finish();
    }

    try sendEos(w);
}

fn parseInsertTarget(sql: []const u8) ?struct { db: []const u8, table: []const u8 } {
    var it = std.mem.tokenizeAny(u8, sql, " \t\r\n");
    const k1 = it.next() orelse return null;
    if (!std.ascii.eqlIgnoreCase(k1, "INSERT")) return null;
    const k2 = it.next() orelse return null;
    if (!std.ascii.eqlIgnoreCase(k2, "INTO")) return null;
    var full = it.next() orelse return null;
    if (std.mem.indexOfScalar(u8, full, '(')) |p| full = full[0..p];
    if (std.mem.indexOfScalar(u8, full, '.')) |dot| {
        return .{ .db = full[0..dot], .table = full[dot + 1..] };
    }
    return .{ .db = "default", .table = full };
}

// ── TCP listener ─────────────────────────────────────────────────────────────

pub fn listenAndServe(ctx: *ServerCtx, port: u16) !void {
    const addr = try net.IpAddress.parseIp4("0.0.0.0", port);
    var listener = try addr.listen(ctx.io, .{ .reuse_address = true });
    defer listener.deinit(ctx.io);
    std.debug.print("tcp: listening on port {d} (ClickHouse Native protocol)\n", .{port});
    while (true) {
        const stream = listener.accept(ctx.io) catch |err| {
            std.debug.print("tcp: accept error: {s}\n", .{@errorName(err)});
            continue;
        };
        // Spawn a thread per connection so accept loop is never blocked.
        const t = std.Thread.spawn(.{}, handleConnection, .{ ctx, stream }) catch |err| {
            std.debug.print("tcp: spawn error: {s}\n", .{@errorName(err)});
            stream.close(ctx.io);
            continue;
        };
        t.detach();
    }
}
