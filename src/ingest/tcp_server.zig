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
    if (col.ch_type) |ct| return ct;
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

const WireKind = enum { string, fixed1, fixed2, fixed4, fixed8 };

fn wireKind(ch_type: []const u8) WireKind {
    if (std.ascii.eqlIgnoreCase(ch_type, "Int8") or std.ascii.eqlIgnoreCase(ch_type, "UInt8")) return .fixed1;
    if (std.ascii.eqlIgnoreCase(ch_type, "Int16") or std.ascii.eqlIgnoreCase(ch_type, "UInt16")) return .fixed2;
    if (std.ascii.eqlIgnoreCase(ch_type, "Int32") or std.ascii.eqlIgnoreCase(ch_type, "UInt32") or
        std.ascii.eqlIgnoreCase(ch_type, "Float32") or std.ascii.eqlIgnoreCase(ch_type, "Date")) return .fixed4;
    if (std.ascii.eqlIgnoreCase(ch_type, "Int64") or std.ascii.eqlIgnoreCase(ch_type, "UInt64") or
        std.ascii.eqlIgnoreCase(ch_type, "Float64")) return .fixed8;
    if (std.ascii.startsWithIgnoreCase(ch_type, "DateTime64")) return .fixed8;
    if (std.ascii.startsWithIgnoreCase(ch_type, "DateTime")) return .fixed4;
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
                    const v = try rd.readInt(i32, .little);
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
        try handleSelect(a, w, s);
    } else {
        try sendEos(w);
    }
}

fn handleSelect(a: std.mem.Allocator, w: *Io.Writer, sql: []const u8) !void {
    var block_buf: std.ArrayListUnmanaged(u8) = .empty;
    defer block_buf.deinit(a);

    if (std.ascii.indexOfIgnoreCase(sql, "system.tables") != null) {
        try writeBlockInfo(&block_buf, a);
        try wuv(&block_buf, a, 1); try wuv(&block_buf, a, 0);
        try wstr(&block_buf, a, "engine_full"); try wstr(&block_buf, a, "String");
        try block_buf.append(a, 0);
    } else if (std.ascii.indexOfIgnoreCase(sql, "system.columns") != null) {
        try writeBlockInfo(&block_buf, a);
        try wuv(&block_buf, a, 2); try wuv(&block_buf, a, 0);
        try wstr(&block_buf, a, "name"); try wstr(&block_buf, a, "String"); try block_buf.append(a, 0);
        try wstr(&block_buf, a, "type"); try wstr(&block_buf, a, "String"); try block_buf.append(a, 0);
    } else {
        try writeBlockInfo(&block_buf, a);
        try wuv(&block_buf, a, 0); try wuv(&block_buf, a, 0);
    }

    try sendData(a, w, block_buf.items);
    try sendProfileInfo(a, w);
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
        handleConnection(ctx, stream);
    }
}
