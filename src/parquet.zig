const std = @import("std");

const compact_stop: u8 = 0;
const compact_true: u8 = 1;
const compact_false: u8 = 2;
const compact_byte: u8 = 3;
const compact_i16: u8 = 4;
const compact_i32: u8 = 5;
const compact_i64: u8 = 6;
const compact_double: u8 = 7;
const compact_binary: u8 = 8;
const compact_list: u8 = 9;
const compact_set: u8 = 10;
const compact_map: u8 = 11;
const compact_struct: u8 = 12;

pub const SchemaElement = struct {
    name: []const u8 = "",
    type_: ?i32 = null,
    repetition: ?i32 = null,
    num_children: ?i32 = null,
    converted_type: ?i32 = null,
};

pub const ColumnMeta = struct {
    path: []const []const u8 = &.{},
    type_: ?i32 = null,
    codec: ?i32 = null,
    encodings: []const i32 = &.{},
    num_values: i64 = 0,
    total_uncompressed_size: i64 = 0,
    total_compressed_size: i64 = 0,
    data_page_offset: ?i64 = null,
    dictionary_page_offset: ?i64 = null,
};

pub const RowGroup = struct {
    columns: []const ColumnMeta = &.{},
    total_byte_size: i64 = 0,
    total_compressed_size: ?i64 = null,
    num_rows: i64 = 0,
};

pub const Metadata = struct {
    version: i32 = 0,
    num_rows: i64 = 0,
    created_by: []const u8 = "",
    schema: []const SchemaElement = &.{},
    row_groups: []const RowGroup = &.{},
};

pub fn rowCountPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !u64 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const stat = try file.stat(io);
    if (stat.size < 12) return error.InvalidParquetFile;

    var trailer: [8]u8 = undefined;
    const trailer_n = try file.readPositionalAll(io, &trailer, stat.size - 8);
    if (trailer_n != trailer.len) return error.InvalidParquetFile;
    if (!std.mem.eql(u8, trailer[4..8], "PAR1")) return error.InvalidParquetFile;
    const footer_len = std.mem.readInt(u32, trailer[0..4], .little);
    if (@as(u64, footer_len) + 8 > stat.size) return error.InvalidParquetFile;

    const footer = try arena.allocator().alloc(u8, footer_len);
    const footer_offset = stat.size - 8 - footer_len;
    const footer_n = try file.readPositionalAll(io, footer, footer_offset);
    if (footer_n != footer.len) return error.InvalidParquetFile;

    var parser = CompactParser{ .allocator = arena.allocator(), .buf = footer };
    const meta = try parser.readFileMetaData();
    if (meta.num_rows < 0) return error.InvalidParquetFile;
    return @intCast(meta.num_rows);
}

pub fn inspectPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const stat = try file.stat(io);
    if (stat.size < 12) return error.InvalidParquetFile;

    var trailer: [8]u8 = undefined;
    const trailer_n = try file.readPositionalAll(io, &trailer, stat.size - 8);
    if (trailer_n != trailer.len) return error.InvalidParquetFile;
    if (!std.mem.eql(u8, trailer[4..8], "PAR1")) return error.InvalidParquetFile;
    const footer_len = std.mem.readInt(u32, trailer[0..4], .little);
    if (@as(u64, footer_len) + 8 > stat.size) return error.InvalidParquetFile;

    const footer = try allocator.alloc(u8, footer_len);
    defer allocator.free(footer);
    const footer_offset = stat.size - 8 - footer_len;
    const footer_n = try file.readPositionalAll(io, footer, footer_offset);
    if (footer_n != footer.len) return error.InvalidParquetFile;

    var parser = CompactParser{ .allocator = allocator, .buf = footer };
    const meta = try parser.readFileMetaData();

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "file={s}\n", .{path});
    try out.print(allocator, "file_size={d}\n", .{stat.size});
    try out.print(allocator, "footer_len={d}\n", .{footer_len});
    try out.print(allocator, "version={d}\n", .{meta.version});
    try out.print(allocator, "num_rows={d}\n", .{meta.num_rows});
    try out.print(allocator, "row_groups={d}\n", .{meta.row_groups.len});
    if (meta.created_by.len > 0) try out.print(allocator, "created_by={s}\n", .{meta.created_by});
    try out.appendSlice(allocator, "schema:\n");
    for (meta.schema, 0..) |el, idx| {
        try out.print(allocator, "  {d}: name={s}", .{ idx, el.name });
        if (el.type_) |v| try out.print(allocator, " type={s}", .{physicalTypeName(v)});
        if (el.repetition) |v| try out.print(allocator, " repetition={s}", .{repetitionName(v)});
        if (el.num_children) |v| try out.print(allocator, " children={d}", .{v});
        if (el.converted_type) |v| try out.print(allocator, " converted={s}", .{convertedTypeName(v)});
        try out.append(allocator, '\n');
    }
    try out.appendSlice(allocator, "row_group_summary:\n");
    for (meta.row_groups, 0..) |rg, idx| {
        try out.print(allocator, "  {d}: rows={d} columns={d} uncompressed={d}", .{ idx, rg.num_rows, rg.columns.len, rg.total_byte_size });
        if (rg.total_compressed_size) |v| try out.print(allocator, " compressed={d}", .{v});
        try out.append(allocator, '\n');
    }
    if (meta.row_groups.len > 0) {
        try out.appendSlice(allocator, "columns_first_row_group:\n");
        for (meta.row_groups[0].columns, 0..) |col, idx| {
            try out.print(allocator, "  {d}: path=", .{idx});
            try appendPath(allocator, &out, col.path);
            try out.print(allocator, " type={s} codec={s} values={d} compressed={d} uncompressed={d}", .{
                physicalTypeName(col.type_ orelse -1),
                codecName(col.codec orelse -1),
                col.num_values,
                col.total_compressed_size,
                col.total_uncompressed_size,
            });
            if (col.data_page_offset) |v| try out.print(allocator, " data_page_offset={d}", .{v});
            if (col.dictionary_page_offset) |v| try out.print(allocator, " dict_page_offset={d}", .{v});
            if (col.encodings.len > 0) {
                try out.appendSlice(allocator, " encodings=");
                for (col.encodings, 0..) |enc, j| {
                    if (j != 0) try out.append(allocator, '|');
                    try out.appendSlice(allocator, encodingName(enc));
                }
            }
            try out.append(allocator, '\n');
        }
    }
    return out.toOwnedSlice(allocator);
}

fn appendPath(allocator: std.mem.Allocator, out: *std.ArrayList(u8), path: []const []const u8) !void {
    for (path, 0..) |part, idx| {
        if (idx != 0) try out.append(allocator, '.');
        try out.appendSlice(allocator, part);
    }
}

const Field = struct { id: i16, type_: u8 };

const CompactParser = struct {
    allocator: std.mem.Allocator,
    buf: []const u8,
    pos: usize = 0,

    fn readFileMetaData(self: *CompactParser) !Metadata {
        var meta = Metadata{};
        var last: i16 = 0;
        while (try self.readField(&last)) |field| switch (field.id) {
            1 => meta.version = try self.readI32(),
            2 => meta.schema = try self.readSchemaList(field.type_),
            3 => meta.num_rows = try self.readI64(),
            4 => meta.row_groups = try self.readRowGroupList(field.type_),
            6 => meta.created_by = try self.readBinary(),
            else => try self.skip(field.type_),
        };
        return meta;
    }

    fn readSchemaElement(self: *CompactParser) !SchemaElement {
        var el = SchemaElement{};
        var last: i16 = 0;
        while (try self.readField(&last)) |field| switch (field.id) {
            1 => el.type_ = try self.readI32(),
            3 => el.repetition = try self.readI32(),
            4 => el.name = try self.readBinary(),
            5 => el.num_children = try self.readI32(),
            6 => el.converted_type = try self.readI32(),
            else => try self.skip(field.type_),
        };
        return el;
    }

    fn readRowGroup(self: *CompactParser) !RowGroup {
        var rg = RowGroup{};
        var last: i16 = 0;
        while (try self.readField(&last)) |field| switch (field.id) {
            1 => rg.columns = try self.readColumnChunkList(field.type_),
            2 => rg.total_byte_size = try self.readI64(),
            3 => rg.num_rows = try self.readI64(),
            6 => rg.total_compressed_size = try self.readI64(),
            else => try self.skip(field.type_),
        };
        return rg;
    }

    fn readColumnChunk(self: *CompactParser) !ColumnMeta {
        var col = ColumnMeta{};
        var last: i16 = 0;
        while (try self.readField(&last)) |field| switch (field.id) {
            3 => col = try self.readColumnMetaData(),
            else => try self.skip(field.type_),
        };
        return col;
    }

    fn readColumnMetaData(self: *CompactParser) !ColumnMeta {
        var col = ColumnMeta{};
        var last: i16 = 0;
        while (try self.readField(&last)) |field| switch (field.id) {
            1 => col.type_ = try self.readI32(),
            2 => col.encodings = try self.readI32List(field.type_),
            3 => col.path = try self.readStringList(field.type_),
            4 => col.codec = try self.readI32(),
            5 => col.num_values = try self.readI64(),
            6 => col.total_uncompressed_size = try self.readI64(),
            7 => col.total_compressed_size = try self.readI64(),
            9 => col.data_page_offset = try self.readI64(),
            11 => col.dictionary_page_offset = try self.readI64(),
            else => try self.skip(field.type_),
        };
        return col;
    }

    fn readSchemaList(self: *CompactParser, field_type: u8) ![]const SchemaElement {
        const h = try self.readListHeader(field_type);
        if (h.elem_type != compact_struct) return error.InvalidParquetMetadata;
        const items = try self.allocator.alloc(SchemaElement, h.len);
        errdefer self.allocator.free(items);
        for (items) |*item| item.* = try self.readSchemaElement();
        return items;
    }

    fn readRowGroupList(self: *CompactParser, field_type: u8) ![]const RowGroup {
        const h = try self.readListHeader(field_type);
        if (h.elem_type != compact_struct) return error.InvalidParquetMetadata;
        const items = try self.allocator.alloc(RowGroup, h.len);
        errdefer self.allocator.free(items);
        for (items) |*item| item.* = try self.readRowGroup();
        return items;
    }

    fn readColumnChunkList(self: *CompactParser, field_type: u8) ![]const ColumnMeta {
        const h = try self.readListHeader(field_type);
        if (h.elem_type != compact_struct) return error.InvalidParquetMetadata;
        const items = try self.allocator.alloc(ColumnMeta, h.len);
        errdefer self.allocator.free(items);
        for (items) |*item| item.* = try self.readColumnChunk();
        return items;
    }

    fn readI32List(self: *CompactParser, field_type: u8) ![]const i32 {
        const h = try self.readListHeader(field_type);
        if (h.elem_type != compact_i32) return error.InvalidParquetMetadata;
        const items = try self.allocator.alloc(i32, h.len);
        errdefer self.allocator.free(items);
        for (items) |*item| item.* = try self.readI32();
        return items;
    }

    fn readStringList(self: *CompactParser, field_type: u8) ![]const []const u8 {
        const h = try self.readListHeader(field_type);
        if (h.elem_type != compact_binary) return error.InvalidParquetMetadata;
        const items = try self.allocator.alloc([]const u8, h.len);
        errdefer self.allocator.free(items);
        for (items) |*item| item.* = try self.readBinary();
        return items;
    }

    fn readField(self: *CompactParser, last: *i16) !?Field {
        const b = try self.readByte();
        const type_: u8 = b & 0x0f;
        if (type_ == compact_stop) return null;
        const delta: u8 = b >> 4;
        const id: i16 = if (delta == 0) try self.readI16() else last.* + @as(i16, @intCast(delta));
        last.* = id;
        return .{ .id = id, .type_ = type_ };
    }

    fn readListHeader(self: *CompactParser, field_type: u8) !struct { len: usize, elem_type: u8 } {
        if (field_type != compact_list and field_type != compact_set) return error.InvalidParquetMetadata;
        const b = try self.readByte();
        const elem_type: u8 = b & 0x0f;
        const small_len: u8 = b >> 4;
        const len: usize = if (small_len == 15) @intCast(try self.readVarUInt()) else small_len;
        return .{ .len = len, .elem_type = elem_type };
    }

    fn skip(self: *CompactParser, type_: u8) !void {
        switch (type_) {
            compact_stop, compact_true, compact_false => {},
            compact_byte => _ = try self.readByte(),
            compact_i16 => _ = try self.readI16(),
            compact_i32 => _ = try self.readI32(),
            compact_i64 => _ = try self.readI64(),
            compact_double => {
                try self.require(8);
                self.pos += 8;
            },
            compact_binary => _ = try self.readBinary(),
            compact_struct => {
                var last: i16 = 0;
                while (try self.readField(&last)) |field| try self.skip(field.type_);
            },
            compact_list, compact_set => {
                const h = try self.readListHeader(type_);
                for (0..h.len) |_| try self.skip(h.elem_type);
            },
            compact_map => {
                const len = try self.readVarUInt();
                if (len == 0) return;
                const types = try self.readByte();
                const key_type = types >> 4;
                const value_type = types & 0x0f;
                for (0..@as(usize, @intCast(len))) |_| {
                    try self.skip(key_type);
                    try self.skip(value_type);
                }
            },
            else => return error.InvalidParquetMetadata,
        }
    }

    fn readBinary(self: *CompactParser) ![]const u8 {
        const len: usize = @intCast(try self.readVarUInt());
        try self.require(len);
        const s = self.buf[self.pos .. self.pos + len];
        self.pos += len;
        return s;
    }

    fn readI16(self: *CompactParser) !i16 {
        return @intCast(zigZagDecode(try self.readVarUInt()));
    }

    fn readI32(self: *CompactParser) !i32 {
        return @intCast(zigZagDecode(try self.readVarUInt()));
    }

    fn readI64(self: *CompactParser) !i64 {
        return zigZagDecode(try self.readVarUInt());
    }

    fn readVarUInt(self: *CompactParser) !u64 {
        var shift: u6 = 0;
        var result: u64 = 0;
        while (true) {
            const b = try self.readByte();
            result |= @as(u64, b & 0x7f) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift >= 63) return error.InvalidParquetMetadata;
        }
    }

    fn readByte(self: *CompactParser) !u8 {
        try self.require(1);
        const b = self.buf[self.pos];
        self.pos += 1;
        return b;
    }

    fn require(self: *CompactParser, n: usize) !void {
        if (self.pos + n > self.buf.len) return error.InvalidParquetMetadata;
    }
};

fn zigZagDecode(n: u64) i64 {
    const half: i64 = @intCast(n >> 1);
    return if ((n & 1) == 0) half else -half - 1;
}

fn physicalTypeName(v: i32) []const u8 {
    return switch (v) {
        0 => "BOOLEAN",
        1 => "INT32",
        2 => "INT64",
        3 => "INT96",
        4 => "FLOAT",
        5 => "DOUBLE",
        6 => "BYTE_ARRAY",
        7 => "FIXED_LEN_BYTE_ARRAY",
        else => "UNKNOWN",
    };
}

fn repetitionName(v: i32) []const u8 {
    return switch (v) {
        0 => "REQUIRED",
        1 => "OPTIONAL",
        2 => "REPEATED",
        else => "UNKNOWN",
    };
}

fn convertedTypeName(v: i32) []const u8 {
    return switch (v) {
        0 => "UTF8",
        6 => "DATE",
        9 => "TIMESTAMP_MILLIS",
        10 => "TIMESTAMP_MICROS",
        11 => "UINT_8",
        12 => "UINT_16",
        13 => "UINT_32",
        14 => "UINT_64",
        15 => "INT_8",
        16 => "INT_16",
        17 => "INT_32",
        18 => "INT_64",
        else => "OTHER",
    };
}

fn codecName(v: i32) []const u8 {
    return switch (v) {
        0 => "UNCOMPRESSED",
        1 => "SNAPPY",
        2 => "GZIP",
        3 => "LZO",
        4 => "BROTLI",
        5 => "LZ4",
        6 => "ZSTD",
        7 => "LZ4_RAW",
        else => "UNKNOWN",
    };
}

fn encodingName(v: i32) []const u8 {
    return switch (v) {
        0 => "PLAIN",
        2 => "PLAIN_DICTIONARY",
        3 => "RLE",
        4 => "BIT_PACKED",
        5 => "DELTA_BINARY_PACKED",
        6 => "DELTA_LENGTH_BYTE_ARRAY",
        7 => "DELTA_BYTE_ARRAY",
        8 => "RLE_DICTIONARY",
        9 => "BYTE_STREAM_SPLIT",
        else => "UNKNOWN",
    };
}

test "zigzag decode" {
    try std.testing.expectEqual(@as(i64, 0), zigZagDecode(0));
    try std.testing.expectEqual(@as(i64, -1), zigZagDecode(1));
    try std.testing.expectEqual(@as(i64, 1), zigZagDecode(2));
    try std.testing.expectEqual(@as(i64, -2), zigZagDecode(3));
}
