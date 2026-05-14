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

pub const DataPageHeader = struct {
    num_values: i32 = 0,
    encoding: ?i32 = null,
    definition_level_encoding: ?i32 = null,
    repetition_level_encoding: ?i32 = null,
};

pub const DictionaryPageHeader = struct {
    num_values: i32 = 0,
    encoding: ?i32 = null,
    is_sorted: ?bool = null,
};

pub const PageHeader = struct {
    type_: i32 = -1,
    uncompressed_page_size: i32 = 0,
    compressed_page_size: i32 = 0,
    data_page: ?DataPageHeader = null,
    dictionary_page: ?DictionaryPageHeader = null,
};

pub const FixedTarget = enum { i16, i32, i64 };

pub const FixedColumnBatch = struct {
    target: FixedTarget,
    bytes: []const u8,
};

pub const SelectedValue = union(enum) {
    int32: i32,
    int64: i64,
    bytes: []const u8,
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

pub fn inspectPageHeadersPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, row_group_index: usize, column_index: usize, max_pages: usize) ![]u8 {
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
    if (row_group_index >= meta.row_groups.len) return error.InvalidRowGroup;
    const rg = meta.row_groups[row_group_index];
    if (column_index >= rg.columns.len) return error.InvalidColumn;
    const col = rg.columns[column_index];
    const start_i64 = col.dictionary_page_offset orelse col.data_page_offset orelse return error.InvalidParquetMetadata;
    if (start_i64 < 0 or col.total_compressed_size < 0) return error.InvalidParquetMetadata;
    var offset: u64 = @intCast(start_i64);
    const end = @min(stat.size, offset + @as(u64, @intCast(col.total_compressed_size)));

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "file={s}\n", .{path});
    try out.print(allocator, "row_group={d} column={d} path=", .{ row_group_index, column_index });
    try appendPath(allocator, &out, col.path);
    try out.append(allocator, '\n');
    try out.print(allocator, "chunk_start={d} chunk_end={d} compressed={d} codec={s}\n", .{ offset, end, col.total_compressed_size, codecName(col.codec orelse -1) });

    const header_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(header_buf);
    var page_index: usize = 0;
    while (offset < end and page_index < max_pages) : (page_index += 1) {
        const remaining = end - offset;
        const read_len: usize = @intCast(@min(@as(u64, header_buf.len), remaining));
        const n = try file.readPositionalAll(io, header_buf[0..read_len], offset);
        if (n == 0) return error.InvalidParquetFile;
        var page_parser = CompactParser{ .allocator = allocator, .buf = header_buf[0..n] };
        const header = try page_parser.readPageHeader();
        const header_len = page_parser.pos;
        if (header.compressed_page_size < 0 or header.uncompressed_page_size < 0) return error.InvalidParquetMetadata;
        const compressed_size: u64 = @intCast(header.compressed_page_size);
        try out.print(allocator, "page {d}: offset={d} header={d} type={s} compressed={d} uncompressed={d}", .{
            page_index,
            offset,
            header_len,
            pageTypeName(header.type_),
            header.compressed_page_size,
            header.uncompressed_page_size,
        });
        if (header.dictionary_page) |dict| {
            try out.print(allocator, " values={d}", .{dict.num_values});
            if (dict.encoding) |enc| try out.print(allocator, " encoding={s}", .{encodingName(enc)});
            if (dict.is_sorted) |is_sorted| try out.print(allocator, " sorted={}", .{is_sorted});
        }
        if (header.data_page) |data| {
            try out.print(allocator, " values={d}", .{data.num_values});
            if (data.encoding) |enc| try out.print(allocator, " encoding={s}", .{encodingName(enc)});
            if (data.definition_level_encoding) |enc| try out.print(allocator, " def={s}", .{encodingName(enc)});
            if (data.repetition_level_encoding) |enc| try out.print(allocator, " rep={s}", .{encodingName(enc)});
        }
        try out.append(allocator, '\n');

        const next = offset + @as(u64, @intCast(header_len)) + compressed_size;
        if (next <= offset or next > end) return error.InvalidParquetMetadata;
        offset = next;
    }
    try out.print(allocator, "pages_printed={d} next_offset={d}\n", .{ page_index, offset });
    return out.toOwnedSlice(allocator);
}

pub fn decodeFixedDictionaryPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, row_group_index: usize, column_index: usize, limit_values: usize) ![]u8 {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const stat = try file.stat(io);
    var loaded = try readMetadataFromFile(allocator, io, &file, stat.size);
    defer loaded.deinit();
    const meta = loaded.meta;
    if (row_group_index >= meta.row_groups.len) return error.InvalidRowGroup;
    const rg = meta.row_groups[row_group_index];
    if (column_index >= rg.columns.len) return error.InvalidColumn;
    const col = rg.columns[column_index];
    const type_ = col.type_ orelse return error.InvalidParquetMetadata;
    const width: usize = switch (type_) {
        1 => 4,
        2 => 8,
        else => return error.UnsupportedParquetType,
    };
    if (col.codec != null and col.codec.? != 1 and col.codec.? != 0) return error.UnsupportedParquetCodec;
    const start_i64 = col.dictionary_page_offset orelse col.data_page_offset orelse return error.InvalidParquetMetadata;
    if (start_i64 < 0 or col.total_compressed_size < 0) return error.InvalidParquetMetadata;
    var offset: u64 = @intCast(start_i64);
    const end = @min(stat.size, offset + @as(u64, @intCast(col.total_compressed_size)));

    var dict: []i64 = &.{};
    defer if (dict.len != 0) allocator.free(dict);

    const wanted = @min(limit_values, @as(usize, @intCast(@max(rg.num_rows, 0))));
    var values = try allocator.alloc(i64, wanted);
    defer allocator.free(values);
    var decoded: usize = 0;

    const header_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(header_buf);
    var page_index: usize = 0;
    while (offset < end and decoded < wanted) : (page_index += 1) {
        const page = try readPage(allocator, io, &file, offset, end, col.codec orelse 0, header_buf);
        defer allocator.free(page.payload);
        offset = page.next_offset;

        if (page.header.dictionary_page) |dict_header| {
            if (dict.len != 0) return error.InvalidParquetMetadata;
            dict = try decodePlainFixedDict(allocator, page.payload, dict_header.num_values, width);
            continue;
        }
        if (page.header.data_page) |data_header| {
            if (dict.len == 0) return error.InvalidParquetMetadata;
            const page_values = @min(@as(usize, @intCast(@max(data_header.num_values, 0))), wanted - decoded);
            const out = values[decoded .. decoded + page_values];
            try decodeDictionaryIdsToFixed(page.payload, dict, out);
            decoded += page_values;
        }
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "file={s}\n", .{path});
    try out.print(allocator, "row_group={d} column={d} path=", .{ row_group_index, column_index });
    try appendPath(allocator, &out, col.path);
    try out.append(allocator, '\n');
    try out.print(allocator, "type={s} codec={s} pages_seen={d} dict_values={d} decoded={d}\n", .{ physicalTypeName(type_), codecName(col.codec orelse -1), page_index, dict.len, decoded });
    if (decoded > 0) {
        var min = values[0];
        var max = values[0];
        var sum: i128 = 0;
        for (values[0..decoded]) |v| {
            min = @min(min, v);
            max = @max(max, v);
            sum += v;
        }
        try out.print(allocator, "min={d} max={d} sum={d}\n", .{ min, max, sum });
        try out.appendSlice(allocator, "first_values=");
        for (values[0..@min(decoded, 16)], 0..) |v, i| {
            if (i != 0) try out.append(allocator, ',');
            try out.print(allocator, "{d}", .{v});
        }
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

pub fn scanFixedColumnPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, column_index: usize, limit_rows: ?usize) ![]u8 {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const started = wallNowNs();
    const stat = try file.stat(io);
    var loaded = try readMetadataFromFile(allocator, io, &file, stat.size);
    defer loaded.deinit();
    const meta = loaded.meta;
    if (meta.row_groups.len == 0) return error.InvalidParquetMetadata;
    if (column_index >= meta.row_groups[0].columns.len) return error.InvalidColumn;
    const first_col = meta.row_groups[0].columns[column_index];
    const type_ = first_col.type_ orelse return error.InvalidParquetMetadata;
    const width: usize = switch (type_) {
        1 => 4,
        2 => 8,
        else => return error.UnsupportedParquetType,
    };

    const row_limit = limit_rows orelse std.math.maxInt(usize);
    var remaining = row_limit;
    var count: usize = 0;
    var groups_scanned: usize = 0;
    var min: i64 = 0;
    var max: i64 = 0;
    var sum: i128 = 0;
    var first_seen = false;
    var first_values: [16]i64 = undefined;
    var first_count: usize = 0;
    const chunk_values = try allocator.alloc(i64, 64 * 1024);
    defer allocator.free(chunk_values);
    const header_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(header_buf);

    for (meta.row_groups, 0..) |rg, rg_index| {
        if (remaining == 0) break;
        if (column_index >= rg.columns.len) return error.InvalidColumn;
        const col = rg.columns[column_index];
        if ((col.type_ orelse -1) != type_) return error.InvalidParquetMetadata;
        const rows_in_group: usize = @intCast(@max(rg.num_rows, 0));
        const want = @min(rows_in_group, remaining);
        var iter = try FixedDictColumnIterator.init(allocator, io, &file, stat.size, col, width, header_buf);
        defer iter.deinit();
        var group_count: usize = 0;
        while (group_count < want) {
            const batch = @min(chunk_values.len, want - group_count);
            const got = try iter.next(chunk_values[0..batch]);
            if (got == 0) return error.InvalidParquetMetadata;
            for (chunk_values[0..got]) |v| {
                if (!first_seen) {
                    min = v;
                    max = v;
                    first_seen = true;
                } else {
                    min = @min(min, v);
                    max = @max(max, v);
                }
                sum += v;
                if (first_count < first_values.len) {
                    first_values[first_count] = v;
                    first_count += 1;
                }
            }
            group_count += got;
            count += got;
            remaining -= got;
        }
        groups_scanned = rg_index + 1;
    }

    const ended = wallNowNs();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "file={s}\n", .{path});
    try out.print(allocator, "column={d} path=", .{column_index});
    try appendPath(allocator, &out, first_col.path);
    try out.append(allocator, '\n');
    try out.print(allocator, "type={s} row_groups_scanned={d} count={d} seconds={d:.6}\n", .{ physicalTypeName(type_), groups_scanned, count, @as(f64, @floatFromInt(ended - started)) / std.time.ns_per_s });
    if (first_seen) {
        try out.print(allocator, "min={d} max={d} sum={d}\n", .{ min, max, sum });
        try out.appendSlice(allocator, "first_values=");
        for (first_values[0..first_count], 0..) |v, i| {
            if (i != 0) try out.append(allocator, ',');
            try out.print(allocator, "{d}", .{v});
        }
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

pub fn decodeByteArrayPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, row_group_index: usize, column_index: usize, limit_values: usize) ![]u8 {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const stat = try file.stat(io);
    var loaded = try readMetadataFromFile(allocator, io, &file, stat.size);
    defer loaded.deinit();
    const meta = loaded.meta;
    if (row_group_index >= meta.row_groups.len) return error.InvalidRowGroup;
    const rg = meta.row_groups[row_group_index];
    if (column_index >= rg.columns.len) return error.InvalidColumn;
    const col = rg.columns[column_index];
    const type_ = col.type_ orelse return error.InvalidParquetMetadata;
    if (type_ != 6) return error.UnsupportedParquetType;
    if (col.codec != null and col.codec.? != 1 and col.codec.? != 0) return error.UnsupportedParquetCodec;
    const start_i64 = col.dictionary_page_offset orelse col.data_page_offset orelse return error.InvalidParquetMetadata;
    if (start_i64 < 0 or col.total_compressed_size < 0) return error.InvalidParquetMetadata;
    var offset: u64 = @intCast(start_i64);
    const end = @min(stat.size, offset + @as(u64, @intCast(col.total_compressed_size)));

    var dict: StringDict = .{};
    defer dict.deinit(allocator);
    var samples: std.ArrayList([]const u8) = .empty;
    defer samples.deinit(allocator);
    var decoded: usize = 0;
    const wanted = @min(limit_values, @as(usize, @intCast(@max(rg.num_rows, 0))));

    const header_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(header_buf);
    var page_index: usize = 0;
    while (offset < end and decoded < wanted) : (page_index += 1) {
        const page = try readPage(allocator, io, &file, offset, end, col.codec orelse 0, header_buf);
        defer allocator.free(page.payload);
        offset = page.next_offset;
        if (page.header.dictionary_page) |dict_header| {
            if (dict.offsets.items.len != 0) return error.InvalidParquetMetadata;
            try dict.decodePlain(allocator, page.payload, dict_header.num_values);
            continue;
        }
        if (page.header.data_page) |data_header| {
            if (data_header.num_values < 0) return error.InvalidParquetMetadata;
            const page_values = @min(@as(usize, @intCast(data_header.num_values)), wanted - decoded);
            const encoding = data_header.encoding orelse return error.InvalidParquetMetadata;
            if (encoding == 0) {
                var pos: usize = 0;
                for (0..page_values) |_| {
                    const s = try readPlainByteArray(page.payload, &pos);
                    try samples.append(allocator, s);
                    decoded += 1;
                }
            } else {
                if (page.payload.len == 0) return error.InvalidParquetMetadata;
                const bit_width = page.payload[0];
                if (bit_width > 32) return error.InvalidParquetMetadata;
                var decoder = RleBitPackedDecoder{ .buf = page.payload[1..], .bit_width = bit_width };
                for (0..page_values) |_| {
                    const id = try decoder.next();
                    try samples.append(allocator, try dict.value(id));
                    decoded += 1;
                }
            }
        }
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "file={s}\n", .{path});
    try out.print(allocator, "row_group={d} column={d} path=", .{ row_group_index, column_index });
    try appendPath(allocator, &out, col.path);
    try out.append(allocator, '\n');
    try out.print(allocator, "type={s} codec={s} pages_seen={d} dict_values={d} decoded={d}\n", .{ physicalTypeName(type_), codecName(col.codec orelse -1), page_index, dict.len(), decoded });
    for (samples.items[0..@min(samples.items.len, 16)], 0..) |s, i| {
        try out.print(allocator, "value {d}: ", .{i});
        try appendEscaped(allocator, &out, s);
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

pub fn scanByteArrayPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, column_index: usize, limit_rows: ?usize) ![]u8 {
    var stats = ByteArrayStats{};
    const started = wallNowNs();
    _ = try streamByteArrayColumnPath(allocator, io, path, column_index, limit_rows, &stats, ByteArrayStats.observe);
    const ended = wallNowNs();

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "file={s}\n", .{path});
    try out.print(allocator, "column={d}\n", .{column_index});
    try out.print(allocator, "count={d} empty={d} total_bytes={d} max_len={d} hash={d} seconds={d:.6}\n", .{
        stats.count,
        stats.empty,
        stats.total_bytes,
        stats.max_len,
        stats.hash,
        @as(f64, @floatFromInt(ended - started)) / std.time.ns_per_s,
    });
    return out.toOwnedSlice(allocator);
}

pub fn streamByteArrayColumnPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, column_index: usize, limit_rows: ?usize, context: anytype, callback: anytype) !usize {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const stat = try file.stat(io);
    var loaded = try readMetadataFromFile(allocator, io, &file, stat.size);
    defer loaded.deinit();
    const meta = loaded.meta;
    if (meta.row_groups.len == 0) return error.InvalidParquetMetadata;
    if (column_index >= meta.row_groups[0].columns.len) return error.InvalidColumn;
    const first_col = meta.row_groups[0].columns[column_index];
    if ((first_col.type_ orelse -1) != 6) return error.UnsupportedParquetType;

    const header_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(header_buf);
    const row_limit = limit_rows orelse std.math.maxInt(usize);
    var remaining = row_limit;
    var count: usize = 0;
    for (meta.row_groups) |rg| {
        if (remaining == 0) break;
        if (column_index >= rg.columns.len) return error.InvalidColumn;
        const col = rg.columns[column_index];
        if ((col.type_ orelse -1) != 6) return error.InvalidParquetMetadata;
        const rows_in_group: usize = @intCast(@max(rg.num_rows, 0));
        const want = @min(rows_in_group, remaining);
        var iter = try ByteArrayColumnIterator.init(allocator, io, &file, stat.size, col, header_buf);
        defer iter.deinit();
        var group_count: usize = 0;
        while (group_count < want) {
            const s = try iter.next() orelse return error.InvalidParquetMetadata;
            try callback(context, s);
            group_count += 1;
            count += 1;
            remaining -= 1;
        }
    }
    return count;
}

pub fn streamFixedColumnPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, column_index: usize, limit_rows: ?usize, context: anytype, callback: anytype) !usize {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const stat = try file.stat(io);
    var loaded = try readMetadataFromFile(allocator, io, &file, stat.size);
    defer loaded.deinit();
    const meta = loaded.meta;
    if (meta.row_groups.len == 0) return error.InvalidParquetMetadata;
    if (column_index >= meta.row_groups[0].columns.len) return error.InvalidColumn;
    const first_col = meta.row_groups[0].columns[column_index];
    const type_ = first_col.type_ orelse return error.InvalidParquetMetadata;
    const width: usize = switch (type_) {
        1 => 4,
        2 => 8,
        else => return error.UnsupportedParquetType,
    };

    const row_limit = limit_rows orelse std.math.maxInt(usize);
    var remaining = row_limit;
    var count: usize = 0;
    const chunk_values = try allocator.alloc(i64, 64 * 1024);
    defer allocator.free(chunk_values);
    const header_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(header_buf);

    for (meta.row_groups) |rg| {
        if (remaining == 0) break;
        if (column_index >= rg.columns.len) return error.InvalidColumn;
        const col = rg.columns[column_index];
        if ((col.type_ orelse -1) != type_) return error.InvalidParquetMetadata;
        const rows_in_group: usize = @intCast(@max(rg.num_rows, 0));
        const want = @min(rows_in_group, remaining);
        var iter = try FixedDictColumnIterator.init(allocator, io, &file, stat.size, col, width, header_buf);
        defer iter.deinit();
        var group_count: usize = 0;
        while (group_count < want) {
            const batch = @min(chunk_values.len, want - group_count);
            const got = try iter.next(chunk_values[0..batch]);
            if (got == 0) return error.InvalidParquetMetadata;
            try callback(context, chunk_values[0..got]);
            group_count += got;
            count += got;
            remaining -= got;
        }
    }
    return count;
}

pub fn streamFixedColumnsPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, column_indices: []const usize, limit_rows: ?usize, context: anytype, callback: anytype) !usize {
    if (column_indices.len == 0) return 0;
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const stat = try file.stat(io);
    var loaded = try readMetadataFromFile(allocator, io, &file, stat.size);
    defer loaded.deinit();
    const meta = loaded.meta;
    if (meta.row_groups.len == 0) return error.InvalidParquetMetadata;

    const types = try allocator.alloc(i32, column_indices.len);
    defer allocator.free(types);
    const widths = try allocator.alloc(usize, column_indices.len);
    defer allocator.free(widths);
    for (column_indices, 0..) |column_index, i| {
        if (column_index >= meta.row_groups[0].columns.len) return error.InvalidColumn;
        const col = meta.row_groups[0].columns[column_index];
        const type_ = col.type_ orelse return error.InvalidParquetMetadata;
        types[i] = type_;
        widths[i] = switch (type_) {
            1 => 4,
            2 => 8,
            else => return error.UnsupportedParquetType,
        };
    }

    const batch_capacity: usize = 64 * 1024;
    const flat_values = try allocator.alloc(i64, column_indices.len * batch_capacity);
    defer allocator.free(flat_values);
    const batches = try allocator.alloc([]const i64, column_indices.len);
    defer allocator.free(batches);
    const header_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(header_buf);
    const iterators = try allocator.alloc(FixedDictColumnIterator, column_indices.len);
    defer allocator.free(iterators);

    const row_limit = limit_rows orelse std.math.maxInt(usize);
    var remaining = row_limit;
    var count: usize = 0;
    for (meta.row_groups) |rg| {
        if (remaining == 0) break;
        const rows_in_group: usize = @intCast(@max(rg.num_rows, 0));
        const want = @min(rows_in_group, remaining);
        var initialized: usize = 0;
        errdefer for (iterators[0..initialized]) |*it| it.deinit();
        for (column_indices, 0..) |column_index, i| {
            if (column_index >= rg.columns.len) return error.InvalidColumn;
            const col = rg.columns[column_index];
            if ((col.type_ orelse -1) != types[i]) return error.InvalidParquetMetadata;
            iterators[i] = try FixedDictColumnIterator.init(allocator, io, &file, stat.size, col, widths[i], header_buf);
            initialized += 1;
        }
        defer for (iterators[0..initialized]) |*it| it.deinit();

        var group_count: usize = 0;
        while (group_count < want) {
            const batch = @min(batch_capacity, want - group_count);
            for (iterators[0..column_indices.len], 0..) |*it, i| {
                const values = flat_values[i * batch_capacity ..][0..batch];
                const got = try it.next(values);
                if (got != batch) return error.InvalidParquetMetadata;
                batches[i] = values;
            }
            try callback(context, batches);
            group_count += batch;
            count += batch;
            remaining -= batch;
        }
    }
    return count;
}

pub fn streamFixedColumnsTypedPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, column_indices: []const usize, targets: []const FixedTarget, limit_rows: ?usize, context: anytype, callback: anytype) !usize {
    if (column_indices.len == 0) return 0;
    if (column_indices.len != targets.len) return error.InvalidParquetMetadata;
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const stat = try file.stat(io);
    var loaded = try readMetadataFromFile(allocator, io, &file, stat.size);
    defer loaded.deinit();
    const meta = loaded.meta;
    if (meta.row_groups.len == 0) return error.InvalidParquetMetadata;

    const types = try allocator.alloc(i32, column_indices.len);
    defer allocator.free(types);
    const widths = try allocator.alloc(usize, column_indices.len);
    defer allocator.free(widths);
    for (column_indices, 0..) |column_index, i| {
        if (column_index >= meta.row_groups[0].columns.len) return error.InvalidColumn;
        const col = meta.row_groups[0].columns[column_index];
        const type_ = col.type_ orelse return error.InvalidParquetMetadata;
        types[i] = type_;
        widths[i] = switch (type_) {
            1 => 4,
            2 => 8,
            else => return error.UnsupportedParquetType,
        };
    }

    const batch_capacity: usize = 64 * 1024;
    const flat_i16 = try allocator.alloc(i16, column_indices.len * batch_capacity);
    defer allocator.free(flat_i16);
    const flat_i32 = try allocator.alloc(i32, column_indices.len * batch_capacity);
    defer allocator.free(flat_i32);
    const flat_i64 = try allocator.alloc(i64, column_indices.len * batch_capacity);
    defer allocator.free(flat_i64);
    const batches = try allocator.alloc(FixedColumnBatch, column_indices.len);
    defer allocator.free(batches);
    const header_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(header_buf);
    const iterators = try allocator.alloc(FixedDictColumnIterator, column_indices.len);
    defer allocator.free(iterators);

    const row_limit = limit_rows orelse std.math.maxInt(usize);
    var remaining = row_limit;
    var count: usize = 0;
    for (meta.row_groups) |rg| {
        if (remaining == 0) break;
        const rows_in_group: usize = @intCast(@max(rg.num_rows, 0));
        const want = @min(rows_in_group, remaining);
        var initialized: usize = 0;
        errdefer for (iterators[0..initialized]) |*it| it.deinit();
        for (column_indices, 0..) |column_index, i| {
            if (column_index >= rg.columns.len) return error.InvalidColumn;
            const col = rg.columns[column_index];
            if ((col.type_ orelse -1) != types[i]) return error.InvalidParquetMetadata;
            iterators[i] = try FixedDictColumnIterator.init(allocator, io, &file, stat.size, col, widths[i], header_buf);
            initialized += 1;
        }
        defer for (iterators[0..initialized]) |*it| it.deinit();

        var group_count: usize = 0;
        while (group_count < want) {
            const batch = @min(batch_capacity, want - group_count);
            for (iterators[0..column_indices.len], 0..) |*it, i| {
                const got = switch (targets[i]) {
                    .i16 => got: {
                        const values = flat_i16[i * batch_capacity ..][0..batch];
                        const got = try it.nextTyped(i16, values);
                        batches[i] = .{ .target = .i16, .bytes = std.mem.sliceAsBytes(values) };
                        break :got got;
                    },
                    .i32 => got: {
                        const values = flat_i32[i * batch_capacity ..][0..batch];
                        const got = try it.nextTyped(i32, values);
                        batches[i] = .{ .target = .i32, .bytes = std.mem.sliceAsBytes(values) };
                        break :got got;
                    },
                    .i64 => got: {
                        const values = flat_i64[i * batch_capacity ..][0..batch];
                        const got = try it.nextTyped(i64, values);
                        batches[i] = .{ .target = .i64, .bytes = std.mem.sliceAsBytes(values) };
                        break :got got;
                    },
                };
                if (got != batch) return error.InvalidParquetMetadata;
            }
            try callback(context, batches);
            group_count += batch;
            count += batch;
            remaining -= batch;
        }
    }
    return count;
}

pub fn scanSelectedRowsPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, row_ids: []const u32, context: anytype, callback: anytype) !void {
    if (row_ids.len == 0) return;
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const stat = try file.stat(io);
    var loaded = try readMetadataFromFile(allocator, io, &file, stat.size);
    defer loaded.deinit();
    const meta = loaded.meta;
    if (meta.row_groups.len == 0) return error.InvalidParquetMetadata;

    const header_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(header_buf);
    const tmp_i32 = try allocator.alloc(i32, 4096);
    defer allocator.free(tmp_i32);
    const tmp_i64 = try allocator.alloc(i64, 4096);
    defer allocator.free(tmp_i64);

    var global_start: u64 = 0;
    var row_pos: usize = 0;
    for (meta.row_groups) |rg| {
        const group_rows: u64 = @intCast(@max(rg.num_rows, 0));
        const global_end = global_start + group_rows;
        while (row_pos < row_ids.len and row_ids[row_pos] < global_start) row_pos += 1;
        const group_begin_pos = row_pos;
        while (row_pos < row_ids.len and row_ids[row_pos] < global_end) row_pos += 1;
        const group_end_pos = row_pos;
        if (group_begin_pos == group_end_pos) {
            global_start = global_end;
            continue;
        }

        for (rg.columns, 0..) |col, column_index| {
            const type_ = col.type_ orelse return error.InvalidParquetMetadata;
            switch (type_) {
                1 => {
                    var it = try FixedDictColumnIterator.init(allocator, io, &file, stat.size, col, 4, header_buf);
                    defer it.deinit();
                    var local_row: usize = 0;
                    for (row_ids[group_begin_pos..group_end_pos], group_begin_pos..) |global_row, selected_index| {
                        const target_local: usize = @intCast(@as(u64, global_row) - global_start);
                        try it.skip(target_local - local_row);
                        local_row = target_local;
                        var one: [1]i32 = undefined;
                        const got = try it.nextTyped(i32, one[0..1]);
                        if (got != 1) return error.InvalidParquetMetadata;
                        local_row += 1;
                        try callback(context, selected_index, column_index, SelectedValue{ .int32 = one[0] });
                    }
                },
                2 => {
                    var it = try FixedDictColumnIterator.init(allocator, io, &file, stat.size, col, 8, header_buf);
                    defer it.deinit();
                    var local_row: usize = 0;
                    for (row_ids[group_begin_pos..group_end_pos], group_begin_pos..) |global_row, selected_index| {
                        const target_local: usize = @intCast(@as(u64, global_row) - global_start);
                        try it.skip(target_local - local_row);
                        local_row = target_local;
                        var one: [1]i64 = undefined;
                        const got = try it.nextTyped(i64, one[0..1]);
                        if (got != 1) return error.InvalidParquetMetadata;
                        local_row += 1;
                        try callback(context, selected_index, column_index, SelectedValue{ .int64 = one[0] });
                    }
                },
                6 => {
                    var it = try ByteArrayColumnIterator.init(allocator, io, &file, stat.size, col, header_buf);
                    defer it.deinit();
                    var local_row: usize = 0;
                    for (row_ids[group_begin_pos..group_end_pos], group_begin_pos..) |global_row, selected_index| {
                        const target_local: usize = @intCast(@as(u64, global_row) - global_start);
                        try it.skip(target_local - local_row);
                        local_row = target_local;
                        const value = (try it.next()) orelse return error.InvalidParquetMetadata;
                        local_row += 1;
                        try callback(context, selected_index, column_index, SelectedValue{ .bytes = value });
                    }
                },
                else => return error.UnsupportedParquetType,
            }
        }
        global_start = global_end;
    }
}

const FixedDictColumnIterator = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    file: *std.Io.File,
    col: ColumnMeta,
    codec: i32,
    width: usize,
    end: u64,
    offset: u64,
    header_buf: []u8,
    is_optional: bool = false,
    dict: []i64 = &.{},
    ids_payload: []u8 = &.{},
    plain_payload: []u8 = &.{},
    plain_pos: usize = 0,
    decoder: ?RleBitPackedDecoder = null,
    page_remaining: usize = 0,
    page_mode: enum { none, dict_ids, plain } = .none,
    done: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, file: *std.Io.File, file_size: u64, col: ColumnMeta, width: usize, header_buf: []u8) !FixedDictColumnIterator {
        if (col.codec != null and col.codec.? != 1 and col.codec.? != 0) return error.UnsupportedParquetCodec;
        const start_i64 = col.dictionary_page_offset orelse col.data_page_offset orelse return error.InvalidParquetMetadata;
        if (start_i64 < 0 or col.total_compressed_size < 0) return error.InvalidParquetMetadata;
        const offset: u64 = @intCast(start_i64);
        return .{
            .allocator = allocator,
            .io = io,
            .file = file,
            .col = col,
            .codec = col.codec orelse 0,
            .width = width,
            .end = @min(file_size, offset + @as(u64, @intCast(col.total_compressed_size))),
            .offset = offset,
            .header_buf = header_buf,
        };
    }

    fn deinit(self: *FixedDictColumnIterator) void {
        if (self.dict.len != 0) self.allocator.free(self.dict);
        if (self.ids_payload.len != 0) self.allocator.free(self.ids_payload);
        if (self.plain_payload.len != 0) self.allocator.free(self.plain_payload);
    }

    fn next(self: *FixedDictColumnIterator, out: []i64) !usize {
        var written: usize = 0;
        while (written < out.len) {
            if (self.page_remaining == 0) {
                if (self.done) return written;
                try self.loadNextDataPage();
                if (self.done and self.page_remaining == 0) return written;
            }
            const n = @min(out.len - written, self.page_remaining);
            switch (self.page_mode) {
                .dict_ids => {
                    var decoder = self.decoder orelse return error.InvalidParquetMetadata;
                    try decoder.nextBatch(self.dict, out[written .. written + n]);
                    self.decoder = decoder;
                },
                .plain => {
                    if (self.plain_pos + n * self.width > self.plain_payload.len) return error.InvalidParquetMetadata;
                    for (out[written .. written + n]) |*slot| {
                        const p = self.plain_payload[self.plain_pos..][0..self.width];
                        slot.* = switch (self.width) {
                            4 => std.mem.readInt(i32, p[0..4], .little),
                            8 => std.mem.readInt(i64, p[0..8], .little),
                            else => unreachable,
                        };
                        self.plain_pos += self.width;
                    }
                },
                .none => return error.InvalidParquetMetadata,
            }
            self.page_remaining -= n;
            written += n;
        }
        return written;
    }

    fn nextTyped(self: *FixedDictColumnIterator, comptime T: type, out: []T) !usize {
        var written: usize = 0;
        while (written < out.len) {
            if (self.page_remaining == 0) {
                if (self.done) return written;
                try self.loadNextDataPage();
                if (self.done and self.page_remaining == 0) return written;
            }
            const n = @min(out.len - written, self.page_remaining);
            switch (self.page_mode) {
                .dict_ids => {
                    var decoder = self.decoder orelse return error.InvalidParquetMetadata;
                    try decoder.nextBatchTyped(T, self.dict, out[written .. written + n]);
                    self.decoder = decoder;
                },
                .plain => {
                    if (self.plain_pos + n * self.width > self.plain_payload.len) return error.InvalidParquetMetadata;
                    for (out[written .. written + n]) |*slot| {
                        const p = self.plain_payload[self.plain_pos..][0..self.width];
                        const v: i64 = switch (self.width) {
                            4 => std.mem.readInt(i32, p[0..4], .little),
                            8 => std.mem.readInt(i64, p[0..8], .little),
                            else => unreachable,
                        };
                        slot.* = @intCast(v);
                        self.plain_pos += self.width;
                    }
                },
                .none => return error.InvalidParquetMetadata,
            }
            self.page_remaining -= n;
            written += n;
        }
        return written;
    }

    fn skip(self: *FixedDictColumnIterator, count: usize) !void {
        var remaining = count;
        while (remaining > 0) {
            if (self.page_remaining == 0) {
                if (self.done) return error.InvalidParquetMetadata;
                try self.skipPagesBeforeTarget(&remaining);
                if (self.page_remaining == 0) continue;
            }
            const n = @min(remaining, self.page_remaining);
            switch (self.page_mode) {
                .dict_ids => {
                    var decoder = self.decoder orelse return error.InvalidParquetMetadata;
                    try decoder.skip(n);
                    self.decoder = decoder;
                },
                .plain => self.plain_pos += n * self.width,
                .none => return error.InvalidParquetMetadata,
            }
            self.page_remaining -= n;
            remaining -= n;
        }
    }

    fn skipPagesBeforeTarget(self: *FixedDictColumnIterator, rows_to_skip: *usize) !void {
        while (self.offset < self.end) {
            const info = try readPageInfo(self.allocator, self.io, self.file, self.offset, self.end, self.header_buf);
            if (info.header.dictionary_page != null) {
                try self.loadNextDataPage();
                return;
            }
            if (info.header.data_page) |data_header| {
                const page_values: usize = @intCast(data_header.num_values);
                if (page_values <= rows_to_skip.*) {
                    self.offset = info.next_offset;
                    rows_to_skip.* -= page_values;
                    if (rows_to_skip.* == 0) return;
                    continue;
                }
            }
            try self.loadNextDataPage();
            return;
        }
        self.done = true;
    }

    fn loadNextDataPage(self: *FixedDictColumnIterator) !void {
        if (self.ids_payload.len != 0) {
            self.allocator.free(self.ids_payload);
            self.ids_payload = &.{};
        }
        if (self.plain_payload.len != 0) {
            self.allocator.free(self.plain_payload);
            self.plain_payload = &.{};
        }
        self.page_mode = .none;
        self.plain_pos = 0;
        while (self.offset < self.end) {
            const page = try readPage(self.allocator, self.io, self.file, self.offset, self.end, self.codec, self.header_buf);
            self.offset = page.next_offset;
            if (page.header.dictionary_page) |dict_header| {
                defer self.allocator.free(page.payload);
                if (self.dict.len != 0) return error.InvalidParquetMetadata;
                self.dict = try decodePlainFixedDict(self.allocator, page.payload, dict_header.num_values, self.width);
                continue;
            }
            if (page.header.data_page) |data_header| {
                if (data_header.num_values < 0) {
                    self.allocator.free(page.payload);
                    return error.InvalidParquetMetadata;
                }
                const encoding = data_header.encoding orelse return error.InvalidParquetMetadata;
                if (encoding == 0) {
                    const values: usize = @intCast(data_header.num_values);
                    // Detect OPTIONAL columns: payload may start with 4-byte def-level
                    // byte-count followed by RLE def-levels, then actual values.
                    // Heuristic: if payload is larger than values*width, skip def levels.
                    var payload_start: usize = 0;
                    if (page.payload.len > values * self.width and page.payload.len >= 4) {
                        const def_len = std.mem.readInt(u32, page.payload[0..4], .little);
                        if (4 + def_len + values * self.width <= page.payload.len) {
                            payload_start = 4 + @as(usize, def_len);
                        }
                    }
                    if (page.payload.len < payload_start + values * self.width) {
                        self.allocator.free(page.payload);
                        return error.InvalidParquetMetadata;
                    }
                    self.plain_payload = page.payload;
                    self.plain_pos = payload_start;
                    self.page_remaining = values;
                    self.page_mode = .plain;
                    return;
                }
                if (self.dict.len == 0 or page.payload.len == 0) {
                    self.allocator.free(page.payload);
                    return error.InvalidParquetMetadata;
                }
                const bit_width = page.payload[0];
                if (bit_width > 32) {
                    self.allocator.free(page.payload);
                    return error.InvalidParquetMetadata;
                }
                self.ids_payload = page.payload;
                self.decoder = .{ .buf = self.ids_payload[1..], .bit_width = bit_width };
                self.page_remaining = @intCast(data_header.num_values);
                self.page_mode = .dict_ids;
                return;
            }
            self.allocator.free(page.payload);
        }
        self.done = true;
    }
};

const ByteArrayColumnIterator = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    file: *std.Io.File,
    codec: i32,
    end: u64,
    offset: u64,
    header_buf: []u8,
    dict: StringDict = .{},
    ids_payload: []u8 = &.{},
    plain_payload: []u8 = &.{},
    plain_pos: usize = 0,
    decoder: ?RleBitPackedDecoder = null,
    page_remaining: usize = 0,
    page_mode: enum { none, dict_ids, plain } = .none,
    done: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, file: *std.Io.File, file_size: u64, col: ColumnMeta, header_buf: []u8) !ByteArrayColumnIterator {
        if (col.codec != null and col.codec.? != 1 and col.codec.? != 0) return error.UnsupportedParquetCodec;
        const start_i64 = col.dictionary_page_offset orelse col.data_page_offset orelse return error.InvalidParquetMetadata;
        if (start_i64 < 0 or col.total_compressed_size < 0) return error.InvalidParquetMetadata;
        const offset: u64 = @intCast(start_i64);
        return .{
            .allocator = allocator,
            .io = io,
            .file = file,
            .codec = col.codec orelse 0,
            .end = @min(file_size, offset + @as(u64, @intCast(col.total_compressed_size))),
            .offset = offset,
            .header_buf = header_buf,
        };
    }

    fn deinit(self: *ByteArrayColumnIterator) void {
        self.dict.deinit(self.allocator);
        if (self.ids_payload.len != 0) self.allocator.free(self.ids_payload);
        if (self.plain_payload.len != 0) self.allocator.free(self.plain_payload);
    }

    fn next(self: *ByteArrayColumnIterator) !?[]const u8 {
        if (self.page_remaining == 0) {
            if (self.done) return null;
            try self.loadNextDataPage();
            if (self.done and self.page_remaining == 0) return null;
        }
        const s = switch (self.page_mode) {
            .dict_ids => blk: {
                var decoder = self.decoder orelse return error.InvalidParquetMetadata;
                const id = try decoder.next();
                self.decoder = decoder;
                break :blk try self.dict.value(id);
            },
            .plain => try readPlainByteArray(self.plain_payload, &self.plain_pos),
            .none => return error.InvalidParquetMetadata,
        };
        self.page_remaining -= 1;
        return s;
    }

    fn skip(self: *ByteArrayColumnIterator, count: usize) !void {
        var remaining = count;
        while (remaining > 0) {
            if (self.page_remaining == 0) {
                if (self.done) return error.InvalidParquetMetadata;
                try self.skipPagesBeforeTarget(&remaining);
                if (self.page_remaining == 0) continue;
            }
            const n = @min(remaining, self.page_remaining);
            switch (self.page_mode) {
                .dict_ids => {
                    var decoder = self.decoder orelse return error.InvalidParquetMetadata;
                    try decoder.skip(n);
                    self.decoder = decoder;
                },
                .plain => {
                    for (0..n) |_| _ = try readPlainByteArray(self.plain_payload, &self.plain_pos);
                },
                .none => return error.InvalidParquetMetadata,
            }
            self.page_remaining -= n;
            remaining -= n;
        }
    }

    fn skipPagesBeforeTarget(self: *ByteArrayColumnIterator, rows_to_skip: *usize) !void {
        while (self.offset < self.end) {
            const info = try readPageInfo(self.allocator, self.io, self.file, self.offset, self.end, self.header_buf);
            if (info.header.dictionary_page != null) {
                try self.loadNextDataPage();
                return;
            }
            if (info.header.data_page) |data_header| {
                const page_values: usize = @intCast(data_header.num_values);
                if (page_values <= rows_to_skip.*) {
                    self.offset = info.next_offset;
                    rows_to_skip.* -= page_values;
                    if (rows_to_skip.* == 0) return;
                    continue;
                }
            }
            try self.loadNextDataPage();
            return;
        }
        self.done = true;
    }

    fn loadNextDataPage(self: *ByteArrayColumnIterator) !void {
        if (self.ids_payload.len != 0) {
            self.allocator.free(self.ids_payload);
            self.ids_payload = &.{};
        }
        if (self.plain_payload.len != 0) {
            self.allocator.free(self.plain_payload);
            self.plain_payload = &.{};
        }
        self.page_mode = .none;
        self.plain_pos = 0;
        while (self.offset < self.end) {
            const page = try readPage(self.allocator, self.io, self.file, self.offset, self.end, self.codec, self.header_buf);
            self.offset = page.next_offset;
            if (page.header.dictionary_page) |dict_header| {
                defer self.allocator.free(page.payload);
                if (self.dict.len() != 0) return error.InvalidParquetMetadata;
                try self.dict.decodePlain(self.allocator, page.payload, dict_header.num_values);
                continue;
            }
            if (page.header.data_page) |data_header| {
                if (data_header.num_values < 0) {
                    self.allocator.free(page.payload);
                    return error.InvalidParquetMetadata;
                }
                const encoding = data_header.encoding orelse return error.InvalidParquetMetadata;
                if (encoding == 0) {
                    self.plain_payload = page.payload;
                    self.page_remaining = @intCast(data_header.num_values);
                    self.page_mode = .plain;
                    return;
                }
                if (self.dict.len() == 0 or page.payload.len == 0) {
                    self.allocator.free(page.payload);
                    return error.InvalidParquetMetadata;
                }
                const bit_width = page.payload[0];
                if (bit_width > 32) {
                    self.allocator.free(page.payload);
                    return error.InvalidParquetMetadata;
                }
                self.ids_payload = page.payload;
                self.decoder = .{ .buf = self.ids_payload[1..], .bit_width = bit_width };
                self.page_remaining = @intCast(data_header.num_values);
                self.page_mode = .dict_ids;
                return;
            }
            self.allocator.free(page.payload);
        }
        self.done = true;
    }
};

const ByteArrayStats = struct {
    count: usize = 0,
    empty: usize = 0,
    total_bytes: usize = 0,
    max_len: usize = 0,
    hash: u64 = 14695981039346656037,

    fn observe(self: *ByteArrayStats, value: []const u8) !void {
        self.count += 1;
        if (value.len == 0) self.empty += 1;
        self.total_bytes += value.len;
        self.max_len = @max(self.max_len, value.len);
        self.hash = fnv1a(self.hash, value);
    }
};

fn fnv1a(seed: u64, bytes: []const u8) u64 {
    var h = seed;
    for (bytes) |b| {
        h ^= b;
        h *%= 1099511628211;
    }
    h ^= 0xff;
    h *%= 1099511628211;
    return h;
}

const LoadedMetadata = struct {
    backing_allocator: std.mem.Allocator,
    arena: *std.heap.ArenaAllocator,
    meta: Metadata,

    fn deinit(self: *LoadedMetadata) void {
        self.arena.deinit();
        self.backing_allocator.destroy(self.arena);
    }
};

fn readMetadataFromFile(allocator: std.mem.Allocator, io: std.Io, file: *std.Io.File, file_size: u64) !LoadedMetadata {
    if (file_size < 12) return error.InvalidParquetFile;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    errdefer allocator.destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    var trailer: [8]u8 = undefined;
    const trailer_n = try file.readPositionalAll(io, &trailer, file_size - 8);
    if (trailer_n != trailer.len) return error.InvalidParquetFile;
    if (!std.mem.eql(u8, trailer[4..8], "PAR1")) return error.InvalidParquetFile;
    const footer_len = std.mem.readInt(u32, trailer[0..4], .little);
    if (@as(u64, footer_len) + 8 > file_size) return error.InvalidParquetFile;

    const footer = try arena.allocator().alloc(u8, footer_len);
    const footer_offset = file_size - 8 - footer_len;
    const footer_n = try file.readPositionalAll(io, footer, footer_offset);
    if (footer_n != footer.len) return error.InvalidParquetFile;

    var parser = CompactParser{ .allocator = arena.allocator(), .buf = footer };
    return .{ .backing_allocator = allocator, .arena = arena, .meta = try parser.readFileMetaData() };
}

const Page = struct {
    header: PageHeader,
    payload: []u8,
    next_offset: u64,
};

const PageInfo = struct {
    header: PageHeader,
    next_offset: u64,
};

fn readPageInfo(allocator: std.mem.Allocator, io: std.Io, file: *std.Io.File, offset: u64, end: u64, header_buf: []u8) !PageInfo {
    const remaining = end - offset;
    const read_len: usize = @intCast(@min(@as(u64, header_buf.len), remaining));
    const n = try file.readPositionalAll(io, header_buf[0..read_len], offset);
    if (n == 0) return error.InvalidParquetFile;
    var parser = CompactParser{ .allocator = allocator, .buf = header_buf[0..n] };
    const header = try parser.readPageHeader();
    if (header.compressed_page_size < 0 or header.uncompressed_page_size < 0) return error.InvalidParquetMetadata;
    const header_len = parser.pos;
    const compressed_size: usize = @intCast(header.compressed_page_size);
    const payload_offset = offset + @as(u64, @intCast(header_len));
    const next = payload_offset + @as(u64, @intCast(compressed_size));
    if (next > end) return error.InvalidParquetMetadata;
    return .{ .header = header, .next_offset = next };
}

fn readPage(allocator: std.mem.Allocator, io: std.Io, file: *std.Io.File, offset: u64, end: u64, codec: i32, header_buf: []u8) !Page {
    const remaining = end - offset;
    const read_len: usize = @intCast(@min(@as(u64, header_buf.len), remaining));
    const n = try file.readPositionalAll(io, header_buf[0..read_len], offset);
    if (n == 0) return error.InvalidParquetFile;
    var parser = CompactParser{ .allocator = allocator, .buf = header_buf[0..n] };
    const header = try parser.readPageHeader();
    if (header.compressed_page_size < 0 or header.uncompressed_page_size < 0) return error.InvalidParquetMetadata;
    const header_len = parser.pos;
    const compressed_size: usize = @intCast(header.compressed_page_size);
    const uncompressed_size: usize = @intCast(header.uncompressed_page_size);
    const payload_offset = offset + @as(u64, @intCast(header_len));
    const next = payload_offset + @as(u64, @intCast(compressed_size));
    if (next > end) return error.InvalidParquetMetadata;

    const compressed = try allocator.alloc(u8, compressed_size);
    defer allocator.free(compressed);
    const payload_n = try file.readPositionalAll(io, compressed, payload_offset);
    if (payload_n != compressed.len) return error.InvalidParquetFile;

    const payload = switch (codec) {
        0 => try allocator.dupe(u8, compressed),
        1 => try snappyDecompress(allocator, compressed, uncompressed_size),
        else => return error.UnsupportedParquetCodec,
    };
    if (payload.len != uncompressed_size) return error.InvalidParquetMetadata;
    return .{ .header = header, .payload = payload, .next_offset = next };
}

fn decodePlainFixedDict(allocator: std.mem.Allocator, payload: []const u8, raw_count: i32, width: usize) ![]i64 {
    if (raw_count < 0) return error.InvalidParquetMetadata;
    const count: usize = @intCast(raw_count);
    if (payload.len < count * width) return error.InvalidParquetMetadata;
    const dict = try allocator.alloc(i64, count);
    errdefer allocator.free(dict);
    for (dict, 0..) |*slot, i| {
        const p = payload[i * width ..][0..width];
        slot.* = switch (width) {
            4 => std.mem.readInt(i32, p[0..4], .little),
            8 => std.mem.readInt(i64, p[0..8], .little),
            else => unreachable,
        };
    }
    return dict;
}

fn decodeDictionaryIdsToFixed(payload: []const u8, dict: []const i64, out: []i64) !void {
    if (payload.len == 0) return error.InvalidParquetMetadata;
    const bit_width = payload[0];
    if (bit_width > 32) return error.InvalidParquetMetadata;
    var decoder = RleBitPackedDecoder{ .buf = payload[1..], .bit_width = bit_width };
    for (out) |*slot| {
        const id = try decoder.next();
        if (id >= dict.len) return error.InvalidParquetMetadata;
        slot.* = dict[id];
    }
}

const RleBitPackedDecoder = struct {
    buf: []const u8,
    pos: usize = 0,
    bit_width: u8,
    rle_left: usize = 0,
    rle_value: usize = 0,
    packed_left: usize = 0,
    packed_bit_pos: usize = 0,

    fn next(self: *RleBitPackedDecoder) !usize {
        if (self.rle_left != 0) {
            self.rle_left -= 1;
            return self.rle_value;
        }
        if (self.packed_left != 0) {
            const v = try self.readPackedValue();
            self.packed_left -= 1;
            return v;
        }
        const header = try self.readVarUInt();
        if ((header & 1) == 0) {
            self.rle_left = @intCast(header >> 1);
            const byte_width: usize = (@as(usize, self.bit_width) + 7) / 8;
            if (self.pos + byte_width > self.buf.len) return error.InvalidParquetMetadata;
            var value: usize = 0;
            for (0..byte_width) |i| value |= @as(usize, self.buf[self.pos + i]) << @intCast(8 * i);
            self.pos += byte_width;
            if (self.rle_left == 0) return error.InvalidParquetMetadata;
            self.rle_left -= 1;
            self.rle_value = value;
            return value;
        }
        self.packed_left = @as(usize, @intCast(header >> 1)) * 8;
        self.packed_bit_pos = self.pos * 8;
        if (self.packed_left == 0) return error.InvalidParquetMetadata;
        const v = try self.readPackedValue();
        self.packed_left -= 1;
        return v;
    }

    fn nextBatch(self: *RleBitPackedDecoder, dict: []const i64, out: []i64) !void {
        var written: usize = 0;
        while (written < out.len) {
            if (self.rle_left != 0) {
                if (self.rle_value >= dict.len) return error.InvalidParquetMetadata;
                const n = @min(self.rle_left, out.len - written);
                @memset(out[written .. written + n], dict[self.rle_value]);
                self.rle_left -= n;
                written += n;
                continue;
            }
            if (self.packed_left != 0) {
                const n = @min(self.packed_left, out.len - written);
                for (out[written .. written + n]) |*slot| {
                    const id = try self.readPackedValue();
                    if (id >= dict.len) return error.InvalidParquetMetadata;
                    slot.* = dict[id];
                }
                self.packed_left -= n;
                written += n;
                continue;
            }
            const header = try self.readVarUInt();
            if ((header & 1) == 0) {
                self.rle_left = @intCast(header >> 1);
                const byte_width: usize = (@as(usize, self.bit_width) + 7) / 8;
                if (self.pos + byte_width > self.buf.len) return error.InvalidParquetMetadata;
                var value: usize = 0;
                for (0..byte_width) |i| value |= @as(usize, self.buf[self.pos + i]) << @intCast(8 * i);
                self.pos += byte_width;
                if (self.rle_left == 0) return error.InvalidParquetMetadata;
                self.rle_value = value;
            } else {
                self.packed_left = @as(usize, @intCast(header >> 1)) * 8;
                self.packed_bit_pos = self.pos * 8;
                if (self.packed_left == 0) return error.InvalidParquetMetadata;
            }
        }
    }

    fn nextBatchTyped(self: *RleBitPackedDecoder, comptime T: type, dict: []const i64, out: []T) !void {
        var written: usize = 0;
        while (written < out.len) {
            if (self.rle_left != 0) {
                if (self.rle_value >= dict.len) return error.InvalidParquetMetadata;
                const n = @min(self.rle_left, out.len - written);
                @memset(out[written .. written + n], @intCast(dict[self.rle_value]));
                self.rle_left -= n;
                written += n;
                continue;
            }
            if (self.packed_left != 0) {
                const n = @min(self.packed_left, out.len - written);
                for (out[written .. written + n]) |*slot| {
                    const id = try self.readPackedValue();
                    if (id >= dict.len) return error.InvalidParquetMetadata;
                    slot.* = @intCast(dict[id]);
                }
                self.packed_left -= n;
                written += n;
                continue;
            }
            const header = try self.readVarUInt();
            if ((header & 1) == 0) {
                self.rle_left = @intCast(header >> 1);
                const byte_width: usize = (@as(usize, self.bit_width) + 7) / 8;
                if (self.pos + byte_width > self.buf.len) return error.InvalidParquetMetadata;
                var value: usize = 0;
                for (0..byte_width) |i| value |= @as(usize, self.buf[self.pos + i]) << @intCast(8 * i);
                self.pos += byte_width;
                if (self.rle_left == 0) return error.InvalidParquetMetadata;
                self.rle_value = value;
            } else {
                self.packed_left = @as(usize, @intCast(header >> 1)) * 8;
                self.packed_bit_pos = self.pos * 8;
                if (self.packed_left == 0) return error.InvalidParquetMetadata;
            }
        }
    }

    fn readPackedValue(self: *RleBitPackedDecoder) !usize {
        if (self.bit_width == 0) return 0;
        const byte_index = self.packed_bit_pos / 8;
        const bit_offset: u6 = @intCast(self.packed_bit_pos % 8);
        const needed_bits: usize = bit_offset + self.bit_width;
        const needed_bytes = (needed_bits + 7) / 8;
        if (byte_index + needed_bytes > self.buf.len) return error.InvalidParquetMetadata;
        var window: u64 = 0;
        for (0..needed_bytes) |i| window |= @as(u64, self.buf[byte_index + i]) << @intCast(8 * i);
        const mask = (@as(u64, 1) << @intCast(self.bit_width)) - 1;
        const value: usize = @intCast((window >> bit_offset) & mask);
        self.packed_bit_pos += self.bit_width;
        self.pos = (self.packed_bit_pos + 7) / 8;
        return value;
    }

    fn readVarUInt(self: *RleBitPackedDecoder) !u64 {
        var shift: u6 = 0;
        var result: u64 = 0;
        while (true) {
            if (self.pos >= self.buf.len) return error.InvalidParquetMetadata;
            const b = self.buf[self.pos];
            self.pos += 1;
            result |= @as(u64, b & 0x7f) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift >= 63) return error.InvalidParquetMetadata;
        }
    }

    fn skip(self: *RleBitPackedDecoder, count: usize) !void {
        for (0..count) |_| _ = try self.next();
    }
};

fn snappyDecompress(allocator: std.mem.Allocator, input: []const u8, expected_len: usize) ![]u8 {
    var pos: usize = 0;
    const decoded_len = try readSnappyLength(input, &pos);
    if (decoded_len != expected_len) return error.InvalidSnappyData;
    const out = try allocator.alloc(u8, decoded_len);
    errdefer allocator.free(out);
    var out_pos: usize = 0;
    while (pos < input.len) {
        const tag = input[pos];
        pos += 1;
        switch (tag & 0x03) {
            0 => {
                const len_code: usize = tag >> 2;
                var len: usize = undefined;
                if (len_code < 60) {
                    len = len_code + 1;
                } else {
                    const len_bytes = len_code - 59;
                    if (pos + len_bytes > input.len) return error.InvalidSnappyData;
                    len = 0;
                    for (0..len_bytes) |i| len |= @as(usize, input[pos + i]) << @intCast(8 * i);
                    pos += len_bytes;
                    len += 1;
                }
                if (pos + len > input.len or out_pos + len > out.len) return error.InvalidSnappyData;
                @memcpy(out[out_pos .. out_pos + len], input[pos .. pos + len]);
                pos += len;
                out_pos += len;
            },
            1 => {
                if (pos >= input.len) return error.InvalidSnappyData;
                const len: usize = 4 + ((tag >> 2) & 0x7);
                const offset = (@as(usize, tag & 0xe0) << 3) | input[pos];
                pos += 1;
                try snappyCopy(out, &out_pos, offset, len);
            },
            2 => {
                if (pos + 2 > input.len) return error.InvalidSnappyData;
                const len: usize = 1 + (tag >> 2);
                const offset = std.mem.readInt(u16, input[pos..][0..2], .little);
                pos += 2;
                try snappyCopy(out, &out_pos, offset, len);
            },
            3 => {
                if (pos + 4 > input.len) return error.InvalidSnappyData;
                const len: usize = 1 + (tag >> 2);
                const offset = std.mem.readInt(u32, input[pos..][0..4], .little);
                pos += 4;
                try snappyCopy(out, &out_pos, offset, len);
            },
            else => unreachable,
        }
    }
    if (out_pos != out.len) return error.InvalidSnappyData;
    return out;
}

fn readSnappyLength(input: []const u8, pos: *usize) !usize {
    var shift: u6 = 0;
    var result: usize = 0;
    while (true) {
        if (pos.* >= input.len) return error.InvalidSnappyData;
        const b = input[pos.*];
        pos.* += 1;
        result |= @as(usize, b & 0x7f) << shift;
        if ((b & 0x80) == 0) return result;
        shift += 7;
        if (shift >= @bitSizeOf(usize)) return error.InvalidSnappyData;
    }
}

fn snappyCopy(out: []u8, out_pos: *usize, raw_offset: anytype, len: usize) !void {
    const offset: usize = @intCast(raw_offset);
    if (offset == 0 or offset > out_pos.* or out_pos.* + len > out.len) return error.InvalidSnappyData;
    if (offset >= len) {
        const start = out_pos.*;
        @memcpy(out[start .. start + len], out[start - offset .. start - offset + len]);
        out_pos.* += len;
        return;
    }
    if (offset == 1) {
        @memset(out[out_pos.* .. out_pos.* + len], out[out_pos.* - 1]);
        out_pos.* += len;
        return;
    }
    for (0..len) |_| {
        out[out_pos.*] = out[out_pos.* - offset];
        out_pos.* += 1;
    }
}

const StringDict = struct {
    offsets: std.ArrayList(u32) = .empty,
    bytes: std.ArrayList(u8) = .empty,

    fn deinit(self: *StringDict, allocator: std.mem.Allocator) void {
        self.offsets.deinit(allocator);
        self.bytes.deinit(allocator);
    }

    fn decodePlain(self: *StringDict, allocator: std.mem.Allocator, payload: []const u8, raw_count: i32) !void {
        if (raw_count < 0) return error.InvalidParquetMetadata;
        try self.offsets.ensureTotalCapacity(allocator, @as(usize, @intCast(raw_count)) + 1);
        try self.offsets.append(allocator, 0);
        var pos: usize = 0;
        for (0..@as(usize, @intCast(raw_count))) |_| {
            const s = try readPlainByteArray(payload, &pos);
            try self.bytes.appendSlice(allocator, s);
            try self.offsets.append(allocator, @intCast(self.bytes.items.len));
        }
    }

    fn len(self: *const StringDict) usize {
        return if (self.offsets.items.len == 0) 0 else self.offsets.items.len - 1;
    }

    fn value(self: *const StringDict, id: usize) ![]const u8 {
        if (id + 1 >= self.offsets.items.len) return error.InvalidParquetMetadata;
        const start = self.offsets.items[id];
        const end = self.offsets.items[id + 1];
        return self.bytes.items[start..end];
    }
};

fn readPlainByteArray(payload: []const u8, pos: *usize) ![]const u8 {
    if (pos.* + 4 > payload.len) return error.InvalidParquetMetadata;
    const len: usize = @intCast(std.mem.readInt(u32, payload[pos.*..][0..4], .little));
    pos.* += 4;
    if (pos.* + len > payload.len) return error.InvalidParquetMetadata;
    const s = payload[pos.* .. pos.* + len];
    pos.* += len;
    return s;
}

fn appendEscaped(allocator: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    try out.append(allocator, '"');
    for (s[0..@min(s.len, 240)]) |b| switch (b) {
        '\n' => try out.appendSlice(allocator, "\\n"),
        '\r' => try out.appendSlice(allocator, "\\r"),
        '\t' => try out.appendSlice(allocator, "\\t"),
        '"' => try out.appendSlice(allocator, "\\\""),
        '\\' => try out.appendSlice(allocator, "\\\\"),
        0x20...0x21, 0x23...0x5b, 0x5d...0x7e => try out.append(allocator, b),
        else => try out.print(allocator, "\\x{x:0>2}", .{b}),
    };
    if (s.len > 240) try out.appendSlice(allocator, "...");
    try out.append(allocator, '"');
}

fn wallNowNs() i128 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec,
        else => return 0,
    }
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

    fn readPageHeader(self: *CompactParser) !PageHeader {
        var header = PageHeader{};
        var last: i16 = 0;
        while (try self.readField(&last)) |field| switch (field.id) {
            1 => header.type_ = try self.readI32(),
            2 => header.uncompressed_page_size = try self.readI32(),
            3 => header.compressed_page_size = try self.readI32(),
            5 => header.data_page = try self.readDataPageHeader(),
            7 => header.dictionary_page = try self.readDictionaryPageHeader(),
            else => try self.skip(field.type_),
        };
        return header;
    }

    fn readDataPageHeader(self: *CompactParser) !DataPageHeader {
        var header = DataPageHeader{};
        var last: i16 = 0;
        while (try self.readField(&last)) |field| switch (field.id) {
            1 => header.num_values = try self.readI32(),
            2 => header.encoding = try self.readI32(),
            3 => header.definition_level_encoding = try self.readI32(),
            4 => header.repetition_level_encoding = try self.readI32(),
            else => try self.skip(field.type_),
        };
        return header;
    }

    fn readDictionaryPageHeader(self: *CompactParser) !DictionaryPageHeader {
        var header = DictionaryPageHeader{};
        var last: i16 = 0;
        while (try self.readField(&last)) |field| switch (field.id) {
            1 => header.num_values = try self.readI32(),
            2 => header.encoding = try self.readI32(),
            3 => header.is_sorted = try self.readBoolField(field.type_),
            else => try self.skip(field.type_),
        };
        return header;
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

    fn readBoolField(self: *CompactParser, type_: u8) !bool {
        _ = self;
        return switch (type_) {
            compact_true => true,
            compact_false => false,
            else => error.InvalidParquetMetadata,
        };
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

fn pageTypeName(v: i32) []const u8 {
    return switch (v) {
        0 => "DATA_PAGE",
        1 => "INDEX_PAGE",
        2 => "DICTIONARY_PAGE",
        3 => "DATA_PAGE_V2",
        else => "UNKNOWN",
    };
}

test "zigzag decode" {
    try std.testing.expectEqual(@as(i64, 0), zigZagDecode(0));
    try std.testing.expectEqual(@as(i64, -1), zigZagDecode(1));
    try std.testing.expectEqual(@as(i64, 1), zigZagDecode(2));
    try std.testing.expectEqual(@as(i64, -2), zigZagDecode(3));
}

test "snappy decompress literal and copy" {
    const compressed = [_]u8{
        10, // uncompressed length
        0x10, 'h', 'e', 'l', 'l', 'o',
        0x05, 5, // copy 5 bytes from offset 5
    };
    const out = try snappyDecompress(std.testing.allocator, &compressed, 10);
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("hellohello", out);
}

test "rle bit-packed decoder" {
    const encoded = [_]u8{
        0x06, 0x05, // RLE: 3 copies of 5, bit width 3 encoded in one byte
        0x03, 0x88, 0xc6, 0xfa, // bit-packed: one group of 8 values 0..7 at width 3
    };
    var decoder = RleBitPackedDecoder{ .buf = &encoded, .bit_width = 3 };
    try std.testing.expectEqual(@as(usize, 5), try decoder.next());
    try std.testing.expectEqual(@as(usize, 5), try decoder.next());
    try std.testing.expectEqual(@as(usize, 5), try decoder.next());
    for (0..8) |i| try std.testing.expectEqual(i, try decoder.next());
}
