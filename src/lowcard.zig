const std = @import("std");
const io_map = @import("io_map.zig");

pub const StringColumn = struct {
    ids: io_map.MappedColumn(u32),
    offsets: io_map.MappedColumn(u32),
    bytes: io_map.Mapping,

    pub fn map(io: std.Io, id_path: []const u8, offsets_path: []const u8, bytes_path: []const u8) !StringColumn {
        const ids = try io_map.mapColumn(u32, io, id_path);
        errdefer ids.mapping.unmap();
        const offsets = try io_map.mapColumn(u32, io, offsets_path);
        errdefer offsets.mapping.unmap();
        const bytes = try io_map.mapFile(io, bytes_path);
        errdefer bytes.unmap();
        if (offsets.values.len == 0) return error.CorruptHotColumns;
        return .{ .ids = ids, .offsets = offsets, .bytes = bytes };
    }

    pub fn unmap(self: *const StringColumn) void {
        self.ids.mapping.unmap();
        self.offsets.mapping.unmap();
        self.bytes.unmap();
    }

    pub fn dictSize(self: *const StringColumn) usize {
        return self.offsets.values.len - 1;
    }

    pub fn value(self: *const StringColumn, id: u32) []const u8 {
        const idx: usize = id;
        const start = self.offsets.values[idx];
        const end = self.offsets.values[idx + 1];
        return self.bytes.raw[start..end];
    }

    pub fn rowValue(self: *const StringColumn, row: usize) []const u8 {
        return self.value(self.ids.values[row]);
    }

    pub fn emptyId(self: *const StringColumn) ?u32 {
        for (0..self.dictSize()) |idx| {
            if (isStoredEmptyString(self.value(@intCast(idx)))) return @intCast(idx);
        }
        return null;
    }

    pub fn less(self: *const StringColumn, a: u32, b: u32) bool {
        return std.mem.lessThan(u8, self.value(a), self.value(b));
    }
};

pub const Dict = struct {
    offsets: io_map.MappedColumn(u32),
    bytes: io_map.Mapping,

    pub fn map(io: std.Io, offsets_path: []const u8, bytes_path: []const u8) !Dict {
        const offsets = try io_map.mapColumn(u32, io, offsets_path);
        errdefer offsets.mapping.unmap();
        const bytes = try io_map.mapFile(io, bytes_path);
        errdefer bytes.unmap();
        if (offsets.values.len == 0) return error.CorruptHotColumns;
        return .{ .offsets = offsets, .bytes = bytes };
    }

    pub fn unmap(self: *const Dict) void {
        self.offsets.mapping.unmap();
        self.bytes.unmap();
    }

    pub fn size(self: *const Dict) usize {
        return self.offsets.values.len - 1;
    }

    pub fn value(self: *const Dict, id: u32) []const u8 {
        const idx: usize = id;
        const start = self.offsets.values[idx];
        const end = self.offsets.values[idx + 1];
        return self.bytes.raw[start..end];
    }

    pub fn emptyId(self: *const Dict) ?u32 {
        for (0..self.size()) |idx| {
            if (isStoredEmptyString(self.value(@intCast(idx)))) return @intCast(idx);
        }
        return null;
    }

    pub fn less(self: *const Dict, a: u32, b: u32) bool {
        return std.mem.lessThan(u8, self.value(a), self.value(b));
    }
};

pub fn isStoredEmptyString(value: []const u8) bool {
    return value.len == 0 or (value.len == 2 and value[0] == '"' and value[1] == '"');
}

test "detects stored empty strings" {
    try std.testing.expect(isStoredEmptyString(""));
    try std.testing.expect(isStoredEmptyString("\"\""));
    try std.testing.expect(!isStoredEmptyString("alpha"));
}
