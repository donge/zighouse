const std = @import("std");
const storage = @import("storage.zig");

pub const StoreReader = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) StoreReader {
        return .{ .allocator = allocator, .io = io, .data_dir = data_dir };
    }

    pub fn resultCsv(self: *StoreReader, file_name: []const u8, limit: usize) ![]u8 {
        const path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.data_dir, file_name });
        defer self.allocator.free(path);
        return std.Io.Dir.cwd().readFileAlloc(self.io, path, self.allocator, .limited(limit));
    }

    pub fn importSource(self: *StoreReader) ![]u8 {
        return storage.readImportSource(self.io, self.allocator, self.data_dir);
    }
};
