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

pub const CsvReader = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, path: []const u8) CsvReader {
        return .{ .allocator = allocator, .io = io, .path = path };
    }

    pub const CountOptions = struct {
        has_header: bool = true,
    };

    pub fn rowCount(self: *CsvReader, options: CountOptions) !u64 {
        var input = if (std.fs.path.isAbsolute(self.path))
            try std.Io.Dir.openFileAbsolute(self.io, self.path, .{})
        else
            try std.Io.Dir.cwd().openFile(self.io, self.path, .{});
        defer input.close(self.io);

        var buf: [64 * 1024]u8 = undefined;
        var lines: u64 = 0;
        var saw_byte = false;
        var last_byte: u8 = 0;
        var prev_cr = false;
        var in_quotes = false;

        while (true) {
            const n = input.readStreaming(self.io, &.{&buf}) catch |err| switch (err) {
                error.EndOfStream => 0,
                else => return err,
            };
            if (n == 0) break;
            saw_byte = true;
            for (buf[0..n]) |b| {
                if (b == '"') {
                    in_quotes = !in_quotes;
                    prev_cr = false;
                } else if (!in_quotes and b == '\n') {
                    if (!prev_cr) lines += 1;
                    prev_cr = false;
                } else if (!in_quotes and b == '\r') {
                    lines += 1;
                    prev_cr = true;
                } else {
                    prev_cr = false;
                }
                last_byte = b;
            }
        }

        if (!saw_byte) return 0;
        if (last_byte != '\n' and last_byte != '\r') lines += 1;
        return if (options.has_header and lines > 0) lines - 1 else lines;
    }
};

test "CsvReader counts data rows after header" {
    const path = "/var/folders/g7/4df4jppn3_x6yz0dhx66nf200000gn/T/opencode/zighouse-reader-test.csv";
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = "a,b\n1,2\n3,4\n" });
    defer std.Io.Dir.cwd().deleteFile(std.testing.io, path) catch {};

    var reader = CsvReader.init(std.testing.allocator, std.testing.io, path);
    try std.testing.expectEqual(@as(u64, 2), try reader.rowCount(.{ .has_header = true }));
}

test "CsvReader counts CR line endings" {
    const path = "/var/folders/g7/4df4jppn3_x6yz0dhx66nf200000gn/T/opencode/zighouse-reader-test-cr.csv";
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = "a,b\r1,2\r3,4\r" });
    defer std.Io.Dir.cwd().deleteFile(std.testing.io, path) catch {};

    var reader = CsvReader.init(std.testing.allocator, std.testing.io, path);
    try std.testing.expectEqual(@as(u64, 2), try reader.rowCount(.{ .has_header = true }));
}

test "CsvReader counts no-header rows and ignores quoted newlines" {
    const path = "/var/folders/g7/4df4jppn3_x6yz0dhx66nf200000gn/T/opencode/zighouse-reader-test-no-header.csv";
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = "1,\"a\nb\"\n2,c\n" });
    defer std.Io.Dir.cwd().deleteFile(std.testing.io, path) catch {};

    var reader = CsvReader.init(std.testing.allocator, std.testing.io, path);
    try std.testing.expectEqual(@as(u64, 2), try reader.rowCount(.{ .has_header = false }));
}
