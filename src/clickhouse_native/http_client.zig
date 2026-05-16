/// ClickHouse HTTP INSERT client.
///
/// Sends batched INSERT requests to CH HTTP interface (port 8123).
/// Data format: RowBinary (row-wise binary encoding, no header).
/// Compression: gzip via std.compress.flate (Zig stdlib, zero deps).
///
/// Protocol:
///   POST http://<host>:<port>/?query=INSERT+INTO+<db>.<table>+FORMAT+RowBinary
///   Authorization: Basic <base64(user:password)>
///   Content-Encoding: gzip
///   <gzip-compressed RowBinary body>
///
/// Usage:
///   var ins = try ChHttpInserter.init(allocator, io, opts);
///   defer ins.deinit();
///   // build RowBinary bytes, append to ins.raw_buf
///   try ins.appendBytes(row_bytes);
///   try ins.maybeFlush("my_table");  // auto-flush when batch_bytes reached
///   try ins.finish("my_table");      // flush remaining bytes

const std = @import("std");
const flate = std.compress.flate;

/// Connection options for the CH HTTP endpoint.
pub const Options = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 8123,
    database: []const u8 = "default",
    user: []const u8 = "default",
    password: []const u8 = "",
    /// Batch size in uncompressed bytes before an automatic flush.
    /// 8 MiB: good balance of memory vs HTTP overhead.
    batch_bytes: usize = 8 * 1024 * 1024,
};

/// Streaming INSERT writer for ClickHouse HTTP.
pub const ChHttpInserter = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    opts: Options,
    /// Uncompressed RowBinary accumulation buffer.
    raw_buf: std.ArrayList(u8),
    /// Total uncompressed bytes sent (for stats).
    total_raw_bytes: u64,
    /// Total HTTP flushes performed.
    total_flushes: u64,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, opts: Options) !ChHttpInserter {
        return .{
            .allocator = allocator,
            .io = io,
            .opts = opts,
            .raw_buf = .empty,
            .total_raw_bytes = 0,
            .total_flushes = 0,
        };
    }

    pub fn deinit(self: *ChHttpInserter) void {
        self.raw_buf.deinit(self.allocator);
    }

    /// Append raw (uncompressed) RowBinary bytes to the pending batch.
    pub fn appendBytes(self: *ChHttpInserter, bytes: []const u8) !void {
        try self.raw_buf.appendSlice(self.allocator, bytes);
    }

    /// Flush if the uncompressed buffer has reached opts.batch_bytes.
    pub fn maybeFlush(self: *ChHttpInserter, table: []const u8) !void {
        if (self.raw_buf.items.len >= self.opts.batch_bytes) {
            try self.flush(table);
        }
    }

    /// Compress and POST the current buffer. Clears raw_buf on success.
    pub fn flush(self: *ChHttpInserter, table: []const u8) !void {
        if (self.raw_buf.items.len == 0) return;
        const raw = self.raw_buf.items;
        self.total_raw_bytes += raw.len;

        // gzip compress into a heap-allocated output buffer.
        // Worst case output ≈ input + gzip header/footer (~30 bytes).
        // For already-binary data gzip rarely expands; add 2×window for safety.
        const out_cap = raw.len + flate.max_window_len + 64;
        const out_buf = try self.allocator.alloc(u8, out_cap);
        defer self.allocator.free(out_buf);

        var window: [flate.max_window_len]u8 = undefined;
        var out_w = std.Io.Writer.fixed(out_buf);
        var compressor = try flate.Compress.init(&out_w, &window, .gzip, flate.Compress.Options.fastest);
        try compressor.writer.writeAll(raw);
        try flate.Compress.finish(&compressor);
        const compressed = std.Io.Writer.buffered(&out_w);

        try self.postCompressed(table, compressed);
        self.raw_buf.items.len = 0;
        self.total_flushes += 1;
    }

    /// Final flush — send any remaining buffered data.
    pub fn finish(self: *ChHttpInserter, table: []const u8) !void {
        try self.flush(table);
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    /// POST gzip-compressed RowBinary to CH HTTP via curl subprocess.
    /// Using curl avoids Zig http.Client/Threaded IO lifecycle issues.
    fn postCompressed(self: *ChHttpInserter, table: []const u8, gz_body: []const u8) !void {
        // Write compressed body to a temp file for curl --data-binary @file
        const tmp_path = try std.fmt.allocPrint(self.allocator, "/tmp/zighouse_ch_insert_{d}.gz", .{
            std.c.getpid(),
        });
        defer self.allocator.free(tmp_path);

        // Write gzip body to temp file
        {
            var f = try std.Io.Dir.createFileAbsolute(self.io, tmp_path, .{ .truncate = true });
            defer f.close(self.io);
            try f.writeStreamingAll(self.io, gz_body);
        }
        defer std.Io.Dir.deleteFileAbsolute(self.io, tmp_path) catch {};

        // Build curl command
        const creds = try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{
            self.opts.user, self.opts.password,
        });
        defer self.allocator.free(creds);

        const url = try std.fmt.allocPrint(
            self.allocator,
            "http://{s}:{d}/?query=INSERT+INTO+{s}.{s}+FORMAT+RowBinary&async_insert=0",
            .{ self.opts.host, self.opts.port, self.opts.database, table },
        );
        defer self.allocator.free(url);

        const data_arg = try std.fmt.allocPrint(self.allocator, "@{s}", .{tmp_path});
        defer self.allocator.free(data_arg);

        // Use a simple process.run with curl
        var io_threaded: std.Io.Threaded = .init(self.allocator, .{});
        // NOTE: we do NOT call io_threaded.deinit() here — it deadlocks on macOS
        // in Zig 0.16 after subprocess execution. The thread pool is leaked, which
        // is safe for a CLI import tool (short-lived process).

        const result = try std.process.run(self.allocator, io_threaded.io(), .{
            .argv = &.{
                "curl",
                "--silent",
                "--show-error",
                "-X", "POST",
                "-u", creds,
                "-H", "Content-Encoding: gzip",
                "-H", "Content-Type: application/octet-stream",
                "--data-binary", data_arg,
                "-w", "\n__HTTP_STATUS__%{http_code}",
                url,
            },
        });
        defer self.allocator.free(result.stdout);
        defer self.allocator.free(result.stderr);

        // Parse HTTP status from stdout suffix
        const status_marker = "\n__HTTP_STATUS__";
        if (std.mem.lastIndexOf(u8, result.stdout, status_marker)) |pos| {
            const status_str = result.stdout[pos + status_marker.len ..];
            const status_code = std.fmt.parseInt(u16, std.mem.trim(u8, status_str, " \n\r"), 10) catch 0;
            if (status_code != 200) {
                const body = result.stdout[0..pos];
                std.debug.print("CH HTTP INSERT error: status={d} response={s}\n", .{
                    status_code, body[0..@min(body.len, 512)],
                });
                return error.ClickHouseInsertFailed;
            }
        } else if (result.term != .exited or result.term.exited != 0) {
            std.debug.print("CH curl error: {s}{s}\n", .{ result.stdout, result.stderr });
            return error.ClickHouseInsertFailed;
        }
    }
};
