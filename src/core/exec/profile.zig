//! Query profiling counters and environment-gated output.
const std = @import("std");

pub const ProfilePhase = enum {
    open,
    mmap,
    scan,
    filter,
    project,
    scatter,
    aggregate,
    merge,
    topk,
    output,
};

pub const QueryProfile = struct {
    enabled: bool = false,
    label: ?[]const u8 = null,
    started_ns: i128 = 0,
    scannable_path: u64 = 0,
    breaker_path: u64 = 0,
    next_chunks: u64 = 0,
    next_rows: u64 = 0,
    range_pushdowns: u64 = 0,
    raw_path_hits: u64 = 0,
    fallback_rows: u64 = 0,
    unsupported: u64 = 0,
    execute_errors: u64 = 0,
    open_ns: u64 = 0,
    mmap_ns: u64 = 0,
    scan_ns: u64 = 0,
    filter_ns: u64 = 0,
    project_ns: u64 = 0,
    scatter_ns: u64 = 0,
    aggregate_ns: u64 = 0,
    merge_ns: u64 = 0,
    topk_ns: u64 = 0,
    output_ns: u64 = 0,
    bytes_read: u64 = 0,
    bytes_mapped: u64 = 0,
    peak_groups: u64 = 0,
    hash_table_grows: u64 = 0,
    last_reason: ?[]const u8 = null,

    pub fn init() QueryProfile {
        const enabled = profileEnvEnabled();
        return .{
            .enabled = enabled,
            .started_ns = if (enabled) profileNowNs() else 0,
        };
    }

    pub fn recordReason(self: *QueryProfile, reason: []const u8) void {
        if (!self.enabled) return;
        self.last_reason = reason;
    }

    pub fn emit(self: *const QueryProfile) void {
        if (!self.enabled) return;
        const elapsed_ns = profileNowNs() - self.started_ns;
        const label = self.label orelse "";
        const reason = self.last_reason orelse "";
        std.debug.print(
            "zighouse_profile query=\"{s}\" elapsed_ms={d:.3} open_ms={d:.3} mmap_ms={d:.3} scan_ms={d:.3} filter_ms={d:.3} project_ms={d:.3} scatter_ms={d:.3} aggregate_ms={d:.3} merge_ms={d:.3} topk_ms={d:.3} output_ms={d:.3} bytes_read={d} bytes_mapped={d} peak_groups={d} hash_table_grows={d} scannable={d} breaker={d} next_chunks={d} next_rows={d} range_pushdowns={d} raw_hits={d} fallback_rows={d} unsupported={d} execute_errors={d} reason=\"{s}\"\n",
            .{
                label,
                nsToMs(elapsed_ns),
                nsToMs(self.open_ns),
                nsToMs(self.mmap_ns),
                nsToMs(self.scan_ns),
                nsToMs(self.filter_ns),
                nsToMs(self.project_ns),
                nsToMs(self.scatter_ns),
                nsToMs(self.aggregate_ns),
                nsToMs(self.merge_ns),
                nsToMs(self.topk_ns),
                nsToMs(self.output_ns),
                self.bytes_read,
                self.bytes_mapped,
                self.peak_groups,
                self.hash_table_grows,
                self.scannable_path,
                self.breaker_path,
                self.next_chunks,
                self.next_rows,
                self.range_pushdowns,
                self.raw_path_hits,
                self.fallback_rows,
                self.unsupported,
                self.execute_errors,
                reason,
            },
        );
    }

    pub fn timestamp(self: *const QueryProfile) i128 {
        return if (self.enabled) profileNowNs() else 0;
    }

    pub fn addElapsed(self: *QueryProfile, phase: ProfilePhase, started_ns: i128) void {
        if (!self.enabled or started_ns == 0) return;
        const elapsed = profileNowNs() - started_ns;
        if (elapsed <= 0) return;
        const elapsed_ns: u64 = @intCast(elapsed);
        switch (phase) {
            .open => self.open_ns +|= elapsed_ns,
            .mmap => self.mmap_ns +|= elapsed_ns,
            .scan => self.scan_ns +|= elapsed_ns,
            .filter => self.filter_ns +|= elapsed_ns,
            .project => self.project_ns +|= elapsed_ns,
            .scatter => self.scatter_ns +|= elapsed_ns,
            .aggregate => self.aggregate_ns +|= elapsed_ns,
            .merge => self.merge_ns +|= elapsed_ns,
            .topk => self.topk_ns +|= elapsed_ns,
            .output => self.output_ns +|= elapsed_ns,
        }
    }
};

fn nsToMs(ns: anytype) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

pub fn profileNowNs() i128 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec,
        else => return 0,
    }
}

pub fn profileEnvEnabled() bool {
    return envFlag("ZIGHOUSE_PROFILE") or envFlag("ZIGHOUSE_QUERY_PROFILE");
}

pub fn emitUnsupportedProfile(label: []const u8, reason: []const u8) void {
    if (!profileEnvEnabled()) return;
    std.debug.print(
        "zighouse_profile query=\"{s}\" elapsed_ms=0.000 scannable=0 breaker=0 next_chunks=0 next_rows=0 range_pushdowns=0 raw_hits=0 fallback_rows=0 unsupported=1 execute_errors=0 reason=\"{s}\"\n",
        .{ label, reason },
    );
}

fn envFlag(name: [:0]const u8) bool {
    const raw = std.c.getenv(name) orelse return false;
    const val = std.mem.span(raw);
    return !(val.len == 0 or
        std.mem.eql(u8, val, "0") or
        std.ascii.eqlIgnoreCase(val, "false") or
        std.ascii.eqlIgnoreCase(val, "off"));
}
