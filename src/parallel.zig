//! Parallel-execution primitives for ClickBench native queries.
//!
//! Designed around DuckDB's morsel-driven parallelism (Leis et al. 2014):
//! a single atomic cursor hands out fixed-size row chunks (morsels) to
//! worker threads, which keeps load balanced even on heterogeneous
//! Apple Silicon (P-core / E-core) without an explicit work-stealing
//! queue.
//!
//! v2: persistent thread pool that eliminates per-query thread spawn
//! overhead (~1ms/thread on macOS). Workers sleep on a condition variable
//! and wake when work is ready.

const std = @import("std");

/// DuckDB row-group default. Empirically: 60 vectors of 2048 tuples.
/// At 100M rows -> 813 morsels, ~100 morsels per worker at T=8.
pub const default_morsel_size: usize = 122_880;

/// Lock-free range partitioner. Workers call `next()` until null.
pub const MorselSource = struct {
    cursor: std.atomic.Value(usize),
    total: usize,
    morsel_size: usize,

    pub fn init(total: usize, morsel_size: usize) MorselSource {
        return .{
            .cursor = .init(0),
            .total = total,
            .morsel_size = morsel_size,
        };
    }

    pub const Morsel = struct { start: usize, end: usize };

    /// Atomically claim the next chunk. Returns null when the source is
    /// exhausted.
    pub fn next(self: *MorselSource) ?Morsel {
        const start = self.cursor.fetchAdd(self.morsel_size, .monotonic);
        if (start >= self.total) return null;
        const end = @min(start + self.morsel_size, self.total);
        return .{ .start = start, .end = end };
    }
};

/// Default worker count. On Apple Silicon we restrict to performance
/// cores (`hw.perflevel0.physicalcpu`) because efficiency-core stragglers
/// dominate wall-clock latency for short morsel workloads. Falls back to
/// `getCpuCount()` on other platforms. Capped at 16 to bound per-thread
/// state allocations (Q12 bitsets are 140 MB each; T=16 would be 2.2 GB).
pub fn defaultThreads() usize {
    if (@import("builtin").os.tag == .macos) {
        var p_cores: c_int = 0;
        var len: usize = @sizeOf(c_int);
        const rc = std.c.sysctlbyname("hw.perflevel0.physicalcpu", &p_cores, &len, null, 0);
        if (rc == 0 and p_cores > 0) {
            return @min(@as(usize, @intCast(p_cores)), 16);
        }
    }
    const n = std.Thread.getCpuCount() catch 4;
    return @min(n, 16);
}

/// Override the default thread count via environment-style setter
/// for one-off experiments. Returns `defaultThreads()` if `n == 0`.
pub fn pickThreads(n: usize) usize {
    if (n == 0) return defaultThreads();
    return @min(n, 16);
}

/// Available physical RAM in MiB. Used by Q12 to decide whether to
/// downscale T (140 MB * T bitsets can OOM on 8 GB Macs).
/// Returns null if detection failed; callers should pick a safe default.
pub fn availableMemoryMiB() ?u64 {
    if (@import("builtin").os.tag != .macos) return null;
    var size: u64 = 0;
    var len: usize = @sizeOf(u64);
    const rc = std.c.sysctlbyname("hw.memsize", &size, &len, null, 0);
    if (rc != 0) return null;
    return size / (1024 * 1024);
}

// ── Persistent Thread Pool ────────────────────────────────────────────────────
//
// Workers sleep on `pool.cv_work` until `pool.task` is non-null.
// After finishing, they decrement `pool.n_busy` and signal `pool.cv_done`.
// The calling thread runs worker[0] inline and waits on `pool.cv_done`.
//
// Uses std.c pthread primitives directly because std.Thread.Mutex /
// std.Thread.Condition were removed in Zig 0.16 (moved to std.Io which
// requires an async Io context).

const c = std.c;

const WorkFn = *const fn (*anyopaque, *MorselSource) void;

const Pool = struct {
    mutex:    c.pthread_mutex_t = c.PTHREAD_MUTEX_INITIALIZER,
    cv_work:  c.pthread_cond_t  = c.PTHREAD_COND_INITIALIZER,
    cv_done:  c.pthread_cond_t  = c.PTHREAD_COND_INITIALIZER,
    work_fn:  ?WorkFn = null,
    ctxs_raw: []usize = &.{},      // raw pointers to worker Ctx structs
    source:   ?*MorselSource = null,
    n_busy:   usize = 0,
    quit:     bool = false,
    inited:   bool = false,
    threads:  [16]std.Thread = undefined,
    n_threads: usize = 0,
    /// Monotonically increasing dispatch counter. Workers only execute a
    /// dispatch if their local gen is behind the pool gen.
    dispatch_gen: u64 = 0,
    worker_gen:   [16]u64 = [_]u64{0} ** 16,
};

var global_pool: Pool = .{};
var pool_init_mutex: c.pthread_mutex_t = c.PTHREAD_MUTEX_INITIALIZER;
// Arena for ctx pointers array in the pool (never freed).
var pool_alloc_buf: [256 * @sizeOf(usize)]u8 = undefined;
var pool_fba: std.heap.FixedBufferAllocator = undefined;

fn workerLoop(worker_idx: usize) void {
    const pool = &global_pool;
    while (true) {
        _ = c.pthread_mutex_lock(&pool.mutex);
        // Wait until there is a new dispatch generation for us, or quit.
        while (pool.worker_gen[worker_idx] >= pool.dispatch_gen and !pool.quit) {
            _ = c.pthread_cond_wait(&pool.cv_work, &pool.mutex);
        }
        if (pool.quit) { _ = c.pthread_mutex_unlock(&pool.mutex); return; }
        const fn_ptr = pool.work_fn.?;
        const ctx_raw = pool.ctxs_raw[worker_idx];
        const src     = pool.source.?;
        pool.worker_gen[worker_idx] = pool.dispatch_gen; // mark as consumed
        _ = c.pthread_mutex_unlock(&pool.mutex);

        fn_ptr(@ptrFromInt(ctx_raw), src);

        _ = c.pthread_mutex_lock(&pool.mutex);
        pool.n_busy -= 1;
        if (pool.n_busy == 0) _ = c.pthread_cond_signal(&pool.cv_done);
        _ = c.pthread_mutex_unlock(&pool.mutex);
    }
}

/// Ensure the global thread pool is initialised with `n_workers - 1` background
/// threads. Safe to call multiple times; subsequent calls are no-ops.
fn ensurePool(allocator: std.mem.Allocator, n_workers: usize) !void {
    _ = allocator;
    if (n_workers <= 1) return;
    _ = c.pthread_mutex_lock(&pool_init_mutex);
    defer _ = c.pthread_mutex_unlock(&pool_init_mutex);
    const pool = &global_pool;
    if (pool.inited) return;
    pool_fba = std.heap.FixedBufferAllocator.init(&pool_alloc_buf);
    const need = n_workers - 1;
    pool.ctxs_raw = try pool_fba.allocator().alloc(usize, need);
    for (0..need) |t| {
        pool.threads[t] = try std.Thread.spawn(.{}, workerLoop, .{t});
    }
    pool.n_threads = need;
    pool.inited = true;
}

/// Spawn `ctxs.len - 1` threads, each running `worker(&ctxs[t], &source)`.
/// The final ctx is run on the calling thread. All threads loop pulling
/// morsels from `source` until exhaustion.
///
/// `worker` signature: `fn(*Ctx, *MorselSource) void`.
pub fn parallelFor(
    allocator: std.mem.Allocator,
    comptime Ctx: type,
    comptime worker: fn (*Ctx, *MorselSource) void,
    ctxs: []Ctx,
    source: *MorselSource,
) !void {
    if (ctxs.len == 0) return;
    if (ctxs.len == 1) {
        worker(&ctxs[0], source);
        return;
    }

    // Try to use persistent pool for background workers.
    try ensurePool(allocator, ctxs.len);

    // Type-erase worker function.
    const EraseCtx = struct {
        fn run(raw: *anyopaque, src: *MorselSource) void {
            worker(@alignCast(@ptrCast(raw)), src);
        }
    };

    const pool = &global_pool;
    const n_bg = ctxs.len - 1; // background workers (1 runs inline)

    if (pool.inited and pool.n_threads >= n_bg) {
        // Store raw pointers for background workers (indices 1..n).
        for (0..n_bg) |t| {
            pool.ctxs_raw[t] = @intFromPtr(&ctxs[t + 1]);
        }

        // Activate workers.
        _ = c.pthread_mutex_lock(&pool.mutex);
        pool.work_fn = EraseCtx.run;
        pool.source  = source;
        pool.n_busy  = n_bg;
        pool.dispatch_gen += 1;
        _ = c.pthread_mutex_unlock(&pool.mutex);
        _ = c.pthread_cond_broadcast(&pool.cv_work);

        // Run worker[0] on the calling thread.
        worker(&ctxs[0], source);

        // Wait for all background workers to finish.
        _ = c.pthread_mutex_lock(&pool.mutex);
        while (pool.n_busy > 0) _ = c.pthread_cond_wait(&pool.cv_done, &pool.mutex);
        pool.work_fn = null;
        _ = c.pthread_mutex_unlock(&pool.mutex);
    } else {
        // Fallback: spawn threads the old way.
        const threads = try allocator.alloc(std.Thread, ctxs.len - 1);
        defer allocator.free(threads);
        for (0..ctxs.len - 1) |t| {
            threads[t] = try std.Thread.spawn(.{}, worker, .{ &ctxs[t + 1], source });
        }
        worker(&ctxs[0], source);
        for (threads) |th| th.join();
    }
}

/// Spawn `n_workers - 1` threads to run `worker(t)` for t in [0, n_workers).
/// The last task runs inline. Use this when the work is sliced by index
/// rather than by morsel (e.g. parallel OR-merge of K bitsets across
/// T threads).
pub fn parallelIndices(
    allocator: std.mem.Allocator,
    comptime Ctx: type,
    comptime worker: fn (*Ctx, usize) void,
    ctx: *Ctx,
    n_workers: usize,
) !void {
    if (n_workers == 0) return;
    if (n_workers == 1) {
        worker(ctx, 0);
        return;
    }
    const threads = try allocator.alloc(std.Thread, n_workers - 1);
    defer allocator.free(threads);
    for (0..n_workers - 1) |t| {
        threads[t] = try std.Thread.spawn(.{}, worker, .{ ctx, t + 1 });
    }
    worker(ctx, 0);
    for (threads) |th| th.join();
}

test "MorselSource hands out non-overlapping chunks" {
    const t = std.testing;
    var src: MorselSource = .init(1000, 256);
    var seen: usize = 0;
    while (src.next()) |m| {
        try t.expect(m.start < m.end);
        try t.expect(m.end <= 1000);
        seen += m.end - m.start;
    }
    try t.expectEqual(@as(usize, 1000), seen);
}

test "MorselSource is empty at total=0" {
    var src: MorselSource = .init(0, 256);
    try std.testing.expect(src.next() == null);
}

const SumCtx = struct {
    values: []const u32,
    out: u64 = 0,
};

fn sumWorker(ctx: *SumCtx, source: *MorselSource) void {
    var acc: u64 = 0;
    while (source.next()) |m| {
        for (ctx.values[m.start..m.end]) |v| acc += v;
    }
    ctx.out = acc;
}

test "parallelFor sums a known sequence" {
    const t = std.testing;
    const N: usize = 100_000;
    const buf = try t.allocator.alloc(u32, N);
    defer t.allocator.free(buf);
    for (buf, 0..) |*v, i| v.* = @intCast(i % 256);

    var src: MorselSource = .init(N, 4096);
    const ctxs = try t.allocator.alloc(SumCtx, 4);
    defer t.allocator.free(ctxs);
    for (ctxs) |*ctx| ctx.* = .{ .values = buf };

    try parallelFor(t.allocator, SumCtx, sumWorker, ctxs, &src);

    var total: u64 = 0;
    for (ctxs) |ctx| total += ctx.out;

    var expected: u64 = 0;
    for (buf) |v| expected += v;
    try t.expectEqual(expected, total);
}

test "defaultThreads is sane" {
    const n = defaultThreads();
    try std.testing.expect(n >= 1);
    try std.testing.expect(n <= 16);
}

