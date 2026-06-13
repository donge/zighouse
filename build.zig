const std = @import("std");

/// Build lz4 as a static library compiled from vendored C sources.
/// Returns the compile step so callers can link against it.
fn buildLz4(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) *std.Build.Step.Compile {
    const lz4_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    lz4_mod.addIncludePath(b.path("vendor/lz4"));
    lz4_mod.addCSourceFiles(.{
        .root = b.path("vendor/lz4"),
        .files = &.{ "lz4.c", "lz4hc.c", "lz4frame.c", "xxhash.c" },
        .flags = &.{ "-O2", "-fPIC" },
    });
    lz4_mod.link_libc = true;
    const lib = b.addLibrary(.{
        .name = "lz4",
        .root_module = lz4_mod,
        .linkage = .static,
    });
    return lib;
}

/// Build zstd as a static library compiled from vendored C sources.
fn buildZstd(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) *std.Build.Step.Compile {
    const zstd_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    zstd_mod.addIncludePath(b.path("vendor/zstd"));
    zstd_mod.addIncludePath(b.path("vendor/zstd/common"));
    zstd_mod.addIncludePath(b.path("vendor/zstd/compress"));
    zstd_mod.addIncludePath(b.path("vendor/zstd/decompress"));
    const flags: []const []const u8 = &.{ "-O2", "-fPIC", "-DXXH_NAMESPACE=ZSTD_" };
    zstd_mod.addCSourceFiles(.{
        .root = b.path("vendor/zstd/common"),
        .files = &.{
            "debug.c", "entropy_common.c", "error_private.c",
            "fse_decompress.c", "pool.c", "threading.c", "xxhash.c",
            "zstd_common.c",
        },
        .flags = flags,
    });
    zstd_mod.addCSourceFiles(.{
        .root = b.path("vendor/zstd/compress"),
        .files = &.{
            "fse_compress.c", "hist.c", "huf_compress.c",
            "zstd_compress.c", "zstd_compress_literals.c",
            "zstd_compress_sequences.c", "zstd_compress_superblock.c",
            "zstd_double_fast.c", "zstd_fast.c", "zstd_lazy.c",
            "zstd_ldm.c", "zstd_opt.c", "zstdmt_compress.c",
            "zstd_preSplit.c",
        },
        .flags = flags,
    });
    zstd_mod.addCSourceFiles(.{
        .root = b.path("vendor/zstd/decompress"),
        .files = &.{
            "huf_decompress.c", "zstd_ddict.c",
            "zstd_decompress.c", "zstd_decompress_block.c",
        },
        .flags = flags,
    });
    // x86-64 assembly fast path for HUF decompression
    zstd_mod.addAssemblyFile(b.path("vendor/zstd/decompress/huf_decompress_amd64.S"));
    zstd_mod.link_libc = true;
    const lib = b.addLibrary(.{
        .name = "zstd",
        .root_module = zstd_mod,
        .linkage = .static,
    });
    return lib;
}

/// Link lz4 into `mod`. When `vendored_lib` is non-null, uses that pre-built
/// static library (cross-compile compatible). When `static` is true without
/// vendored lib, adds the host .a archive. Otherwise links the system dylib.
fn linkLz4(mod: *std.Build.Module, include: []const u8, lib: []const u8, static_path: []const u8, static: bool) void {
    mod.addIncludePath(.{ .cwd_relative = include });
    if (static) {
        mod.addObjectFile(.{ .cwd_relative = static_path });
    } else {
        mod.addLibraryPath(.{ .cwd_relative = lib });
        mod.addRPath(.{ .cwd_relative = lib });
        mod.linkSystemLibrary("lz4", .{});
    }
}

fn linkLz4Vendored(mod: *std.Build.Module, lz4_lib: *std.Build.Step.Compile) void {
    mod.addIncludePath(lz4_lib.step.owner.path("vendor/lz4"));
    mod.linkLibrary(lz4_lib);
}

/// Link zstd into `mod`. When `static` is true, adds the .a archive directly.
fn linkZstd(mod: *std.Build.Module, include: []const u8, lib: []const u8, static_path: []const u8, static: bool) void {
    mod.addIncludePath(.{ .cwd_relative = include });
    if (static) {
        mod.addObjectFile(.{ .cwd_relative = static_path });
    } else {
        mod.addLibraryPath(.{ .cwd_relative = lib });
        mod.addRPath(.{ .cwd_relative = lib });
        mod.linkSystemLibrary("zstd", .{});
    }
}

fn linkZstdVendored(mod: *std.Build.Module, zstd_lib: *std.Build.Step.Compile) void {
    mod.addIncludePath(zstd_lib.step.owner.path("vendor/zstd"));
    mod.linkLibrary(zstd_lib);
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    // Default to ReleaseFast: every benchmark in this repo measures hot loops
    // over 100M-row hot columns, where Debug builds are 2.5-7x slower because
    // of integer-overflow and bounds checks. Override with `-Doptimize=Debug`
    // (or any other mode) to opt out, e.g. when working on stack traces.
    //
    // We bypass `standardOptimizeOption`'s `preferred_optimize_mode` because
    // that only takes effect when the user passes `-Drelease=true`; we want
    // ReleaseFast for the bare `zig build` invocation as well.
    const optimize: std.builtin.OptimizeMode = b.option(
        std.builtin.OptimizeMode,
        "optimize",
        "Prioritize performance, safety, or binary size (default: ReleaseFast)",
    ) orelse .ReleaseFast;
    // -Dstatic-libs=true: link lz4 and zstd as static archives instead of dylibs.
    // Default false to preserve current dev behaviour; set true for release builds.
    const static_libs = b.option(bool, "static-libs", "Statically link lz4 and zstd") orelse false;
    // Always use vendored C sources for lz4/zstd when static-libs is on.
    // This ensures the build works on any machine regardless of system libraries
    // and avoids Homebrew-specific paths on Linux CI.
    const use_vendored = static_libs;
    const zstd_prefix_early = b.option([]const u8, "zstd-prefix", "ZSTD installation prefix") orelse "/opt/homebrew/opt/zstd";
    const zstd_include_early = b.fmt("{s}/include", .{zstd_prefix_early});
    const zstd_lib_early = b.fmt("{s}/lib", .{zstd_prefix_early});

    // Build vendored zstd early so unit_tests / generic_sql_tests can use it.
    const vendored_zstd_early = if (use_vendored) buildZstd(b, target, optimize) else null;
    const ZstdCtxType = struct {
        zstd_include: []const u8, zstd_lib: []const u8, zstd_static_path: []const u8,
        static_libs: bool, vendored: ?*std.Build.Step.Compile,
        fn link(self: @This(), mod: *std.Build.Module) void {
            if (self.vendored) |vl| { linkZstdVendored(mod, vl); }
            else { linkZstd(mod, self.zstd_include, self.zstd_lib, self.zstd_static_path, self.static_libs); }
        }
    };
    const zstdctx_early = ZstdCtxType{
        .zstd_include = zstd_include_early, .zstd_lib = zstd_lib_early,
        .zstd_static_path = b.fmt("{s}/lib/libzstd.a", .{zstd_prefix_early}),
        .static_libs = static_libs, .vendored = vendored_zstd_early,
    };
    const install_bench_tools = b.option(bool, "bench-tools", "Install benchmark helper executables") orelse true;

    // Shared schema module (used by clickhouse_format test targets)
    const schema_mod = b.createModule(.{
        .root_source_file = b.path("src/schema.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Shared CSV parser module (RFC 4180).
    const csv_mod = b.createModule(.{
        .root_source_file = b.path("src/csv.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "zighouse",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    if (b.option(bool, "strip", "Strip debug symbols from installed executable") orelse false) {
        exe.root_module.strip = true;
    }
    exe.root_module.link_libc = true;

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run zighouse");
    run_step.dependOn(&run_cmd.step);

    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    unit_tests.root_module.link_libc = true;
    zstdctx_early.link(unit_tests.root_module);
    const test_cmd = b.addRunArtifact(unit_tests);

    const simd_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/simd.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const simd_test_cmd = b.addRunArtifact(simd_tests);

    const parallel_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/parallel.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const parallel_test_cmd = b.addRunArtifact(parallel_tests);

    const hashmap_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/hashmap.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const hashmap_test_cmd = b.addRunArtifact(hashmap_tests);


    const generic_sql_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/generic_sql.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    generic_sql_tests.root_module.link_libc = true;
    zstdctx_early.link(generic_sql_tests.root_module);
    const generic_sql_test_cmd = b.addRunArtifact(generic_sql_tests);

    const lowcard_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lowcard.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const lowcard_test_cmd = b.addRunArtifact(lowcard_tests);

    const parquet_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/parquet.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    zstdctx_early.link(parquet_tests.root_module);
    const parquet_test_cmd = b.addRunArtifact(parquet_tests);

    const schema_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/schema.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const schema_test_cmd = b.addRunArtifact(schema_tests);


    const schema_infer_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/schema_infer.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const schema_infer_test_cmd = b.addRunArtifact(schema_infer_tests);

    const generic_store_mod = b.createModule(.{
        .root_source_file = b.path("src/generic_store.zig"),
        .target   = target,
        .optimize = optimize,
    });
    generic_store_mod.addImport("schema", schema_mod);

    const parallel_mod = b.createModule(.{
        .root_source_file = b.path("src/parallel.zig"),
        .target   = target,
        .optimize = optimize,
    });

    const hashmap_mod = b.createModule(.{
        .root_source_file = b.path("src/hashmap.zig"),
        .target   = target,
        .optimize = optimize,
    });

    const generic_store_tests = b.addTest(.{ .root_module = generic_store_mod });
    generic_store_tests.root_module.link_libc = true;
    const generic_store_test_cmd = b.addRunArtifact(generic_store_tests);

    // parquet_mod: used by loader, schema_infer, native, main, and generic_executor
    const parquet_mod = b.createModule(.{
        .root_source_file = b.path("src/parquet.zig"),
        .target = target,
        .optimize = optimize,
    });
    zstdctx_early.link(parquet_mod);

    // ── sql_parser module (native Zig SQL parser, no DuckDB dependency) ────────
    const sql_parser_mod = b.createModule(.{
        .root_source_file = b.path("src/sql/sql_parser.zig"),
        .target = target,
        .optimize = optimize,
    });

    // generic_sql_mod: named module for generic_executor and ingest_server
    const generic_sql_mod = b.createModule(.{
        .root_source_file = b.path("src/generic_sql.zig"),
        .target = target,
        .optimize = optimize,
    });
    generic_sql_mod.addImport("sql_parser", sql_parser_mod);
    generic_sql_mod.link_libc = true;

    // Wire sql_parser_mod into test targets
    generic_sql_tests.root_module.addImport("sql_parser", sql_parser_mod);
    unit_tests.root_module.addImport("sql_parser", sql_parser_mod);

    // ── clickhouse_format tests ─────────────────────────────────────────────
    const lz4_prefix = b.option([]const u8, "lz4-prefix", "LZ4 installation prefix") orelse "/opt/homebrew/opt/lz4";
    const lz4_include = b.fmt("{s}/include", .{lz4_prefix});
    const lz4_lib = b.fmt("{s}/lib", .{lz4_prefix});
    const lz4_static_path = b.fmt("{s}/lib/liblz4.a", .{lz4_prefix});
    // zstd paths already declared above as zstd_prefix_early / zstd_include_early / zstd_lib_early
    const zstd_include = zstd_include_early;
    const zstd_lib = zstd_lib_early;
    const zstd_static_path = b.fmt("{s}/lib/libzstd.a", .{zstd_prefix_early});

    // When cross-compiling with static-libs, build lz4/zstd from vendored C sources.
    const vendored_lz4 = if (use_vendored) buildLz4(b, target, optimize) else null;
    const vendored_zstd = if (use_vendored) buildZstd(b, target, optimize) else null;

    // Dispatch helpers — use vendored libs when cross-compiling, else system path.
    const lz4ctx = struct {
        lz4_include: []const u8,
        lz4_lib: []const u8,
        lz4_static_path: []const u8,
        static_libs: bool,
        vendored: ?*std.Build.Step.Compile,
        fn link(self: @This(), mod: *std.Build.Module) void {
            if (self.vendored) |vl| {
                linkLz4Vendored(mod, vl);
            } else {
                linkLz4(mod, self.lz4_include, self.lz4_lib, self.lz4_static_path, self.static_libs);
            }
        }
    }{ .lz4_include = lz4_include, .lz4_lib = lz4_lib, .lz4_static_path = lz4_static_path, .static_libs = static_libs, .vendored = vendored_lz4 };

    const zstdctx = struct {
        zstd_include: []const u8,
        zstd_lib: []const u8,
        zstd_static_path: []const u8,
        static_libs: bool,
        vendored: ?*std.Build.Step.Compile,
        fn link(self: @This(), mod: *std.Build.Module) void {
            if (self.vendored) |vl| {
                linkZstdVendored(mod, vl);
            } else {
                linkZstd(mod, self.zstd_include, self.zstd_lib, self.zstd_static_path, self.static_libs);
            }
        }
    }{ .zstd_include = zstd_include, .zstd_lib = zstd_lib, .zstd_static_path = zstd_static_path, .static_libs = static_libs, .vendored = vendored_zstd };
    const ch_block_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clickhouse_format/block.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    ch_block_tests.root_module.link_libc = true;
    lz4ctx.link(ch_block_tests.root_module);
    zstdctx.link(ch_block_tests.root_module);
    const ch_block_test_cmd = b.addRunArtifact(ch_block_tests);

    // types.zig — no lz4 dependency
    const ch_types_mod = b.createModule(.{
        .root_source_file = b.path("src/clickhouse_format/types.zig"),
        .target = target,
        .optimize = optimize,
    });
    ch_types_mod.addImport("schema", schema_mod);
    const ch_types_tests = b.addTest(.{
        .root_module = ch_types_mod,
    });
    const ch_types_test_cmd = b.addRunArtifact(ch_types_tests);

    // columns_txt.zig
    const ch_columns_txt_mod = b.createModule(.{
        .root_source_file = b.path("src/clickhouse_format/columns_txt.zig"),
        .target = target,
        .optimize = optimize,
    });
    ch_columns_txt_mod.addImport("schema", schema_mod);
    ch_columns_txt_mod.addImport("types", ch_types_mod);
    const ch_columns_txt_tests = b.addTest(.{
        .root_module = ch_columns_txt_mod,
    });
    const ch_columns_txt_test_cmd = b.addRunArtifact(ch_columns_txt_tests);

    // count_txt.zig
    const ch_count_txt_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clickhouse_format/count_txt.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const ch_count_txt_test_cmd = b.addRunArtifact(ch_count_txt_tests);

    // marks.zig
    const ch_marks_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clickhouse_format/marks.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const ch_marks_test_cmd = b.addRunArtifact(ch_marks_tests);

    // primary_idx.zig
    const ch_primary_idx_mod = b.createModule(.{
        .root_source_file = b.path("src/clickhouse_format/primary_idx.zig"),
        .target = target,
        .optimize = optimize,
    });
    ch_primary_idx_mod.addImport("schema", schema_mod);
    ch_primary_idx_mod.addImport("types", ch_types_mod);
    const ch_primary_idx_tests = b.addTest(.{
        .root_module = ch_primary_idx_mod,
    });
    const ch_primary_idx_test_cmd = b.addRunArtifact(ch_primary_idx_tests);

    // checksums.zig — needs lz4 + zstd (transitively via block.zig)
    const ch_checksums_mod = b.createModule(.{
        .root_source_file = b.path("src/clickhouse_format/checksums.zig"),
        .target = target,
        .optimize = optimize,
    });
    ch_checksums_mod.link_libc = true;
    lz4ctx.link(ch_checksums_mod);
    zstdctx.link(ch_checksums_mod);
    const ch_checksums_tests = b.addTest(.{
        .root_module = ch_checksums_mod,
    });
    const ch_checksums_test_cmd = b.addRunArtifact(ch_checksums_tests);

    // string_codec.zig
    const ch_string_codec_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clickhouse_format/string_codec.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const ch_string_codec_test_cmd = b.addRunArtifact(ch_string_codec_tests);

    // part.zig — needs lz4 + zstd + schema + all sub-modules
    const ch_part_mod = b.createModule(.{
        .root_source_file = b.path("src/clickhouse_format/part.zig"),
        .target = target,
        .optimize = optimize,
    });
    ch_part_mod.link_libc = true;
    lz4ctx.link(ch_part_mod);
    zstdctx.link(ch_part_mod);
    ch_part_mod.addImport("schema", schema_mod);
    ch_part_mod.addImport("types", ch_types_mod);
    const ch_part_tests = b.addTest(.{
        .root_module = ch_part_mod,
    });
    const ch_part_test_cmd = b.addRunArtifact(ch_part_tests);

    // ── loader module ────────────────────────────────────────────────────────
    const loader_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/loader.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    loader_tests.root_module.link_libc = true;
    loader_tests.root_module.addImport("parquet", parquet_mod);
    loader_tests.root_module.addImport("generic_store", generic_store_mod);
    const loader_test_cmd = b.addRunArtifact(loader_tests);

    // ── Wire schema_mod + ch_part_mod + lz4 + zstd into exe and all test targets ──
    // All src/ files now use @import("schema") named module. Inject schema_mod
    // into every target whose root (or transitive imports) uses @import("schema").
    const lz4_targets = [_]*std.Build.Module{
        exe.root_module,
        unit_tests.root_module,
        loader_tests.root_module,
    };
    for (lz4_targets) |mod| {
        mod.addImport("schema", schema_mod);
        mod.addImport("ch_part", ch_part_mod);
        lz4ctx.link(mod);
        zstdctx.link(mod);
    }
    schema_infer_tests.root_module.addImport("schema", schema_mod);
    schema_infer_tests.root_module.addImport("parquet", parquet_mod);
    generic_store_tests.root_module.addImport("schema", schema_mod);
    exe.root_module.addImport("generic_store", generic_store_mod);
    unit_tests.root_module.addImport("generic_store", generic_store_mod);

    // ── ingest module tests ─────────────────────────────────────────────────
    const type_mapping_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/type_mapping.zig"),
        .target = target,
        .optimize = optimize,
    });
    type_mapping_mod.addImport("schema", schema_mod);
    const type_mapping_tests = b.addTest(.{ .root_module = type_mapping_mod });
    const type_mapping_test_cmd = b.addRunArtifact(type_mapping_tests);

    const row_binary_decoder_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/row_binary_decoder.zig"),
        .target = target,
        .optimize = optimize,
    });
    row_binary_decoder_mod.addImport("schema", schema_mod);
    row_binary_decoder_mod.addImport("type_mapping", type_mapping_mod);
    const row_binary_decoder_tests = b.addTest(.{ .root_module = row_binary_decoder_mod });
    const row_binary_decoder_test_cmd = b.addRunArtifact(row_binary_decoder_tests);

    const schema_config_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/schema_config.zig"),
        .target = target,
        .optimize = optimize,
    });
    schema_config_mod.addImport("schema", schema_mod);
    schema_config_mod.addImport("type_mapping", type_mapping_mod);
    const schema_config_tests = b.addTest(.{ .root_module = schema_config_mod });
    const schema_config_test_cmd = b.addRunArtifact(schema_config_tests);

    const part_writer_session_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/part_writer_session.zig"),
        .target = target,
        .optimize = optimize,
    });
    part_writer_session_mod.addImport("schema", schema_mod);
    part_writer_session_mod.addImport("ch_part", ch_part_mod);
    part_writer_session_mod.addImport("row_binary_decoder", row_binary_decoder_mod);
    part_writer_session_mod.link_libc = true;
    lz4ctx.link(part_writer_session_mod);
    const part_writer_session_tests = b.addTest(.{ .root_module = part_writer_session_mod });
    const part_writer_session_test_cmd = b.addRunArtifact(part_writer_session_tests);

    const tcp_server_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/tcp_server.zig"),
        .target = target,
        .optimize = optimize,
    });
    tcp_server_mod.addImport("schema", schema_mod);
    tcp_server_mod.addImport("schema_config", schema_config_mod);
    tcp_server_mod.addImport("row_binary_decoder", row_binary_decoder_mod);
    tcp_server_mod.addImport("part_writer_session", part_writer_session_mod);
    tcp_server_mod.link_libc = true;
    lz4ctx.link(tcp_server_mod);

    const schema_persist_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/schema_persist.zig"),
        .target = target,
        .optimize = optimize,
    });
    schema_persist_mod.addImport("schema", schema_mod);
    schema_persist_mod.addImport("schema_config", schema_config_mod);
    const schema_persist_tests = b.addTest(.{ .root_module = schema_persist_mod });
    const schema_persist_test_cmd = b.addRunArtifact(schema_persist_tests);
    tcp_server_mod.addImport("schema_persist", schema_persist_mod);

    const part_scanner_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/part_scanner.zig"),
        .target = target,
        .optimize = optimize,
    });
    const part_scanner_tests = b.addTest(.{ .root_module = part_scanner_mod });
    const part_scanner_test_cmd = b.addRunArtifact(part_scanner_tests);

    const native_block_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/native_block.zig"),
        .target = target,
        .optimize = optimize,
    });
    const native_block_tests = b.addTest(.{ .root_module = native_block_mod });
    const native_block_test_cmd = b.addRunArtifact(native_block_tests);

    const ddl_parser_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/ddl_parser.zig"),
        .target = target,
        .optimize = optimize,
    });
    ddl_parser_mod.addImport("schema", schema_mod);
    ddl_parser_mod.addImport("schema_config", schema_config_mod);
    ddl_parser_mod.addImport("type_mapping", type_mapping_mod);
    tcp_server_mod.addImport("ddl_parser", ddl_parser_mod);
    const ddl_parser_tests = b.addTest(.{ .root_module = ddl_parser_mod });
    const ddl_parser_test_cmd = b.addRunArtifact(ddl_parser_tests);

    // ── mv_parse module (CREATE MATERIALIZED VIEW DDL parser) ─────────────────
    const mv_parse_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/mv_parse.zig"),
        .target = target,
        .optimize = optimize,
    });
    const mv_parse_tests = b.addTest(.{ .root_module = mv_parse_mod });
    const mv_parse_test_cmd = b.addRunArtifact(mv_parse_tests);

    // ── mv_persist module (MV metadata save/load) ─────────────────────────────
    const mv_persist_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/mv_persist.zig"),
        .target = target,
        .optimize = optimize,
    });
    mv_persist_mod.addImport("mv_parse", mv_parse_mod);

    const ingest_server_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/server.zig"),
        .target = target,
        .optimize = optimize,
    });
    ingest_server_mod.addImport("schema", schema_mod);
    ingest_server_mod.addImport("schema_config", schema_config_mod);
    ingest_server_mod.addImport("schema_persist", schema_persist_mod);
    ingest_server_mod.addImport("part_scanner", part_scanner_mod);
    ingest_server_mod.addImport("row_binary_decoder", row_binary_decoder_mod);
    ingest_server_mod.addImport("part_writer_session", part_writer_session_mod);
    ingest_server_mod.addImport("generic_sql", generic_sql_mod);
    ingest_server_mod.addImport("ddl_parser", ddl_parser_mod);
    ingest_server_mod.addImport("mv_parse", mv_parse_mod);
    ingest_server_mod.addImport("mv_persist", mv_persist_mod);
    ingest_server_mod.addImport("native_block", native_block_mod);
    ingest_server_mod.addImport("csv", csv_mod);
    ingest_server_mod.addImport("tcp_server", tcp_server_mod);
    ingest_server_mod.link_libc = true;
    lz4ctx.link(ingest_server_mod);
    const ingest_server_tests = b.addTest(.{ .root_module = ingest_server_mod });
    const ingest_server_test_cmd = b.addRunArtifact(ingest_server_tests);

    // Wire ingest modules into main exe and unit tests
     exe.root_module.addImport("ingest_server", ingest_server_mod);
     exe.root_module.addImport("ingest_schema_config", schema_config_mod);
     exe.root_module.addImport("ingest_schema_persist", schema_persist_mod);
     exe.root_module.addImport("generic_sql", generic_sql_mod);
    exe.root_module.addImport("parquet", parquet_mod);
    exe.root_module.addImport("mv_persist", mv_persist_mod);
    exe.root_module.addImport("mv_parse", mv_parse_mod);

    // ── compactor module ───────────────────────────────────────────────────────
    const compactor_mod = b.createModule(.{
        .root_source_file = b.path("src/compactor.zig"),
        .target = target,
        .optimize = optimize,
    });
    compactor_mod.addImport("schema", schema_mod);
    compactor_mod.addImport("schema_config", schema_config_mod);
    compactor_mod.addImport("schema_persist", schema_persist_mod);
    compactor_mod.addImport("ch_part", ch_part_mod);
    compactor_mod.addImport("part_scanner", part_scanner_mod);
    compactor_mod.addImport("mv_parse", mv_parse_mod);
    compactor_mod.addImport("mv_persist", mv_persist_mod);
    compactor_mod.link_libc = true;
    lz4ctx.link(compactor_mod);
    zstdctx.link(compactor_mod);
    exe.root_module.addImport("compactor", compactor_mod);

     unit_tests.root_module.addImport("ingest_server", ingest_server_mod);
     unit_tests.root_module.addImport("ingest_schema_config", schema_config_mod);
     unit_tests.root_module.addImport("ingest_schema_persist", schema_persist_mod);
     unit_tests.root_module.addImport("generic_sql", generic_sql_mod);
    unit_tests.root_module.addImport("parquet", parquet_mod);

    // ── core module (shared engine, no external deps) ───────────────────────
    // All of src/core/ is a single named module so that internal relative
    // @import paths resolve correctly when run as a test target.
    const core_mod = b.createModule(.{
        .root_source_file = b.path("src/core/core.zig"),
        .target   = target,
        .optimize = optimize,
    });
    core_mod.addImport("parallel", parallel_mod);
    core_mod.addImport("hashmap",  hashmap_mod);
    const core_tests = b.addTest(.{ .root_module = core_mod });
    const core_test_cmd = b.addRunArtifact(core_tests);
    unit_tests.root_module.addImport("parallel", parallel_mod);

    // ── ir_planner module (generic_sql.Plan → PhysicalNode IR) ───────────────
    const ir_planner_mod = b.createModule(.{
        .root_source_file = b.path("src/core/exec/planner.zig"),
        .target   = target,
        .optimize = optimize,
    });
    ir_planner_mod.addImport("generic_sql", generic_sql_mod);
    ir_planner_mod.addImport("schema",      schema_mod);
    ir_planner_mod.addImport("core",        core_mod);
    const ir_planner_tests   = b.addTest(.{ .root_module = ir_planner_mod });
    const ir_planner_test_cmd = b.addRunArtifact(ir_planner_tests);

    // ── generic_store_bridge module (generic part → SourceIface bridge) ─────
    const generic_store_bridge_mod = b.createModule(.{
        .root_source_file = b.path("src/core/source/generic_store_bridge.zig"),
        .target   = target,
        .optimize = optimize,
    });
    generic_store_bridge_mod.addImport("schema", schema_mod);
    generic_store_bridge_mod.addImport("core", core_mod);
    generic_store_bridge_mod.addImport("generic_store", generic_store_mod);
    const generic_store_bridge_tests = b.addTest(.{ .root_module = generic_store_bridge_mod });
    const generic_store_bridge_test_cmd = b.addRunArtifact(generic_store_bridge_tests);

    // ── part_scan_bridge module (part.zig → SourceIface bridge) ─────────────
    const part_scan_bridge_mod = b.createModule(.{
        .root_source_file = b.path("src/core/source/part_scan_bridge.zig"),
        .target   = target,
        .optimize = optimize,
    });
    part_scan_bridge_mod.addImport("schema", schema_mod);
    part_scan_bridge_mod.addImport("core",   core_mod);
    part_scan_bridge_mod.addImport("part",   ch_part_mod);
    part_scan_bridge_mod.addImport("generic_store", generic_store_mod);
    part_scan_bridge_mod.addImport("generic_store_bridge", generic_store_bridge_mod);
    part_scan_bridge_mod.link_libc = true;
    lz4ctx.link(part_scan_bridge_mod);
    const part_scan_bridge_tests = b.addTest(.{ .root_module = part_scan_bridge_mod });
    const part_scan_bridge_test_cmd = b.addRunArtifact(part_scan_bridge_tests);

     exe.root_module.addImport("ir_planner", ir_planner_mod);
     exe.root_module.addImport("core",       core_mod);
     exe.root_module.addImport("parallel",   parallel_mod);
     exe.root_module.addImport("generic_store_bridge", generic_store_bridge_mod);
     unit_tests.root_module.addImport("generic_store_bridge", generic_store_bridge_mod);

    // ── serializer module (ResultSet → Native block) ─────────────────────────
    const serializer_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/serializer.zig"),
        .target   = target,
        .optimize = optimize,
    });
    serializer_mod.addImport("core", core_mod);
    serializer_mod.addImport("schema", schema_mod);
    serializer_mod.addImport("csv", csv_mod);
    const serializer_tests = b.addTest(.{ .root_module = serializer_mod });
    const serializer_test_cmd = b.addRunArtifact(serializer_tests);

    // Wire query engine into tcp_server (for SELECT on real tables)
    tcp_server_mod.addImport("generic_sql", generic_sql_mod);
    tcp_server_mod.addImport("serializer", serializer_mod);
    tcp_server_mod.addImport("part_scanner", part_scanner_mod);
    tcp_server_mod.addImport("csv", csv_mod);
    tcp_server_mod.addImport("ir_planner", ir_planner_mod);
    tcp_server_mod.addImport("core", core_mod);
    tcp_server_mod.addImport("part_scan_bridge", part_scan_bridge_mod);

    // Wire serializer into ingest_server
    ingest_server_mod.addImport("core", core_mod);
    ingest_server_mod.addImport("serializer", serializer_mod);
    ingest_server_mod.addImport("ir_planner", ir_planner_mod);
    ingest_server_mod.addImport("part_scan_bridge", part_scan_bridge_mod);

    exe.root_module.addImport("serializer", serializer_mod);

    // Wire query engine into compactor (for MV apply)
    compactor_mod.addImport("generic_sql", generic_sql_mod);
    compactor_mod.addImport("ir_planner", ir_planner_mod);
    compactor_mod.addImport("core", core_mod);
    compactor_mod.addImport("part_scan_bridge", part_scan_bridge_mod);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&ir_planner_test_cmd.step);
    test_step.dependOn(&core_test_cmd.step);
    test_step.dependOn(&serializer_test_cmd.step);
    test_step.dependOn(&test_cmd.step);
    test_step.dependOn(&simd_test_cmd.step);
    test_step.dependOn(&parallel_test_cmd.step);
    test_step.dependOn(&hashmap_test_cmd.step);
    test_step.dependOn(&generic_sql_test_cmd.step);
    test_step.dependOn(&lowcard_test_cmd.step);
    test_step.dependOn(&parquet_test_cmd.step);
    test_step.dependOn(&schema_test_cmd.step);
    test_step.dependOn(&schema_infer_test_cmd.step);
    test_step.dependOn(&generic_store_test_cmd.step);
    test_step.dependOn(&loader_test_cmd.step);
    test_step.dependOn(&ch_block_test_cmd.step);
    test_step.dependOn(&ch_types_test_cmd.step);
    test_step.dependOn(&ch_columns_txt_test_cmd.step);
    test_step.dependOn(&ch_count_txt_test_cmd.step);
    test_step.dependOn(&ch_marks_test_cmd.step);
    test_step.dependOn(&ch_primary_idx_test_cmd.step);
    test_step.dependOn(&ch_checksums_test_cmd.step);
    test_step.dependOn(&ch_string_codec_test_cmd.step);
    test_step.dependOn(&ch_part_test_cmd.step);
    test_step.dependOn(&type_mapping_test_cmd.step);
    test_step.dependOn(&generic_store_bridge_test_cmd.step);
    test_step.dependOn(&part_scan_bridge_test_cmd.step);
    test_step.dependOn(&row_binary_decoder_test_cmd.step);
    test_step.dependOn(&schema_config_test_cmd.step);
    test_step.dependOn(&schema_persist_test_cmd.step);
    test_step.dependOn(&part_scanner_test_cmd.step);
    test_step.dependOn(&part_writer_session_test_cmd.step);
    test_step.dependOn(&ddl_parser_test_cmd.step);
    test_step.dependOn(&mv_parse_test_cmd.step);
    test_step.dependOn(&native_block_test_cmd.step);
    test_step.dependOn(&ingest_server_test_cmd.step);

    if (!install_bench_tools) return;

    const bench_simd = b.addExecutable(.{
        .name = "bench-simd",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_simd.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(bench_simd);
    const bench_simd_run = b.addRunArtifact(bench_simd);
    bench_simd_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| bench_simd_run.addArgs(args);
    const bench_simd_step = b.step("bench-simd", "Run A.1 SIMD vs scalar micro-benchmarks");
    bench_simd_step.dependOn(&bench_simd_run.step);

    const bench_parallel = b.addExecutable(.{
        .name = "bench-parallel",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_parallel.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(bench_parallel);
    const bench_parallel_run = b.addRunArtifact(bench_parallel);
    bench_parallel_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| bench_parallel_run.addArgs(args);
    const bench_parallel_step = b.step("bench-parallel", "Run A.3 parallel fan-out micro-benchmark");
    bench_parallel_step.dependOn(&bench_parallel_run.step);

    const bench_mmap = b.addExecutable(.{
        .name = "bench-mmap",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_mmap.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    bench_mmap.root_module.link_libc = true;
    b.installArtifact(bench_mmap);
    const bench_mmap_run = b.addRunArtifact(bench_mmap);
    bench_mmap_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| bench_mmap_run.addArgs(args);
    const bench_mmap_step = b.step("bench-mmap", "Run A.4 mmap vs readAlloc micro-benchmark");
    bench_mmap_step.dependOn(&bench_mmap_run.step);

    const bench_filter = b.addExecutable(.{
        .name = "bench-filter",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_filter.zig"),
            .target   = target,
            .optimize = optimize,
        }),
    });
    bench_filter.root_module.addImport("core", core_mod);
    b.installArtifact(bench_filter);
    const bench_filter_run = b.addRunArtifact(bench_filter);
    bench_filter_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| bench_filter_run.addArgs(args);
    const bench_filter_step = b.step("bench-filter", "A.5 scalar evalExpr vs IntCmpCond vs evalExprBatch SIMD filter");
    bench_filter_step.dependOn(&bench_filter_run.step);

    const bench_distinct = b.addExecutable(.{
        .name = "bench-distinct",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_distinct.zig"),
            .target   = target,
            .optimize = optimize,
        }),
    });
    bench_distinct.root_module.addImport("hashmap", hashmap_mod);
    b.installArtifact(bench_distinct);
    const bench_distinct_run = b.addRunArtifact(bench_distinct);
    bench_distinct_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| bench_distinct_run.addArgs(args);
    const bench_distinct_step = b.step("bench-distinct", "COUNT DISTINCT hash-set strategy micro-benchmark");
    bench_distinct_step.dependOn(&bench_distinct_run.step);
}
