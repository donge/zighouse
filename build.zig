const std = @import("std");

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
    const enable_duckdb = b.option(bool, "duckdb", "Link DuckDB and enable DuckDB-backed commands") orelse true;
    const duckdb_prefix = b.option([]const u8, "duckdb-prefix", "DuckDB installation prefix") orelse "/opt/homebrew/opt/duckdb";
    const install_bench_tools = b.option(bool, "bench-tools", "Install benchmark helper executables") orelse true;
    const options = b.addOptions();
    options.addOption(bool, "duckdb", enable_duckdb);
    const fixture_parquet_path = b.fmt("{s}/data/fixture_hits.parquet", .{b.build_root.path orelse "."});
    options.addOption([]const u8, "fixture_parquet_path", fixture_parquet_path);
    // Single shared module for build_options to avoid duplicate-module errors
    // when generic_executor_mod / generic_sql_mod are imported into exe.root_module.
    const options_mod = options.createModule();

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
    exe.root_module.addImport("build_options", options_mod);
    exe.root_module.link_libc = true;
    if (enable_duckdb) {
        const duckdb_include = b.fmt("{s}/include", .{duckdb_prefix});
        const duckdb_lib = b.fmt("{s}/lib", .{duckdb_prefix});
        exe.root_module.addIncludePath(.{ .cwd_relative = duckdb_include });
        exe.root_module.addLibraryPath(.{ .cwd_relative = duckdb_lib });
        exe.root_module.addRPath(.{ .cwd_relative = duckdb_lib });
        exe.root_module.linkSystemLibrary("duckdb", .{});
    }

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
    unit_tests.root_module.addImport("build_options", options_mod);
    unit_tests.root_module.link_libc = true;
    if (enable_duckdb) {
        const duckdb_include = b.fmt("{s}/include", .{duckdb_prefix});
        const duckdb_lib = b.fmt("{s}/lib", .{duckdb_prefix});
        unit_tests.root_module.addIncludePath(.{ .cwd_relative = duckdb_include });
        unit_tests.root_module.addLibraryPath(.{ .cwd_relative = duckdb_lib });
        unit_tests.root_module.addRPath(.{ .cwd_relative = duckdb_lib });
        unit_tests.root_module.linkSystemLibrary("duckdb", .{});
    }
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

    const planner_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/planner.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const planner_test_cmd = b.addRunArtifact(planner_tests);

    const reader_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/reader.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const reader_test_cmd = b.addRunArtifact(reader_tests);

    const generic_sql_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/generic_sql.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    generic_sql_tests.root_module.addImport("build_options", options_mod);
    generic_sql_tests.root_module.link_libc = true;
    if (enable_duckdb) {
        const duckdb_include = b.fmt("{s}/include", .{duckdb_prefix});
        const duckdb_lib = b.fmt("{s}/lib", .{duckdb_prefix});
        generic_sql_tests.root_module.addIncludePath(.{ .cwd_relative = duckdb_include });
        generic_sql_tests.root_module.addLibraryPath(.{ .cwd_relative = duckdb_lib });
        generic_sql_tests.root_module.addRPath(.{ .cwd_relative = duckdb_lib });
        generic_sql_tests.root_module.linkSystemLibrary("duckdb", .{});
    }
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
    const parquet_test_cmd = b.addRunArtifact(parquet_tests);

    const schema_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/schema.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const schema_test_cmd = b.addRunArtifact(schema_tests);

    const generic_executor_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/generic_executor.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    generic_executor_tests.root_module.addImport("build_options", options_mod);
    generic_executor_tests.root_module.link_libc = true;
    if (enable_duckdb) {
        const duckdb_include = b.fmt("{s}/include", .{duckdb_prefix});
        const duckdb_lib = b.fmt("{s}/lib", .{duckdb_prefix});
        generic_executor_tests.root_module.addIncludePath(.{ .cwd_relative = duckdb_include });
        generic_executor_tests.root_module.addLibraryPath(.{ .cwd_relative = duckdb_lib });
        generic_executor_tests.root_module.addRPath(.{ .cwd_relative = duckdb_lib });
        generic_executor_tests.root_module.linkSystemLibrary("duckdb", .{});
    }
    const generic_executor_test_cmd = b.addRunArtifact(generic_executor_tests);
    generic_executor_test_cmd.setCwd(b.path("."));

    const schema_infer_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/schema_infer.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const schema_infer_test_cmd = b.addRunArtifact(schema_infer_tests);

    const generic_store_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/generic_store.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    generic_store_tests.root_module.link_libc = true;
    const generic_store_test_cmd = b.addRunArtifact(generic_store_tests);

    // parquet_mod: used by loader, schema_infer, native, main, and generic_executor
    const parquet_mod = b.createModule(.{
        .root_source_file = b.path("src/parquet.zig"),
        .target = target,
        .optimize = optimize,
    });

    // generic_sql_mod: named module for generic_executor and ingest_server
    const generic_sql_mod = b.createModule(.{
        .root_source_file = b.path("src/generic_sql.zig"),
        .target = target,
        .optimize = optimize,
    });
    generic_sql_mod.addImport("build_options", options_mod);
    generic_sql_mod.link_libc = true;
    if (enable_duckdb) {
        const duckdb_include_path = b.fmt("{s}/include", .{duckdb_prefix});
        const duckdb_lib_path = b.fmt("{s}/lib", .{duckdb_prefix});
        generic_sql_mod.addIncludePath(.{ .cwd_relative = duckdb_include_path });
        generic_sql_mod.addLibraryPath(.{ .cwd_relative = duckdb_lib_path });
        generic_sql_mod.addRPath(.{ .cwd_relative = duckdb_lib_path });
        generic_sql_mod.linkSystemLibrary("duckdb", .{});
    }

    const loader_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/loader.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    loader_tests.root_module.link_libc = true;
    loader_tests.root_module.addImport("parquet", parquet_mod);
    const loader_test_cmd = b.addRunArtifact(loader_tests);

    // ── clickhouse_format tests ─────────────────────────────────────────────
    const lz4_prefix = b.option([]const u8, "lz4-prefix", "LZ4 installation prefix") orelse "/opt/homebrew/opt/lz4";
    const lz4_include = b.fmt("{s}/include", .{lz4_prefix});
    const lz4_lib = b.fmt("{s}/lib", .{lz4_prefix});
    const ch_block_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clickhouse_format/block.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    ch_block_tests.root_module.link_libc = true;
    ch_block_tests.root_module.addIncludePath(.{ .cwd_relative = lz4_include });
    ch_block_tests.root_module.addLibraryPath(.{ .cwd_relative = lz4_lib });
    ch_block_tests.root_module.addRPath(.{ .cwd_relative = lz4_lib });
    ch_block_tests.root_module.linkSystemLibrary("lz4", .{});
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

    // checksums.zig — needs lz4
    const ch_checksums_mod = b.createModule(.{
        .root_source_file = b.path("src/clickhouse_format/checksums.zig"),
        .target = target,
        .optimize = optimize,
    });
    ch_checksums_mod.link_libc = true;
    ch_checksums_mod.addIncludePath(.{ .cwd_relative = lz4_include });
    ch_checksums_mod.addLibraryPath(.{ .cwd_relative = lz4_lib });
    ch_checksums_mod.addRPath(.{ .cwd_relative = lz4_lib });
    ch_checksums_mod.linkSystemLibrary("lz4", .{});
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

    // part.zig — needs lz4 + schema + all sub-modules
    const ch_part_mod = b.createModule(.{
        .root_source_file = b.path("src/clickhouse_format/part.zig"),
        .target = target,
        .optimize = optimize,
    });
    ch_part_mod.link_libc = true;
    ch_part_mod.addIncludePath(.{ .cwd_relative = lz4_include });
    ch_part_mod.addLibraryPath(.{ .cwd_relative = lz4_lib });
    ch_part_mod.addRPath(.{ .cwd_relative = lz4_lib });
    ch_part_mod.linkSystemLibrary("lz4", .{});
    ch_part_mod.addImport("schema", schema_mod);
    ch_part_mod.addImport("types", ch_types_mod);
    const ch_part_tests = b.addTest(.{
        .root_module = ch_part_mod,
    });
    const ch_part_test_cmd = b.addRunArtifact(ch_part_tests);

    // ── Wire schema_mod + ch_part_mod + lz4 into exe and all test targets ──────
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
        mod.addIncludePath(.{ .cwd_relative = lz4_include });
        mod.addLibraryPath(.{ .cwd_relative = lz4_lib });
        mod.addRPath(.{ .cwd_relative = lz4_lib });
        mod.linkSystemLibrary("lz4", .{});
    }
    // Other test targets that use @import("schema") but not ch_part/lz4:
    generic_executor_tests.root_module.addImport("schema", schema_mod);
    generic_executor_tests.root_module.addImport("ch_part", ch_part_mod);
    generic_executor_tests.root_module.addImport("generic_sql", generic_sql_mod);
    generic_executor_tests.root_module.addImport("parquet", parquet_mod);
    generic_executor_tests.root_module.addImport("csv", csv_mod);
    generic_executor_tests.root_module.addIncludePath(.{ .cwd_relative = lz4_include });
    generic_executor_tests.root_module.addLibraryPath(.{ .cwd_relative = lz4_lib });
    generic_executor_tests.root_module.addRPath(.{ .cwd_relative = lz4_lib });
    generic_executor_tests.root_module.linkSystemLibrary("lz4", .{});
    schema_infer_tests.root_module.addImport("schema", schema_mod);
    schema_infer_tests.root_module.addImport("parquet", parquet_mod);
    generic_store_tests.root_module.addImport("schema", schema_mod);

    // ── ingest module tests ─────────────────────────────────────────────────
    const row_binary_decoder_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/row_binary_decoder.zig"),
        .target = target,
        .optimize = optimize,
    });
    row_binary_decoder_mod.addImport("schema", schema_mod);
    const row_binary_decoder_tests = b.addTest(.{ .root_module = row_binary_decoder_mod });
    const row_binary_decoder_test_cmd = b.addRunArtifact(row_binary_decoder_tests);

    const schema_config_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/schema_config.zig"),
        .target = target,
        .optimize = optimize,
    });
    schema_config_mod.addImport("schema", schema_mod);
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
    part_writer_session_mod.addIncludePath(.{ .cwd_relative = lz4_include });
    part_writer_session_mod.addLibraryPath(.{ .cwd_relative = lz4_lib });
    part_writer_session_mod.addRPath(.{ .cwd_relative = lz4_lib });
    part_writer_session_mod.linkSystemLibrary("lz4", .{});
    const part_writer_session_tests = b.addTest(.{ .root_module = part_writer_session_mod });
    const part_writer_session_test_cmd = b.addRunArtifact(part_writer_session_tests);

    const schema_persist_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/schema_persist.zig"),
        .target = target,
        .optimize = optimize,
    });
    schema_persist_mod.addImport("schema", schema_mod);
    schema_persist_mod.addImport("schema_config", schema_config_mod);
    const schema_persist_tests = b.addTest(.{ .root_module = schema_persist_mod });
    const schema_persist_test_cmd = b.addRunArtifact(schema_persist_tests);

    const part_scanner_mod = b.createModule(.{
        .root_source_file = b.path("src/ingest/part_scanner.zig"),
        .target = target,
        .optimize = optimize,
    });
    const part_scanner_tests = b.addTest(.{ .root_module = part_scanner_mod });
    const part_scanner_test_cmd = b.addRunArtifact(part_scanner_tests);

    // generic_sql and generic_executor as named modules for server dependency.
    const generic_executor_mod = b.createModule(.{
        .root_source_file = b.path("src/generic_executor.zig"),
        .target = target,
        .optimize = optimize,
    });
    generic_executor_mod.addImport("build_options", options_mod);
    generic_executor_mod.addImport("csv", csv_mod);
    generic_executor_mod.link_libc = true;
    generic_executor_mod.addImport("schema", schema_mod);
    generic_executor_mod.addImport("ch_part", ch_part_mod);
    generic_executor_mod.addImport("generic_sql", generic_sql_mod);
    generic_executor_mod.addImport("parquet", parquet_mod);
    generic_executor_mod.addIncludePath(.{ .cwd_relative = lz4_include });
    generic_executor_mod.addLibraryPath(.{ .cwd_relative = lz4_lib });
    generic_executor_mod.addRPath(.{ .cwd_relative = lz4_lib });
    generic_executor_mod.linkSystemLibrary("lz4", .{});
    if (enable_duckdb) {
        const duckdb_include_path = b.fmt("{s}/include", .{duckdb_prefix});
        const duckdb_lib_path = b.fmt("{s}/lib", .{duckdb_prefix});
        generic_executor_mod.addIncludePath(.{ .cwd_relative = duckdb_include_path });
        generic_executor_mod.addLibraryPath(.{ .cwd_relative = duckdb_lib_path });
        generic_executor_mod.addRPath(.{ .cwd_relative = duckdb_lib_path });
        generic_executor_mod.linkSystemLibrary("duckdb", .{});
    }

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
    const ddl_parser_tests = b.addTest(.{ .root_module = ddl_parser_mod });
    const ddl_parser_test_cmd = b.addRunArtifact(ddl_parser_tests);

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
    ingest_server_mod.addImport("generic_executor", generic_executor_mod);
    ingest_server_mod.addImport("generic_sql", generic_sql_mod);
    ingest_server_mod.addImport("ddl_parser", ddl_parser_mod);
    ingest_server_mod.addImport("native_block", native_block_mod);
    ingest_server_mod.addImport("csv", csv_mod);
    ingest_server_mod.link_libc = true;
    ingest_server_mod.addIncludePath(.{ .cwd_relative = lz4_include });
    ingest_server_mod.addLibraryPath(.{ .cwd_relative = lz4_lib });
    ingest_server_mod.addRPath(.{ .cwd_relative = lz4_lib });
    ingest_server_mod.linkSystemLibrary("lz4", .{});
    if (enable_duckdb) {
        const duckdb_include_path = b.fmt("{s}/include", .{duckdb_prefix});
        const duckdb_lib_path = b.fmt("{s}/lib", .{duckdb_prefix});
        ingest_server_mod.addIncludePath(.{ .cwd_relative = duckdb_include_path });
        ingest_server_mod.addLibraryPath(.{ .cwd_relative = duckdb_lib_path });
        ingest_server_mod.addRPath(.{ .cwd_relative = duckdb_lib_path });
        ingest_server_mod.linkSystemLibrary("duckdb", .{});
    }
    const ingest_server_tests = b.addTest(.{ .root_module = ingest_server_mod });
    const ingest_server_test_cmd = b.addRunArtifact(ingest_server_tests);

    // Wire ingest modules into main exe and unit tests
    exe.root_module.addImport("ingest_server", ingest_server_mod);
    exe.root_module.addImport("ingest_schema_config", schema_config_mod);
    exe.root_module.addImport("generic_executor", generic_executor_mod);
    exe.root_module.addImport("generic_sql", generic_sql_mod);
    exe.root_module.addImport("parquet", parquet_mod);
    unit_tests.root_module.addImport("ingest_server", ingest_server_mod);
    unit_tests.root_module.addImport("ingest_schema_config", schema_config_mod);
    unit_tests.root_module.addImport("generic_executor", generic_executor_mod);
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
    const core_tests = b.addTest(.{ .root_module = core_mod });
    const core_test_cmd = b.addRunArtifact(core_tests);

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

    // ── part_scan_bridge module (part.zig → SourceIface bridge) ─────────────
    const part_scan_bridge_mod = b.createModule(.{
        .root_source_file = b.path("src/core/source/part_scan_bridge.zig"),
        .target   = target,
        .optimize = optimize,
    });
    part_scan_bridge_mod.addImport("schema", schema_mod);
    part_scan_bridge_mod.addImport("core",   core_mod);
    part_scan_bridge_mod.addImport("part",   ch_part_mod);
    part_scan_bridge_mod.link_libc = true;
    part_scan_bridge_mod.addIncludePath(.{ .cwd_relative = lz4_include });
    part_scan_bridge_mod.addLibraryPath(.{ .cwd_relative = lz4_lib });
    part_scan_bridge_mod.addRPath(.{ .cwd_relative = lz4_lib });
    part_scan_bridge_mod.linkSystemLibrary("lz4", .{});

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

    // Wire serializer into ingest_server
    ingest_server_mod.addImport("core", core_mod);
    ingest_server_mod.addImport("serializer", serializer_mod);
    ingest_server_mod.addImport("ir_planner", ir_planner_mod);
    ingest_server_mod.addImport("part_scan_bridge", part_scan_bridge_mod);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&ir_planner_test_cmd.step);
    test_step.dependOn(&core_test_cmd.step);
    test_step.dependOn(&serializer_test_cmd.step);
    test_step.dependOn(&test_cmd.step);
    test_step.dependOn(&simd_test_cmd.step);
    test_step.dependOn(&parallel_test_cmd.step);
    test_step.dependOn(&hashmap_test_cmd.step);
    test_step.dependOn(&planner_test_cmd.step);
    test_step.dependOn(&reader_test_cmd.step);
    test_step.dependOn(&generic_sql_test_cmd.step);
    test_step.dependOn(&lowcard_test_cmd.step);
    test_step.dependOn(&parquet_test_cmd.step);
    test_step.dependOn(&schema_test_cmd.step);
    test_step.dependOn(&generic_executor_test_cmd.step);
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
    test_step.dependOn(&row_binary_decoder_test_cmd.step);
    test_step.dependOn(&schema_config_test_cmd.step);
    test_step.dependOn(&schema_persist_test_cmd.step);
    test_step.dependOn(&part_scanner_test_cmd.step);
    test_step.dependOn(&part_writer_session_test_cmd.step);
    test_step.dependOn(&ddl_parser_test_cmd.step);
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

    const bench_hashmap = b.addExecutable(.{
        .name = "bench-hashmap",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_hashmap.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    bench_hashmap.root_module.link_libc = true;
    b.installArtifact(bench_hashmap);
    const bench_hashmap_run = b.addRunArtifact(bench_hashmap);
    bench_hashmap_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| bench_hashmap_run.addArgs(args);
    const bench_hashmap_step = b.step("bench-hashmap", "Compare custom HashU64Count vs std.AutoHashMap on Q17 workload");
    bench_hashmap_step.dependOn(&bench_hashmap_run.step);
}
