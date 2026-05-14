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
    exe.root_module.addOptions("build_options", options);
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
    unit_tests.root_module.addOptions("build_options", options);
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
    generic_sql_tests.root_module.addOptions("build_options", options);
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
    generic_executor_tests.root_module.addOptions("build_options", options);
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

    const test_step = b.step("test", "Run unit tests");
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
