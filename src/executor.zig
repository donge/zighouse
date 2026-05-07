const std = @import("std");
const planner = @import("planner.zig");
const reader = @import("reader.zig");

pub fn execute(store: *reader.StoreReader, physical: planner.PhysicalPlan) ![]u8 {
    return switch (physical) {
        .artifact_csv => |p| store.resultCsv(p.file_name, p.limit),
        .csv_count => error.InvalidPlanInput,
    };
}

pub fn executeCsv(allocator: std.mem.Allocator, csv: *reader.CsvReader, physical: planner.PhysicalPlan) ![]u8 {
    return switch (physical) {
        .csv_count => |p| blk: {
            const count = try csv.rowCount(.{ .has_header = p.has_header });
            break :blk std.fmt.allocPrint(allocator, "count_star()\n{d}\n", .{count});
        },
        .artifact_csv => error.InvalidPlanInput,
    };
}
