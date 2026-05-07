const planner = @import("planner.zig");
const reader = @import("reader.zig");

pub fn execute(store: *reader.StoreReader, physical: planner.PhysicalPlan) ![]u8 {
    return switch (physical) {
        .artifact_csv => |p| store.resultCsv(p.file_name, p.limit),
    };
}
