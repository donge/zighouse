/// Shared RFC 4180 CSV field parser used by both server.zig and generic_executor.zig.
const std = @import("std");

/// Parse the next CSV field from `line` starting at `pos`.
///
/// Handles quoted fields (double-quote escaping per RFC 4180):
///   - Unquoted: reads until next ',' or end-of-line; returns a slice of the original line.
///   - Quoted: reads until closing '"', unescaping '""' → '"'; returns buf.items.
///
/// On return, `pos` is advanced past the field and its trailing comma (if any).
/// The returned slice is valid as long as either `line` (unquoted) or `buf` (quoted) is alive.
pub fn parseCsvField(
    line: []const u8,
    pos: *usize,
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
) []const u8 {
    const start = pos.*;
    if (start >= line.len) return "";

    if (line[start] == '"') {
        // Quoted field
        buf.clearRetainingCapacity();
        var i = start + 1;
        while (i < line.len) {
            const ch = line[i];
            if (ch == '"') {
                if (i + 1 < line.len and line[i + 1] == '"') {
                    // Escaped double-quote ""  →  "
                    buf.append(allocator, '"') catch {};
                    i += 2;
                } else {
                    // End of quoted field
                    i += 1; // skip closing quote
                    break;
                }
            } else {
                buf.append(allocator, ch) catch {};
                i += 1;
            }
        }
        // Skip trailing comma
        if (i < line.len and line[i] == ',') i += 1;
        pos.* = i;
        return buf.items;
    } else {
        // Unquoted field: find next comma
        var i = start;
        while (i < line.len and line[i] != ',') : (i += 1) {}
        const field = line[start..i];
        pos.* = if (i < line.len) i + 1 else i; // skip comma
        return field;
    }
}
