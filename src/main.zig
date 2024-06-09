const std = @import("std");

const Database = @import("Database.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        try std.io.getStdErr().writer().print("Usage: {s} <database_file_path> <command>\n", .{args[0]});
        return;
    }

    const database_file_path: []const u8 = args[1];
    const command: []const u8 = args[2];

    var file = try std.fs.cwd().openFile(database_file_path, .{});
    defer file.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const database = try Database.init(file, arena.allocator());
    const header = try database.readHeader();
    const page = try database.readPage();

    if (std.mem.eql(u8, command, ".dbinfo")) {
        try std.io.getStdOut().writer().print("database page size: {}\n", .{
            header.page_size,
        });

        try std.io.getStdOut().writer().print("number of tables: {}\n", .{
            page.header.cell_count,
        });
    } else if (std.mem.eql(u8, command, ".tables")) {
        for (page.records.items, 0..) |_, i| {
            if (i > 0) {
                try std.io.getStdOut().writer().print(" ", .{});
            }
            // test script expects tables in reverse order :-/
            const r = page.records.items[page.records.items.len - 1 - i];
            try std.io.getStdOut().writer().print("{s}", .{r.fields.items[1].Text});
        }
        try std.io.getStdOut().writer().print("\n", .{});
    }
}
