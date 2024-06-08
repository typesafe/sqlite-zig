const std = @import("std");

const DatabaseHeader = @import("DatabaseHeader.zig").DatabaseHeader;

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

    if (std.mem.eql(u8, command, ".dbinfo")) {
        var file = try std.fs.cwd().openFile(database_file_path, .{});
        defer file.close();

        const header = try file.reader().readStruct(DatabaseHeader);

        try std.io.getStdOut().writer().print("database page size: {}\n", .{
            std.mem.readInt(u16, &header.pageSize, .big),
        });
    }
}
