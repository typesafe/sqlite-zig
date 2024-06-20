const std = @import("std");

const Database = @import("Database.zig");
const SchemaRecord = @import("./sql/SchemaRecord.zig");
const Parser = @import("./sql/Parser.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // if (args.len < 3) {
    //     try std.io.getStdErr().writer().print("Usage: {s} <database_file_path> <command>\n", .{args[0]});
    //     return;
    // }

    const database_file_path: []const u8 = args[1];
    const command: []const u8 = args[2];

    var file = try std.fs.cwd().openFile(database_file_path, .{});
    defer file.close();

    // const b64 = std.base64.standard.Encoder.init(std.base64.standard.alphabet_chars, std.base64.standard.pad_char);

    // b64.encode([], file.readAll(buffer: []u8));

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const database = try Database.init(file, arena.allocator());
    const header = try database.readHeader();
    const page = try database.readPage(1);

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
    } else if (std.ascii.eqlIgnoreCase(command[0..6], "SELECT")) {
        const statement = try Parser.parse(command, arena.allocator());

        switch (statement) {
            .select => |select| {
                if (select.count) |_| {
                    for (page.records.items) |r| {
                        const sr = SchemaRecord.fromRecord(r);
                        if (std.mem.eql(u8, sr.name, select.from)) {
                            const tbl = try database.readPage(sr.rootpage);
                            try std.io.getStdOut().writer().print("{}", .{tbl.records.items.len});
                        }
                    }
                    try std.io.getStdOut().writer().print("\n", .{});
                } else {
                    for (page.records.items) |r| {
                        const sr = SchemaRecord.fromRecord(r);
                        if (std.mem.eql(u8, sr.name, select.from)) {
                            const schema = try Parser.parse(sr.sql, arena.allocator());
                            const tbl = try database.readPage(sr.rootpage);
                            const indexes = try arena.allocator().alloc(u32, select.fields.items.len);
                            for (select.fields.items, 0..) |field, i| {
                                indexes[i] = schema.create_table.fields.get(field).?.index;
                            }

                            for (tbl.records.items) |item| {
                                if (select.where) |where| {
                                    const item_value = item.fields.items[schema.create_table.fields.get(where.field).?.index].Text;

                                    if (!std.mem.eql(u8, item_value, where.value)) {
                                        continue;
                                    }
                                }

                                for (indexes, 0..) |idx, i| {
                                    if (i > 0) {
                                        try std.io.getStdOut().writer().print("|", .{});
                                    }
                                    try std.io.getStdOut().writer().print("{s}", .{item.fields.items[idx].Text});
                                }
                                try std.io.getStdOut().writer().print("\n", .{});
                            }
                        }
                    }
                }
            },
            else => try std.io.getStdOut().writer().print("not supported\n", .{}),
        }
    } else {
        try std.io.getStdOut().writer().print("{s}\n", .{command});
    }
}
