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

    const database_file_path: []const u8 = args[1];
    const command: []const u8 = args[2];

    var file = try std.fs.cwd().openFile(database_file_path, .{});
    defer file.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const database = try Database.init(file, arena.allocator());

    const page = (try database.readPage(1)).leaf_table;

    if (std.mem.eql(u8, command, ".dbinfo")) {
        try std.io.getStdOut().writer().print("database page size: {}\n", .{
            database.header.page_size,
        });

        try std.io.getStdOut().writer().print("number of tables: {}\n", .{
            page.header.cell_count,
        });
    } else if (std.mem.eql(u8, command, ".tables")) {
        for (page.records.items, 0..) |_, i| {
            if (i > 0) {
                try std.io.getStdOut().writer().print(" ", .{});
            }

            const r = page.records.items[i];
            try std.io.getStdOut().writer().print("{s}", .{r.fields.items[1].Text});
        }
        try std.io.getStdOut().writer().print("\n", .{});
    } else if (std.ascii.eqlIgnoreCase(command[0..6], "SELECT")) {
        const statement = try Parser.parse(command, arena.allocator());

        switch (statement) {
            .select => |select| {
                if (select.count) |_| {
                    const res = try database.countTableRecords(select.from);
                    try std.io.getStdOut().writer().print("{}\n", .{res});
                } else {
                    var it = try database.iterateTableRecords(select.from, arena.allocator());
                    defer it.deinit();

                    const schema = try Parser.parse((try database.getTableSchema(select.from)).sql, arena.allocator());
                    //const tbl = (try database.readPage(schema.rootpage)).leaf_table;

                    const field_indexes = try arena.allocator().alloc(u32, select.fields.items.len);
                    for (select.fields.items, 0..) |field, i| {
                        field_indexes[i] = schema.create_table.fields.get(field).?.index;
                    }

                    while (try it.next()) |item| {
                        if (select.where) |where| {
                            const matches = switch (item.fields.items[schema.create_table.fields.get(where.field).?.index]) {
                                .Text => |v| std.mem.eql(u8, v, where.value),
                                else => false,
                            };
                            if (!matches) {
                                continue;
                            }
                        }

                        for (field_indexes, 0..) |idx, i| {
                            if (i > 0) {
                                try std.io.getStdOut().writer().print("|", .{});
                            }

                            if (idx == 0) {
                                try std.io.getStdOut().writer().print("{}", .{item.id});
                            } else {
                                switch (item.fields.items[idx]) {
                                    .Null => try std.io.getStdOut().writer().print("NULL", .{}),
                                    .Text => |v| try std.io.getStdOut().writer().print("{s}", .{v}),
                                    else => try std.io.getStdOut().writer().print("{any}", .{item.fields.items[idx]}),
                                }
                            }
                        }
                        try std.io.getStdOut().writer().print("\n", .{});
                    }
                }
            },
            else => try std.io.getStdOut().writer().print("not supported\n", .{}),
        }
    } else {
        try std.io.getStdOut().writer().print("{s}\n", .{command});
    }
}
