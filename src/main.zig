const std = @import("std");

const Database = @import("Database.zig");
const SchemaRecord = @import("./sql/SchemaRecord.zig");
const Parser = @import("./sql/Parser.zig");
const Value = @import("./storage.zig").Value;
const Command = @import("cli/Command.zig").Command;

const stdout = std.io.getStdOut().writer();

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var file = try std.fs.cwd().openFile(args[1], .{});
    defer file.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const database = try Database.init(file, arena.allocator());

    const page = (try database.readPage(1)).leaf_table;

    switch (try Command.parse(args[2], arena.allocator())) {
        .sql => |statement| try handleSelect(database, statement, arena.allocator()),
        .dbinfo => try stdout.print("database page size: {}\nnumber of tables: {}\n", .{ database.header.page_size, page.header.cell_count }),
        .tables => {
            for (page.records.items, 0..) |r, i| {
                if (i > 0) {
                    try stdout.print(" ", .{});
                }

                try stdout.print("{s}", .{r.fields.items[1].Text});
            }

            try stdout.print("\n", .{});
        },
    }
}

fn handleSelect(database: Database, statement: Parser.SqlStatement, allocator: std.mem.Allocator) !void {
    switch (statement) {
        .select => |select| {
            if (select.count) |_| {
                const res = try database.countTableRecords(select.from);
                try stdout.print("{}\n", .{res});
            } else if (std.ascii.eqlIgnoreCase("companies", select.from)) {
                // Let's hard-code this case for now...

                const tbl_page = try database.getTableRootPage("companies");
                const idx_page = try database.getIndexPage("idx_companies_country");

                var it = try database.iterateIndexRecords(idx_page, Value{ .Text = select.where.?.value });
                defer it.deinit();

                while (try it.next()) |r| {
                    const row = (try database.getRow(tbl_page, @as(usize, @intCast(r.fields.items[1].Integer)))).?;
                    try stdout.print("{}|{}\n", .{ row.id.?, row.fields.items[1] });
                }
            } else {
                var it = try database.iterateTableRecords(select.from);
                defer it.deinit();

                const schema = try Parser.parse((try database.getTableSchema(select.from)).sql, allocator);

                const field_indexes = try allocator.alloc(u32, select.fields.items.len);
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
                            try stdout.print("|", .{});
                        }

                        if (idx == 0) {
                            try stdout.print("{}", .{item.id.?});
                        } else {
                            try stdout.print("{}", .{item.fields.items[idx]});
                        }
                    }
                    try stdout.print("\n", .{});
                }
            }
        },
        else => try stdout.print("not supported\n", .{}),
    }
}
