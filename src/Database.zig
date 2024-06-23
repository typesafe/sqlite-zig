const std = @import("std");

const storage = @import("./storage.zig");
const SchemaRecord = @import("./sql/SchemaRecord.zig");

const Self = @This();

file: std.fs.File,
allocator: std.mem.Allocator,
header: storage.DatabaseHeader,

pub fn init(file: std.fs.File, allocator: std.mem.Allocator) !Self {
    const header = try storage.DatabaseHeader.parse(file.reader());

    return Self{
        .file = file,
        .allocator = allocator,
        .header = header,
    };
}

pub fn readPage(self: Self, number: usize) !storage.Page {
    // first page contains database header of 100 bytes
    try self.file.seekTo(if (number == 1) 100 else self.header.page_size * (number - 1));

    return try storage.Page.parse(self.file.reader(), self.allocator);
}

pub fn getTableRootPage(self: Self, table_name: []const u8) !storage.Page {
    return try self.readPage((try self.getTableSchema(table_name)).rootpage);
}

pub fn getIndexPage(self: Self, index_name: []const u8) !storage.Page {
    const schema_page = (try self.readPage(1)).leaf_table;
    for (schema_page.cells.items) |r| {
        const sr = SchemaRecord.fromValues(r.fields.items);
        if (std.ascii.eqlIgnoreCase(sr.type, "index") and std.ascii.eqlIgnoreCase(sr.name, index_name)) {
            return try self.readPage(sr.rootpage);
        }
    }

    return error.TableNotFound;
}

pub fn getTableSchema(self: Self, table: []const u8) !SchemaRecord {
    const schema_page = (try self.readPage(1)).leaf_table;
    for (schema_page.cells.items) |r| {
        const sr = SchemaRecord.fromValues(r.fields.items);
        if (std.ascii.eqlIgnoreCase(sr.name, table)) {
            return sr;
        }
    }

    return error.TableNotFound;
}

pub fn countTableRecords(self: Self, table: []const u8) !usize {
    return self.countPageRecords(try self.getTableRootPage(table));
}

pub fn countPageRecords(self: Self, page: storage.Page) !usize {
    return switch (page) {
        .leaf_table => |t| t.cells.items.len,
        .internal_table => |t| {
            var count: usize = 0;
            for (t.cells.items) |ptr| {
                count += try self.countPageRecords(try self.readPage(ptr.page_number));
            }
            count += try self.countPageRecords(try self.readPage(t.header.right_most_pointer.?));
            return count;
        },
        else => error.NoTablePage,
    };
}

pub fn getRow(self: Self, page: storage.Page, id: usize) !?storage.LeafTableCell {
    return switch (page) {
        .leaf_table => |t| blk: {
            for (t.cells.items) |rec| {
                if (rec.id == id) {
                    break :blk rec;
                }
            }
            break :blk null;
        },
        .internal_table => |t| {
            for (t.cells.items) |ptr| {
                if (id <= ptr.id) {
                    return try self.getRow(try self.readPage(ptr.page_number), id);
                }
            }
            return try self.getRow(try self.readPage(t.header.right_most_pointer.?), id);
        },
        else => error.NoTablePage,
    };
}

pub fn iterateTable(self: Self, table: []const u8) !TableIterator {
    const page = try self.getTableRootPage(table);
    return self.iterateTableBTree(page);
}

pub fn iterateTableBTree(self: Self, root_page: storage.Page) !TableIterator {
    return try TableIterator.init(self, root_page);
}

pub fn iterateIndexBTree(self: Self, root_page: storage.Page, value: storage.Value) !IndexIterator {
    return try IndexIterator.init(self, root_page, value);
}

pub const TableIterator = struct {
    arena: std.heap.ArenaAllocator,
    database: Self,
    stack: std.ArrayList(OffsettedPage),

    pub fn init(database: Self, root_page: storage.Page) !TableIterator {
        var arena = std.heap.ArenaAllocator.init(database.allocator);
        var stack = std.ArrayList(OffsettedPage).init(arena.allocator());
        try stack.append(.{ .page = root_page, .offset = 0 });

        return .{
            .arena = arena,
            .database = database,
            .stack = stack,
        };
    }

    pub fn deinit(self: *TableIterator) void {
        self.arena.deinit();
    }

    pub fn next(self: *TableIterator) !?storage.Cell {
        if (self.stack.items.len == 0) {
            return null;
        }

        var op = &self.stack.items[self.stack.items.len - 1];

        while (true) {
            switch (op.page) {
                .leaf_table => |t| {
                    const ret = t.cells.items[op.offset];
                    op.offset += 1;
                    if (op.offset == t.cells.items.len) {
                        _ = self.stack.pop();
                    }
                    return .{ .leaf_table = ret };
                },
                .internal_table => |t| {
                    const child_page_nr = blk: {
                        if (op.offset == t.cells.items.len) {
                            _ = self.stack.pop();
                            break :blk t.header.right_most_pointer.?;
                        } else {
                            const ptr = t.cells.items[op.offset];
                            op.offset += 1;
                            break :blk ptr.page_number;
                        }
                    };

                    try self.stack.append(.{ .page = try self.database.readPage(child_page_nr), .offset = 0 });
                    op = &self.stack.items[self.stack.items.len - 1];
                },

                else => unreachable,
            }
        }
    }

    const OffsettedPage = struct {
        page: storage.Page,
        offset: usize,
    };
};

pub const IndexIterator = struct {
    arena: std.heap.ArenaAllocator,
    database: Self,
    stack: std.ArrayList(OffsettedPage),
    value: storage.Value,

    pub fn init(database: Self, root_page: storage.Page, value: storage.Value) !IndexIterator {
        var arena = std.heap.ArenaAllocator.init(database.allocator);
        var stack = std.ArrayList(OffsettedPage).init(arena.allocator());
        try stack.append(.{ .page = root_page, .offset = 0 });

        return .{
            .arena = arena,
            .database = database,
            .stack = stack,
            .value = value,
        };
    }

    pub fn deinit(self: *IndexIterator) void {
        self.arena.deinit();
    }

    pub fn next(self: *IndexIterator) !?storage.Cell {
        if (self.stack.items.len == 0) {
            return null;
        }

        var op = &self.stack.items[self.stack.items.len - 1];

        while (true) {
            switch (op.page) {
                .internal_index => |t| {
                    const page_nr = blk: {
                        while (op.offset < t.cells.items.len) {
                            const r = t.cells.items[op.offset];
                            op.offset += 1;

                            switch (r.fields.items[0].compare(self.value)) {
                                .lt => continue,
                                .gt => {
                                    _ = self.stack.pop();
                                },
                                .eq => {},
                            }

                            break :blk r.page_number;
                        }

                        _ = self.stack.pop();
                        break :blk t.header.right_most_pointer.?;
                    };

                    try self.stack.append(.{ .page = try self.database.readPage(page_nr), .offset = 0 });
                    op = &self.stack.items[self.stack.items.len - 1];
                },
                .leaf_index => |t| {
                    while (op.offset < t.cells.items.len) {
                        const r = t.cells.items[op.offset];
                        op.offset += 1;

                        switch (r.fields.items[0].compare(self.value)) {
                            .eq => return .{ .leaf_index = r },
                            .gt => break,
                            .lt => continue,
                        }
                    }

                    return null;
                },
                else => unreachable,
            }
        }
    }

    const OffsettedPage = struct {
        page: storage.Page,
        offset: usize,
    };
};
