const std = @import("std");

const storage = @import("./storage.zig");
const SchemaRecord = @import("./sql/SchemaRecord.zig");

const Self = @This();

file: std.fs.File,
allocator: std.mem.Allocator,

pub fn init(file: std.fs.File, allocator: std.mem.Allocator) !Self {
    return Self{
        .file = file,
        .allocator = allocator,
    };
}

pub fn readHeader(self: Self) !storage.DatabaseHeader {
    try self.file.seekTo(0);

    return try storage.DatabaseHeader.parse(self.file.reader());
}

pub fn readPage(self: Self, number: usize) !storage.Page {
    const header = try self.readHeader();

    if (number == 1) {
        // page 1 contains the DB header
        try self.file.seekTo(100);
    } else {
        try self.file.seekTo(header.page_size * (number - 1));
    }

    const reader = self.file.reader();

    return try storage.Page.parse(reader, self.allocator);
}

pub fn getTableRootPage(self: Self, table_name: []const u8) !storage.Page {
    return try self.readPage((try self.getTableSchema(table_name)).rootpage);
}

pub fn getTableSchema(self: Self, table: []const u8) !SchemaRecord {
    const schema_page = (try self.readPage(1)).leaf_table;
    for (schema_page.records.items) |r| {
        const sr = SchemaRecord.fromRecord(r);
        if (std.ascii.eqlIgnoreCase(sr.name, table)) {
            return sr;
        }
    }

    return error.TableNotFound;
}

pub fn countTableRecords(self: Self, table: []const u8) !usize {
    const page = try self.getTableRootPage(table);

    return self.countPageRecords(page);
}

pub fn countPageRecords(self: Self, page: storage.Page) !usize {
    return switch (page) {
        .leaf_table => |t| t.records.items.len,
        .internal_table => |t| {
            var count: usize = 0;
            for (t.pointers.items) |ptr| {
                count += try self.countPageRecords(try self.readPage(ptr.page_number));
            }
            count += try self.countPageRecords(try self.readPage(t.header.right_most_pointer.?));
            return count;
        },
    };
}

pub fn iterateTableRecords(self: Self, table: []const u8, allocator: std.mem.Allocator) !RecordIterator {
    const page = try self.getTableRootPage(table);
    return self.iterateBtreeRecords(page, allocator);
}

pub fn iterateBtreeRecords(self: Self, root_page: storage.Page, allocator: std.mem.Allocator) !RecordIterator {
    return try RecordIterator.init(self, root_page, allocator);
}

pub const RecordIterator = struct {
    arena: std.heap.ArenaAllocator,
    database: Self,
    stack: std.ArrayList(OffsettedPage),

    pub fn init(database: Self, root_page: storage.Page, allocator: std.mem.Allocator) !RecordIterator {
        var arena = std.heap.ArenaAllocator.init(allocator);
        var stack = std.ArrayList(OffsettedPage).init(arena.allocator());
        try stack.append(.{ .page = root_page, .offset = 0 });

        return .{
            .arena = arena,
            .database = database,
            .stack = stack,
        };
    }

    pub fn deinit(self: *RecordIterator) void {
        self.arena.deinit();
    }

    pub fn next(self: *RecordIterator) !?storage.Record {
        if (self.stack.items.len == 0) {
            return null;
        }

        var op = &self.stack.items[self.stack.items.len - 1];

        while (true) {
            switch (op.page) {
                .leaf_table => |t| {
                    const ret = t.records.items[op.offset];
                    op.offset += 1;
                    if (op.offset == t.records.items.len) {
                        _ = self.stack.pop();
                    }
                    return ret;
                },
                .internal_table => |t| {
                    const page_nr = blk: {
                        if (op.offset == t.pointers.items.len) {
                            _ = self.stack.pop();
                            break :blk t.header.right_most_pointer.?;
                        } else {
                            const ptr = t.pointers.items[op.offset];
                            op.offset += 1;
                            break :blk ptr.page_number;
                        }
                    };

                    try self.stack.append(.{ .page = try self.database.readPage(page_nr), .offset = 0 });
                    op = &self.stack.items[self.stack.items.len - 1];
                },
            }
        }
    }

    const OffsettedPage = struct {
        page: storage.Page,
        offset: usize,
    };
};
