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
    if (number == 1) {
        // page 1 contains the DB header
        try self.file.seekTo(100);
    } else {
        try self.file.seekTo(self.header.page_size * (number - 1));
    }

    const reader = self.file.reader();

    return try storage.Page.parse(reader, self.allocator);
}

pub fn getTableRootPage(self: Self, table_name: []const u8) !storage.Page {
    return try self.readPage((try self.getTableSchema(table_name)).rootpage);
}

pub fn getIndexPage(self: Self, index_name: []const u8) !storage.Page {
    const schema_page = (try self.readPage(1)).leaf_table;
    for (schema_page.records.items) |r| {
        const sr = SchemaRecord.fromRecord(r);
        if (std.ascii.eqlIgnoreCase(sr.type, "index") and std.ascii.eqlIgnoreCase(sr.name, index_name)) {
            return try self.readPage(sr.rootpage);
        }
    }

    return error.TableNotFound;
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
        else => unreachable,
    };
}

pub fn getRow(self: Self, page: storage.Page, id: usize) !?storage.Record {
    return switch (page) {
        .leaf_table => |t| blk: {
            for (t.records.items) |rec| {
                if (rec.id == id) {
                    break :blk rec;
                }
            }
            break :blk null;
        },
        .internal_table => |t| {
            for (t.pointers.items) |ptr| {
                // TODO: binary search?
                if (id <= ptr.id) {
                    return try self.getRow(try self.readPage(ptr.page_number), id);
                }
            }
            return try self.getRow(try self.readPage(t.header.right_most_pointer.?), id);
        },
        else => unreachable,
    };
}

pub fn iterateTableRecords(self: Self, table: []const u8) !RecordIterator {
    const page = try self.getTableRootPage(table);
    return self.iterateBtreeRecords(page);
}

pub fn iterateBtreeRecords(self: Self, root_page: storage.Page) !RecordIterator {
    return try RecordIterator.init(self, root_page, null);
}

pub fn iterateIndexRecords(self: Self, root_page: storage.Page, value: storage.Value) !RecordIterator {
    return try RecordIterator.init(self, root_page, value);
}

pub const RecordIterator = struct {
    arena: std.heap.ArenaAllocator,
    database: Self,
    stack: std.ArrayList(OffsettedPage),
    value: ?storage.Value,

    pub fn init(database: Self, root_page: storage.Page, value: ?storage.Value) !RecordIterator {
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

    pub fn deinit(self: *RecordIterator) void {
        self.arena.deinit();
    }

    pub fn next(self: *RecordIterator) !?storage.Record {
        if (self.stack.items.len == 0) {
            return null;
        }

        var op = &self.stack.items[self.stack.items.len - 1];
        if (op.pop) |v| {
            op.pop = null;
            _ = try std.io.getStdOut().write("pop\n");
            return v;
        }

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
                .internal_index => |t| {
                    const page_nr = blk: {
                        if (self.value) |v| {
                            while (op.offset < t.records.items.len) {
                                const r = t.records.items[op.offset];
                                op.offset += 1;
                                if (r.fields.items[0].compare(v) == .gt) {
                                    //_ = try std.io.getStdOut().write("GT\n");
                                    _ = self.stack.pop();
                                    break :blk r.page_number.?;
                                } else if (r.fields.items[0].compare(v) == .eq) {
                                    //_ = try std.io.getStdOut().write("EQ\n");

                                    op.pop = r;
                                    break :blk r.page_number.?;
                                }
                            }
                            if (self.stack.items.len > 0) {
                                _ = self.stack.pop();
                            }
                            break :blk t.header.right_most_pointer;
                        } else if (op.offset == t.records.items.len) {
                            _ = self.stack.pop();
                            break :blk t.header.right_most_pointer.?;
                        } else {
                            const ptr = t.records.items[op.offset];
                            op.offset += 1;
                            break :blk ptr.page_number.?;
                        }
                    };
                    if (page_nr) |pn| {
                        try self.stack.append(.{ .page = try self.database.readPage(pn), .offset = 0 });
                        op = &self.stack.items[self.stack.items.len - 1];
                    }
                },
                .leaf_index => |t| {
                    if (self.value) |v| {
                        while (op.offset < t.records.items.len) {
                            const r = t.records.items[op.offset];
                            op.offset += 1;
                            if (r.fields.items[0].compare(v) == .gt) {
                                //_ = try std.io.getStdOut().write("GT LEAF\n");
                                return null;
                            } else if (r.fields.items[0].compare(v) == .eq) {
                                //_ = try std.io.getStdOut().write("MATCH\n");
                                return r;
                            }
                        }

                        return null;
                    } else {
                        const ret = t.records.items[op.offset];
                        op.offset += 1;
                        return ret;
                    }
                },
            }
        }
    }

    const OffsettedPage = struct {
        page: storage.Page,
        offset: usize,
        pop: ?storage.Record = null,
    };
};
