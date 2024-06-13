const std = @import("std");
const Record = @import("../storage.zig").Record;

const Self = @This();

type: []const u8,
name: []const u8,
tbl_name: []const u8,
rootpage: usize,
sql: []const u8,

pub fn fromRecord(r: Record) Self {
    return .{
        .type = r.fields.items[0].Text,
        .name = r.fields.items[1].Text,
        .tbl_name = r.fields.items[2].Text,
        .rootpage = @bitCast(r.fields.items[3].Integer),
        .sql = r.fields.items[4].Text,
    };
}
