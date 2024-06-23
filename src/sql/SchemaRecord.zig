const std = @import("std");
const Value = @import("../storage.zig").Value;

const Self = @This();

type: []const u8,
name: []const u8,
tbl_name: []const u8,
rootpage: usize,
sql: []const u8,

pub fn fromValues(values: []const Value) Self {
    return .{
        .type = values[0].Text,
        .name = values[1].Text,
        .tbl_name = values[2].Text,
        .rootpage = @bitCast(values[3].Integer),
        .sql = values[4].Text,
    };
}
