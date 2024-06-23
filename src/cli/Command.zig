const std = @import("std");
const Parser = @import("../sql/Parser.zig");

pub const Command = union(enum) {
    const Self = @This();

    dbinfo: void,
    tables: void,
    sql: Parser.SqlStatement,

    pub fn parse(value: []const u8, allocator: std.mem.Allocator) !Self {
        if (std.mem.eql(u8, value, ".dbinfo")) {
            return .dbinfo;
        }

        if (std.mem.eql(u8, value, ".tables")) {
            return .tables;
        }

        return .{ .sql = try Parser.parse(value, allocator) };
    }
};
