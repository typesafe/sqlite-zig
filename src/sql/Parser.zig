const std = @import("std");
const Lexer = @import("./Lexer.zig");
const TokenReader = @import("./TokenReader.zig");

const Self = @This();

pub fn parse(sql: []const u8, allocator: std.mem.Allocator) !SqlStatement {
    var it = Lexer.get_tokens(sql);
    var tr = TokenReader.init(&it);

    while (tr.current) |t| {
        switch (t) {
            .keyword => |kw| {
                return switch (kw) {
                    .create => try parseCreateTable(&tr, allocator),
                    .select => try parseSelect(&tr, allocator),
                    else => unreachable,
                };
            },
            .eof => break,
            else => unreachable,
        }
    }

    unreachable;
}

fn parseCreateTable(tr: *TokenReader, allocator: std.mem.Allocator) !SqlStatement {
    _ = tr.advance(); // create -> table
    _ = tr.advance(); // table -> name

    const name = blk: {
        if (tr.current.? == .dquote) {
            _ = tr.advance();
            const res = tr.current.?.identifier;
            _ = tr.advance();
            break :blk res;
        }

        break :blk tr.current.?.identifier;
    };

    _ = tr.advance(); // name -> '('

    return .{
        .create_table = .{
            .name = name,
            .fields = try parseFields(tr, allocator),
        },
    };
}

fn parseSelect(tr: *TokenReader, allocator: std.mem.Allocator) !SqlStatement {
    var select = SqlStatement.Select{ .fields = std.ArrayList([]const u8).init(allocator), .from = "" };

    // first iteration goes from select to first field
    while (tr.advance()) |t| {
        switch (t) {
            .identifier => |name| (try select.fields.addOne()).* = try allocator.dupe(u8, name),
            .comma => {}, // skip
            .keyword => |kw| {
                switch (kw) {
                    .count => {
                        _ = tr.advance(); // (

                        select.count = "*";
                        _ = tr.advance();

                        _ = tr.advance(); // )
                    },
                    .from => {
                        const from = tr.advance().?;
                        select.from = from.identifier;
                    },
                    .where => {
                        const field = tr.advance().?.identifier;
                        _ = tr.advance(); // =

                        const value = tr.advance().?.identifier;

                        select.where = .{ .field = field, .value = value };
                    },
                    else => unreachable,
                }
            },
            else => break,
        }
    }

    return .{ .select = select };
}

fn parseFields(tr: *TokenReader, allocator: std.mem.Allocator) !std.StringHashMap(SqlStatement.Field) {
    var fields = std.StringHashMap(SqlStatement.Field).init(allocator);

    // ( <- this is where we currently are...
    //     f1 t1 opt opt, <- these fields need to be parsed
    //     f2 t2
    //     PK, FK, ... <- these are not supported for now
    // )

    while (tr.advance()) |t| {
        if (t == .rparen) {
            break;
        }

        switch (t) {
            .identifier => |id| try fields.put(id, try parseFieldDefinition(tr, fields.count(), allocator)),
            // TODO: parse PK, FK, etc.: .keyword => |kw| ...
            else => unreachable,
        }
    }

    return fields;
}

/// Assumes tr.current is field name, consumes the comma, if any, does not consume the rparen after the last field definition
fn parseFieldDefinition(tr: *TokenReader, index: u32, _: std.mem.Allocator) !SqlStatement.Field {
    var field = SqlStatement.Field{
        .index = index,
        .typ = SqlStatement.FieldType.text,
    };

    while (tr.advance()) |t| {
        switch (t) {
            .keyword => |kw| switch (kw) {
                .text => field.typ = SqlStatement.FieldType.text,
                .integer => field.typ = SqlStatement.FieldType.int,
                .autoincrement => field.autoincrement = true,
                .primary => {
                    _ = tr.advance(); // consume `key`
                    field.pk = true;
                },
                .not => {
                    _ = tr.advance(); // consume `null`
                    //field.pk = true;
                },
                else => unreachable,
            },

            else => unreachable,
        }

        if (tr.next) |next| {
            if (next == .comma) {
                _ = tr.advance();
                return field;
            }
            if (next == .rparen) {
                return field;
            }
        }
    }

    return field;
}

pub const SqlStatement = union(enum) {
    create_table: CreateTable,
    select: Select,

    pub const Insert = struct {};

    pub const CreateTable = struct {
        name: []const u8,
        fields: std.StringHashMap(Field),
    };

    pub const Field = struct {
        index: u32,
        typ: FieldType,
        autoincrement: bool = false,
        pk: bool = false,
    };

    pub const FieldType = enum {
        int,
        text,
    };

    pub const Select = struct {
        fields: std.ArrayList([]const u8),
        from: []const u8,
        count: ?[]const u8 = null,
        where: ?WhereClause = null,
    };

    pub const WhereClause = struct {
        field: []const u8,
        value: []const u8,
    };
};

test "parse CREATE TABLE" {
    const sql =
        \\CREATE TABLE oranges
        \\(
        \\  id integer primary key autoincrement,
        \\  name text,
        \\  description text
        \\);;
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const res = try Self.parse(sql, arena.allocator());

    try std.testing.expect(res.create_table.fields.count() == 3);
    try std.testing.expect(res.create_table.fields.get("name").?.index == 1);
}

test "SELECT name FROM apples" {
    const sql = "SELECT name FROM apples";
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const res = try Self.parse(sql, arena.allocator());

    try std.testing.expect(res.select.fields.items.len == 1);
    try std.testing.expectEqualStrings("name", res.select.fields.items[0]);
    try std.testing.expectEqualStrings("apples", res.select.from);
}

test "SELECT f1, f2 FROM tbl" {
    const sql = "SELECT f1, f2 FROM tbl";
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const res = try Self.parse(sql, arena.allocator());

    try std.testing.expect(res.select.fields.items.len == 2);
    try std.testing.expectEqualStrings("f1", res.select.fields.items[0]);
    try std.testing.expectEqualStrings("f2", res.select.fields.items[1]);
    try std.testing.expectEqualStrings("tbl", res.select.from);
}

test "SELECT f1, f2 FROM tbl WHERE f1 = 'foo bar'" {
    const sql = "SELECT f1, f2 FROM tbl WHERE f1 = 'foo bar'";
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const res = try Self.parse(sql, arena.allocator());

    try std.testing.expect(res.select.fields.items.len == 2);
    try std.testing.expectEqualStrings("f1", res.select.fields.items[0]);
    try std.testing.expectEqualStrings("f2", res.select.fields.items[1]);
    try std.testing.expectEqualStrings("tbl", res.select.from);
    try std.testing.expectEqualStrings("f1", res.select.where.?.field);
    try std.testing.expectEqualStrings("foo bar", res.select.where.?.value);
}

test "SELECT COUNT(*) FROM tbl" {
    const sql = "SELECT COUNT(*) FROM tbl";
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const res = try Self.parse(sql, arena.allocator());

    try std.testing.expect(res.select.fields.items.len == 0);
    try std.testing.expectEqualStrings("*", res.select.count.?);
    try std.testing.expectEqualStrings("tbl", res.select.from);
}
