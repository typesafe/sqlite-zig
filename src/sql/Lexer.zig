const std = @import("std");

pub fn get_tokens(input: []const u8) TokenIterator {
    return TokenIterator{ .input = input };
}

pub const TokenIterator = struct {
    const Self = @This();

    input: []const u8,
    i: usize = 0,

    /// Returns the next token until the input is exhausted.
    pub fn next(self: *Self) ?Token {
        const char = self.readChar();

        if (char) |c| {
            return switch (c) {

                // operators
                '-' => if (self.readMatchingChar('-')) |_| .{ .comment = self.readCommentLine() } else .{ .operator = .minus },
                //'!' => if (self.readMatchingChar('=')) |_| .{ .operator = .ne } else .{ .operator = .bang },
                '=' => if (self.readMatchingChar('=')) |_| .{ .operator = .eq } else .{ .operator = .assign },
                '+' => .{ .operator = .plus },
                '*' => .{ .operator = .asterisk },
                '<' => .{ .operator = .lt },
                '>' => .{ .operator = .gt },

                // separators
                ';' => .semicolon,
                ',' => .comma,
                '.' => .dot,
                '{' => .lbrace,
                '}' => .rbrace,
                '(' => .lparen,
                ')' => .rparen,
                '[' => .lbracket,
                ']' => .rbracket,
                '\'', '"' => .{ .identifier = self.readLiteralString() },

                '0'...'9' => return .{ .integer = self.readInteger() },

                'a'...'z', 'A'...'Z', '_' => {
                    const v = self.readIdentifier();
                    return if (Keywords.get(v)) |kw| .{ .keyword = kw } else .{ .identifier = v };
                },

                else => return .{ .illegal = c },
            };
        }

        return null;
    }

    /// Returns the next non-whitespace character or null when the end of the input is reached.
    /// i is incremented to match the index of the _next_ character.
    fn readChar(self: *Self) ?u8 {
        while (true) {
            if (self.i >= self.input.len) {
                return null;
            }

            const char = self.input[self.i];
            self.i = self.i + 1;

            if (std.ascii.isWhitespace(char)) continue;

            return char;
        }
    }

    fn peekChar(self: *Self) ?u8 {
        if (self.i >= self.input.len) {
            return null;
        }

        return self.input[self.i];
    }

    /// Returns the next character (and increments the index) if it matches the given character.
    fn readMatchingChar(self: *Self, c: u8) ?u8 {
        if (self.peekChar() == c) {
            self.i = self.i + 1;
            return c;
        } else {
            return null;
        }
    }

    fn readCommentLine(self: *@This()) []const u8 {
        return self.readMatching(isNoLinebeak);
    }

    fn readLiteralString(self: *@This()) []const u8 {
        _ = self.readChar(); // left quote
        const res = self.readMatching(isNoQuote);
        _ = self.readChar(); // right quote
        return res;
    }

    fn readIdentifier(self: *@This()) []const u8 {
        return self.readMatching(isIdentifierChar);
    }

    fn readInteger(self: *@This()) []const u8 {
        return self.readMatching(std.ascii.isDigit);
    }

    fn readMatching(self: *Self, m: anytype) []const u8 {
        const offset = self.i - 1;

        while (if (self.peekChar()) |c| m(c) else false) {
            self.i = self.i + 1;
        }

        return self.input[offset..self.i];
    }

    fn isNoQuote(c: ?u8) bool {
        return if (c) |v| v != '"' and v != '\'' else true;
    }

    fn isIdentifierChar(c: ?u8) bool {
        return if (c) |v| std.ascii.isAlphanumeric(v) or v == '_' else false;
    }

    fn isNoLinebeak(c: ?u8) bool {
        return (c != '\n' and c != '\r');
    }
};

pub const Token = union(enum) {
    eof,
    illegal: u8,
    comment: []const u8,
    identifier: []const u8,
    //literal_string: []const u8,
    integer: []const u8,

    operator: Operator,
    keyword: Keyword,

    // separators
    semicolon,
    comma,
    dot,
    lbrace,
    rbrace,
    lparen,
    rparen,
    lbracket,
    rbracket,

    quote,
    dquote,

    pub fn format(value: @This(), comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        return switch (value) {
            .keyword => writer.print("<keyword:{s}>", .{@tagName(value.keyword)}),
            .identifier => writer.print("<symbol:{s}>", .{value.identifier}),
            .operator => writer.print("<op:{s}>", .{@tagName(value.operator)}),
            else => writer.print("<{s}>", .{@tagName(value)}),
        };
    }
};

pub const Keyword = enum {
    create,
    table,
    bigint,
    primary,
    key,
    autoincrement,
    text,
    integer,
    select,
    from,
    count,
    where,
    not,
    null,
};

const Keywords = std.ComptimeStringMapWithEql(Keyword, .{
    .{ "create", .create },
    .{ "table", .table },
    .{ "bigint", .bigint },
    .{ "primary", .primary },
    .{ "key", .key },
    .{ "autoincrement", .autoincrement },
    .{ "text", .text },
    .{ "integer", .integer },
    .{ "select", .select },
    .{ "from", .from },
    .{ "count", .count },
    .{ "where", .where },
    .{ "not", .not },
    .{ "null", .null },
}, std.ascii.eqlIgnoreCase);

/// Operators, sorted by their precedence.
pub const Operator = enum {
    bang,
    fslash,
    assign,
    lt,
    gt,
    eq,
    ne,
    plus,
    minus,
    asterisk,
};
