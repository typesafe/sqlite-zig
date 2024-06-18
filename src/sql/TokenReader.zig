const std = @import("std");
const Lexer = @import("./Lexer.zig");

const Self = @This();

iterator: *Lexer.TokenIterator,

current: ?Lexer.Token = null,
next: ?Lexer.Token = null,

pub fn init(iterator: *Lexer.TokenIterator) Self {
    var reader = Self{
        .iterator = iterator,
    };

    // initialize current and next values
    _ = (&reader).advance();
    _ = (&reader).advance();

    return reader;
}

pub fn advance(self: *Self) ?Lexer.Token {
    self.current = self.next;
    self.next = self.iterator.next();
    return self.current;
}

pub fn ensureNext(self: *Self, t: Lexer.Token) !Lexer.Token {
    return if (std.mem.eql(u8, @tagName(self.next.?), @tagName(t))) t else error.UnexpectedToken;
}
