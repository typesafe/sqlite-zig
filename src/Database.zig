const std = @import("std");

const storage = @import("./storage.zig");

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

pub fn readPage(self: Self) !storage.Page {
    try self.file.seekTo(100);
    const reader = self.file.reader();

    return try storage.Page.parse(reader, self.allocator);
}
