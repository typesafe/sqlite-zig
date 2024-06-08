const std = @import("std");

const storage = @import("./storage.zig");

const Self = @This();

file: std.fs.File,

pub fn init(file: std.fs.File) !Self {
    return Self{ .file = file };
}

pub fn readHeader(self: Self) !storage.DatabaseHeader {
    try self.file.seekTo(0);

    return try storage.DatabaseHeader.parse(self.file.reader());
}

pub fn readPage(self: Self) !storage.Page {
    try self.file.seekTo(100);
    const reader = self.file.reader();

    return try storage.Page.parse(reader);
}
