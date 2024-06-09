const std = @import("std");

const Chunk = packed struct {
    value: u7,
    more: u1,
};

pub fn parse(reader: std.io.AnyReader) !usize {
    var result: usize = 0;

    for (0..9) |_| {
        const chunk: Chunk = @bitCast(try reader.readByte());

        result = (result << 7) | chunk.value;

        if (chunk.more == 0) {
            break;
        }
    }

    return result;
}

test "parse" {
    var s = std.io.fixedBufferStream(&[2]u8{ 0b10000111, 0b01101000 });
    const res = try parse(s.reader().any());

    try std.testing.expect(res == 0b0000_0011_1110_1000); // 1000
}
