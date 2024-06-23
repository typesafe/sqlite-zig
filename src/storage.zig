const std = @import("std");

const Varint = @import("./storage/Varint.zig");

pub const PageType = enum(u8) {
    branch_index = 0x02,
    branch_table = 0x05,
    leaf_index = 0x0a,
    leaf_table = 0x0d,
};

pub const PageHeader = struct {
    type: PageType,
    /// The start of the first freeblock on the page, zero if there are no freeblocks.
    freeblock_offset: u16,
    /// The number of cells on the page.
    cell_count: u16,
    /// The start of the cell content area.
    cell_content_offset: u16,
    /// The number of fragmented free bytes within the cell content area.
    fragmented_free_bytes_count: u8,
    cell_offsets: []const usize,
    right_most_pointer: ?u32,

    pub fn parse(reader: std.fs.File.Reader, allocator: std.mem.Allocator) !PageHeader {
        const typ = try reader.readEnum(PageType, .big);
        const freeblock_offset = try reader.readInt(u16, .big);
        const cell_count = try reader.readInt(u16, .big);
        const cell_content_offset = try reader.readInt(u16, .big);
        const fragmented_free_bytes_count = try reader.readInt(u8, .big);
        const right_most_pointer = if (typ == PageType.branch_table or typ == PageType.branch_index)
            try reader.readInt(u32, .big)
        else
            null;

        return .{
            .type = typ,
            .freeblock_offset = freeblock_offset,
            .cell_count = cell_count,
            .cell_content_offset = cell_content_offset,
            .fragmented_free_bytes_count = fragmented_free_bytes_count,
            .cell_offsets = blk: {
                var offsets = try std.ArrayList(usize).initCapacity(allocator, cell_count);
                var values = try offsets.addManyAsSlice(cell_count);
                for (0..cell_count) |i| {
                    values[i] = try reader.readInt(u16, .big);
                }
                break :blk offsets.items;
            },
            .right_most_pointer = right_most_pointer,
        };
    }
};

pub const Value = union(enum) {
    Null: void,
    Text: []const u8,
    Integer: isize,
    Float: f64,

    pub fn format(value: @This(), comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        return switch (value) {
            .Null => writer.print("NULL", .{}),
            .Text => |v| writer.print("{s}", .{v}),
            .Integer => |v| writer.print("{any}", .{v}),
            .Float => |v| writer.print("{any}", .{v}),
        };
    }

    pub fn compare(self: Value, rhs: Value) std.math.Order {
        return switch (self) {
            .Null => .lt,
            .Text => |v| std.ascii.orderIgnoreCase(v, rhs.Text),
            .Integer => |v| std.math.order(v, rhs.Integer),
            .Float => |v| std.math.order(v, rhs.Float),
        };
    }
};

fn parseFields(reader: std.fs.File.Reader, allocator: std.mem.Allocator) !std.ArrayList(Value) {

    // we need to deal with this annoying structure:
    // PL L1 L2 L2 L3 V1 V1 V1 V2 V2 V2 V2 V2 V3 V3
    // -- -- ----- -- -------- -------------- -----
    // PL (payload offset) -> point to first byte of V1
    // Ln -> length (and type) of Value n
    // Vn -> actual value of field
    //
    // A number of fields instead of an offset in bytes would have been so much more practical! :-/
    // now we need to keep track of how many bytes we read... (instead of just moving forward)

    const payload_start = try reader.context.getPos(); // points to PL now
    const payload_offset = try Varint.parse(reader.any()); // could be more than 1 byte

    var len_offset = try reader.context.getPos(); // points to L1
    var value_offset = payload_start + payload_offset; // points to V1

    var fields = std.ArrayList(Value).init(allocator);

    while (true) {
        if (len_offset == payload_start + payload_offset) {
            break;
        }

        const field = try fields.addOne();

        try reader.context.seekTo(len_offset);
        const serial_type = try Varint.parse(reader.any());
        len_offset = try reader.context.getPos();

        try reader.context.seekTo(value_offset);

        field.* = switch (serial_type) {
            0 => .{ .Null = {} },
            1 => .{ .Integer = try reader.readInt(i8, .big) },
            2 => .{ .Integer = try reader.readInt(i16, .big) },
            3 => .{ .Integer = try reader.readInt(i24, .big) },
            4 => .{ .Integer = try reader.readInt(i32, .big) },
            5 => .{ .Integer = try reader.readInt(i48, .big) },
            6 => .{ .Integer = try reader.readInt(i64, .big) },
            7 => blk: {
                var bytes: [8]u8 = undefined;
                _ = try reader.readAll(&bytes);

                break :blk .{ .Float = std.mem.bytesToValue(f64, &bytes) };
            },
            8 => .{ .Integer = 0 },
            9 => .{ .Integer = 1 },
            10...11 => unreachable,
            else => blk: {
                // text = 13, blob = 12
                const t: usize = if (serial_type & 1 == 1) 13 else 12;

                const l = (serial_type - t) >> 1;
                var value = try std.ArrayList(u8).initCapacity(allocator, l);
                _ = try value.addManyAt(0, l);
                _ = try reader.read(value.items);

                break :blk .{ .Text = value.items };
            },
        };
        value_offset = try reader.context.getPos();
    }

    return fields;
}

pub const Pointer = struct {
    page_number: u32,
    id: usize,

    pub fn parse(reader: std.fs.File.Reader) !Pointer {
        const page_number = try reader.readInt(u32, .big);
        const id = try Varint.parse(reader.any());

        return .{
            .page_number = page_number,
            .id = id,
        };
    }
};

pub const Record = struct {
    page_number: ?u32 = null,
    id: ?usize = null,
    len: ?usize = null,
    fields: std.ArrayList(Value),

    pub fn parseInteriorIndexCell(reader: std.fs.File.Reader, allocator: std.mem.Allocator) !Record {
        const left_page_nr = try reader.readInt(u32, .big);
        const len = try Varint.parse(reader.any());

        return .{
            .page_number = left_page_nr,
            .len = len,
            .fields = try parseFields(reader, allocator),
        };
    }
    pub fn parseLeafIndexCell(reader: std.fs.File.Reader, allocator: std.mem.Allocator) !Record {
        const len = try Varint.parse(reader.any());

        return .{
            .len = len,
            .fields = try parseFields(reader, allocator),
        };
    }

    pub fn parse(reader: std.fs.File.Reader, allocator: std.mem.Allocator) !Record {
        // Record header:
        const len = try Varint.parse(reader.any());
        const id = try Varint.parse(reader.any());

        // we need to deal with this annoying structure:
        // PL L1 L2 L2 L3 V1 V1 V1 V2 V2 V2 V2 V2 V3 V3
        // -- -- ----- -- -------- -------------- -----
        // PL (payload offset) -> point to first byte of V1
        // Ln -> length (and type) of Value n
        // Vn -> actual value of field
        //
        // A number of fields instead of an offset in bytes would have been so much more practical! :-/
        // now we need to keep track of how many bytes we read... (instead of just moving forward)

        const payload_start = try reader.context.getPos(); // points to PL now
        const payload_offset = try Varint.parse(reader.any()); // could be more than 1 byte

        var len_offset = try reader.context.getPos(); // points to L1
        var value_offset = payload_start + payload_offset; // points to V1

        var fields = std.ArrayList(Value).init(allocator);

        while (true) {
            if (len_offset == payload_start + payload_offset) {
                break;
            }

            const field = try fields.addOne();

            try reader.context.seekTo(len_offset);
            const serial_type = try Varint.parse(reader.any());
            len_offset = try reader.context.getPos();

            try reader.context.seekTo(value_offset);

            field.* = switch (serial_type) {
                0 => .{ .Null = {} },
                1 => .{ .Integer = try reader.readInt(i8, .big) },
                2 => .{ .Integer = try reader.readInt(i16, .big) },
                3 => .{ .Integer = try reader.readInt(i24, .big) },
                4 => .{ .Integer = try reader.readInt(i32, .big) },
                5 => .{ .Integer = try reader.readInt(i48, .big) },
                6 => .{ .Integer = try reader.readInt(i64, .big) },
                7 => blk: {
                    var bytes: [8]u8 = undefined;
                    _ = try reader.readAll(&bytes);

                    break :blk .{ .Float = std.mem.bytesToValue(f64, &bytes) };
                },
                8 => .{ .Integer = 0 },
                9 => .{ .Integer = 1 },
                10...11 => unreachable,
                else => blk: {
                    // text = 13, blob = 12
                    const t: usize = if (serial_type & 1 == 1) 13 else 12;

                    const l = (serial_type - t) >> 1;
                    var value = try std.ArrayList(u8).initCapacity(allocator, l);
                    _ = try value.addManyAt(0, l);
                    _ = try reader.read(value.items);

                    break :blk .{ .Text = value.items };
                },
            };
            value_offset = try reader.context.getPos();
        }

        return .{
            .id = id,
            .len = len,
            .fields = fields,
        };
    }
};

pub fn Table(comptime R: type) type {
    return struct {
        pub const RecordIterator = struct {
            pub fn next(_: *@This()) ?*R {
                return null;
            }
        };

        pub fn recordIterator() RecordIterator {
            return RecordIterator{};
        }
    };
}

pub const Page = union(enum) {
    leaf_table: LeafTable,
    internal_table: InternalTable,
    leaf_index: LeafIndex,
    internal_index: InternalIndex,

    pub const InternalTable = struct {
        header: PageHeader,
        pointers: std.ArrayList(Pointer),
    };

    pub const LeafTable = struct {
        header: PageHeader,

        records: std.ArrayList(Record),
    };

    pub const LeafIndex = struct {
        header: PageHeader,
        records: std.ArrayList(Record),
    };

    pub const InternalIndex = struct {
        header: PageHeader,
        records: std.ArrayList(Record),
    };

    pub fn parse(reader: std.fs.File.Reader, allocator: std.mem.Allocator) !Page {
        var page_offset = try reader.context.getPos();
        // I assume there was a good reason (alignment?) for including the db header in the
        // first page, but it introduces some annoying "points of attention"...
        if (page_offset == 100) {
            page_offset = 0;
        }

        const header = try PageHeader.parse(reader, allocator);

        return switch (header.type) {
            .leaf_table => {
                var records = std.ArrayList(Record).init(allocator);
                for (header.cell_offsets) |offset| {
                    try reader.context.seekTo(page_offset + offset);

                    const r = try records.addOne();
                    r.* = try Record.parse(reader, allocator);
                }
                return .{ .leaf_table = .{
                    .header = header,
                    .records = records,
                } };
            },
            .branch_table => {
                var pointers = std.ArrayList(Pointer).init(allocator);
                for (header.cell_offsets) |offset| {
                    try reader.context.seekTo(page_offset + offset);

                    const r = try pointers.addOne();
                    r.* = try Pointer.parse(reader);
                }
                return .{ .internal_table = .{
                    .header = header,
                    .pointers = pointers,
                } };
            },
            .branch_index => {
                var records = std.ArrayList(Record).init(allocator);
                for (header.cell_offsets) |offset| {
                    try reader.context.seekTo(page_offset + offset);

                    const r = try records.addOne();
                    r.* = try Record.parseInteriorIndexCell(reader, allocator);
                }
                return .{ .internal_index = .{
                    .header = header,
                    .records = records,
                } };
            },
            .leaf_index => {
                var records = std.ArrayList(Record).init(allocator);
                for (header.cell_offsets) |offset| {
                    try reader.context.seekTo(page_offset + offset);

                    const r = try records.addOne();
                    r.* = try Record.parseLeafIndexCell(reader, allocator);
                }
                return .{ .leaf_index = .{
                    .header = header,
                    .records = records,
                } };
            },
        };
    }
};

pub const DatabaseHeader = struct {
    /// The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
    page_size: u16,
    /// File format write version. 1 for legacy; 2 for WAL.
    write_version: u8,
    /// 19	1	File format read version. 1 for legacy; 2 for WAL.
    read_version: u8,
    /// 20	1	Bytes of unused "reserved" space at the end of each page. Usually 0.
    unused_page_reserve: u8,
    /// 21	1	Maximum embedded payload fraction. Must be 64.
    maximum_embedded_payloadfraction: u8,
    ///22	1	Minimum embedded payload fraction. Must be 32.
    minimum_embedded_payload_fraction: u8,
    ///23	1	Leaf payload fraction. Must be 32.
    leaf_payload_fraction: u8,
    ///24	4	File change counter.
    file_change_counter: u32,
    ///28	4	Size of the database file in pages. The "in-header database size".
    db_size_in_pages: u32,
    ///32	4	Page number of the first freelist trunk page.
    first_freelist_trunk_page: u32,
    ///36	4	Total number of freelist pages.
    freelist_page_count: u32,
    ///40	4	The schema cookie.
    schema_cookie: u32,
    ///44	4	The schema format number. Supported schema formats are 1, 2, 3, and 4.
    schema_format_number: u32,
    ///48	4	Default page cache size.
    default_page_cach_size: u32,
    ///52	4	The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    largest_root_btree_page_number: u32,
    ///56	4	The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
    text_encoding: u32,
    ///60	4	The "user version" as read and set by the user_version pragma.
    user_version: u32,
    ///64	4	True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    incremental_vacuum_mode: u32,
    ///68	4	The "Application ID" set by PRAGMA application_id.
    application_id: u32,
    // ///72	20	Reserved for expansion. Must be zero.
    // reserved: [20]u8,
    // ///92	4	The version-valid-for number.
    // version_valid_for: u32,
    // ///96	4	SQLITE_VERSION_NUMBER
    // sqllite_version_number: u32,

    pub fn parse(reader: std.fs.File.Reader) !DatabaseHeader {
        try reader.skipBytes(16, .{});

        return .{
            .page_size = try reader.readInt(u16, .big),
            .write_version = try reader.readInt(u8, .big),
            .read_version = try reader.readInt(u8, .big),
            .unused_page_reserve = try reader.readInt(u8, .big),
            .maximum_embedded_payloadfraction = try reader.readInt(u8, .big),
            .minimum_embedded_payload_fraction = try reader.readInt(u8, .big),
            .leaf_payload_fraction = try reader.readInt(u8, .big),
            .file_change_counter = try reader.readInt(u32, .big),
            .db_size_in_pages = try reader.readInt(u32, .big),
            .first_freelist_trunk_page = try reader.readInt(u32, .big),
            .freelist_page_count = try reader.readInt(u32, .big),
            .schema_cookie = try reader.readInt(u32, .big),
            .schema_format_number = try reader.readInt(u32, .big),
            .default_page_cach_size = try reader.readInt(u32, .big),
            .largest_root_btree_page_number = try reader.readInt(u32, .big),
            .text_encoding = try reader.readInt(u32, .big),
            .user_version = try reader.readInt(u32, .big),
            .incremental_vacuum_mode = try reader.readInt(u32, .big),
            .application_id = try reader.readInt(u32, .big),
            // .reserved = reader.readStruct(u32, .big),
            // .version_valid_for = reader.readInt(u32, .big),
            // .sqllite_version_number = reader.readInt(u32, .big),
        };
    }
};
