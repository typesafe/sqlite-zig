const std = @import("std");

pub const PageType = enum(u8) {
    branch_index = 0x02,
    branch_table = 0x05,
    leaf_index = 0x0a,
    leaf_table = 0x0d,
};

pub const PageHeader = struct {
    typ: PageType,
    /// The start of the first freeblock on the page, zero if there are no freeblocks.
    free_block_offset: u16,
    /// The number of cells on the page.
    cell_count: u16,
    /// The start of the cell content area.
    cell_content_offset: u16,
    /// The number of fragmented free bytes within the cell content area.
    fragmented_free_bytes_count: u8,
    /// the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
    right_pointer: ?u32,

    pub fn parse(reader: std.fs.File.Reader) !PageHeader {
        const typ = try reader.readEnum(PageType, .big);

        return .{
            .typ = typ,
            .free_block_offset = try reader.readInt(u16, .big),
            .cell_count = try reader.readInt(u16, .big),
            .cell_content_offset = try reader.readInt(u16, .big),
            .fragmented_free_bytes_count = try reader.readInt(u8, .big),
            .right_pointer = switch (typ) {
                .branch_index, .branch_table => try reader.readInt(u32, .big),
                else => null,
            },
        };
    }
};

pub const Page = struct {
    header: PageHeader,

    pub fn parse(reader: std.fs.File.Reader) !Page {
        return .{ .header = try PageHeader.parse(reader) };
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
