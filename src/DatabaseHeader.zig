// packed does not work: https://github.com/ziglang/zig/issues/12547#issuecomment-1229227353
// fields are defined as arrays to avoid endianness issues
pub const DatabaseHeader = extern struct {
    id: [16]u8,
    /// The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
    pageSize: [2]u8,
    /// File format write version. 1 for legacy; 2 for WAL.
    writeVersion: u8,
    /// 19	1	File format read version. 1 for legacy; 2 for WAL.
    readVersin: u8,
    /// 20	1	Bytes of unused "reserved" space at the end of each page. Usually 0.
    unusedPageReserve: u8,
    /// 21	1	Maximum embedded payload fraction. Must be 64.
    maximumEmbeddedPayloadFraction: u8,
    ///22	1	Minimum embedded payload fraction. Must be 32.
    minimumEmbeddedPayloadFraction: u8,
    ///23	1	Leaf payload fraction. Must be 32.
    leafPayloadFraction: u8,
    ///24	4	File change counter.
    fileChangeCounter: [4]u8,
    ///28	4	Size of the database file in pages. The "in-header database size".
    dbSizeInPages: [4]u8,
    ///32	4	Page number of the first freelist trunk page.
    firstFreelistTrunkPage: [4]u8,
    ///36	4	Total number of freelist pages.
    freelistPageCount: [4]u8,
    ///40	4	The schema cookie.
    schemaCookie: [4]u8,
    ///44	4	The schema format number. Supported schema formats are 1, 2, 3, and 4.
    schemaFormatNumber: [4]u8,
    ///48	4	Default page cache size.
    defaultPageCachSize: [4]u8,
    ///52	4	The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    largestRootBTreePageNumber: [4]u8,
    ///56	4	The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
    textEncoding: [4]u8,
    ///60	4	The "user version" as read and set by the user_version pragma.
    userVersion: [4]u8,
    ///64	4	True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    incrementalVacuumMode: [4]u8,
    ///68	4	The "Application ID" set by PRAGMA application_id.
    applicationId: [4]u8,
    ///72	20	Reserved for expansion. Must be zero.
    reserved: [20]u8,
    ///92	4	The version-valid-for number.
    versionValidFor: [4]u8,
    ///96	4	SQLITE_VERSION_NUMBER
    sqlLiteVersionNumber: [4]u8,
};
