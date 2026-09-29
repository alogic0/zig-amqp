//! AMQP 0-9-1 wire format serialization and deserialization primitives.
const std = @import("std");

pub const protocol_header = "AMQP\x00\x00\x09\x01";

pub const Error = error{
    BufferTooSmall,
    UnexpectedEof,
    InvalidStringLength,
    InvalidFieldType,
    MalformedTable,
    MalformedArray,
    StringTooLong,
};

/// Reads a big-endian u16 from a byte slice.
pub fn readU16(bytes: []const u8) Error!u16 {
    if (bytes.len < 2) return Error.UnexpectedEof;
    return std.mem.readInt(u16, bytes[0..2], .big);
}

/// Reads a big-endian u32 from a byte slice.
pub fn readU32(bytes: []const u8) Error!u32 {
    if (bytes.len < 4) return Error.UnexpectedEof;
    return std.mem.readInt(u32, bytes[0..4], .big);
}

/// Reads a big-endian u64 from a byte slice.
pub fn readU64(bytes: []const u8) Error!u64 {
    if (bytes.len < 8) return Error.UnexpectedEof;
    return std.mem.readInt(u64, bytes[0..8], .big);
}

/// Reads an 8-bit integer.
pub fn readU8(bytes: []const u8) Error!u8 {
    if (bytes.len < 1) return Error.UnexpectedEof;
    return bytes[0];
}

/// Writes a big-endian u16 to a byte slice, returning number of bytes written.
pub fn writeU16(dest: []u8, value: u16) Error!usize {
    if (dest.len < 2) return Error.BufferTooSmall;
    std.mem.writeInt(u16, dest[0..2], value, .big);
    return 2;
}

/// Writes a big-endian u32 to a byte slice, returning number of bytes written.
pub fn writeU32(dest: []u8, value: u32) Error!usize {
    if (dest.len < 4) return Error.BufferTooSmall;
    std.mem.writeInt(u32, dest[0..4], value, .big);
    return 4;
}

/// Writes a big-endian u64 to a byte slice, returning number of bytes written.
pub fn writeU64(dest: []u8, value: u64) Error!usize {
    if (dest.len < 8) return Error.BufferTooSmall;
    std.mem.writeInt(u64, dest[0..8], value, .big);
    return 8;
}

/// Writes an 8-bit integer to a byte slice.
pub fn writeU8(dest: []u8, value: u8) Error!usize {
    if (dest.len < 1) return Error.BufferTooSmall;
    dest[0] = value;
    return 1;
}

/// Writes a short string (1-byte length prefix + string payload). Max 255 bytes.
pub fn writeShortString(dest: []u8, str: []const u8) Error!usize {
    if (str.len > 255) return Error.StringTooLong;
    if (dest.len < 1 + str.len) return Error.BufferTooSmall;
    dest[0] = @intCast(str.len);
    @memcpy(dest[1 .. 1 + str.len], str);
    return 1 + str.len;
}

/// Reads a short string (1-byte length prefix + string payload).
pub fn readShortString(bytes: []const u8) Error!struct { str: []const u8, consumed: usize } {
    if (bytes.len < 1) return Error.UnexpectedEof;
    const len: usize = bytes[0];
    if (bytes.len < 1 + len) return Error.UnexpectedEof;
    return .{
        .str = bytes[1 .. 1 + len],
        .consumed = 1 + len,
    };
}

/// Writes a long string (4-byte length prefix + payload).
pub fn writeLongString(dest: []u8, str: []const u8) Error!usize {
    if (str.len > std.math.maxInt(u32)) return Error.StringTooLong;
    if (dest.len < 4 + str.len) return Error.BufferTooSmall;
    std.mem.writeInt(u32, dest[0..4], @intCast(str.len), .big);
    @memcpy(dest[4 .. 4 + str.len], str);
    return 4 + str.len;
}

/// Reads a long string (4-byte length prefix + payload).
pub fn readLongString(bytes: []const u8) Error!struct { str: []const u8, consumed: usize } {
    if (bytes.len < 4) return Error.UnexpectedEof;
    const len: usize = @intCast(std.mem.readInt(u32, bytes[0..4], .big));
    if (bytes.len < 4 + len) return Error.UnexpectedEof;
    return .{
        .str = bytes[4 .. 4 + len],
        .consumed = 4 + len,
    };
}

/// Supported value types in an AMQP 0-9-1 Field Table.
pub const FieldValue = union(enum) {
    bool_val: bool,
    short_short_int: i8,
    short_short_uint: u8,
    short_int: i16,
    short_uint: u16,
    long_int: i32,
    long_uint: u32,
    long_long_int: i64,
    float_val: f32,
    double_val: f64,
    short_string: []const u8,
    long_string: []const u8,
    timestamp: i64,
    void_val: void,
    raw_table: []const u8,
    raw_array: []const u8,
};

/// A single key-value entry in a field table.
pub const FieldEntry = struct {
    name: []const u8,
    value: FieldValue,
};

/// Encodes an AMQP 0-9-1 field value to destination buffer.
pub fn writeFieldValue(dest: []u8, value: FieldValue) Error!usize {
    if (dest.len < 1) return Error.BufferTooSmall;
    switch (value) {
        .bool_val => |b| {
            dest[0] = 't';
            const written = try writeU8(dest[1..], if (b) 1 else 0);
            return 1 + written;
        },
        .short_short_int => |i| {
            dest[0] = 'b';
            const written = try writeU8(dest[1..], @bitCast(i));
            return 1 + written;
        },
        .short_short_uint => |u| {
            dest[0] = 'B';
            const written = try writeU8(dest[1..], u);
            return 1 + written;
        },
        .short_int => |i| {
            dest[0] = 's';
            const written = try writeU16(dest[1..], @bitCast(i));
            return 1 + written;
        },
        .short_uint => |u| {
            dest[0] = 'u';
            const written = try writeU16(dest[1..], u);
            return 1 + written;
        },
        .long_int => |i| {
            dest[0] = 'I';
            const written = try writeU32(dest[1..], @bitCast(i));
            return 1 + written;
        },
        .long_uint => |u| {
            dest[0] = 'i';
            const written = try writeU32(dest[1..], u);
            return 1 + written;
        },
        .long_long_int => |i| {
            dest[0] = 'l';
            const written = try writeU64(dest[1..], @bitCast(i));
            return 1 + written;
        },
        .float_val => |f| {
            dest[0] = 'f';
            const written = try writeU32(dest[1..], @bitCast(f));
            return 1 + written;
        },
        .double_val => |d| {
            dest[0] = 'd';
            const written = try writeU64(dest[1..], @bitCast(d));
            return 1 + written;
        },
        .short_string => |s| {
            dest[0] = 's';
            const written = try writeShortString(dest[1..], s);
            return 1 + written;
        },
        .long_string => |s| {
            dest[0] = 'S';
            const written = try writeLongString(dest[1..], s);
            return 1 + written;
        },
        .timestamp => |t| {
            dest[0] = 'T';
            const written = try writeU64(dest[1..], @bitCast(t));
            return 1 + written;
        },
        .void_val => {
            dest[0] = 'V';
            return 1;
        },
        .raw_table => |t| {
            dest[0] = 'F';
            if (dest.len < 1 + 4 + t.len) return Error.BufferTooSmall;
            std.mem.writeInt(u32, dest[1..5], @intCast(t.len), .big);
            @memcpy(dest[5 .. 5 + t.len], t);
            return 1 + 4 + t.len;
        },
        .raw_array => |a| {
            dest[0] = 'A';
            if (dest.len < 1 + 4 + a.len) return Error.BufferTooSmall;
            std.mem.writeInt(u32, dest[1..5], @intCast(a.len), .big);
            @memcpy(dest[5 .. 5 + a.len], a);
            return 1 + 4 + a.len;
        },
    }
}

/// Reads an AMQP field value from bytes.
pub fn readFieldValue(bytes: []const u8) Error!struct { value: FieldValue, consumed: usize } {
    if (bytes.len < 1) return Error.UnexpectedEof;
    const type_tag = bytes[0];
    const rest = bytes[1..];
    switch (type_tag) {
        't' => {
            if (rest.len < 1) return Error.UnexpectedEof;
            return .{ .value = .{ .bool_val = rest[0] != 0 }, .consumed = 2 };
        },
        'b' => {
            if (rest.len < 1) return Error.UnexpectedEof;
            return .{ .value = .{ .short_short_int = @bitCast(rest[0]) }, .consumed = 2 };
        },
        'B' => {
            if (rest.len < 1) return Error.UnexpectedEof;
            return .{ .value = .{ .short_short_uint = rest[0] }, .consumed = 2 };
        },
        's' => {
            if (rest.len < 2) return Error.UnexpectedEof;
            const val = std.mem.readInt(i16, rest[0..2], .big);
            return .{ .value = .{ .short_int = val }, .consumed = 3 };
        },
        'u' => {
            if (rest.len < 2) return Error.UnexpectedEof;
            const val = std.mem.readInt(u16, rest[0..2], .big);
            return .{ .value = .{ .short_uint = val }, .consumed = 3 };
        },
        'I' => {
            if (rest.len < 4) return Error.UnexpectedEof;
            const val = std.mem.readInt(i32, rest[0..4], .big);
            return .{ .value = .{ .long_int = val }, .consumed = 5 };
        },
        'i' => {
            if (rest.len < 4) return Error.UnexpectedEof;
            const val = std.mem.readInt(u32, rest[0..4], .big);
            return .{ .value = .{ .long_uint = val }, .consumed = 5 };
        },
        'l' => {
            if (rest.len < 8) return Error.UnexpectedEof;
            const val = std.mem.readInt(i64, rest[0..8], .big);
            return .{ .value = .{ .long_long_int = val }, .consumed = 9 };
        },
        'f' => {
            if (rest.len < 4) return Error.UnexpectedEof;
            const bits = std.mem.readInt(u32, rest[0..4], .big);
            return .{ .value = .{ .float_val = @bitCast(bits) }, .consumed = 5 };
        },
        'd' => {
            if (rest.len < 8) return Error.UnexpectedEof;
            const bits = std.mem.readInt(u64, rest[0..8], .big);
            return .{ .value = .{ .double_val = @bitCast(bits) }, .consumed = 9 };
        },
        'S' => {
            const parsed = try readLongString(rest);
            return .{ .value = .{ .long_string = parsed.str }, .consumed = 1 + parsed.consumed };
        },
        'T' => {
            if (rest.len < 8) return Error.UnexpectedEof;
            const val = std.mem.readInt(i64, rest[0..8], .big);
            return .{ .value = .{ .timestamp = val }, .consumed = 9 };
        },
        'V' => {
            return .{ .value = .void_val, .consumed = 1 };
        },
        'F' => {
            if (rest.len < 4) return Error.UnexpectedEof;
            const table_len = @as(usize, @intCast(std.mem.readInt(u32, rest[0..4], .big)));
            if (rest.len < 4 + table_len) return Error.UnexpectedEof;
            return .{ .value = .{ .raw_table = rest[4 .. 4 + table_len] }, .consumed = 1 + 4 + table_len };
        },
        'A' => {
            if (rest.len < 4) return Error.UnexpectedEof;
            const array_len = @as(usize, @intCast(std.mem.readInt(u32, rest[0..4], .big)));
            if (rest.len < 4 + array_len) return Error.UnexpectedEof;
            return .{ .value = .{ .raw_array = rest[4 .. 4 + array_len] }, .consumed = 1 + 4 + array_len };
        },
        else => return Error.InvalidFieldType,
    }
}

/// Helper to serialize a Table of FieldEntries into destination.
pub fn writeTable(dest: []u8, entries: []const FieldEntry) Error!usize {
    if (dest.len < 4) return Error.BufferTooSmall;
    var cursor: usize = 4;
    for (entries) |entry| {
        const key_len = try writeShortString(dest[cursor..], entry.name);
        cursor += key_len;
        const val_len = try writeFieldValue(dest[cursor..], entry.value);
        cursor += val_len;
    }
    const table_len: u32 = @intCast(cursor - 4);
    std.mem.writeInt(u32, dest[0..4], table_len, .big);
    return cursor;
}

/// Iterator to parse a raw FieldTable buffer without heap allocations.
pub const TableIterator = struct {
    bytes: []const u8,
    cursor: usize = 0,

    pub fn init(table_bytes: []const u8) TableIterator {
        return .{ .bytes = table_bytes };
    }

    pub fn next(self: *TableIterator) Error!?FieldEntry {
        if (self.cursor >= self.bytes.len) return null;
        const key_info = try readShortString(self.bytes[self.cursor..]);
        self.cursor += key_info.consumed;
        const val_info = try readFieldValue(self.bytes[self.cursor..]);
        self.cursor += val_info.consumed;
        return FieldEntry{
            .name = key_info.str,
            .value = val_info.value,
        };
    }
};

test "wire integer primitives roundtrip" {
    var buf: [16]u8 = undefined;

    const w16 = try writeU16(&buf, 0x1234);
    try std.testing.expectEqual(@as(usize, 2), w16);
    try std.testing.expectEqual(@as(u16, 0x1234), try readU16(buf[0..2]));

    const w32 = try writeU32(&buf, 0xDEADBEEF);
    try std.testing.expectEqual(@as(usize, 4), w32);
    try std.testing.expectEqual(@as(u32, 0xDEADBEEF), try readU32(buf[0..4]));

    const w64 = try writeU64(&buf, 0x0123456789ABCDEF);
    try std.testing.expectEqual(@as(usize, 8), w64);
    try std.testing.expectEqual(@as(u64, 0x0123456789ABCDEF), try readU64(buf[0..8]));
}

test "wire string primitives roundtrip" {
    var buf: [64]u8 = undefined;

    const short_text = "callapp-telephony";
    const w_short = try writeShortString(&buf, short_text);
    try std.testing.expectEqual(1 + short_text.len, w_short);
    const r_short = try readShortString(buf[0..w_short]);
    try std.testing.expectEqualStrings(short_text, r_short.str);

    const long_text = "rabbitmq_message_payload_over_boundary";
    const w_long = try writeLongString(&buf, long_text);
    try std.testing.expectEqual(4 + long_text.len, w_long);
    const r_long = try readLongString(buf[0..w_long]);
    try std.testing.expectEqualStrings(long_text, r_long.str);
}

test "field table serialization and streaming iterator" {
    var buf: [512]u8 = undefined;

    const entries = [_]FieldEntry{
        .{ .name = "connection_name", .value = .{ .long_string = "callapp-worker/1" } },
        .{ .name = "version", .value = .{ .long_string = "1.0.0" } },
        .{ .name = "heartbeat", .value = .{ .short_int = 60 } },
        .{ .name = "active", .value = .{ .bool_val = true } },
    };

    const written = try writeTable(&buf, &entries);
    const table_len = std.mem.readInt(u32, buf[0..4], .big);
    try std.testing.expectEqual(@as(u32, @intCast(written - 4)), table_len);

    var iter = TableIterator.init(buf[4..written]);

    const e1 = (try iter.next()).?;
    try std.testing.expectEqualStrings("connection_name", e1.name);
    try std.testing.expectEqualStrings("callapp-worker/1", e1.value.long_string);

    const e2 = (try iter.next()).?;
    try std.testing.expectEqualStrings("version", e2.name);
    try std.testing.expectEqualStrings("1.0.0", e2.value.long_string);

    const e3 = (try iter.next()).?;
    try std.testing.expectEqualStrings("heartbeat", e3.name);
    try std.testing.expectEqual(@as(i16, 60), e3.value.short_int);

    const e4 = (try iter.next()).?;
    try std.testing.expectEqualStrings("active", e4.name);
    try std.testing.expect(e4.value.bool_val);

    try std.testing.expect((try iter.next()) == null);
}
