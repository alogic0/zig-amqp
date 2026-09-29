//! AMQP 0-9-1 Basic Content Header and Properties (Class 60).
const std = @import("std");
const wire = @import("wire.zig");

pub const CLASS_BASIC: u16 = 60;

pub const Flag = struct {
    pub const CONTENT_TYPE: u16 = 1 << 15; // 0x8000
    pub const CONTENT_ENCODING: u16 = 1 << 14; // 0x4000
    pub const HEADERS: u16 = 1 << 13; // 0x2000
    pub const DELIVERY_MODE: u16 = 1 << 12; // 0x1000
    pub const PRIORITY: u16 = 1 << 11; // 0x0800
    pub const CORRELATION_ID: u16 = 1 << 10; // 0x0400
    pub const REPLY_TO: u16 = 1 << 9; // 0x0200
    pub const EXPIRATION: u16 = 1 << 8; // 0x0100
    pub const MESSAGE_ID: u16 = 1 << 7; // 0x0080
    pub const TIMESTAMP: u16 = 1 << 6; // 0x0040
    pub const TYPE: u16 = 1 << 5; // 0x0020
    pub const USER_ID: u16 = 1 << 4; // 0x0010
    pub const APP_ID: u16 = 1 << 3; // 0x0008
    pub const CLUSTER_ID: u16 = 1 << 2; // 0x0004
};

pub const DeliveryMode = enum(u8) {
    non_persistent = 1,
    persistent = 2,
};

pub const BasicProperties = struct {
    content_type: ?[]const u8 = null,
    content_encoding: ?[]const u8 = null,
    headers: ?[]const u8 = null, // Raw encoded table bytes (including 4-byte length prefix)
    delivery_mode: ?u8 = null, // 1 = non-persistent, 2 = persistent
    priority: ?u8 = null,
    correlation_id: ?[]const u8 = null,
    reply_to: ?[]const u8 = null,
    expiration: ?[]const u8 = null,
    message_id: ?[]const u8 = null,
    timestamp: ?i64 = null,
    type_name: ?[]const u8 = null,
    user_id: ?[]const u8 = null,
    app_id: ?[]const u8 = null,
    cluster_id: ?[]const u8 = null,

    /// Computes the 16-bit property flags word for the set properties.
    pub fn computeFlags(self: BasicProperties) u16 {
        var flags: u16 = 0;
        if (self.content_type != null) flags |= Flag.CONTENT_TYPE;
        if (self.content_encoding != null) flags |= Flag.CONTENT_ENCODING;
        if (self.headers != null) flags |= Flag.HEADERS;
        if (self.delivery_mode != null) flags |= Flag.DELIVERY_MODE;
        if (self.priority != null) flags |= Flag.PRIORITY;
        if (self.correlation_id != null) flags |= Flag.CORRELATION_ID;
        if (self.reply_to != null) flags |= Flag.REPLY_TO;
        if (self.expiration != null) flags |= Flag.EXPIRATION;
        if (self.message_id != null) flags |= Flag.MESSAGE_ID;
        if (self.timestamp != null) flags |= Flag.TIMESTAMP;
        if (self.type_name != null) flags |= Flag.TYPE;
        if (self.user_id != null) flags |= Flag.USER_ID;
        if (self.app_id != null) flags |= Flag.APP_ID;
        if (self.cluster_id != null) flags |= Flag.CLUSTER_ID;
        return flags;
    }
};

pub const ContentHeader = struct {
    class_id: u16,
    weight: u16 = 0,
    body_size: u64,
    properties: BasicProperties,
};

/// Encodes a Content Header frame payload (Class 60) into dest.
pub fn encodeContentHeader(dest: []u8, class_id: u16, body_size: u64, props: BasicProperties) wire.Error!usize {
    // Header preamble: class_id (2B) + weight (2B = 0) + body_size (8B) + flags (2B) = 14 bytes
    if (dest.len < 14) return wire.Error.BufferTooSmall;
    _ = try wire.writeU16(dest[0..2], class_id);
    _ = try wire.writeU16(dest[2..4], 0); // weight
    _ = try wire.writeU64(dest[4..12], body_size);

    const flags = props.computeFlags();
    _ = try wire.writeU16(dest[12..14], flags);

    var cursor: usize = 14;

    if (props.content_type) |ct| {
        cursor += try wire.writeShortString(dest[cursor..], ct);
    }
    if (props.content_encoding) |ce| {
        cursor += try wire.writeShortString(dest[cursor..], ce);
    }
    if (props.headers) |h| {
        if (dest.len < cursor + h.len) return wire.Error.BufferTooSmall;
        @memcpy(dest[cursor .. cursor + h.len], h);
        cursor += h.len;
    }
    if (props.delivery_mode) |dm| {
        cursor += try wire.writeU8(dest[cursor..], dm);
    }
    if (props.priority) |p| {
        cursor += try wire.writeU8(dest[cursor..], p);
    }
    if (props.correlation_id) |cid| {
        cursor += try wire.writeShortString(dest[cursor..], cid);
    }
    if (props.reply_to) |rt| {
        cursor += try wire.writeShortString(dest[cursor..], rt);
    }
    if (props.expiration) |exp| {
        cursor += try wire.writeShortString(dest[cursor..], exp);
    }
    if (props.message_id) |mid| {
        cursor += try wire.writeShortString(dest[cursor..], mid);
    }
    if (props.timestamp) |ts| {
        cursor += try wire.writeU64(dest[cursor..], @bitCast(ts));
    }
    if (props.type_name) |tn| {
        cursor += try wire.writeShortString(dest[cursor..], tn);
    }
    if (props.user_id) |uid| {
        cursor += try wire.writeShortString(dest[cursor..], uid);
    }
    if (props.app_id) |aid| {
        cursor += try wire.writeShortString(dest[cursor..], aid);
    }
    if (props.cluster_id) |cid| {
        cursor += try wire.writeShortString(dest[cursor..], cid);
    }

    return cursor;
}

/// Decodes a Content Header payload from bytes.
pub fn decodeContentHeader(bytes: []const u8) wire.Error!ContentHeader {
    if (bytes.len < 14) return wire.Error.UnexpectedEof;
    const class_id = try wire.readU16(bytes[0..2]);
    const weight = try wire.readU16(bytes[2..4]);
    const body_size = try wire.readU64(bytes[4..12]);
    const flags = try wire.readU16(bytes[12..14]);

    var cursor: usize = 14;
    var props = BasicProperties{};

    if (flags & Flag.CONTENT_TYPE != 0) {
        const res = try wire.readShortString(bytes[cursor..]);
        props.content_type = res.str;
        cursor += res.consumed;
    }
    if (flags & Flag.CONTENT_ENCODING != 0) {
        const res = try wire.readShortString(bytes[cursor..]);
        props.content_encoding = res.str;
        cursor += res.consumed;
    }
    if (flags & Flag.HEADERS != 0) {
        if (bytes.len < cursor + 4) return wire.Error.UnexpectedEof;
        const table_len = @as(usize, @intCast(try wire.readU32(bytes[cursor .. cursor + 4])));
        const total_table_bytes = 4 + table_len;
        if (bytes.len < cursor + total_table_bytes) return wire.Error.UnexpectedEof;
        props.headers = bytes[cursor .. cursor + total_table_bytes];
        cursor += total_table_bytes;
    }
    if (flags & Flag.DELIVERY_MODE != 0) {
        props.delivery_mode = try wire.readU8(bytes[cursor..]);
        cursor += 1;
    }
    if (flags & Flag.PRIORITY != 0) {
        props.priority = try wire.readU8(bytes[cursor..]);
        cursor += 1;
    }
    if (flags & Flag.CORRELATION_ID != 0) {
        const res = try wire.readShortString(bytes[cursor..]);
        props.correlation_id = res.str;
        cursor += res.consumed;
    }
    if (flags & Flag.REPLY_TO != 0) {
        const res = try wire.readShortString(bytes[cursor..]);
        props.reply_to = res.str;
        cursor += res.consumed;
    }
    if (flags & Flag.EXPIRATION != 0) {
        const res = try wire.readShortString(bytes[cursor..]);
        props.expiration = res.str;
        cursor += res.consumed;
    }
    if (flags & Flag.MESSAGE_ID != 0) {
        const res = try wire.readShortString(bytes[cursor..]);
        props.message_id = res.str;
        cursor += res.consumed;
    }
    if (flags & Flag.TIMESTAMP != 0) {
        props.timestamp = @bitCast(try wire.readU64(bytes[cursor..]));
        cursor += 8;
    }
    if (flags & Flag.TYPE != 0) {
        const res = try wire.readShortString(bytes[cursor..]);
        props.type_name = res.str;
        cursor += res.consumed;
    }
    if (flags & Flag.USER_ID != 0) {
        const res = try wire.readShortString(bytes[cursor..]);
        props.user_id = res.str;
        cursor += res.consumed;
    }
    if (flags & Flag.APP_ID != 0) {
        const res = try wire.readShortString(bytes[cursor..]);
        props.app_id = res.str;
        cursor += res.consumed;
    }
    if (flags & Flag.CLUSTER_ID != 0) {
        const res = try wire.readShortString(bytes[cursor..]);
        props.cluster_id = res.str;
        cursor += res.consumed;
    }

    return ContentHeader{
        .class_id = class_id,
        .weight = weight,
        .body_size = body_size,
        .properties = props,
    };
}

test "content header encode and decode roundtrip" {
    var buf: [512]u8 = undefined;

    // Create a mini header table
    var table_buf: [128]u8 = undefined;
    const entries = [_]wire.FieldEntry{
        .{ .name = "x-origin", .value = .{ .string = "callapp-zg" } },
    };
    const tlen = try wire.writeTable(&table_buf, &entries);

    const props = BasicProperties{
        .content_type = "application/json",
        .content_encoding = "utf-8",
        .headers = table_buf[0..tlen],
        .delivery_mode = @intFromEnum(DeliveryMode.persistent),
        .priority = 5,
        .correlation_id = "corr-12345",
        .reply_to = "zg.responses",
        .message_id = "msg-9999",
        .timestamp = 1727584000,
        .type_name = "event.lead.scrubbed",
        .app_id = "lead-scrubber",
    };

    const written = try encodeContentHeader(&buf, CLASS_BASIC, 1024, props);
    try std.testing.expect(written > 14);

    const decoded = try decodeContentHeader(buf[0..written]);
    try std.testing.expectEqual(@as(u16, CLASS_BASIC), decoded.class_id);
    try std.testing.expectEqual(@as(u16, 0), decoded.weight);
    try std.testing.expectEqual(@as(u64, 1024), decoded.body_size);

    const dec_props = decoded.properties;
    try std.testing.expectEqualStrings("application/json", dec_props.content_type.?);
    try std.testing.expectEqualStrings("utf-8", dec_props.content_encoding.?);
    try std.testing.expectEqual(@as(u8, 2), dec_props.delivery_mode.?);
    try std.testing.expectEqual(@as(u8, 5), dec_props.priority.?);
    try std.testing.expectEqualStrings("corr-12345", dec_props.correlation_id.?);
    try std.testing.expectEqualStrings("zg.responses", dec_props.reply_to.?);
    try std.testing.expectEqualStrings("msg-9999", dec_props.message_id.?);
    try std.testing.expectEqual(@as(i64, 1727584000), dec_props.timestamp.?);
    try std.testing.expectEqualStrings("event.lead.scrubbed", dec_props.type_name.?);
    try std.testing.expectEqualStrings("lead-scrubber", dec_props.app_id.?);
    try std.testing.expect(dec_props.expiration == null);
    try std.testing.expect(dec_props.user_id == null);

    // Verify raw header table roundtrip
    var iter = wire.TableIterator.init(dec_props.headers.?[4..]);
    const entry = (try iter.next()).?;
    try std.testing.expectEqualStrings("x-origin", entry.name);
    try std.testing.expectEqualStrings("callapp-zg", entry.value.string);
    try std.testing.expect((try iter.next()) == null);
}
