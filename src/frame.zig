//! AMQP 0-9-1 Frame serialization and deserialization.
const std = @import("std");
const wire = @import("wire.zig");

pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;
pub const FRAME_END: u8 = 0xCE;

pub const HEADER_SIZE: usize = 7; // type (1B) + channel (2B) + size (4B)
pub const FOOTER_SIZE: usize = 1; // 0xCE (1B)
pub const OVERHEAD_SIZE: usize = HEADER_SIZE + FOOTER_SIZE; // 8 bytes

pub const Error = error{
    BufferTooSmall,
    IncompleteFrame,
    InvalidFrameType,
    InvalidFrameEnd,
    PayloadTooLarge,
    UnexpectedEof,
};

pub const FrameType = enum(u8) {
    method = FRAME_METHOD,
    header = FRAME_HEADER,
    body = FRAME_BODY,
    heartbeat = FRAME_HEARTBEAT,

    pub fn fromU8(val: u8) Error!FrameType {
        return switch (val) {
            FRAME_METHOD => .method,
            FRAME_HEADER => .header,
            FRAME_BODY => .body,
            FRAME_HEARTBEAT => .heartbeat,
            else => Error.InvalidFrameType,
        };
    }
};

pub const FrameHeader = struct {
    frame_type: FrameType,
    channel: u16,
    length: u32,
};

pub const Frame = struct {
    frame_type: FrameType,
    channel: u16,
    payload: []const u8,
};

/// Parses only the 7-byte frame header from a slice.
pub fn parseFrameHeader(bytes: []const u8) Error!FrameHeader {
    if (bytes.len < HEADER_SIZE) return Error.IncompleteFrame;
    const ftype = try FrameType.fromU8(bytes[0]);
    const channel = std.mem.readInt(u16, bytes[1..3], .big);
    const length = std.mem.readInt(u32, bytes[3..7], .big);
    return FrameHeader{
        .frame_type = ftype,
        .channel = channel,
        .length = length,
    };
}

/// Parses a complete frame from a slice buffer.
/// Returns the parsed Frame (referencing payload within bytes) and total bytes consumed.
pub fn parseFrame(bytes: []const u8) Error!struct { frame: Frame, consumed: usize } {
    const hdr = try parseFrameHeader(bytes);
    const total_len = HEADER_SIZE + @as(usize, hdr.length) + FOOTER_SIZE;
    if (bytes.len < total_len) return Error.IncompleteFrame;

    if (bytes[total_len - 1] != FRAME_END) {
        return Error.InvalidFrameEnd;
    }

    const payload = bytes[HEADER_SIZE .. HEADER_SIZE + hdr.length];
    return .{
        .frame = .{
            .frame_type = hdr.frame_type,
            .channel = hdr.channel,
            .payload = payload,
        },
        .consumed = total_len,
    };
}

/// Writes an AMQP frame into a destination buffer.
pub fn writeFrame(dest: []u8, frame_type: FrameType, channel: u16, payload: []const u8) Error!usize {
    const total_size = HEADER_SIZE + payload.len + FOOTER_SIZE;
    if (dest.len < total_size) return Error.BufferTooSmall;
    if (payload.len > std.math.maxInt(u32)) return Error.PayloadTooLarge;

    dest[0] = @backingInt(frame_type);
    std.mem.writeInt(u16, dest[1..3], channel, .big);
    std.mem.writeInt(u32, dest[3..7], @intCast(payload.len), .big);
    if (payload.len > 0) {
        @memcpy(dest[HEADER_SIZE .. HEADER_SIZE + payload.len], payload);
    }
    dest[total_size - 1] = FRAME_END;
    return total_size;
}

/// Writes a heartbeat frame (type 8, channel 0, length 0, 0xCE) into destination buffer.
pub fn writeHeartbeat(dest: []u8) Error!usize {
    return writeFrame(dest, .heartbeat, 0, &.{});
}

/// Reads only the 7-byte frame header from reader.
pub fn readFrameHeaderFromReader(reader: anytype) !FrameHeader {
    var hdr_buf: [HEADER_SIZE]u8 = undefined;
    const T = @TypeOf(reader);
    const P = if (@typeInfo(T) == .pointer) @typeInfo(T).pointer.child else T;

    if (@hasDecl(P, "readSliceAll")) {
        try reader.readSliceAll(&hdr_buf);
    } else if (@hasDecl(P, "readNoEof")) {
        try reader.readNoEof(&hdr_buf);
    } else {
        @compileError("Unsupported reader type");
    }

    return parseFrameHeader(&hdr_buf);
}

/// Reads the payload and validates the trailing FRAME_END byte from reader.
pub fn readFramePayloadAndEnd(reader: anytype, payload: []u8) !void {
    const T = @TypeOf(reader);
    const P = if (@typeInfo(T) == .pointer) @typeInfo(T).pointer.child else T;

    if (payload.len > 0) {
        if (@hasDecl(P, "readSliceAll")) {
            try reader.readSliceAll(payload);
        } else {
            try reader.readNoEof(payload);
        }
    }

    const end_byte = if (@hasDecl(P, "takeByte"))
        try reader.takeByte()
    else
        try reader.readByte();

    if (end_byte != FRAME_END) return Error.InvalidFrameEnd;
}

/// Reads a frame from any stream/reader into a caller-supplied payload buffer.
pub fn readFrameFromReader(reader: anytype, dest_payload: []u8) !Frame {
    const hdr = try readFrameHeaderFromReader(reader);
    if (dest_payload.len < hdr.length) return Error.BufferTooSmall;
    const payload = dest_payload[0..hdr.length];
    try readFramePayloadAndEnd(reader, payload);
    return Frame{
        .frame_type = hdr.frame_type,
        .channel = hdr.channel,
        .payload = payload,
    };
}

test "frame serialization and parsing" {
    var buf: [128]u8 = undefined;
    const payload_data = "Hello RabbitMQ!";

    const written = try writeFrame(&buf, .method, 1, payload_data);
    try std.testing.expectEqual(HEADER_SIZE + payload_data.len + FOOTER_SIZE, written);
    try std.testing.expectEqual(@as(u8, FRAME_METHOD), buf[0]);
    try std.testing.expectEqual(@as(u16, 1), std.mem.readInt(u16, buf[1..3], .big));
    try std.testing.expectEqual(@as(u32, @intCast(payload_data.len)), std.mem.readInt(u32, buf[3..7], .big));
    try std.testing.expectEqualStrings(payload_data, buf[HEADER_SIZE .. HEADER_SIZE + payload_data.len]);
    try std.testing.expectEqual(@as(u8, FRAME_END), buf[written - 1]);

    const parsed = try parseFrame(buf[0..written]);
    try std.testing.expectEqual(written, parsed.consumed);
    try std.testing.expectEqual(FrameType.method, parsed.frame.frame_type);
    try std.testing.expectEqual(@as(u16, 1), parsed.frame.channel);
    try std.testing.expectEqualStrings(payload_data, parsed.frame.payload);
}

test "heartbeat frame roundtrip" {
    var buf: [16]u8 = undefined;
    const written = try writeHeartbeat(&buf);
    try std.testing.expectEqual(OVERHEAD_SIZE, written);

    const parsed = try parseFrame(buf[0..written]);
    try std.testing.expectEqual(FrameType.heartbeat, parsed.frame.frame_type);
    try std.testing.expectEqual(@as(u16, 0), parsed.frame.channel);
    try std.testing.expectEqual(@as(usize, 0), parsed.frame.payload.len);
}

test "frame error handling" {
    var buf: [128]u8 = undefined;
    const written = try writeFrame(&buf, .body, 5, "chunk");

    // Incomplete buffer
    try std.testing.expectError(Error.IncompleteFrame, parseFrame(buf[0..4]));
    try std.testing.expectError(Error.IncompleteFrame, parseFrame(buf[0 .. written - 1]));

    // Corrupted end byte
    buf[written - 1] = 0xAA;
    try std.testing.expectError(Error.InvalidFrameEnd, parseFrame(buf[0..written]));

    // Corrupted type
    buf[0] = 99;
    try std.testing.expectError(Error.InvalidFrameType, parseFrame(buf[0..written]));
}

test "readFrameFromReader stream" {
    var buf: [64]u8 = undefined;
    const written = try writeFrame(&buf, .header, 2, "sample_header");

    var reader = std.Io.Reader.fixed(buf[0..written]);
    var payload_buf: [32]u8 = undefined;
    const frame = try readFrameFromReader(&reader, &payload_buf);

    try std.testing.expectEqual(FrameType.header, frame.frame_type);
    try std.testing.expectEqual(@as(u16, 2), frame.channel);
    try std.testing.expectEqualStrings("sample_header", frame.payload);
}
