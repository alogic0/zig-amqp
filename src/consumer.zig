//! AMQP 0-9-1 Message Delivery and Consumer Engine.
const std = @import("std");
const wire = @import("wire.zig");
const frame = @import("frame.zig");
const method = @import("method.zig");
const properties = @import("properties.zig");
const channel_mod = @import("channel.zig");

pub const MAX_HEADER_BUF: usize = 2048;

pub const Error = error{
    UnexpectedFrameType,
    UnexpectedMethod,
    IncompleteMessageBody,
    BufferTooSmall,
    ChannelClosed,
    ConnectionClosed,
} || channel_mod.Error;

pub const Message = struct {
    channel: *channel_mod.Channel,

    consumer_tag_buf: [256]u8 = undefined,
    consumer_tag_len: u8 = 0,

    delivery_tag: u64,
    redelivered: bool,

    exchange_buf: [256]u8 = undefined,
    exchange_len: u8 = 0,

    routing_key_buf: [256]u8 = undefined,
    routing_key_len: u8 = 0,

    header_buf: [MAX_HEADER_BUF]u8 = undefined,
    header_buf_len: usize = 0,
    properties: properties.BasicProperties = .{},

    body: []const u8,
    owned: bool = false,

    pub fn consumerTag(self: *const Message) []const u8 {
        return self.consumer_tag_buf[0..self.consumer_tag_len];
    }

    pub fn exchange(self: *const Message) []const u8 {
        return self.exchange_buf[0..self.exchange_len];
    }

    pub fn routingKey(self: *const Message) []const u8 {
        return self.routing_key_buf[0..self.routing_key_len];
    }

    pub fn ack(self: Message, multiple: bool) !void {
        return self.channel.ack(self.delivery_tag, multiple);
    }

    pub fn nack(self: Message, multiple: bool, requeue: bool) !void {
        return self.channel.nack(self.delivery_tag, multiple, requeue);
    }

    pub fn reject(self: Message, requeue: bool) !void {
        return self.channel.reject(self.delivery_tag, requeue);
    }

    pub fn deinit(self: *Message, allocator: std.mem.Allocator) void {
        if (self.owned and self.body.len > 0) {
            allocator.free(self.body);
            self.owned = false;
        }
    }
};

/// Reads the next incoming message on this channel, reassembling:
/// Basic.Deliver method frame -> Content Header frame -> Body frame(s).
pub fn readMessage(channel: *channel_mod.Channel, allocator: std.mem.Allocator) !Message {
    const tr = if (channel.connection.transport) |*t| t else return Error.ConnectionClosed;

    var payload_buf: [4096]u8 = undefined;
    var msg: Message = .{
        .channel = channel,
        .delivery_tag = 0,
        .redelivered = false,
        .body = &.{},
    };

    // 1. Await Basic.Deliver method frame on this channel
    while (true) {
        const f = try tr.readFrame(&payload_buf);
        if (f.frame_type == .heartbeat) continue;
        if (f.frame_type != .method) continue;
        if (f.channel != channel.id) continue;

        const m = try method.decodeMethod(f.payload);
        switch (m) {
            .basic_deliver => |d| {
                if (d.consumer_tag.len > msg.consumer_tag_buf.len) return Error.BufferTooSmall;
                @memcpy(msg.consumer_tag_buf[0..d.consumer_tag.len], d.consumer_tag);
                msg.consumer_tag_len = @intCast(d.consumer_tag.len);

                if (d.exchange.len > msg.exchange_buf.len) return Error.BufferTooSmall;
                @memcpy(msg.exchange_buf[0..d.exchange.len], d.exchange);
                msg.exchange_len = @intCast(d.exchange.len);

                if (d.routing_key.len > msg.routing_key_buf.len) return Error.BufferTooSmall;
                @memcpy(msg.routing_key_buf[0..d.routing_key.len], d.routing_key);
                msg.routing_key_len = @intCast(d.routing_key.len);

                msg.delivery_tag = d.delivery_tag;
                msg.redelivered = d.redelivered;
                break;
            },
            .channel_close => {
                channel.state = .closed;
                return Error.ChannelClosed;
            },
            else => {},
        }
    }

    // 2. Await Content Header frame
    while (true) {
        const header_frame = try tr.readFrame(&payload_buf);
        if (header_frame.frame_type == .heartbeat) continue;
        if (header_frame.frame_type == .header and header_frame.channel == channel.id) {
            if (header_frame.payload.len > msg.header_buf.len) return Error.BufferTooSmall;
            @memcpy(msg.header_buf[0..header_frame.payload.len], header_frame.payload);
            msg.header_buf_len = header_frame.payload.len;
            break;
        }
        return Error.UnexpectedFrameType;
    }

    const content_hdr = try properties.decodeContentHeader(msg.header_buf[0..msg.header_buf_len]);
    msg.properties = content_hdr.properties;
    const body_len: usize = @intCast(content_hdr.body_size);

    // 3. Await Content Body frame(s) until total received == body_size
    if (body_len > 0) {
        const body = try allocator.alloc(u8, body_len);
        errdefer allocator.free(body);

        var received: usize = 0;
        while (received < body_len) {
            const body_frame = try tr.readFrame(body[received..]);
            if (body_frame.frame_type == .heartbeat) continue;
            if (body_frame.frame_type != .body or body_frame.channel != channel.id) {
                return Error.UnexpectedFrameType;
            }
            received += body_frame.payload.len;
        }
        msg.body = body;
        msg.owned = true;
    }

    return msg;
}

test "consumer receive, parse and ack against live rabbitmq" {
    const connection_mod = @import("connection.zig");

    const config = connection_mod.ConnectionConfig{
        .host = "127.0.0.1",
        .port = 5674,
        .username = "guest",
        .password = "guest",
        .virtual_host = "/",
    };
    var conn = connection_mod.Connection.init(std.testing.allocator, std.testing.io, config);
    defer conn.deinit();

    conn.connect() catch |err| {
        if (err == error.ConnectionRefused or err == error.ConnectionFailed) return;
        return err;
    };
    defer conn.close() catch {};

    var ch = try conn.openChannel(3);
    defer ch.close() catch {};

    // 1. Declare Queue
    _ = try ch.declareQueue("zg.test.consume_pipeline", false, false, true);

    // 2. Start consumer
    try ch.consume("zg.test.consume_pipeline", "consumer_tag_test", false);

    // 3. Publish message
    const send_body = "{\"lead_id\": 4200, \"status\": \"scrubbed_valid\"}";
    const send_props = properties.BasicProperties{
        .content_type = "application/json",
        .correlation_id = "corr-cons-test",
        .delivery_mode = @intFromEnum(properties.DeliveryMode.persistent),
    };
    try ch.publish("", "zg.test.consume_pipeline", send_body, send_props, false);

    // 4. Read message
    var msg = try ch.readMessage(std.testing.allocator);
    defer msg.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("consumer_tag_test", msg.consumerTag());
    try std.testing.expect(msg.delivery_tag >= 1);
    try std.testing.expectEqualStrings(send_body, msg.body);
    try std.testing.expectEqualStrings("application/json", msg.properties.content_type.?);
    try std.testing.expectEqualStrings("corr-cons-test", msg.properties.correlation_id.?);

    // 5. Acknowledge message
    try msg.ack(false);
}
