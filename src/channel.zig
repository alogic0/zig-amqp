//! AMQP 0-9-1 Channel Multiplexing and Publisher Engine.
const std = @import("std");
const wire = @import("wire.zig");
const frame = @import("frame.zig");
const method = @import("method.zig");
const properties = @import("properties.zig");
const connection_mod = @import("connection.zig");

pub const ChannelState = enum {
    closed,
    opening,
    open,
    closing,
};

pub const Error = error{
    ChannelClosed,
    ConnectionClosed,
    UnexpectedMethod,
    ProtocolViolation,
    BrokerError,
} || connection_mod.Error;

pub const Channel = struct {
    id: u16,
    connection: *connection_mod.Connection,
    state: ChannelState = .closed,

    pub fn open(self: *Channel) !void {
        if (self.state == .open) return;
        if (self.connection.state != .open) return Error.ConnectionClosed;
        self.state = .opening;

        // Send Channel.Open (class 20, method 10)
        const open_m = method.Method{
            .channel_open = .{
                .reserved_1 = "",
            },
        };
        try self.connection.sendMethod(self.id, open_m);

        // Receive Channel.OpenOk (class 20, method 11)
        var payload_buf: [512]u8 = undefined;
        const resp = try self.connection.readMethod(&payload_buf);
        if (resp.channel != self.id) return Error.ProtocolViolation;

        switch (resp.method) {
            .channel_open_ok => {
                self.state = .open;
            },
            .channel_close => |cc| {
                self.state = .closed;
                _ = cc;
                return Error.BrokerError;
            },
            else => return Error.UnexpectedMethod,
        }
    }

    pub fn declareQueue(
        self: *Channel,
        queue: []const u8,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
    ) !method.QueueDeclareOk {
        if (self.state != .open) return Error.ChannelClosed;

        const dec_m = method.Method{
            .queue_declare = .{
                .queue = queue,
                .passive = false,
                .durable = durable,
                .exclusive = exclusive,
                .auto_delete = auto_delete,
                .no_wait = false,
            },
        };
        try self.connection.sendMethod(self.id, dec_m);

        var payload_buf: [1024]u8 = undefined;
        const resp = try self.connection.readMethod(&payload_buf);
        if (resp.channel != self.id) return Error.ProtocolViolation;

        switch (resp.method) {
            .queue_declare_ok => |ok| return ok,
            .channel_close => {
                self.state = .closed;
                return Error.BrokerError;
            },
            else => return Error.UnexpectedMethod,
        }
    }

    pub fn bindQueue(
        self: *Channel,
        queue: []const u8,
        exchange: []const u8,
        routing_key: []const u8,
    ) !void {
        if (self.state != .open) return Error.ChannelClosed;

        const bind_m = method.Method{
            .queue_bind = .{
                .queue = queue,
                .exchange = exchange,
                .routing_key = routing_key,
                .no_wait = false,
            },
        };
        try self.connection.sendMethod(self.id, bind_m);

        var payload_buf: [512]u8 = undefined;
        const resp = try self.connection.readMethod(&payload_buf);
        if (resp.channel != self.id) return Error.ProtocolViolation;

        switch (resp.method) {
            .queue_bind_ok => {},
            .channel_close => {
                self.state = .closed;
                return Error.BrokerError;
            },
            else => return Error.UnexpectedMethod,
        }
    }

    pub fn declareExchange(
        self: *Channel,
        exchange: []const u8,
        type_name: []const u8,
        durable: bool,
    ) !void {
        if (self.state != .open) return Error.ChannelClosed;

        const ex_m = method.Method{
            .exchange_declare = .{
                .exchange = exchange,
                .type_name = type_name,
                .durable = durable,
                .auto_delete = false,
                .no_wait = false,
            },
        };
        try self.connection.sendMethod(self.id, ex_m);

        var payload_buf: [512]u8 = undefined;
        const resp = try self.connection.readMethod(&payload_buf);
        if (resp.channel != self.id) return Error.ProtocolViolation;

        switch (resp.method) {
            .exchange_declare_ok => {},
            .channel_close => {
                self.state = .closed;
                return Error.BrokerError;
            },
            else => return Error.UnexpectedMethod,
        }
    }

    pub fn setQos(self: *Channel, prefetch_count: u16, global: bool) !void {
        if (self.state != .open) return Error.ChannelClosed;

        const qos_m = method.Method{
            .basic_qos = .{
                .prefetch_size = 0,
                .prefetch_count = prefetch_count,
                .global = global,
            },
        };
        try self.connection.sendMethod(self.id, qos_m);

        var payload_buf: [512]u8 = undefined;
        const resp = try self.connection.readMethod(&payload_buf);
        if (resp.channel != self.id) return Error.ProtocolViolation;

        switch (resp.method) {
            .basic_qos_ok => {},
            .channel_close => {
                self.state = .closed;
                return Error.BrokerError;
            },
            else => return Error.UnexpectedMethod,
        }
    }

    /// Publishes a message through this channel following the AMQP 3-frame sequence:
    /// Basic.Publish method frame -> Content Header frame -> Body frame(s)
    pub fn publish(
        self: *Channel,
        exchange: []const u8,
        routing_key: []const u8,
        body: []const u8,
        props: properties.BasicProperties,
        mandatory: bool,
    ) !void {
        if (self.state != .open) return Error.ChannelClosed;
        const tr = if (self.connection.transport) |*t| t else return Error.ConnectionClosed;

        // 1. Send Basic.Publish method frame
        const pub_method = method.Method{
            .basic_publish = .{
                .exchange = exchange,
                .routing_key = routing_key,
                .mandatory = mandatory,
                .immediate = false,
            },
        };
        var method_buf: [1024]u8 = undefined;
        const method_len = try method.encodeMethod(&method_buf, pub_method);
        try tr.sendFrame(.method, self.id, method_buf[0..method_len]);

        // 2. Send Content Header frame
        var header_buf: [2048]u8 = undefined;
        const header_len = try properties.encodeContentHeader(&header_buf, properties.CLASS_BASIC, body.len, props);
        try tr.sendFrame(.header, self.id, header_buf[0..header_len]);

        // 3. Send Content Body frame(s)
        if (body.len > 0) {
            const frame_max = self.connection.negotiated_frame_max;
            const max_chunk = if (frame_max > frame.OVERHEAD_SIZE)
                frame_max - frame.OVERHEAD_SIZE
            else
                4088;

            var offset: usize = 0;
            while (offset < body.len) {
                const chunk_len = @min(body.len - offset, max_chunk);
                const chunk = body[offset .. offset + chunk_len];
                try tr.sendFrame(.body, self.id, chunk);
                offset += chunk_len;
            }
        }
    }

    pub fn close(self: *Channel) !void {
        if (self.state != .open) return;
        self.state = .closing;

        const close_m = method.Method{
            .channel_close = .{
                .reply_code = 200,
                .reply_text = "Normal channel shutdown",
                .class_id = 0,
                .method_id = 0,
            },
        };
        try self.connection.sendMethod(self.id, close_m);

        var payload_buf: [512]u8 = undefined;
        const resp = try self.connection.readMethod(&payload_buf);
        if (resp.channel == self.id) {
            switch (resp.method) {
                .channel_close_ok => {},
                else => {},
            }
        }
        self.state = .closed;
    }
};

test "channel operations and publishing against live rabbitmq" {
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

    var ch = try conn.openChannel(1);
    try std.testing.expectEqual(ChannelState.open, ch.state);

    // 1. Declare Exchange
    try ch.declareExchange("zg.test.exchange", "direct", false);

    // 2. Declare Queue
    const q_ok = try ch.declareQueue("zg.test.publish_queue", false, false, true);
    try std.testing.expectEqualStrings("zg.test.publish_queue", q_ok.queue);

    // 3. Bind Queue
    try ch.bindQueue("zg.test.publish_queue", "zg.test.exchange", "zg.test.routing_key");

    // 4. Set QoS
    try ch.setQos(20, false);

    // 5. Publish small message
    const small_payload = "{\"event\": \"lead_received\", \"id\": 1001}";
    const props = properties.BasicProperties{
        .content_type = "application/json",
        .delivery_mode = @intFromEnum(properties.DeliveryMode.persistent),
        .correlation_id = "corr-test-1",
    };
    try ch.publish("zg.test.exchange", "zg.test.routing_key", small_payload, props, false);

    // 6. Publish large message exceeding frame chunk size to verify multi-chunk body assembly
    var large_payload: [150000]u8 = undefined;
    @memset(&large_payload, 'X');
    try ch.publish("zg.test.exchange", "zg.test.routing_key", &large_payload, props, false);

    // 7. Close Channel
    try ch.close();
    try std.testing.expectEqual(ChannelState.closed, ch.state);
}
