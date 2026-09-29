//! AMQP 0-9-1 Channel Multiplexing and Publisher Engine.
const std = @import("std");
const wire = @import("wire.zig");
const frame = @import("frame.zig");
const method = @import("method.zig");
const properties = @import("properties.zig");
const connection_mod = @import("connection.zig");
const consumer_mod = @import("consumer.zig");

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
    ConfirmsNotEnabled,
    MessageNacked,
} || connection_mod.Error;

pub const Channel = struct {
    id: u16,
    connection: *connection_mod.Connection,
    state: ChannelState = .closed,
    confirms_enabled: bool = false,
    next_publish_seq: u64 = 1,
    last_acked_seq: u64 = 0,

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

    /// Declares a queue with optional broker arguments (e.g. x-message-ttl, x-dead-letter-exchange).
    pub fn declareQueueWithArgs(
        self: *Channel,
        queue: []const u8,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        args: []const wire.FieldEntry,
    ) !method.QueueDeclareOk {
        if (self.state != .open) return Error.ChannelClosed;

        var args_buf: [2048]u8 = undefined;
        const args_len = try wire.writeTable(&args_buf, args);

        const dec_m = method.Method{
            .queue_declare = .{
                .queue = queue,
                .passive = false,
                .durable = durable,
                .exclusive = exclusive,
                .auto_delete = auto_delete,
                .no_wait = false,
                .arguments = args_buf[0..args_len],
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
        return self.bindQueueWithArgs(queue, exchange, routing_key, &.{});
    }

    pub fn bindQueueWithArgs(
        self: *Channel,
        queue: []const u8,
        exchange: []const u8,
        routing_key: []const u8,
        args: []const wire.FieldEntry,
    ) !void {
        if (self.state != .open) return Error.ChannelClosed;

        var args_buf: [2048]u8 = undefined;
        const args_len = try wire.writeTable(&args_buf, args);

        const bind_m = method.Method{
            .queue_bind = .{
                .queue = queue,
                .exchange = exchange,
                .routing_key = routing_key,
                .no_wait = false,
                .arguments = args_buf[0..args_len],
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
        return self.declareExchangeWithArgs(exchange, type_name, durable, &.{});
    }

    pub fn declareExchangeWithArgs(
        self: *Channel,
        exchange: []const u8,
        type_name: []const u8,
        durable: bool,
        args: []const wire.FieldEntry,
    ) !void {
        if (self.state != .open) return Error.ChannelClosed;

        var args_buf: [2048]u8 = undefined;
        const args_len = try wire.writeTable(&args_buf, args);

        const ex_m = method.Method{
            .exchange_declare = .{
                .exchange = exchange,
                .type_name = type_name,
                .durable = durable,
                .passive = false,
                .auto_delete = false,
                .internal = false,
                .no_wait = false,
                .arguments = args_buf[0..args_len],
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

        if (self.confirms_enabled) {
            self.next_publish_seq += 1;
        }
    }

    /// Activates publisher confirms on this channel (Confirm.Select).
    pub fn enableConfirms(self: *Channel) !void {
        if (self.state != .open) return Error.ChannelClosed;
        if (self.confirms_enabled) return;

        const select_m = method.Method{
            .confirm_select = .{ .nowait = false },
        };
        try self.connection.sendMethod(self.id, select_m);

        var payload_buf: [512]u8 = undefined;
        const resp = try self.connection.readMethod(&payload_buf);
        if (resp.channel != self.id) return Error.ProtocolViolation;

        switch (resp.method) {
            .confirm_select_ok => {
                self.confirms_enabled = true;
                self.next_publish_seq = 1;
                self.last_acked_seq = 0;
            },
            .channel_close => {
                self.state = .closed;
                return Error.BrokerError;
            },
            else => return Error.UnexpectedMethod,
        }
    }

    /// Publishes a message and synchronously awaits broker acknowledgment (Basic.Ack).
    pub fn publishConfirm(
        self: *Channel,
        exchange: []const u8,
        routing_key: []const u8,
        body: []const u8,
        props: properties.BasicProperties,
        mandatory: bool,
    ) !void {
        if (!self.confirms_enabled) return Error.ConfirmsNotEnabled;
        const tag = self.next_publish_seq;

        try self.publish(exchange, routing_key, body, props, mandatory);

        var payload_buf: [1024]u8 = undefined;
        while (true) {
            const tr = if (self.connection.transport) |*t| t else return Error.ConnectionClosed;
            const f = try tr.readFrame(&payload_buf);
            if (f.frame_type == .heartbeat) {
                continue;
            }
            if (f.frame_type == .method and f.channel == self.id) {
                const m = try method.decodeMethod(f.payload);
                switch (m) {
                    .basic_ack => |a| {
                        if (a.multiple) {
                            if (tag <= a.delivery_tag) {
                                self.last_acked_seq = a.delivery_tag;
                                return;
                            }
                        } else {
                            if (a.delivery_tag == tag) {
                                self.last_acked_seq = tag;
                                return;
                            }
                        }
                    },
                    .basic_nack => |n| {
                        if (n.multiple) {
                            if (tag <= n.delivery_tag) return Error.MessageNacked;
                        } else {
                            if (n.delivery_tag == tag) return Error.MessageNacked;
                        }
                    },
                    .channel_close => {
                        self.state = .closed;
                        return Error.BrokerError;
                    },
                    else => {},
                }
            }
        }
    }

    /// Subscribes to a queue to receive delivered messages (Basic.Consume).
    pub fn consume(
        self: *Channel,
        queue: []const u8,
        consumer_tag: []const u8,
        no_ack: bool,
    ) !void {
        if (self.state != .open) return Error.ChannelClosed;

        const consume_m = method.Method{
            .basic_consume = .{
                .queue = queue,
                .consumer_tag = consumer_tag,
                .no_local = false,
                .no_ack = no_ack,
                .exclusive = false,
                .no_wait = false,
            },
        };
        try self.connection.sendMethod(self.id, consume_m);

        var payload_buf: [512]u8 = undefined;
        const resp = try self.connection.readMethod(&payload_buf);
        if (resp.channel != self.id) return Error.ProtocolViolation;

        switch (resp.method) {
            .basic_consume_ok => {},
            .channel_close => {
                self.state = .closed;
                return Error.BrokerError;
            },
            else => return Error.UnexpectedMethod,
        }
    }

    /// Performs a polling poll (`Basic.Get`) on a queue.
    /// Returns the message if one was available, or `null` if the queue was empty.
    pub fn get(self: *Channel, allocator: std.mem.Allocator, queue: []const u8, no_ack: bool) !?consumer_mod.Message {
        if (self.state != .open) return Error.ChannelClosed;

        const get_m = method.Method{
            .basic_get = .{
                .reserved_1 = 0,
                .queue = queue,
                .no_ack = no_ack,
            },
        };
        try self.connection.sendMethod(self.id, get_m);

        var payload_buf: [4096]u8 = undefined;
        while (true) {
            const f = try self.connection.transport.?.readFrame(&payload_buf);
            if (f.frame_type == .heartbeat) continue;
            if (f.frame_type != .method) continue;
            if (f.channel != self.id) continue;

            const m = try method.decodeMethod(f.payload);
            switch (m) {
                .basic_get_empty => return null,
                .basic_get_ok => |ok| {
                    var msg = consumer_mod.Message{
                        .channel = self,
                        .delivery_tag = ok.delivery_tag,
                        .redelivered = ok.redelivered,
                        .body = &.{},
                    };

                    if (ok.exchange.len > msg.exchange_buf.len) return Error.ProtocolViolation;
                    @memcpy(msg.exchange_buf[0..ok.exchange.len], ok.exchange);
                    msg.exchange_len = @intCast(ok.exchange.len);

                    if (ok.routing_key.len > msg.routing_key_buf.len) return Error.ProtocolViolation;
                    @memcpy(msg.routing_key_buf[0..ok.routing_key.len], ok.routing_key);
                    msg.routing_key_len = @intCast(ok.routing_key.len);

                    try consumer_mod.reassembleContent(self, allocator, &msg);
                    return msg;
                },
                .channel_close => {
                    self.state = .closed;
                    return Error.BrokerError;
                },
                else => return Error.UnexpectedMethod,
            }
        }
    }

    /// Reads and reassembles the next incoming message delivered to this channel.
    pub fn readMessage(self: *Channel, allocator: std.mem.Allocator) !consumer_mod.Message {
        return consumer_mod.readMessage(self, allocator);
    }

    /// Acknowledges one or more messages (Basic.Ack).
    pub fn ack(self: *Channel, delivery_tag: u64, multiple: bool) !void {
        if (self.state != .open) return Error.ChannelClosed;
        const m = method.Method{
            .basic_ack = .{
                .delivery_tag = delivery_tag,
                .multiple = multiple,
            },
        };
        try self.connection.sendMethod(self.id, m);
    }

    /// Negatively acknowledges one or more messages with requeue option (Basic.Nack).
    pub fn nack(self: *Channel, delivery_tag: u64, multiple: bool, requeue: bool) !void {
        if (self.state != .open) return Error.ChannelClosed;
        const m = method.Method{
            .basic_nack = .{
                .delivery_tag = delivery_tag,
                .multiple = multiple,
                .requeue = requeue,
            },
        };
        try self.connection.sendMethod(self.id, m);
    }

    /// Rejects a message with requeue option (Basic.Reject).
    pub fn reject(self: *Channel, delivery_tag: u64, requeue: bool) !void {
        if (self.state != .open) return Error.ChannelClosed;
        const m = method.Method{
            .basic_reject = .{
                .delivery_tag = delivery_tag,
                .requeue = requeue,
            },
        };
        try self.connection.sendMethod(self.id, m);
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

test "publisher confirms against live rabbitmq" {
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

    var ch = try conn.openChannel(2);
    try std.testing.expectEqual(ChannelState.open, ch.state);

    // 1. Enable Confirms
    try ch.enableConfirms();
    try std.testing.expect(ch.confirms_enabled);

    // 2. Declare queue
    _ = try ch.declareQueue("zg.test.confirms_queue", false, false, true);

    // 3. Publish and wait for ACK
    const payload1 = "{\"event\": \"lead_confirmed_1\"}";
    const props = properties.BasicProperties{
        .content_type = "application/json",
        .delivery_mode = @intFromEnum(properties.DeliveryMode.persistent),
    };
    try ch.publishConfirm("", "zg.test.confirms_queue", payload1, props, false);
    try std.testing.expect(ch.last_acked_seq >= 1);

    // 4. Publish second message and wait for ACK
    const payload2 = "{\"event\": \"lead_confirmed_2\"}";
    try ch.publishConfirm("", "zg.test.confirms_queue", payload2, props, false);
    try std.testing.expect(ch.last_acked_seq >= 2);

    try ch.close();
    try std.testing.expectEqual(ChannelState.closed, ch.state);
}

test "queue declare with broker arguments against live rabbitmq" {
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

    // 1. Declare queue with broker arguments (x-message-ttl = 60000ms)
    const args = [_]wire.FieldEntry{
        .{ .name = "x-message-ttl", .value = .{ .long_int = 60000 } },
    };
    const q_name = "zg.test.get_args_queue";
    const ok = try ch.declareQueueWithArgs(q_name, false, false, true, &args);
    try std.testing.expectEqualStrings(q_name, ok.queue);
}

test "basic get polling consumption against live rabbitmq" {
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

    var ch = try conn.openChannel(4);
    defer ch.close() catch {};

    const q_name = "zg.test.polling_get_queue";
    _ = try ch.declareQueue(q_name, false, false, true);

    // 1. Initial queue poll returns null (Basic.GetEmpty)
    const empty_msg = try ch.get(std.testing.allocator, q_name, true);
    try std.testing.expect(empty_msg == null);

    // 2. Publish message
    const test_payload = "polling message payload";
    try ch.publish("", q_name, test_payload, .{}, false);

    // 3. Poll receives message (Basic.GetOk)
    var opt_msg = try ch.get(std.testing.allocator, q_name, true);
    try std.testing.expect(opt_msg != null);
    if (opt_msg) |*msg| {
        defer msg.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings(test_payload, msg.body);
    }
}
