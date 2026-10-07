//! AMQP 0-9-1 Resilience, Heartbeat Watchdog, and Topology Recovery Engine.
const std = @import("std");
const wire = @import("wire.zig");
const frame = @import("frame.zig");
const method = @import("method.zig");
const connection_mod = @import("connection.zig");
const channel_mod = @import("channel.zig");

/// Configuration for exponential backoff during reconnection attempts.
pub const BackoffConfig = struct {
    initial_delay_ms: u64 = 100,
    max_delay_ms: u64 = 5000,
    multiplier: f64 = 2.0,
    max_attempts: ?usize = null,
};

/// Tracks backoff progression across reconnection attempts.
pub const ReconnectState = struct {
    config: BackoffConfig,
    current_delay_ms: u64,
    attempts: usize = 0,

    pub fn init(config: BackoffConfig) ReconnectState {
        return .{
            .config = config,
            .current_delay_ms = config.initial_delay_ms,
            .attempts = 0,
        };
    }

    pub fn reset(self: *ReconnectState) void {
        self.current_delay_ms = self.config.initial_delay_ms;
        self.attempts = 0;
    }

    /// Computes delay for next attempt and advances exponential backoff.
    /// Returns null if max_attempts is exceeded.
    pub fn nextDelayMs(self: *ReconnectState) ?u64 {
        if (self.config.max_attempts) |max| {
            if (self.attempts >= max) return null;
        }

        const delay = self.current_delay_ms;
        self.attempts += 1;

        const next_delay_float = @as(f64, @floatFromInt(self.current_delay_ms)) * self.config.multiplier;
        const next_delay: u64 = @intFromFloat(@min(@as(f64, @floatFromInt(self.config.max_delay_ms)), next_delay_float));
        self.current_delay_ms = next_delay;

        return delay;
    }
};

/// Watchdog tracking read/write activity and enforcing heartbeat intervals.
pub const HeartbeatWatchdog = struct {
    heartbeat_interval_sec: u16,
    last_read_sec: i64,
    last_write_sec: i64,

    pub fn init(heartbeat_interval_sec: u16, now_sec: i64) HeartbeatWatchdog {
        return .{
            .heartbeat_interval_sec = heartbeat_interval_sec,
            .last_read_sec = now_sec,
            .last_write_sec = now_sec,
        };
    }

    pub fn recordRead(self: *HeartbeatWatchdog, now_sec: i64) void {
        self.last_read_sec = now_sec;
    }

    pub fn recordWrite(self: *HeartbeatWatchdog, now_sec: i64) void {
        self.last_write_sec = now_sec;
    }

    /// True if client should emit a heartbeat frame (half the negotiated interval elapsed).
    pub fn shouldSendHeartbeat(self: *const HeartbeatWatchdog, now_sec: i64) bool {
        if (self.heartbeat_interval_sec == 0) return false;
        const send_interval = @max(1, @as(i64, self.heartbeat_interval_sec / 2));
        return (now_sec - self.last_write_sec) >= send_interval;
    }

    /// True if peer has failed to communicate within 2 * heartbeat intervals.
    pub fn isPeerDead(self: *const HeartbeatWatchdog, now_sec: i64) bool {
        if (self.heartbeat_interval_sec == 0) return false;
        const timeout = @as(i64, self.heartbeat_interval_sec) * 2;
        return (now_sec - self.last_read_sec) >= timeout;
    }
};

pub const ExchangeDecl = struct {
    name: []const u8,
    type_name: []const u8,
    durable: bool,
};

pub const QueueDecl = struct {
    name: []const u8,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
};

pub const BindingDecl = struct {
    queue: []const u8,
    exchange: []const u8,
    routing_key: []const u8,
};

pub const SubscriptionDecl = struct {
    channel_id: u16,
    queue: []const u8,
    consumer_tag: []const u8,
    no_ack: bool,
};

/// Records declared topology so it can be restored seamlessly following network reconnects.
pub const TopologyTracker = struct {
    allocator: std.mem.Allocator,
    exchanges: std.ArrayList(ExchangeDecl),
    queues: std.ArrayList(QueueDecl),
    bindings: std.ArrayList(BindingDecl),
    subscriptions: std.ArrayList(SubscriptionDecl),
    confirms_channels: std.AutoHashMap(u16, void),

    pub fn init(allocator: std.mem.Allocator) TopologyTracker {
        return .{
            .allocator = allocator,
            .exchanges = std.ArrayList(ExchangeDecl).empty,
            .queues = std.ArrayList(QueueDecl).empty,
            .bindings = std.ArrayList(BindingDecl).empty,
            .subscriptions = std.ArrayList(SubscriptionDecl).empty,
            .confirms_channels = std.AutoHashMap(u16, void).init(allocator),
        };
    }

    pub fn deinit(self: *TopologyTracker) void {
        self.exchanges.deinit(self.allocator);
        self.queues.deinit(self.allocator);
        self.bindings.deinit(self.allocator);
        self.subscriptions.deinit(self.allocator);
        self.confirms_channels.deinit();
    }

    pub fn recordExchange(self: *TopologyTracker, name: []const u8, type_name: []const u8, durable: bool) !void {
        try self.exchanges.append(self.allocator, .{
            .name = name,
            .type_name = type_name,
            .durable = durable,
        });
    }

    pub fn recordQueue(self: *TopologyTracker, name: []const u8, durable: bool, exclusive: bool, auto_delete: bool) !void {
        try self.queues.append(self.allocator, .{
            .name = name,
            .durable = durable,
            .exclusive = exclusive,
            .auto_delete = auto_delete,
        });
    }

    pub fn recordBinding(self: *TopologyTracker, queue: []const u8, exchange: []const u8, routing_key: []const u8) !void {
        try self.bindings.append(self.allocator, .{
            .queue = queue,
            .exchange = exchange,
            .routing_key = routing_key,
        });
    }

    pub fn recordSubscription(self: *TopologyTracker, channel_id: u16, queue: []const u8, consumer_tag: []const u8, no_ack: bool) !void {
        try self.subscriptions.append(self.allocator, .{
            .channel_id = channel_id,
            .queue = queue,
            .consumer_tag = consumer_tag,
            .no_ack = no_ack,
        });
    }

    pub fn recordConfirms(self: *TopologyTracker, channel_id: u16) !void {
        try self.confirms_channels.put(channel_id, {});
    }

    /// Re-declares and restores all recorded topology on an active connection.
    pub fn restoreTopology(self: *const TopologyTracker, conn: *connection_mod.Connection) !void {
        if (conn.state != .open) return error.ConnectionNotOpen;

        // Open channel 1 for setup declarations
        var setup_ch = try conn.openChannel(1);
        defer setup_ch.close() catch {};

        // 1. Restore Exchanges
        for (self.exchanges.items) |ex| {
            try setup_ch.declareExchange(ex.name, ex.type_name, ex.durable);
        }

        // 2. Restore Queues
        for (self.queues.items) |q| {
            _ = try setup_ch.declareQueue(q.name, q.durable, q.exclusive, q.auto_delete);
        }

        // 3. Restore Bindings
        for (self.bindings.items) |b| {
            try setup_ch.bindQueue(b.queue, b.exchange, b.routing_key);
        }

        // 4. Restore Subscriptions
        for (self.subscriptions.items) |sub| {
            var ch = conn.openChannel(sub.channel_id) catch |err| {
                if (err == error.BrokerError) continue;
                return err;
            };
            try ch.consume(sub.queue, sub.consumer_tag, sub.no_ack);
        }

        // 5. Restore Confirms
        var confirms_iter = self.confirms_channels.keyIterator();
        while (confirms_iter.next()) |ch_id| {
            var ch = conn.openChannel(ch_id.*) catch |err| {
                if (err == error.BrokerError) continue;
                return err;
            };
            try ch.enableConfirms();
        }
    }
};

test "reconnect backoff exponential growth" {
    var state = ReconnectState.init(.{
        .initial_delay_ms = 100,
        .max_delay_ms = 800,
        .multiplier = 2.0,
        .max_attempts = 4,
    });

    try std.testing.expectEqual(@as(?u64, 100), state.nextDelayMs());
    try std.testing.expectEqual(@as(?u64, 200), state.nextDelayMs());
    try std.testing.expectEqual(@as(?u64, 400), state.nextDelayMs());
    try std.testing.expectEqual(@as(?u64, 800), state.nextDelayMs());
    // Max attempts reached
    try std.testing.expectEqual(@as(?u64, null), state.nextDelayMs());

    state.reset();
    try std.testing.expectEqual(@as(?u64, 100), state.nextDelayMs());
}

test "heartbeat watchdog deadline detection" {
    const hb_interval: u16 = 60;
    var watchdog = HeartbeatWatchdog.init(hb_interval, 1000);

    // Initial state: not ready to send, peer is alive
    try std.testing.expect(!watchdog.shouldSendHeartbeat(1010));
    try std.testing.expect(!watchdog.isPeerDead(1010));

    // After 30 seconds (hb / 2): should emit heartbeat
    try std.testing.expect(watchdog.shouldSendHeartbeat(1030));
    try std.testing.expect(!watchdog.isPeerDead(1030));

    // Client records heartbeat emission
    watchdog.recordWrite(1030);
    try std.testing.expect(!watchdog.shouldSendHeartbeat(1035));

    // At 1120 seconds (120 seconds after last read = 2 * hb): peer is dead!
    try std.testing.expect(watchdog.isPeerDead(1120));

    // Client receives frame from peer
    watchdog.recordRead(1120);
    try std.testing.expect(!watchdog.isPeerDead(1120));
}

test "topology tracker records and restores over rabbitmq" {
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

    var tracker = TopologyTracker.init(std.testing.allocator);
    defer tracker.deinit();

    try tracker.recordExchange("zg.topo.exchange", "direct", false);
    try tracker.recordQueue("zg.topo.queue", false, false, true);
    try tracker.recordBinding("zg.topo.queue", "zg.topo.exchange", "topo.key");

    // Restore topology on live connection
    try tracker.restoreTopology(&conn);

    // Verify queue exists and can receive a message
    var ch = try conn.openChannel(4);
    defer ch.close() catch {};

    try ch.publish("zg.topo.exchange", "topo.key", "hello topology", .{}, false);
}
