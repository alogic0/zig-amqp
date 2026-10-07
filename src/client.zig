//! AMQP 0-9-1 High-Level Ergonomic Client with Topology Management.
const std = @import("std");
const Io = std.Io;
const connection_mod = @import("connection.zig");
const channel_mod = @import("channel.zig");
const recovery_mod = @import("recovery.zig");
const properties_mod = @import("properties.zig");

pub const Config = connection_mod.ConnectionConfig;
pub const Channel = channel_mod.Channel;
pub const ChannelState = channel_mod.ChannelState;
pub const BasicProperties = properties_mod.BasicProperties;
pub const DeliveryMode = properties_mod.DeliveryMode;

pub const Client = struct {
    allocator: std.mem.Allocator,
    io: Io,
    config: Config,
    connection: connection_mod.Connection,
    topology: recovery_mod.TopologyTracker,
    reconnect_state: recovery_mod.ReconnectState,
    uri_arena: ?std.heap.ArenaAllocator = null,

    pub fn init(allocator: std.mem.Allocator, io: Io, config: Config) Client {
        return .{
            .allocator = allocator,
            .io = io,
            .config = config,
            .connection = connection_mod.Connection.init(allocator, io, config),
            .topology = recovery_mod.TopologyTracker.init(allocator),
            .reconnect_state = recovery_mod.ReconnectState.init(.{}),
        };
    }

    /// Initializes a client by parsing an AMQP connection URI (e.g. "amqp://guest:guest@localhost:5672/").
    pub fn initUri(allocator: std.mem.Allocator, io: Io, uri: []const u8) !Client {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const config = try Config.fromUri(arena.allocator(), uri);
        var c = Client.init(allocator, io, config);
        c.uri_arena = arena;
        return c;
    }

    pub fn deinit(self: *Client) void {
        self.connection.deinit();
        self.topology.deinit();
        if (self.uri_arena) |*arena| {
            arena.deinit();
            self.uri_arena = null;
        }
    }

    /// Establishes the connection and runs the AMQP 0-9-1 handshake.
    pub fn connect(self: *Client) !void {
        try self.connection.connect();
        self.reconnect_state.reset();
    }

    /// Reconnects to the broker using exponential backoff and transparently restores all declared topology.
    pub fn reconnect(self: *Client) !void {
        self.connection.deinit();
        self.reconnect_state.reset();

        while (self.reconnect_state.nextDelayMs()) |delay_ms| {
            // Sleep for backoff interval
            self.io.sleep(.fromMilliseconds(@intCast(delay_ms)), .awake) catch {};

            self.connection = connection_mod.Connection.init(self.allocator, self.io, self.config);
            if (self.connection.connect()) |_| {
                // Connection re-established; restore all exchanges, queues, bindings, subscriptions
                self.topology.restoreTopology(&self.connection) catch |err| {
                    self.connection.deinit();
                    return err;
                };
                self.reconnect_state.reset();
                return;
            } else |_| {
                self.connection.deinit();
                continue;
            }
        }

        return error.ReconnectFailed;
    }

    /// Opens an AMQP channel.
    pub fn openChannel(self: *Client, channel_id: u16) !Channel {
        return self.connection.openChannel(channel_id);
    }

    /// Closes the connection gracefully.
    pub fn close(self: *Client) !void {
        try self.connection.close();
    }
};

test "client connect, channel, publish, and clean shutdown" {
    const config = Config{
        .host = "127.0.0.1",
        .port = 5674,
        .username = "guest",
        .password = "guest",
        .virtual_host = "/",
    };

    var client = Client.init(std.testing.allocator, std.testing.io, config);
    defer client.deinit();

    client.connect() catch |err| {
        if (err == error.ConnectionRefused or err == error.ConnectionFailed) return;
        return err;
    };
    defer client.close() catch {};

    var ch = try client.openChannel(5);
    defer ch.close() catch {};

    _ = try ch.declareQueue("zg.test.client_queue", false, false, true);
    try ch.publish("", "zg.test.client_queue", "{\"msg\": \"hello client\"}", .{
        .content_type = "application/json",
    }, false);
}

test "client initUri against live rabbitmq" {
    var client = try Client.initUri(std.testing.allocator, std.testing.io, "amqp://guest:guest@127.0.0.1:5674/%2F");
    defer client.deinit();

    client.connect() catch |err| {
        if (err == error.ConnectionRefused or err == error.ConnectionFailed) return;
        return err;
    };
    defer client.close() catch {};

    try std.testing.expectEqual(connection_mod.ConnectionState.open, client.connection.state);
}
