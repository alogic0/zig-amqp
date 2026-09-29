//! AMQP 0-9-1 Connection Handshake and Lifecycle Management.
const std = @import("std");
const Io = std.Io;
const wire = @import("wire.zig");
const frame = @import("frame.zig");
const method = @import("method.zig");
const transport_mod = @import("transport.zig");
const channel_mod = @import("channel.zig");

pub const ConnectionState = enum {
    closed,
    connecting,
    negotiating,
    open,
    closing,
};

pub const ConnectionConfig = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 5672,
    username: []const u8 = "guest",
    password: []const u8 = "guest",
    virtual_host: []const u8 = "/",
    tls: bool = false,
    tls_hostname: ?[]const u8 = null,
    desired_channel_max: u16 = 2047,
    desired_frame_max: u32 = 131072,
    desired_heartbeat: u16 = 60,
    client_name: []const u8 = "zig-amqp",

    /// Parses an AMQP URI (e.g. amqp://guest:guest@localhost:5672/ or amqps://.../%2F).
    pub fn fromUri(allocator: std.mem.Allocator, uri_str: []const u8) !ConnectionConfig {
        var cfg = ConnectionConfig{};
        var rem: []const u8 = undefined;

        if (std.mem.startsWith(u8, uri_str, "amqp://")) {
            cfg.tls = false;
            cfg.port = 5672;
            rem = uri_str[7..];
        } else if (std.mem.startsWith(u8, uri_str, "amqps://")) {
            cfg.tls = true;
            cfg.port = 5671;
            rem = uri_str[8..];
        } else {
            return error.InvalidUriScheme;
        }

        // Check for userinfo: [user[:pass]@]host_port[/vhost]
        if (std.mem.indexOfScalar(u8, rem, '@')) |at_idx| {
            const userinfo = rem[0..at_idx];
            rem = rem[at_idx + 1 ..];
            if (std.mem.indexOfScalar(u8, userinfo, ':')) |colon_idx| {
                cfg.username = try percentDecode(allocator, userinfo[0..colon_idx]);
                cfg.password = try percentDecode(allocator, userinfo[colon_idx + 1 ..]);
            } else {
                cfg.username = try percentDecode(allocator, userinfo);
                cfg.password = "";
            }
        }

        // Separate host_port from path (vhost)
        const host_port = if (std.mem.indexOfScalar(u8, rem, '/')) |slash_idx| blk: {
            const raw_vhost = rem[slash_idx + 1 ..];
            if (raw_vhost.len > 0) {
                if (std.mem.eql(u8, raw_vhost, "%2F") or std.mem.eql(u8, raw_vhost, "%2f")) {
                    cfg.virtual_host = "/";
                } else {
                    cfg.virtual_host = try percentDecode(allocator, raw_vhost);
                }
            } else {
                cfg.virtual_host = "/";
            }
            break :blk rem[0..slash_idx];
        } else blk: {
            cfg.virtual_host = "/";
            break :blk rem;
        };

        // Parse host_port into host and optional port
        if (std.mem.indexOfScalar(u8, host_port, ':')) |port_colon| {
            cfg.host = host_port[0..port_colon];
            const port_str = host_port[port_colon + 1 ..];
            cfg.port = try std.fmt.parseInt(u16, port_str, 10);
        } else {
            if (host_port.len > 0) {
                cfg.host = host_port;
            }
        }

        if (cfg.tls and cfg.tls_hostname == null) {
            cfg.tls_hostname = cfg.host;
        }

        return cfg;
    }
};

fn percentDecode(allocator: std.mem.Allocator, input: []const u8) ![]const u8 {
    if (std.mem.indexOfScalar(u8, input, '%') == null) {
        return input;
    }
    const out = try allocator.alloc(u8, input.len);
    var in_idx: usize = 0;
    var out_idx: usize = 0;
    while (in_idx < input.len) {
        if (input[in_idx] == '%' and in_idx + 2 < input.len) {
            const hex = input[in_idx + 1 .. in_idx + 3];
            const byte = std.fmt.parseInt(u8, hex, 16) catch {
                out[out_idx] = input[in_idx];
                out_idx += 1;
                in_idx += 1;
                continue;
            };
            out[out_idx] = byte;
            out_idx += 1;
            in_idx += 3;
        } else {
            out[out_idx] = input[in_idx];
            out_idx += 1;
            in_idx += 1;
        }
    }
    return out[0..out_idx];
}

pub const Error = error{
    ConnectionClosed,
    UnexpectedFrameType,
    UnexpectedMethod,
    HandshakeFailed,
    CredentialsTooLong,
    ProtocolViolation,
    InvalidUriScheme,
} || wire.Error || frame.Error || method.Error || transport_mod.Error;

pub const Connection = struct {
    allocator: std.mem.Allocator,
    io: Io,
    config: ConnectionConfig,
    transport: ?transport_mod.Transport = null,
    state: ConnectionState = .closed,

    negotiated_channel_max: u16 = 0,
    negotiated_frame_max: u32 = 0,
    negotiated_heartbeat: u16 = 0,

    heartbeat_thread: ?std.Thread = null,
    heartbeat_stop: std.atomic.Value(bool) = .init(false),

    pub fn init(allocator: std.mem.Allocator, io: Io, config: ConnectionConfig) Connection {
        return .{
            .allocator = allocator,
            .io = io,
            .config = config,
        };
    }

    /// Establishes the TCP/TLS connection and performs the AMQP 0-9-1 handshake.
    pub fn connect(self: *Connection) !void {
        self.state = .connecting;

        var tr = try transport_mod.Transport.connect(
            self.allocator,
            self.io,
            self.config.host,
            self.config.port,
            self.config.tls,
            self.config.tls_hostname,
        );
        errdefer tr.close();

        self.transport = tr;
        self.state = .negotiating;

        // 1. Send Protocol Header: AMQP\x00\x00\x09\x01
        try self.transport.?.writeAll(wire.protocol_header);
        try self.transport.?.flush();

        // 2. Receive Connection.Start (Channel 0)
        var payload_buf: [4096]u8 = undefined;
        const start_f = try self.transport.?.readFrame(&payload_buf);
        if (start_f.frame_type != .method or start_f.channel != 0) return Error.ProtocolViolation;

        const start_m = try method.decodeMethod(start_f.payload);
        switch (start_m) {
            .connection_start => |s| {
                if (s.version_major != 0 or s.version_minor != 9) {
                    return Error.HandshakeFailed;
                }
            },
            else => return Error.UnexpectedMethod,
        }

        // 3. Send Connection.StartOk (Channel 0)
        var auth_buf: [256]u8 = undefined;
        if (2 + self.config.username.len + self.config.password.len > auth_buf.len) {
            return Error.CredentialsTooLong;
        }
        auth_buf[0] = 0;
        @memcpy(auth_buf[1 .. 1 + self.config.username.len], self.config.username);
        auth_buf[1 + self.config.username.len] = 0;
        @memcpy(
            auth_buf[2 + self.config.username.len .. 2 + self.config.username.len + self.config.password.len],
            self.config.password,
        );
        const auth_response = auth_buf[0 .. 2 + self.config.username.len + self.config.password.len];

        var client_props_buf: [256]u8 = undefined;
        const entries = [_]wire.FieldEntry{
            .{ .name = "product", .value = .{ .string = self.config.client_name } },
            .{ .name = "version", .value = .{ .string = "0.1.0" } },
            .{ .name = "platform", .value = .{ .string = "Zig" } },
        };
        const props_len = try wire.writeTable(&client_props_buf, &entries);

        const start_ok = method.Method{
            .connection_start_ok = .{
                .client_properties = client_props_buf[0..props_len],
                .mechanism = "PLAIN",
                .response = auth_response,
                .locale = "en_US",
            },
        };
        try self.sendMethod(0, start_ok);

        // 4. Receive Connection.Tune (Channel 0)
        const tune_f = try self.transport.?.readFrame(&payload_buf);
        if (tune_f.frame_type != .method or tune_f.channel != 0) return Error.ProtocolViolation;

        const tune_m = try method.decodeMethod(tune_f.payload);
        switch (tune_m) {
            .connection_tune => |t| {
                self.negotiated_channel_max = if (t.channel_max == 0 or self.config.desired_channel_max == 0)
                    @max(t.channel_max, self.config.desired_channel_max)
                else
                    @min(t.channel_max, self.config.desired_channel_max);

                self.negotiated_frame_max = if (t.frame_max == 0)
                    self.config.desired_frame_max
                else
                    @min(t.frame_max, self.config.desired_frame_max);

                self.negotiated_heartbeat = if (t.heartbeat == 0 or self.config.desired_heartbeat == 0)
                    0
                else
                    @min(t.heartbeat, self.config.desired_heartbeat);
            },
            else => return Error.UnexpectedMethod,
        }

        // 5. Send Connection.TuneOk (Channel 0)
        const tune_ok = method.Method{
            .connection_tune_ok = .{
                .channel_max = self.negotiated_channel_max,
                .frame_max = self.negotiated_frame_max,
                .heartbeat = self.negotiated_heartbeat,
            },
        };
        try self.sendMethod(0, tune_ok);

        // 6. Send Connection.Open (Channel 0)
        const open = method.Method{
            .connection_open = .{
                .virtual_host = self.config.virtual_host,
            },
        };
        try self.sendMethod(0, open);

        // 7. Receive Connection.OpenOk (Channel 0)
        const open_ok_f = try self.transport.?.readFrame(&payload_buf);
        if (open_ok_f.frame_type != .method or open_ok_f.channel != 0) return Error.ProtocolViolation;

        const open_ok_m = try method.decodeMethod(open_ok_f.payload);
        switch (open_ok_m) {
            .connection_open_ok => {},
            else => return Error.UnexpectedMethod,
        }

        self.state = .open;

        if (self.negotiated_heartbeat > 0) {
            self.heartbeat_stop.store(false, .release);
            const interval_sec = @max(1, self.negotiated_heartbeat / 2);
            self.heartbeat_thread = std.Thread.spawn(.{}, heartbeatWorker, .{ self, interval_sec }) catch null;
        }
    }

    /// Sends an AMQP method payload encoded inside a Frame.
    pub fn sendMethod(self: *Connection, channel_id: u16, m: method.Method) !void {
        if (self.transport == null) return Error.ConnectionClosed;
        var method_buf: [2048]u8 = undefined;
        const written = try method.encodeMethod(&method_buf, m);
        try self.transport.?.sendFrame(.method, channel_id, method_buf[0..written]);
    }

    /// Reads the next frame and decodes it as an AMQP method.
    pub fn readMethod(self: *Connection, dest_payload: []u8) !struct { channel: u16, method: method.Method } {
        if (self.transport == null) return Error.ConnectionClosed;
        const f = try self.transport.?.readFrame(dest_payload);
        if (f.frame_type != .method) return Error.UnexpectedFrameType;
        const m = try method.decodeMethod(f.payload);
        return .{
            .channel = f.channel,
            .method = m,
        };
    }

    pub fn stopHeartbeat(self: *Connection) void {
        if (self.heartbeat_thread) |t| {
            self.heartbeat_stop.store(true, .release);
            t.join();
            self.heartbeat_thread = null;
        }
    }

    fn heartbeatWorker(self: *Connection, interval_sec: u16) void {
        const total_steps = @as(u32, interval_sec) * 4;
        var counter: u32 = 0;
        const req = std.os.linux.timespec{
            .sec = 0,
            .nsec = 250 * 1000_000,
        };

        while (!self.heartbeat_stop.load(.acquire)) {
            _ = std.os.linux.nanosleep(&req, null);
            counter += 1;
            if (counter >= total_steps) {
                counter = 0;
                if (self.state != .open) break;
                if (self.transport) |*tr| {
                    tr.sendFrame(.heartbeat, 0, "") catch break;
                }
            }
        }
    }

    /// Closes the connection gracefully with Connection.Close and Connection.CloseOk.
    pub fn close(self: *Connection) !void {
        self.stopHeartbeat();
        if (self.state != .open or self.transport == null) return;
        self.state = .closing;

        // Send Connection.Close
        const close_m = method.Method{
            .connection_close = .{
                .reply_code = 200,
                .reply_text = "Normal shutdown",
                .class_id = 0,
                .method_id = 0,
            },
        };
        self.sendMethod(0, close_m) catch {};

        // Await Connection.CloseOk
        var payload_buf: [512]u8 = undefined;
        if (self.transport.?.readFrame(&payload_buf)) |f| {
            if (f.frame_type == .method and f.channel == 0) {
                _ = method.decodeMethod(f.payload) catch {};
            }
        } else |_| {}

        self.transport.?.close();
        self.transport = null;
        self.state = .closed;
    }

    pub fn openChannel(self: *Connection, channel_id: u16) !channel_mod.Channel {
        var ch = channel_mod.Channel{
            .id = channel_id,
            .connection = self,
            .state = .closed,
        };
        try ch.open();
        return ch;
    }

    pub fn deinit(self: *Connection) void {
        self.stopHeartbeat();
        if (self.transport) |*tr| {
            tr.close();
            self.transport = null;
        }
        self.state = .closed;
    }
};

test "live connection handshake against rabbitmq on port 5674" {
    const config = ConnectionConfig{
        .host = "127.0.0.1",
        .port = 5674,
        .username = "guest",
        .password = "guest",
        .virtual_host = "/",
    };
    var conn = Connection.init(std.testing.allocator, std.testing.io, config);
    defer conn.deinit();

    conn.connect() catch |err| {
        if (err == error.ConnectionRefused or err == error.ConnectionFailed) return;
        return err;
    };

    try std.testing.expectEqual(ConnectionState.open, conn.state);
    try std.testing.expect(conn.negotiated_frame_max >= 4096);
    try std.testing.expect(conn.negotiated_channel_max > 0);

    try conn.close();
    try std.testing.expectEqual(ConnectionState.closed, conn.state);
}

test "parse amqp URI variants" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    // 1. Plain amqp with default port and vhost
    {
        const cfg = try ConnectionConfig.fromUri(alloc, "amqp://127.0.0.1:5674");
        try std.testing.expectEqualStrings("127.0.0.1", cfg.host);
        try std.testing.expectEqual(@as(u16, 5674), cfg.port);
        try std.testing.expectEqualStrings("guest", cfg.username);
        try std.testing.expectEqualStrings("guest", cfg.password);
        try std.testing.expectEqualStrings("/", cfg.virtual_host);
        try std.testing.expect(!cfg.tls);
    }

    // 2. amqp with user, pass, and %2F root vhost
    {
        const cfg = try ConnectionConfig.fromUri(alloc, "amqp://app_user:secret_pass@rabbitmq.internal:5672/%2F");
        try std.testing.expectEqualStrings("rabbitmq.internal", cfg.host);
        try std.testing.expectEqual(@as(u16, 5672), cfg.port);
        try std.testing.expectEqualStrings("app_user", cfg.username);
        try std.testing.expectEqualStrings("secret_pass", cfg.password);
        try std.testing.expectEqualStrings("/", cfg.virtual_host);
        try std.testing.expect(!cfg.tls);
    }

    // 3. amqps with custom vhost and percent-encoded characters
    {
        const cfg = try ConnectionConfig.fromUri(alloc, "amqps://admin:p%40ss@broker.example.com:5671/my_vhost");
        try std.testing.expectEqualStrings("broker.example.com", cfg.host);
        try std.testing.expectEqual(@as(u16, 5671), cfg.port);
        try std.testing.expectEqualStrings("admin", cfg.username);
        try std.testing.expectEqualStrings("p@ss", cfg.password);
        try std.testing.expectEqualStrings("my_vhost", cfg.virtual_host);
        try std.testing.expect(cfg.tls);
        try std.testing.expectEqualStrings("broker.example.com", cfg.tls_hostname.?);
    }

    // 4. Invalid scheme error
    try std.testing.expectError(error.InvalidUriScheme, ConnectionConfig.fromUri(alloc, "http://localhost:5672"));
}
