//! AMQP 0-9-1 Connection Handshake and Lifecycle Management.
const std = @import("std");
const Io = std.Io;
const wire = @import("wire.zig");
const frame = @import("frame.zig");
const method = @import("method.zig");
const transport_mod = @import("transport.zig");

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
};

pub const Error = error{
    ConnectionClosed,
    UnexpectedFrameType,
    UnexpectedMethod,
    HandshakeFailed,
    CredentialsTooLong,
    ProtocolViolation,
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

    /// Closes the connection gracefully with Connection.Close and Connection.CloseOk.
    pub fn close(self: *Connection) !void {
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

    pub fn deinit(self: *Connection) void {
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
