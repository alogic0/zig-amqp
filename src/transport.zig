//! Network Transport layer supporting plain TCP and TLS for AMQP 0-9-1.
const std = @import("std");
const Io = std.Io;
const net = Io.net;
const tls = std.crypto.tls;
const frame = @import("frame.zig");

pub const BUFFER_SIZE: usize = 32768; // 32KB buffer satisfies TLS min_buffer_len (16645)

pub const Error = error{
    ConnectionFailed,
    TlsHandshakeFailed,
    Closed,
    BufferTooSmall,
} || Io.Reader.Error || Io.Writer.Error;

pub const Transport = struct {
    allocator: std.mem.Allocator,
    io: Io,
    stream: net.Stream,
    is_tls: bool,

    read_buf: []u8,
    write_buf: []u8,

    net_reader: net.Stream.Reader,
    net_writer: net.Stream.Writer,

    tls_read_buf: ?[]u8 = null,
    tls_write_buf: ?[]u8 = null,
    tls_client: ?tls.Client = null,
    write_mutex: Io.Mutex = .init,

    pub fn connect(
        allocator: std.mem.Allocator,
        io: Io,
        host: []const u8,
        port: u16,
        use_tls: bool,
        tls_hostname: ?[]const u8,
    ) !Transport {
        const stream = try connectStream(io, host, port);

        const read_buf = try allocator.alloc(u8, BUFFER_SIZE);
        errdefer allocator.free(read_buf);
        const write_buf = try allocator.alloc(u8, BUFFER_SIZE);
        errdefer allocator.free(write_buf);

        var self = Transport{
            .allocator = allocator,
            .io = io,
            .stream = stream,
            .is_tls = use_tls,
            .read_buf = read_buf,
            .write_buf = write_buf,
            .net_reader = stream.reader(io, read_buf),
            .net_writer = stream.writer(io, write_buf),
            .tls_client = null,
        };

        if (use_tls) {
            const tls_read_buf = try allocator.alloc(u8, BUFFER_SIZE);
            errdefer allocator.free(tls_read_buf);
            const tls_write_buf = try allocator.alloc(u8, BUFFER_SIZE);
            errdefer allocator.free(tls_write_buf);

            var entropy: [tls.Client.Options.entropy_len]u8 = undefined;
            io.random(&entropy);

            const now = Io.Clock.real.now(io);
            const h = tls_hostname orelse host;

            const tls_cli = tls.Client.init(
                &self.net_reader.interface,
                &self.net_writer.interface,
                .{
                    .host = if (tls_hostname != null or host.len > 0) .{ .explicit = h } else .no_verification,
                    .ca = .{ .no_verification = {} },
                    .read_buffer = tls_read_buf,
                    .write_buffer = tls_write_buf,
                    .entropy = &entropy,
                    .realtime_now = now,
                },
            ) catch return Error.TlsHandshakeFailed;
            self.tls_read_buf = tls_read_buf;
            self.tls_write_buf = tls_write_buf;
            self.tls_client = tls_cli;
        }

        return self;
    }

    fn connectStream(io: Io, host: []const u8, port: u16) !net.Stream {
        if (std.mem.eql(u8, host, "localhost") or std.mem.eql(u8, host, "127.0.0.1")) {
            const addr = try net.IpAddress.parse("127.0.0.1", port);
            return addr.connect(io, .{ .mode = .stream });
        }

        if (net.IpAddress.parse(host, port)) |addr| {
            return addr.connect(io, .{ .mode = .stream });
        } else |_| {}

        const hn = net.HostName.init(host) catch return Error.ConnectionFailed;
        return hn.connect(io, port, .{ .mode = .stream });
    }

    pub fn reader(self: *Transport) *Io.Reader {
        if (self.is_tls) {
            return &self.tls_client.?.reader;
        }
        return &self.net_reader.interface;
    }

    pub fn writer(self: *Transport) *Io.Writer {
        if (self.is_tls) {
            return &self.tls_client.?.writer;
        }
        return &self.net_writer.interface;
    }

    pub fn writeAll(self: *Transport, bytes: []const u8) !void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        try self.writer().writeAll(bytes);
    }

    pub fn flush(self: *Transport) !void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        try self.writer().flush();
    }

    pub fn readFrame(self: *Transport, dest_payload: []u8) !frame.Frame {
        return frame.readFrameFromReader(self.reader(), dest_payload);
    }

    pub fn sendFrame(self: *Transport, frame_type: frame.FrameType, channel: u16, payload: []const u8) !void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);

        var stack_buf: [4096]u8 = undefined;
        const total_needed = frame.HEADER_SIZE + payload.len + frame.FOOTER_SIZE;
        if (total_needed <= stack_buf.len) {
            const n = try frame.writeFrame(&stack_buf, frame_type, channel, payload);
            try self.writer().writeAll(stack_buf[0..n]);
            try self.writer().flush();
        } else {
            const heap_buf = try self.allocator.alloc(u8, total_needed);
            defer self.allocator.free(heap_buf);
            const n = try frame.writeFrame(heap_buf, frame_type, channel, payload);
            try self.writer().writeAll(heap_buf[0..n]);
            try self.writer().flush();
        }
    }

    pub fn close(self: *Transport) void {
        if (self.read_buf.len > 0) {
            self.stream.close(self.io);
            self.allocator.free(self.read_buf);
            self.read_buf = &.{};
            self.allocator.free(self.write_buf);
            self.write_buf = &.{};
            if (self.tls_read_buf) |b| {
                self.allocator.free(b);
                self.tls_read_buf = null;
            }
            if (self.tls_write_buf) |b| {
                self.allocator.free(b);
                self.tls_write_buf = null;
            }
        }
    }
};
