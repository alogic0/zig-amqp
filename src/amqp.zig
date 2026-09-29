//! Native AMQP 0-9-1 (RabbitMQ) Client in Zig
const std = @import("std");

pub const wire = @import("wire.zig");
pub const frame = @import("frame.zig");
pub const properties = @import("properties.zig");
pub const method = @import("method.zig");
pub const transport = @import("transport.zig");
pub const connection = @import("connection.zig");

test {
    std.testing.refAllDecls(@This());
    _ = wire;
    _ = frame;
    _ = properties;
    _ = method;
    _ = transport;
    _ = connection;
}
