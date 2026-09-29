//! Native AMQP 0-9-1 (RabbitMQ) Client in Zig
const std = @import("std");

pub const wire = @import("wire.zig");
pub const frame = @import("frame.zig");

test {
    std.testing.refAllDecls(@This());
    _ = wire;
    _ = frame;
}
