//! Native AMQP 0-9-1 (RabbitMQ) Client in Zig
const std = @import("std");

pub const wire = @import("wire.zig");
pub const frame = @import("frame.zig");
pub const properties = @import("properties.zig");
pub const method = @import("method.zig");
pub const transport = @import("transport.zig");
pub const connection = @import("connection.zig");
pub const channel = @import("channel.zig");
pub const consumer = @import("consumer.zig");
pub const recovery = @import("recovery.zig");
pub const client = @import("client.zig");

// Re-export primary user-facing types
pub const Client = client.Client;
pub const Config = client.Config;
pub const Channel = client.Channel;
pub const ChannelState = client.ChannelState;
pub const Message = consumer.Message;
pub const BasicProperties = properties.BasicProperties;
pub const DeliveryMode = properties.DeliveryMode;
pub const FieldValue = wire.FieldValue;
pub const FieldEntry = wire.FieldEntry;
pub const ReplyCode = method.ReplyCode;

test {
    std.testing.refAllDecls(@This());
    _ = wire;
    _ = frame;
    _ = properties;
    _ = method;
    _ = transport;
    _ = connection;
    _ = channel;
    _ = consumer;
    _ = recovery;
    _ = client;
}
