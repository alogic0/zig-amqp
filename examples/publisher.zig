//! Example: AMQP 0-9-1 Publisher
const std = @import("std");
const amqp = @import("amqp");

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;

    var client = amqp.Client.init(allocator, io, .{
        .host = "127.0.0.1",
        .port = 5674,
        .username = "guest",
        .password = "guest",
        .virtual_host = "/",
        .client_name = "example-publisher",
    });
    defer client.deinit();

    std.debug.print("Connecting to RabbitMQ...\n", .{});
    try client.connect();
    defer client.close() catch {};
    std.debug.print("Connected!\n", .{});

    var ch = try client.openChannel(1);
    defer ch.close() catch {};

    // Declare queue
    _ = try ch.declareQueue("example.queue", false, false, false);

    // Publish message
    const payload = "{\"event\": \"task_created\", \"timestamp\": 1727584000}";
    try ch.publish("", "example.queue", payload, .{
        .content_type = "application/json",
        .delivery_mode = @backingInt(amqp.properties.DeliveryMode.persistent),
        .message_id = "msg-001",
    }, false);

    std.debug.print("Published message: {s}\n", .{payload});
}
