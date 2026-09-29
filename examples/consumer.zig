//! Example: AMQP 0-9-1 Consumer
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
        .client_name = "example-consumer",
    });
    defer client.deinit();

    std.debug.print("Connecting to RabbitMQ...\n", .{});
    try client.connect();
    defer client.close() catch {};

    var ch = try client.openChannel(1);
    defer ch.close() catch {};

    _ = try ch.declareQueue("example.queue", false, false, false);
    try ch.consume("example.queue", "my_worker", false);

    std.debug.print("Subscribed to example.queue. Waiting for message...\n", .{});

    var msg = try ch.readMessage(allocator);
    defer msg.deinit(allocator);

    std.debug.print("Received message: tag={d}, body={s}\n", .{ msg.delivery_tag, msg.body });
    try msg.ack(false);
    std.debug.print("Acknowledged delivery tag {d}.\n", .{msg.delivery_tag});
}
