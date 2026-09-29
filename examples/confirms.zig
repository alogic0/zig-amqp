//! Example: AMQP 0-9-1 Reliable Publishing with Confirms
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
        .client_name = "example-confirms",
    });
    defer client.deinit();

    std.debug.print("Connecting to RabbitMQ...\n", .{});
    try client.connect();
    defer client.close() catch {};

    var ch = try client.openChannel(1);
    defer ch.close() catch {};

    try ch.enableConfirms();
    std.debug.print("Publisher confirms enabled.\n", .{});

    _ = try ch.declareQueue("example.confirms_queue", false, false, false);

    const payload = "{\"event\": \"critical_order\", \"amount\": 499.99}";
    std.debug.print("Publishing critical message and waiting for broker confirmation...\n", .{});

    try ch.publishConfirm("", "example.confirms_queue", payload, .{
        .content_type = "application/json",
        .delivery_mode = @intFromEnum(amqp.properties.DeliveryMode.persistent),
    }, false);

    std.debug.print("Broker confirmed delivery tag {d}!\n", .{ch.last_acked_seq});
}
