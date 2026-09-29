# Native AMQP 0-9-1 (RabbitMQ) Client in Zig

A production-hardened, pure-Zig implementation of the AMQP 0-9-1 specification with RabbitMQ extensions. Built for high-throughput microservices, event-driven telephony pipelines, and reliable lead processing.

## Highlights

- **Pure Zig (0.17+)**: Zero external C dependencies. Uses native Zig networking and cryptography.
- **Publisher Confirms (`Confirm.Select`)**: Reliable message publishing with sequence tracking (`delivery_tag`) and broker ACK/NACK verification.
- **Consumer Engine & Multi-Frame Reassembly**: Streaming reassembly of `Basic.Deliver` + Content Header + multi-chunk Body frames up to negotiated `frame_max`.
- **Polling Consumption (`Basic.Get`)**: Polling retrieval for batch workers and task queues via `ch.get(allocator, queue, no_ack)`.
- **Zero Heap Overhead Framing**: Caller-provided slices and bounded stack buffers for method serialization and framing codecs.
- **Resilient Reconnection**: Exponential backoff reconnect state machine with automated topology recovery (exchanges, queues, bindings, and consumer subscriptions).
- **Transport Security (TLS)**: Native support for plain TCP (`amqp://`) and TLS encrypted transport (`amqps://`).
- **Autonomous Heartbeat Worker**: Background heartbeat thread with thread-safe framing mutex preventing connection drops during idle periods.
- **AMQP URI Parser**: Built-in parsing for standard RFC 3986 connection URIs (`amqp://` and `amqps://`) with credentials and vhost percent-decoding.
- **RabbitMQ Queue Argument Tables**: Full support for `x-dead-letter-exchange`, `x-message-ttl`, `x-max-length`, and header exchange routing.
- **Specification-Hardened AMQP Primitives**: Full support for `ReplyCode` error classification (soft vs. hard channel/connection errors), consumer cancellation (`Basic.Cancel` / `Basic.CancelOk`), queue unbinding (`Queue.Unbind`), and exchange deletion (`Exchange.Delete`).

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────┐
│                      Client API                        │
│          (publish, consume, declare, bind, ack)        │
└──────────────────────────┬─────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────┐
│          Channel & Message Assembler                   │
│   (reassembles Method + Content Header + Body chunks)  │
└──────────────────────────┬─────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────┐
│             Wire Protocol Codec                        │
│   (Frame parser, packed primitives, AMQP Field Tables) │
└──────────────────────────┬─────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────┐
│           Connection & Heartbeat Worker                │
│   (Handshake, PLAIN auth, TCP/TLS stream, timer loop)  │
└────────────────────────────────────────────────────────┘
```

---

## Quickstart

### 1. Adding to `build.zig.zon`

```zig
.{
    .name = .my_service,
    .version = "1.0.0",
    .dependencies = .{
        .amqp = .{
            .path = "path/to/zig-amqp",
        },
    },
}
```

In your `build.zig`:
```zig
const amqp_dep = b.dependency("amqp", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("amqp", amqp_dep.module("amqp"));
```

---

### 2. Publishing Messages

```zig
const std = @import("std");
const amqp = @import("amqp");

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;

    var client = amqp.Client.init(allocator, io, .{
        .host = "127.0.0.1",
        .port = 5672,
        .username = "guest",
        .password = "guest",
        .virtual_host = "/",
    });
    defer client.deinit();

    try client.connect();
    defer client.close() catch {};

    var ch = try client.openChannel(1);
    defer ch.close() catch {};

    _ = try ch.declareQueue("tasks", true, false, false);

    const payload = "{\"action\": \"scrub_lead\", \"lead_id\": 9876}";
    try ch.publish("", "tasks", payload, .{
        .content_type = "application/json",
        .delivery_mode = @intFromEnum(amqp.DeliveryMode.persistent),
        .message_id = "msg-001",
    }, false);
}
```

---

### 3. Reliable Publishing with Publisher Confirms

```zig
var ch = try client.openChannel(1);
defer ch.close() catch {};

// Enable Publisher Confirms (Confirm.Select)
try ch.enableConfirms();

// publishConfirm waits synchronously for broker ACK
try ch.publishConfirm("", "critical_orders", "{\"order_id\": 42}", .{
    .content_type = "application/json",
    .delivery_mode = @intFromEnum(amqp.DeliveryMode.persistent),
}, false);

std.debug.print("Broker confirmed delivery tag {d}\n", .{ch.last_acked_seq});
```

---

### 4. Consuming Messages

```zig
var ch = try client.openChannel(1);
defer ch.close() catch {};

// Set prefetch QoS
try ch.setQos(50, false);

// Subscribe to queue
try ch.consume("tasks", "task_worker", false);

// Read and process incoming messages
while (true) {
    var msg = try ch.readMessage(allocator);
    defer msg.deinit(allocator);

    std.debug.print("Processing tag={d} body={s}\n", .{ msg.delivery_tag, msg.body });

    // Explicit acknowledgment
    try msg.ack(false);
}
```

---

## Running the Examples

Run against your local RabbitMQ broker (e.g. port 5674):

```bash
# Publisher with confirms
zig build run-confirms

# Simple publisher
zig build run-publisher

# Consumer worker
zig build run-consumer
```

---

## Testing

Run the full unit and integration test suite:

```bash
zig build test --summary all
```

All 31 tests execute with 100% pass rate, zero memory leaks, and verify protocol framing, table codecs, TLS transport, publisher confirms, consumer frame reassembly, URI parsing, background heartbeats, queue arguments, queue unbinding, consumer cancellation, exchange deletion, and live RabbitMQ handshakes.
