# Native AMQP 0-9-1 (RabbitMQ) Client in Zig — Implementation Plan

> **Target Repository**: `~/prog/zig-amqp`
> **Target Zig Version**: `0.17.0-dev.2015+3fdcbc03d`
> **Specification**: AMQP 0-9-1 (with RabbitMQ extensions)
> **Goal**: Production-hardened, zero-allocation capable, pure-Zig AMQP 0-9-1 client supporting publisher confirms, automatic reconnection, and TLS.

---

## 1. Executive Summary & Goals

This project implements a native, high-performance AMQP 0-9-1 client in pure Zig with no external C dependencies. It is specifically designed for high-throughput systems, event-driven microservices, lead-scrubbing pipelines, and telephony telemetry.

### Core Requirements
1. **Full Protocol Framing**: Strict adherence to AMQP 0-9-1 framing, packed big-endian primitives, and RabbitMQ-compatible Field Tables.
2. **Publisher Confirms (`Confirm.Select`)**: Reliable message publishing with sequence tracking (`delivery_tag`) and `Basic.Ack` / `Basic.Nack` resolution.
3. **Consumer & QoS**: `Basic.Consume`, streaming multi-frame reassembly (Method + Header + Body chunks), prefetch QoS tuning (`Basic.Qos`), and explicit acknowledgment (`Basic.Ack`, `Basic.Nack`, `Basic.Reject`).
4. **Resilient Reconnection**: Exponential backoff, connection state preservation, and transparent re-establishment of channels and subscriptions.
5. **Transport Security (TLS)**: Support for plain TCP (`amqp://`) and TLS encrypted transport (`amqps://`) via native Zig crypto / TLS streams.
6. **Zero Heap Allocation Capable**: Core frame serialization and parsing operate over bounded stack buffers and caller-provided slices without heap churn.

---

## 2. Protocol Architecture

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

### AMQP 0-9-1 Frame Envelope
```
+----------+------------+---------------+---------------------+-------------+
| Type(1B) | Channel(2B)| Length(4B BE) | Payload(Length B)   | EndByte(1B) |
+----------+------------+---------------+---------------------+-------------+
```
* **Frame Types**:
  * `0x01` (`Method`): Command invocations (`Connection.StartOk`, `Basic.Publish`, etc.).
  * `0x02` (`Header`): Message metadata (class ID 60, body byte size, properties bitmask).
  * `0x03` (`Body`): Raw message payload bytes (chunked to `frame_max`).
  * `0x08` (`Heartbeat`): Keepalive tick.
* **End Byte**: Constant `0xCE` (206) used to validate framing integrity.

---

## 3. Phased Slice Roadmap

The implementation is broken into 8 sequential slices, each verified with unit tests and committed atomically:

### Slice 1: Protocol Types, Wire Primitives & Framing Codec
- AMQP frame constants and header structures.
- Big-endian reader/writer helpers for integer primitives (`u8`, `u16`, `u32`, `u64`).
- Short string (`ShortString`) and long string (`LongString`) encoding/decoding.
- RabbitMQ Field Table parser and builder (strings, integers, booleans, timestamps, tables, arrays).
- Frame envelope serialization and deserialization with `0xCE` validation.
- Unit tests covering all wire codec edge cases.

### Slice 2: AMQP 0-9-1 Method Definitions & Properties
- Handshake methods: `Connection.Start`, `Connection.StartOk`, `Connection.Tune`, `Connection.TuneOk`, `Connection.Open`, `Connection.OpenOk`, `Connection.Close`, `Connection.CloseOk`.
- Channel methods: `Channel.Open`, `Channel.OpenOk`, `Channel.Close`, `Channel.CloseOk`.
- Exchange & Queue methods: `Exchange.Declare`, `Queue.Declare`, `Queue.DeclareOk`, `Queue.Bind`, `Queue.BindOk`, `Queue.Purge`.
- Basic methods: `Basic.Publish`, `Basic.Consume`, `Basic.ConsumeOk`, `Basic.Deliver`, `Basic.Ack`, `Basic.Nack`, `Basic.Reject`, `Basic.Qos`, `Basic.QosOk`.
- Publisher confirm methods: `Confirm.Select`, `Confirm.SelectOk`.
- Basic Content Header properties: `content_type`, `content_encoding`, `headers`, `delivery_mode`, `priority`, `correlation_id`, `reply_to`, `message_id`, `timestamp`.

### Slice 3: Connection Handshake & Network Transport (TCP & TLS)
- Network stream abstraction supporting plain TCP (`amqp://`) and TLS streams (`amqps://`).
- Protocol header exchange (`AMQP\x00\x00\x09\x01`).
- PLAIN authentication formatting (`\0username\0password`).
- Connection tuning negotiation (`channel_max`, `frame_max`, `heartbeat_sec`).
- Vhost selection and connection establishment.
- Loopback connection and handshake test suite.

### Slice 4: Channel Multiplexer & Publisher Engine
- Channel state management (opening, tracking, closing).
- `Basic.Publish` 3-frame sequence assembler (Method frame + Content Header + Body chunks).
- Automatic chunking for payloads exceeding negotiated `frame_max`.
- Non-allocating publishing path using caller-provided buffers.

### Slice 5: Publisher Confirms (`Confirm.Select`)
- Enable confirms on channel via `Confirm.Select`.
- Sequence tracking (`delivery_tag` increments for each published message).
- Handling server `Basic.Ack` and `Basic.Nack` responses with `multiple` flag support.
- Synchronous `publishConfirm()` awaiting broker acknowledgment.

### Slice 6: Consumer Engine (`Basic.Consume` & Message Assembly)
- `Basic.Consume` subscription registration with consumer tags.
- Prefetch tuning via `Basic.Qos`.
- Inbound multi-frame reassembly: receiving `Basic.Deliver`, followed by Content Header, followed by Body chunk(s).
- Ergonomic `Message` struct providing access to body, headers, routing key, and exchange.
- Acknowledgment methods: `message.ack()`, `message.nack()`, `message.reject()`.

### Slice 7: Heartbeats & Resilient Auto-Reconnection
- Background heartbeat timer: emits type-8 heartbeat frames every `heartbeat / 2` seconds.
- Missing heartbeat detection: flags connection drop if no frame received within `2 * heartbeat` seconds.
- Exponential backoff reconnection loop.
- Channel and queue topology re-establishment upon reconnect.

### Slice 8: High-Level Client, Examples & Build Integration
- Clean, ergonomic top-level `Client` / `Connection` / `Channel` API.
- Integration tests against live RabbitMQ broker (e.g. `127.0.0.1:5674`).
- Example programs: `publisher.zig`, `consumer.zig`, `confirms.zig`.
- Comprehensive `README.md`, `build.zig`, and `build.zig.zon`.

---

## 4. Quality Gates & Verification Checklist

Every slice must satisfy:
1. `zig build test`: 100% of unit tests pass with zero memory leaks.
2. `git diff --check`: Zero whitespace or formatting defects.
3. Strict bounds checking on all wire decoders to prevent buffer overruns or malformed frame panics.
