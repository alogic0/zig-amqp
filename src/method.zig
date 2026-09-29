//! AMQP 0-9-1 Method Frame Payloads and Serialization.
const std = @import("std");
const wire = @import("wire.zig");

pub const CLASS_CONNECTION: u16 = 10;
pub const CLASS_CHANNEL: u16 = 20;
pub const CLASS_EXCHANGE: u16 = 40;
pub const CLASS_QUEUE: u16 = 50;
pub const CLASS_BASIC: u16 = 60;
pub const CLASS_CONFIRM: u16 = 85;

pub const METHOD_CONNECTION_START: u16 = 10;
pub const METHOD_CONNECTION_START_OK: u16 = 11;
pub const METHOD_CONNECTION_TUNE: u16 = 30;
pub const METHOD_CONNECTION_TUNE_OK: u16 = 31;
pub const METHOD_CONNECTION_OPEN: u16 = 40;
pub const METHOD_CONNECTION_OPEN_OK: u16 = 41;
pub const METHOD_CONNECTION_CLOSE: u16 = 50;
pub const METHOD_CONNECTION_CLOSE_OK: u16 = 51;
pub const METHOD_CONNECTION_BLOCKED: u16 = 60;
pub const METHOD_CONNECTION_UNBLOCKED: u16 = 61;

pub const METHOD_CHANNEL_OPEN: u16 = 10;
pub const METHOD_CHANNEL_OPEN_OK: u16 = 11;
pub const METHOD_CHANNEL_CLOSE: u16 = 40;
pub const METHOD_CHANNEL_CLOSE_OK: u16 = 41;

pub const METHOD_EXCHANGE_DECLARE: u16 = 10;
pub const METHOD_EXCHANGE_DECLARE_OK: u16 = 11;

pub const METHOD_QUEUE_DECLARE: u16 = 10;
pub const METHOD_QUEUE_DECLARE_OK: u16 = 11;
pub const METHOD_QUEUE_BIND: u16 = 20;
pub const METHOD_QUEUE_BIND_OK: u16 = 21;
pub const METHOD_QUEUE_PURGE: u16 = 30;
pub const METHOD_QUEUE_PURGE_OK: u16 = 31;
pub const METHOD_QUEUE_DELETE: u16 = 40;
pub const METHOD_QUEUE_DELETE_OK: u16 = 41;

pub const METHOD_BASIC_QOS: u16 = 10;
pub const METHOD_BASIC_QOS_OK: u16 = 11;
pub const METHOD_BASIC_CONSUME: u16 = 20;
pub const METHOD_BASIC_CONSUME_OK: u16 = 21;
pub const METHOD_BASIC_PUBLISH: u16 = 40;
pub const METHOD_BASIC_RETURN: u16 = 50;
pub const METHOD_BASIC_DELIVER: u16 = 60;
pub const METHOD_BASIC_GET: u16 = 70;
pub const METHOD_BASIC_GET_OK: u16 = 71;
pub const METHOD_BASIC_GET_EMPTY: u16 = 72;
pub const METHOD_BASIC_ACK: u16 = 80;
pub const METHOD_BASIC_REJECT: u16 = 90;
pub const METHOD_BASIC_NACK: u16 = 120;

pub const METHOD_CONFIRM_SELECT: u16 = 10;
pub const METHOD_CONFIRM_SELECT_OK: u16 = 11;

pub const Error = wire.Error || error{
    UnknownClass,
    UnknownMethod,
};

// --- Connection Methods ---

pub const ConnectionStart = struct {
    version_major: u8 = 0,
    version_minor: u8 = 9,
    server_properties: []const u8 = &[_]u8{ 0, 0, 0, 0 }, // raw table bytes
    mechanisms: []const u8,
    locales: []const u8,
};

pub const ConnectionStartOk = struct {
    client_properties: []const u8 = &[_]u8{ 0, 0, 0, 0 }, // raw table bytes
    mechanism: []const u8 = "PLAIN",
    response: []const u8,
    locale: []const u8 = "en_US",
};

pub const ConnectionTune = struct {
    channel_max: u16 = 2047,
    frame_max: u32 = 131072,
    heartbeat: u16 = 60,
};

pub const ConnectionTuneOk = struct {
    channel_max: u16,
    frame_max: u32,
    heartbeat: u16,
};

pub const ConnectionOpen = struct {
    virtual_host: []const u8 = "/",
    reserved_1: []const u8 = "",
    reserved_2: bool = false,
};

pub const ConnectionOpenOk = struct {
    reserved_1: []const u8 = "",
};

pub const ConnectionClose = struct {
    reply_code: u16 = 200,
    reply_text: []const u8 = "",
    class_id: u16 = 0,
    method_id: u16 = 0,
};

pub const ConnectionCloseOk = struct {};

pub const ConnectionBlocked = struct {
    reason: []const u8,
};

pub const ConnectionUnblocked = struct {};

// --- Channel Methods ---

pub const ChannelOpen = struct {
    reserved_1: []const u8 = "",
};

pub const ChannelOpenOk = struct {
    reserved_1: []const u8 = "",
};

pub const ChannelClose = struct {
    reply_code: u16 = 200,
    reply_text: []const u8 = "",
    class_id: u16 = 0,
    method_id: u16 = 0,
};

pub const ChannelCloseOk = struct {};

// --- Exchange Methods ---

pub const ExchangeDeclare = struct {
    reserved_1: u16 = 0,
    exchange: []const u8,
    type_name: []const u8 = "direct",
    passive: bool = false,
    durable: bool = true,
    auto_delete: bool = false,
    internal: bool = false,
    no_wait: bool = false,
    arguments: []const u8 = &[_]u8{ 0, 0, 0, 0 }, // raw table
};

pub const ExchangeDeclareOk = struct {};

// --- Queue Methods ---

pub const QueueDeclare = struct {
    reserved_1: u16 = 0,
    queue: []const u8 = "",
    passive: bool = false,
    durable: bool = true,
    exclusive: bool = false,
    auto_delete: bool = false,
    no_wait: bool = false,
    arguments: []const u8 = &[_]u8{ 0, 0, 0, 0 }, // raw table
};

pub const QueueDeclareOk = struct {
    queue: []const u8,
    message_count: u32,
    consumer_count: u32,
};

pub const QueueBind = struct {
    reserved_1: u16 = 0,
    queue: []const u8,
    exchange: []const u8,
    routing_key: []const u8 = "",
    no_wait: bool = false,
    arguments: []const u8 = &[_]u8{ 0, 0, 0, 0 }, // raw table
};

pub const QueueBindOk = struct {};

pub const QueuePurge = struct {
    reserved_1: u16 = 0,
    queue: []const u8,
    no_wait: bool = false,
};

pub const QueuePurgeOk = struct {
    message_count: u32,
};

pub const QueueDelete = struct {
    reserved_1: u16 = 0,
    queue: []const u8,
    if_unused: bool = false,
    if_empty: bool = false,
    no_wait: bool = false,
};

pub const QueueDeleteOk = struct {
    message_count: u32,
};

// --- Basic Methods ---

pub const BasicQos = struct {
    prefetch_size: u32 = 0,
    prefetch_count: u16 = 1,
    global: bool = false,
};

pub const BasicQosOk = struct {};

pub const BasicConsume = struct {
    reserved_1: u16 = 0,
    queue: []const u8,
    consumer_tag: []const u8 = "",
    no_local: bool = false,
    no_ack: bool = false,
    exclusive: bool = false,
    no_wait: bool = false,
    arguments: []const u8 = &[_]u8{ 0, 0, 0, 0 }, // raw table
};

pub const BasicConsumeOk = struct {
    consumer_tag: []const u8,
};

pub const BasicPublish = struct {
    reserved_1: u16 = 0,
    exchange: []const u8 = "",
    routing_key: []const u8 = "",
    mandatory: bool = false,
    immediate: bool = false,
};

pub const BasicDeliver = struct {
    consumer_tag: []const u8,
    delivery_tag: u64,
    redelivered: bool,
    exchange: []const u8,
    routing_key: []const u8,
};

pub const BasicAck = struct {
    delivery_tag: u64,
    multiple: bool = false,
};

pub const BasicReject = struct {
    delivery_tag: u64,
    requeue: bool = true,
};

pub const BasicNack = struct {
    delivery_tag: u64,
    multiple: bool = false,
    requeue: bool = true,
};

pub const BasicReturn = struct {
    reply_code: u16,
    reply_text: []const u8,
    exchange: []const u8,
    routing_key: []const u8,
};

pub const BasicGet = struct {
    reserved_1: u16 = 0,
    queue: []const u8,
    no_ack: bool = false,
};

pub const BasicGetOk = struct {
    delivery_tag: u64,
    redelivered: bool,
    exchange: []const u8,
    routing_key: []const u8,
    message_count: u32,
};

pub const BasicGetEmpty = struct {
    reserved_1: []const u8 = "",
};

// --- Confirm Methods ---

pub const ConfirmSelect = struct {
    nowait: bool = false,
};

pub const ConfirmSelectOk = struct {};

// --- Tagged Union of All Methods ---

pub const Method = union(enum) {
    connection_start: ConnectionStart,
    connection_start_ok: ConnectionStartOk,
    connection_tune: ConnectionTune,
    connection_tune_ok: ConnectionTuneOk,
    connection_open: ConnectionOpen,
    connection_open_ok: ConnectionOpenOk,
    connection_close: ConnectionClose,
    connection_close_ok: ConnectionCloseOk,
    connection_blocked: ConnectionBlocked,
    connection_unblocked: ConnectionUnblocked,

    channel_open: ChannelOpen,
    channel_open_ok: ChannelOpenOk,
    channel_close: ChannelClose,
    channel_close_ok: ChannelCloseOk,

    exchange_declare: ExchangeDeclare,
    exchange_declare_ok: ExchangeDeclareOk,

    queue_declare: QueueDeclare,
    queue_declare_ok: QueueDeclareOk,
    queue_bind: QueueBind,
    queue_bind_ok: QueueBindOk,
    queue_purge: QueuePurge,
    queue_purge_ok: QueuePurgeOk,
    queue_delete: QueueDelete,
    queue_delete_ok: QueueDeleteOk,

    basic_qos: BasicQos,
    basic_qos_ok: BasicQosOk,
    basic_consume: BasicConsume,
    basic_consume_ok: BasicConsumeOk,
    basic_publish: BasicPublish,
    basic_return: BasicReturn,
    basic_deliver: BasicDeliver,
    basic_get: BasicGet,
    basic_get_ok: BasicGetOk,
    basic_get_empty: BasicGetEmpty,
    basic_ack: BasicAck,
    basic_reject: BasicReject,
    basic_nack: BasicNack,

    confirm_select: ConfirmSelect,
    confirm_select_ok: ConfirmSelectOk,
};

/// Encodes any AMQP method into dest buffer (including 2B class ID + 2B method ID).
pub fn encodeMethod(dest: []u8, method: Method) Error!usize {
    if (dest.len < 4) return wire.Error.BufferTooSmall;
    var cursor: usize = 4;

    switch (method) {
        .connection_start => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CONNECTION);
            _ = try wire.writeU16(dest[2..4], METHOD_CONNECTION_START);
            cursor += try wire.writeU8(dest[cursor..], m.version_major);
            cursor += try wire.writeU8(dest[cursor..], m.version_minor);
            if (dest.len < cursor + m.server_properties.len) return wire.Error.BufferTooSmall;
            @memcpy(dest[cursor .. cursor + m.server_properties.len], m.server_properties);
            cursor += m.server_properties.len;
            cursor += try wire.writeLongString(dest[cursor..], m.mechanisms);
            cursor += try wire.writeLongString(dest[cursor..], m.locales);
        },
        .connection_start_ok => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CONNECTION);
            _ = try wire.writeU16(dest[2..4], METHOD_CONNECTION_START_OK);
            if (dest.len < cursor + m.client_properties.len) return wire.Error.BufferTooSmall;
            @memcpy(dest[cursor .. cursor + m.client_properties.len], m.client_properties);
            cursor += m.client_properties.len;
            cursor += try wire.writeShortString(dest[cursor..], m.mechanism);
            cursor += try wire.writeLongString(dest[cursor..], m.response);
            cursor += try wire.writeShortString(dest[cursor..], m.locale);
        },
        .connection_tune => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CONNECTION);
            _ = try wire.writeU16(dest[2..4], METHOD_CONNECTION_TUNE);
            cursor += try wire.writeU16(dest[cursor..], m.channel_max);
            cursor += try wire.writeU32(dest[cursor..], m.frame_max);
            cursor += try wire.writeU16(dest[cursor..], m.heartbeat);
        },
        .connection_tune_ok => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CONNECTION);
            _ = try wire.writeU16(dest[2..4], METHOD_CONNECTION_TUNE_OK);
            cursor += try wire.writeU16(dest[cursor..], m.channel_max);
            cursor += try wire.writeU32(dest[cursor..], m.frame_max);
            cursor += try wire.writeU16(dest[cursor..], m.heartbeat);
        },
        .connection_open => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CONNECTION);
            _ = try wire.writeU16(dest[2..4], METHOD_CONNECTION_OPEN);
            cursor += try wire.writeShortString(dest[cursor..], m.virtual_host);
            cursor += try wire.writeShortString(dest[cursor..], m.reserved_1);
            const bits: u8 = if (m.reserved_2) 1 else 0;
            cursor += try wire.writeU8(dest[cursor..], bits);
        },
        .connection_open_ok => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CONNECTION);
            _ = try wire.writeU16(dest[2..4], METHOD_CONNECTION_OPEN_OK);
            cursor += try wire.writeShortString(dest[cursor..], m.reserved_1);
        },
        .connection_close => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CONNECTION);
            _ = try wire.writeU16(dest[2..4], METHOD_CONNECTION_CLOSE);
            cursor += try wire.writeU16(dest[cursor..], m.reply_code);
            cursor += try wire.writeShortString(dest[cursor..], m.reply_text);
            cursor += try wire.writeU16(dest[cursor..], m.class_id);
            cursor += try wire.writeU16(dest[cursor..], m.method_id);
        },
        .connection_close_ok => {
            _ = try wire.writeU16(dest[0..2], CLASS_CONNECTION);
            _ = try wire.writeU16(dest[2..4], METHOD_CONNECTION_CLOSE_OK);
        },
        .connection_blocked => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CONNECTION);
            _ = try wire.writeU16(dest[2..4], METHOD_CONNECTION_BLOCKED);
            cursor += try wire.writeShortString(dest[cursor..], m.reason);
        },
        .connection_unblocked => {
            _ = try wire.writeU16(dest[0..2], CLASS_CONNECTION);
            _ = try wire.writeU16(dest[2..4], METHOD_CONNECTION_UNBLOCKED);
        },
        .channel_open => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CHANNEL);
            _ = try wire.writeU16(dest[2..4], METHOD_CHANNEL_OPEN);
            cursor += try wire.writeShortString(dest[cursor..], m.reserved_1);
        },
        .channel_open_ok => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CHANNEL);
            _ = try wire.writeU16(dest[2..4], METHOD_CHANNEL_OPEN_OK);
            cursor += try wire.writeLongString(dest[cursor..], m.reserved_1);
        },
        .channel_close => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CHANNEL);
            _ = try wire.writeU16(dest[2..4], METHOD_CHANNEL_CLOSE);
            cursor += try wire.writeU16(dest[cursor..], m.reply_code);
            cursor += try wire.writeShortString(dest[cursor..], m.reply_text);
            cursor += try wire.writeU16(dest[cursor..], m.class_id);
            cursor += try wire.writeU16(dest[cursor..], m.method_id);
        },
        .channel_close_ok => {
            _ = try wire.writeU16(dest[0..2], CLASS_CHANNEL);
            _ = try wire.writeU16(dest[2..4], METHOD_CHANNEL_CLOSE_OK);
        },
        .exchange_declare => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_EXCHANGE);
            _ = try wire.writeU16(dest[2..4], METHOD_EXCHANGE_DECLARE);
            cursor += try wire.writeU16(dest[cursor..], m.reserved_1);
            cursor += try wire.writeShortString(dest[cursor..], m.exchange);
            cursor += try wire.writeShortString(dest[cursor..], m.type_name);
            var bits: u8 = 0;
            if (m.passive) bits |= 1 << 0;
            if (m.durable) bits |= 1 << 1;
            if (m.auto_delete) bits |= 1 << 2;
            if (m.internal) bits |= 1 << 3;
            if (m.no_wait) bits |= 1 << 4;
            cursor += try wire.writeU8(dest[cursor..], bits);
            if (dest.len < cursor + m.arguments.len) return wire.Error.BufferTooSmall;
            @memcpy(dest[cursor .. cursor + m.arguments.len], m.arguments);
            cursor += m.arguments.len;
        },
        .exchange_declare_ok => {
            _ = try wire.writeU16(dest[0..2], CLASS_EXCHANGE);
            _ = try wire.writeU16(dest[2..4], METHOD_EXCHANGE_DECLARE_OK);
        },
        .queue_declare => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_QUEUE);
            _ = try wire.writeU16(dest[2..4], METHOD_QUEUE_DECLARE);
            cursor += try wire.writeU16(dest[cursor..], m.reserved_1);
            cursor += try wire.writeShortString(dest[cursor..], m.queue);
            var bits: u8 = 0;
            if (m.passive) bits |= 1 << 0;
            if (m.durable) bits |= 1 << 1;
            if (m.exclusive) bits |= 1 << 2;
            if (m.auto_delete) bits |= 1 << 3;
            if (m.no_wait) bits |= 1 << 4;
            cursor += try wire.writeU8(dest[cursor..], bits);
            if (dest.len < cursor + m.arguments.len) return wire.Error.BufferTooSmall;
            @memcpy(dest[cursor .. cursor + m.arguments.len], m.arguments);
            cursor += m.arguments.len;
        },
        .queue_declare_ok => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_QUEUE);
            _ = try wire.writeU16(dest[2..4], METHOD_QUEUE_DECLARE_OK);
            cursor += try wire.writeShortString(dest[cursor..], m.queue);
            cursor += try wire.writeU32(dest[cursor..], m.message_count);
            cursor += try wire.writeU32(dest[cursor..], m.consumer_count);
        },
        .queue_bind => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_QUEUE);
            _ = try wire.writeU16(dest[2..4], METHOD_QUEUE_BIND);
            cursor += try wire.writeU16(dest[cursor..], m.reserved_1);
            cursor += try wire.writeShortString(dest[cursor..], m.queue);
            cursor += try wire.writeShortString(dest[cursor..], m.exchange);
            cursor += try wire.writeShortString(dest[cursor..], m.routing_key);
            const bits: u8 = if (m.no_wait) 1 else 0;
            cursor += try wire.writeU8(dest[cursor..], bits);
            if (dest.len < cursor + m.arguments.len) return wire.Error.BufferTooSmall;
            @memcpy(dest[cursor .. cursor + m.arguments.len], m.arguments);
            cursor += m.arguments.len;
        },
        .queue_bind_ok => {
            _ = try wire.writeU16(dest[0..2], CLASS_QUEUE);
            _ = try wire.writeU16(dest[2..4], METHOD_QUEUE_BIND_OK);
        },
        .queue_purge => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_QUEUE);
            _ = try wire.writeU16(dest[2..4], METHOD_QUEUE_PURGE);
            cursor += try wire.writeU16(dest[cursor..], m.reserved_1);
            cursor += try wire.writeShortString(dest[cursor..], m.queue);
            const bits: u8 = if (m.no_wait) 1 else 0;
            cursor += try wire.writeU8(dest[cursor..], bits);
        },
        .queue_purge_ok => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_QUEUE);
            _ = try wire.writeU16(dest[2..4], METHOD_QUEUE_PURGE_OK);
            cursor += try wire.writeU32(dest[cursor..], m.message_count);
        },
        .queue_delete => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_QUEUE);
            _ = try wire.writeU16(dest[2..4], METHOD_QUEUE_DELETE);
            cursor += try wire.writeU16(dest[cursor..], m.reserved_1);
            cursor += try wire.writeShortString(dest[cursor..], m.queue);
            var bits: u8 = 0;
            if (m.if_unused) bits |= 1 << 0;
            if (m.if_empty) bits |= 1 << 1;
            if (m.no_wait) bits |= 1 << 2;
            cursor += try wire.writeU8(dest[cursor..], bits);
        },
        .queue_delete_ok => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_QUEUE);
            _ = try wire.writeU16(dest[2..4], METHOD_QUEUE_DELETE_OK);
            cursor += try wire.writeU32(dest[cursor..], m.message_count);
        },
        .basic_qos => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_QOS);
            cursor += try wire.writeU32(dest[cursor..], m.prefetch_size);
            cursor += try wire.writeU16(dest[cursor..], m.prefetch_count);
            const bits: u8 = if (m.global) 1 else 0;
            cursor += try wire.writeU8(dest[cursor..], bits);
        },
        .basic_qos_ok => {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_QOS_OK);
        },
        .basic_consume => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_CONSUME);
            cursor += try wire.writeU16(dest[cursor..], m.reserved_1);
            cursor += try wire.writeShortString(dest[cursor..], m.queue);
            cursor += try wire.writeShortString(dest[cursor..], m.consumer_tag);
            var bits: u8 = 0;
            if (m.no_local) bits |= 1 << 0;
            if (m.no_ack) bits |= 1 << 1;
            if (m.exclusive) bits |= 1 << 2;
            if (m.no_wait) bits |= 1 << 3;
            cursor += try wire.writeU8(dest[cursor..], bits);
            if (dest.len < cursor + m.arguments.len) return wire.Error.BufferTooSmall;
            @memcpy(dest[cursor .. cursor + m.arguments.len], m.arguments);
            cursor += m.arguments.len;
        },
        .basic_consume_ok => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_CONSUME_OK);
            cursor += try wire.writeShortString(dest[cursor..], m.consumer_tag);
        },
        .basic_publish => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_PUBLISH);
            cursor += try wire.writeU16(dest[cursor..], m.reserved_1);
            cursor += try wire.writeShortString(dest[cursor..], m.exchange);
            cursor += try wire.writeShortString(dest[cursor..], m.routing_key);
            var bits: u8 = 0;
            if (m.mandatory) bits |= 1 << 0;
            if (m.immediate) bits |= 1 << 1;
            cursor += try wire.writeU8(dest[cursor..], bits);
        },
        .basic_return => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_RETURN);
            cursor += try wire.writeU16(dest[cursor..], m.reply_code);
            cursor += try wire.writeShortString(dest[cursor..], m.reply_text);
            cursor += try wire.writeShortString(dest[cursor..], m.exchange);
            cursor += try wire.writeShortString(dest[cursor..], m.routing_key);
        },
        .basic_deliver => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_DELIVER);
            cursor += try wire.writeShortString(dest[cursor..], m.consumer_tag);
            cursor += try wire.writeU64(dest[cursor..], m.delivery_tag);
            const bits: u8 = if (m.redelivered) 1 else 0;
            cursor += try wire.writeU8(dest[cursor..], bits);
            cursor += try wire.writeShortString(dest[cursor..], m.exchange);
            cursor += try wire.writeShortString(dest[cursor..], m.routing_key);
        },
        .basic_get => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_GET);
            cursor += try wire.writeU16(dest[cursor..], m.reserved_1);
            cursor += try wire.writeShortString(dest[cursor..], m.queue);
            const bits: u8 = if (m.no_ack) 1 else 0;
            cursor += try wire.writeU8(dest[cursor..], bits);
        },
        .basic_get_ok => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_GET_OK);
            cursor += try wire.writeU64(dest[cursor..], m.delivery_tag);
            const bits: u8 = if (m.redelivered) 1 else 0;
            cursor += try wire.writeU8(dest[cursor..], bits);
            cursor += try wire.writeShortString(dest[cursor..], m.exchange);
            cursor += try wire.writeShortString(dest[cursor..], m.routing_key);
            cursor += try wire.writeU32(dest[cursor..], m.message_count);
        },
        .basic_get_empty => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_GET_EMPTY);
            cursor += try wire.writeShortString(dest[cursor..], m.reserved_1);
        },
        .basic_ack => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_ACK);
            cursor += try wire.writeU64(dest[cursor..], m.delivery_tag);
            const bits: u8 = if (m.multiple) 1 else 0;
            cursor += try wire.writeU8(dest[cursor..], bits);
        },
        .basic_reject => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_REJECT);
            cursor += try wire.writeU64(dest[cursor..], m.delivery_tag);
            const bits: u8 = if (m.requeue) 1 else 0;
            cursor += try wire.writeU8(dest[cursor..], bits);
        },
        .basic_nack => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_BASIC);
            _ = try wire.writeU16(dest[2..4], METHOD_BASIC_NACK);
            cursor += try wire.writeU64(dest[cursor..], m.delivery_tag);
            var bits: u8 = 0;
            if (m.multiple) bits |= 1 << 0;
            if (m.requeue) bits |= 1 << 1;
            cursor += try wire.writeU8(dest[cursor..], bits);
        },
        .confirm_select => |m| {
            _ = try wire.writeU16(dest[0..2], CLASS_CONFIRM);
            _ = try wire.writeU16(dest[2..4], METHOD_CONFIRM_SELECT);
            const bits: u8 = if (m.nowait) 1 else 0;
            cursor += try wire.writeU8(dest[cursor..], bits);
        },
        .confirm_select_ok => {
            _ = try wire.writeU16(dest[0..2], CLASS_CONFIRM);
            _ = try wire.writeU16(dest[2..4], METHOD_CONFIRM_SELECT_OK);
        },
    }

    return cursor;
}

/// Decodes an AMQP method from raw payload bytes (beginning with class_id and method_id).
pub fn decodeMethod(bytes: []const u8) Error!Method {
    if (bytes.len < 4) return wire.Error.UnexpectedEof;
    const class_id = try wire.readU16(bytes[0..2]);
    const method_id = try wire.readU16(bytes[2..4]);
    var cursor: usize = 4;

    switch (class_id) {
        CLASS_CONNECTION => switch (method_id) {
            METHOD_CONNECTION_START => {
                const major = try wire.readU8(bytes[cursor..]);
                cursor += 1;
                const minor = try wire.readU8(bytes[cursor..]);
                cursor += 1;
                if (bytes.len < cursor + 4) return wire.Error.UnexpectedEof;
                const table_len = @as(usize, @intCast(try wire.readU32(bytes[cursor .. cursor + 4])));
                const total_table = 4 + table_len;
                if (bytes.len < cursor + total_table) return wire.Error.UnexpectedEof;
                const table_slice = bytes[cursor .. cursor + total_table];
                cursor += total_table;
                const mech = try wire.readLongString(bytes[cursor..]);
                cursor += mech.consumed;
                const loc = try wire.readLongString(bytes[cursor..]);
                return .{ .connection_start = .{
                    .version_major = major,
                    .version_minor = minor,
                    .server_properties = table_slice,
                    .mechanisms = mech.str,
                    .locales = loc.str,
                } };
            },
            METHOD_CONNECTION_START_OK => {
                if (bytes.len < cursor + 4) return wire.Error.UnexpectedEof;
                const table_len = @as(usize, @intCast(try wire.readU32(bytes[cursor .. cursor + 4])));
                const total_table = 4 + table_len;
                if (bytes.len < cursor + total_table) return wire.Error.UnexpectedEof;
                const table_slice = bytes[cursor .. cursor + total_table];
                cursor += total_table;
                const mech = try wire.readShortString(bytes[cursor..]);
                cursor += mech.consumed;
                const resp = try wire.readLongString(bytes[cursor..]);
                cursor += resp.consumed;
                const loc = try wire.readShortString(bytes[cursor..]);
                return .{ .connection_start_ok = .{
                    .client_properties = table_slice,
                    .mechanism = mech.str,
                    .response = resp.str,
                    .locale = loc.str,
                } };
            },
            METHOD_CONNECTION_TUNE => {
                const ch_max = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const fr_max = try wire.readU32(bytes[cursor..]);
                cursor += 4;
                const hb = try wire.readU16(bytes[cursor..]);
                return .{ .connection_tune = .{
                    .channel_max = ch_max,
                    .frame_max = fr_max,
                    .heartbeat = hb,
                } };
            },
            METHOD_CONNECTION_TUNE_OK => {
                const ch_max = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const fr_max = try wire.readU32(bytes[cursor..]);
                cursor += 4;
                const hb = try wire.readU16(bytes[cursor..]);
                return .{ .connection_tune_ok = .{
                    .channel_max = ch_max,
                    .frame_max = fr_max,
                    .heartbeat = hb,
                } };
            },
            METHOD_CONNECTION_OPEN => {
                const vh = try wire.readShortString(bytes[cursor..]);
                cursor += vh.consumed;
                const r1 = try wire.readShortString(bytes[cursor..]);
                cursor += r1.consumed;
                const b = try wire.readU8(bytes[cursor..]);
                return .{ .connection_open = .{
                    .virtual_host = vh.str,
                    .reserved_1 = r1.str,
                    .reserved_2 = (b & 1) != 0,
                } };
            },
            METHOD_CONNECTION_OPEN_OK => {
                const r1 = try wire.readShortString(bytes[cursor..]);
                return .{ .connection_open_ok = .{ .reserved_1 = r1.str } };
            },
            METHOD_CONNECTION_CLOSE => {
                const code = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const text = try wire.readShortString(bytes[cursor..]);
                cursor += text.consumed;
                const cid = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const mid = try wire.readU16(bytes[cursor..]);
                return .{ .connection_close = .{
                    .reply_code = code,
                    .reply_text = text.str,
                    .class_id = cid,
                    .method_id = mid,
                } };
            },
            METHOD_CONNECTION_CLOSE_OK => {
                return .{ .connection_close_ok = .{} };
            },
            METHOD_CONNECTION_BLOCKED => {
                const reason = try wire.readShortString(bytes[cursor..]);
                return .{ .connection_blocked = .{ .reason = reason.str } };
            },
            METHOD_CONNECTION_UNBLOCKED => {
                return .{ .connection_unblocked = .{} };
            },
            else => return Error.UnknownMethod,
        },
        CLASS_CHANNEL => switch (method_id) {
            METHOD_CHANNEL_OPEN => {
                const r1 = try wire.readShortString(bytes[cursor..]);
                return .{ .channel_open = .{ .reserved_1 = r1.str } };
            },
            METHOD_CHANNEL_OPEN_OK => {
                const r1 = try wire.readLongString(bytes[cursor..]);
                return .{ .channel_open_ok = .{ .reserved_1 = r1.str } };
            },
            METHOD_CHANNEL_CLOSE => {
                const code = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const text = try wire.readShortString(bytes[cursor..]);
                cursor += text.consumed;
                const cid = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const mid = try wire.readU16(bytes[cursor..]);
                return .{ .channel_close = .{
                    .reply_code = code,
                    .reply_text = text.str,
                    .class_id = cid,
                    .method_id = mid,
                } };
            },
            METHOD_CHANNEL_CLOSE_OK => {
                return .{ .channel_close_ok = .{} };
            },
            else => return Error.UnknownMethod,
        },
        CLASS_EXCHANGE => switch (method_id) {
            METHOD_EXCHANGE_DECLARE => {
                const r1 = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const ex = try wire.readShortString(bytes[cursor..]);
                cursor += ex.consumed;
                const tn = try wire.readShortString(bytes[cursor..]);
                cursor += tn.consumed;
                const bits = try wire.readU8(bytes[cursor..]);
                cursor += 1;
                if (bytes.len < cursor + 4) return wire.Error.UnexpectedEof;
                const tlen = @as(usize, @intCast(try wire.readU32(bytes[cursor .. cursor + 4])));
                const total_t = 4 + tlen;
                if (bytes.len < cursor + total_t) return wire.Error.UnexpectedEof;
                const args = bytes[cursor .. cursor + total_t];
                return .{ .exchange_declare = .{
                    .reserved_1 = r1,
                    .exchange = ex.str,
                    .type_name = tn.str,
                    .passive = (bits & (1 << 0)) != 0,
                    .durable = (bits & (1 << 1)) != 0,
                    .auto_delete = (bits & (1 << 2)) != 0,
                    .internal = (bits & (1 << 3)) != 0,
                    .no_wait = (bits & (1 << 4)) != 0,
                    .arguments = args,
                } };
            },
            METHOD_EXCHANGE_DECLARE_OK => {
                return .{ .exchange_declare_ok = .{} };
            },
            else => return Error.UnknownMethod,
        },
        CLASS_QUEUE => switch (method_id) {
            METHOD_QUEUE_DECLARE => {
                const r1 = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const q = try wire.readShortString(bytes[cursor..]);
                cursor += q.consumed;
                const bits = try wire.readU8(bytes[cursor..]);
                cursor += 1;
                if (bytes.len < cursor + 4) return wire.Error.UnexpectedEof;
                const tlen = @as(usize, @intCast(try wire.readU32(bytes[cursor .. cursor + 4])));
                const total_t = 4 + tlen;
                if (bytes.len < cursor + total_t) return wire.Error.UnexpectedEof;
                const args = bytes[cursor .. cursor + total_t];
                return .{ .queue_declare = .{
                    .reserved_1 = r1,
                    .queue = q.str,
                    .passive = (bits & (1 << 0)) != 0,
                    .durable = (bits & (1 << 1)) != 0,
                    .exclusive = (bits & (1 << 2)) != 0,
                    .auto_delete = (bits & (1 << 3)) != 0,
                    .no_wait = (bits & (1 << 4)) != 0,
                    .arguments = args,
                } };
            },
            METHOD_QUEUE_DECLARE_OK => {
                const q = try wire.readShortString(bytes[cursor..]);
                cursor += q.consumed;
                const mc = try wire.readU32(bytes[cursor..]);
                cursor += 4;
                const cc = try wire.readU32(bytes[cursor..]);
                return .{ .queue_declare_ok = .{
                    .queue = q.str,
                    .message_count = mc,
                    .consumer_count = cc,
                } };
            },
            METHOD_QUEUE_BIND => {
                const r1 = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const q = try wire.readShortString(bytes[cursor..]);
                cursor += q.consumed;
                const ex = try wire.readShortString(bytes[cursor..]);
                cursor += ex.consumed;
                const rk = try wire.readShortString(bytes[cursor..]);
                cursor += rk.consumed;
                const bits = try wire.readU8(bytes[cursor..]);
                cursor += 1;
                if (bytes.len < cursor + 4) return wire.Error.UnexpectedEof;
                const tlen = @as(usize, @intCast(try wire.readU32(bytes[cursor .. cursor + 4])));
                const total_t = 4 + tlen;
                if (bytes.len < cursor + total_t) return wire.Error.UnexpectedEof;
                const args = bytes[cursor .. cursor + total_t];
                return .{ .queue_bind = .{
                    .reserved_1 = r1,
                    .queue = q.str,
                    .exchange = ex.str,
                    .routing_key = rk.str,
                    .no_wait = (bits & (1 << 0)) != 0,
                    .arguments = args,
                } };
            },
            METHOD_QUEUE_BIND_OK => {
                return .{ .queue_bind_ok = .{} };
            },
            METHOD_QUEUE_PURGE => {
                const r1 = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const q = try wire.readShortString(bytes[cursor..]);
                cursor += q.consumed;
                const bits = try wire.readU8(bytes[cursor..]);
                return .{ .queue_purge = .{
                    .reserved_1 = r1,
                    .queue = q.str,
                    .no_wait = (bits & (1 << 0)) != 0,
                } };
            },
            METHOD_QUEUE_PURGE_OK => {
                const mc = try wire.readU32(bytes[cursor..]);
                return .{ .queue_purge_ok = .{ .message_count = mc } };
            },
            METHOD_QUEUE_DELETE => {
                const r1 = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const q = try wire.readShortString(bytes[cursor..]);
                cursor += q.consumed;
                const bits = try wire.readU8(bytes[cursor..]);
                return .{ .queue_delete = .{
                    .reserved_1 = r1,
                    .queue = q.str,
                    .if_unused = (bits & (1 << 0)) != 0,
                    .if_empty = (bits & (1 << 1)) != 0,
                    .no_wait = (bits & (1 << 2)) != 0,
                } };
            },
            METHOD_QUEUE_DELETE_OK => {
                const mc = try wire.readU32(bytes[cursor..]);
                return .{ .queue_delete_ok = .{ .message_count = mc } };
            },
            else => return Error.UnknownMethod,
        },
        CLASS_BASIC => switch (method_id) {
            METHOD_BASIC_QOS => {
                const ps = try wire.readU32(bytes[cursor..]);
                cursor += 4;
                const pc = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const bits = try wire.readU8(bytes[cursor..]);
                return .{ .basic_qos = .{
                    .prefetch_size = ps,
                    .prefetch_count = pc,
                    .global = (bits & 1) != 0,
                } };
            },
            METHOD_BASIC_QOS_OK => {
                return .{ .basic_qos_ok = .{} };
            },
            METHOD_BASIC_CONSUME => {
                const r1 = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const q = try wire.readShortString(bytes[cursor..]);
                cursor += q.consumed;
                const ctag = try wire.readShortString(bytes[cursor..]);
                cursor += ctag.consumed;
                const bits = try wire.readU8(bytes[cursor..]);
                cursor += 1;
                if (bytes.len < cursor + 4) return wire.Error.UnexpectedEof;
                const tlen = @as(usize, @intCast(try wire.readU32(bytes[cursor .. cursor + 4])));
                const total_t = 4 + tlen;
                if (bytes.len < cursor + total_t) return wire.Error.UnexpectedEof;
                const args = bytes[cursor .. cursor + total_t];
                return .{ .basic_consume = .{
                    .reserved_1 = r1,
                    .queue = q.str,
                    .consumer_tag = ctag.str,
                    .no_local = (bits & (1 << 0)) != 0,
                    .no_ack = (bits & (1 << 1)) != 0,
                    .exclusive = (bits & (1 << 2)) != 0,
                    .no_wait = (bits & (1 << 3)) != 0,
                    .arguments = args,
                } };
            },
            METHOD_BASIC_CONSUME_OK => {
                const ctag = try wire.readShortString(bytes[cursor..]);
                return .{ .basic_consume_ok = .{ .consumer_tag = ctag.str } };
            },
            METHOD_BASIC_PUBLISH => {
                const r1 = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const ex = try wire.readShortString(bytes[cursor..]);
                cursor += ex.consumed;
                const rk = try wire.readShortString(bytes[cursor..]);
                cursor += rk.consumed;
                const bits = try wire.readU8(bytes[cursor..]);
                return .{ .basic_publish = .{
                    .reserved_1 = r1,
                    .exchange = ex.str,
                    .routing_key = rk.str,
                    .mandatory = (bits & (1 << 0)) != 0,
                    .immediate = (bits & (1 << 1)) != 0,
                } };
            },
            METHOD_BASIC_RETURN => {
                const code = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const text = try wire.readShortString(bytes[cursor..]);
                cursor += text.consumed;
                const ex = try wire.readShortString(bytes[cursor..]);
                cursor += ex.consumed;
                const rk = try wire.readShortString(bytes[cursor..]);
                return .{ .basic_return = .{
                    .reply_code = code,
                    .reply_text = text.str,
                    .exchange = ex.str,
                    .routing_key = rk.str,
                } };
            },
            METHOD_BASIC_DELIVER => {
                const ctag = try wire.readShortString(bytes[cursor..]);
                cursor += ctag.consumed;
                const dtag = try wire.readU64(bytes[cursor..]);
                cursor += 8;
                const bits = try wire.readU8(bytes[cursor..]);
                cursor += 1;
                const ex = try wire.readShortString(bytes[cursor..]);
                cursor += ex.consumed;
                const rk = try wire.readShortString(bytes[cursor..]);
                return .{ .basic_deliver = .{
                    .consumer_tag = ctag.str,
                    .delivery_tag = dtag,
                    .redelivered = (bits & 1) != 0,
                    .exchange = ex.str,
                    .routing_key = rk.str,
                } };
            },
            METHOD_BASIC_GET => {
                const r1 = try wire.readU16(bytes[cursor..]);
                cursor += 2;
                const q = try wire.readShortString(bytes[cursor..]);
                cursor += q.consumed;
                const bits = try wire.readU8(bytes[cursor..]);
                return .{ .basic_get = .{
                    .reserved_1 = r1,
                    .queue = q.str,
                    .no_ack = (bits & 1) != 0,
                } };
            },
            METHOD_BASIC_GET_OK => {
                const dtag = try wire.readU64(bytes[cursor..]);
                cursor += 8;
                const bits = try wire.readU8(bytes[cursor..]);
                cursor += 1;
                const ex = try wire.readShortString(bytes[cursor..]);
                cursor += ex.consumed;
                const rk = try wire.readShortString(bytes[cursor..]);
                cursor += rk.consumed;
                const mc = try wire.readU32(bytes[cursor..]);
                return .{ .basic_get_ok = .{
                    .delivery_tag = dtag,
                    .redelivered = (bits & 1) != 0,
                    .exchange = ex.str,
                    .routing_key = rk.str,
                    .message_count = mc,
                } };
            },
            METHOD_BASIC_GET_EMPTY => {
                const r1 = try wire.readShortString(bytes[cursor..]);
                return .{ .basic_get_empty = .{ .reserved_1 = r1.str } };
            },
            METHOD_BASIC_ACK => {
                const dtag = try wire.readU64(bytes[cursor..]);
                cursor += 8;
                const bits = try wire.readU8(bytes[cursor..]);
                return .{ .basic_ack = .{
                    .delivery_tag = dtag,
                    .multiple = (bits & 1) != 0,
                } };
            },
            METHOD_BASIC_REJECT => {
                const dtag = try wire.readU64(bytes[cursor..]);
                cursor += 8;
                const bits = try wire.readU8(bytes[cursor..]);
                return .{ .basic_reject = .{
                    .delivery_tag = dtag,
                    .requeue = (bits & 1) != 0,
                } };
            },
            METHOD_BASIC_NACK => {
                const dtag = try wire.readU64(bytes[cursor..]);
                cursor += 8;
                const bits = try wire.readU8(bytes[cursor..]);
                return .{ .basic_nack = .{
                    .delivery_tag = dtag,
                    .multiple = (bits & (1 << 0)) != 0,
                    .requeue = (bits & (1 << 1)) != 0,
                } };
            },
            else => return Error.UnknownMethod,
        },
        CLASS_CONFIRM => switch (method_id) {
            METHOD_CONFIRM_SELECT => {
                const bits = try wire.readU8(bytes[cursor..]);
                return .{ .confirm_select = .{ .nowait = (bits & 1) != 0 } };
            },
            METHOD_CONFIRM_SELECT_OK => {
                return .{ .confirm_select_ok = .{} };
            },
            else => return Error.UnknownMethod,
        },
        else => return Error.UnknownClass,
    }
}

test "connection start and start_ok encode and decode" {
    var buf: [256]u8 = undefined;

    const start_method = Method{
        .connection_start = .{
            .version_major = 0,
            .version_minor = 9,
            .server_properties = &[_]u8{ 0, 0, 0, 0 },
            .mechanisms = "PLAIN AMQPLAIN",
            .locales = "en_US",
        },
    };

    const written = try encodeMethod(&buf, start_method);
    const decoded = try decodeMethod(buf[0..written]);
    switch (decoded) {
        .connection_start => |s| {
            try std.testing.expectEqual(@as(u8, 0), s.version_major);
            try std.testing.expectEqual(@as(u8, 9), s.version_minor);
            try std.testing.expectEqualStrings("PLAIN AMQPLAIN", s.mechanisms);
            try std.testing.expectEqualStrings("en_US", s.locales);
        },
        else => return error.UnexpectedMethod,
    }

    const start_ok_method = Method{
        .connection_start_ok = .{
            .client_properties = &[_]u8{ 0, 0, 0, 0 },
            .mechanism = "PLAIN",
            .response = "\x00guest\x00guest",
            .locale = "en_US",
        },
    };

    const written_ok = try encodeMethod(&buf, start_ok_method);
    const decoded_ok = try decodeMethod(buf[0..written_ok]);
    switch (decoded_ok) {
        .connection_start_ok => |ok| {
            try std.testing.expectEqualStrings("PLAIN", ok.mechanism);
            try std.testing.expectEqualStrings("\x00guest\x00guest", ok.response);
            try std.testing.expectEqualStrings("en_US", ok.locale);
        },
        else => return error.UnexpectedMethod,
    }
}

test "queue declare and basic publish roundtrip" {
    var buf: [256]u8 = undefined;

    const q_declare = Method{
        .queue_declare = .{
            .queue = "telemetry-queue",
            .passive = false,
            .durable = true,
            .exclusive = false,
            .auto_delete = false,
            .no_wait = false,
        },
    };
    const written_q = try encodeMethod(&buf, q_declare);
    const dec_q = try decodeMethod(buf[0..written_q]);
    switch (dec_q) {
        .queue_declare => |q| {
            try std.testing.expectEqualStrings("telemetry-queue", q.queue);
            try std.testing.expect(q.durable);
            try std.testing.expect(!q.exclusive);
        },
        else => return error.UnexpectedMethod,
    }

    const pub_method = Method{
        .basic_publish = .{
            .exchange = "amq.direct",
            .routing_key = "lead.scrubbed",
            .mandatory = true,
            .immediate = false,
        },
    };
    const written_pub = try encodeMethod(&buf, pub_method);
    const dec_pub = try decodeMethod(buf[0..written_pub]);
    switch (dec_pub) {
        .basic_publish => |p| {
            try std.testing.expectEqualStrings("amq.direct", p.exchange);
            try std.testing.expectEqualStrings("lead.scrubbed", p.routing_key);
            try std.testing.expect(p.mandatory);
            try std.testing.expect(!p.immediate);
        },
        else => return error.UnexpectedMethod,
    }
}

test "confirm select and basic ack / nack roundtrip" {
    var buf: [64]u8 = undefined;

    const confirm = Method{ .confirm_select = .{ .nowait = false } };
    const w_conf = try encodeMethod(&buf, confirm);
    const dec_conf = try decodeMethod(buf[0..w_conf]);
    switch (dec_conf) {
        .confirm_select => |c| try std.testing.expect(!c.nowait),
        else => return error.UnexpectedMethod,
    }

    const ack = Method{ .basic_ack = .{ .delivery_tag = 42, .multiple = true } };
    const w_ack = try encodeMethod(&buf, ack);
    const dec_ack = try decodeMethod(buf[0..w_ack]);
    switch (dec_ack) {
        .basic_ack => |a| {
            try std.testing.expectEqual(@as(u64, 42), a.delivery_tag);
            try std.testing.expect(a.multiple);
        },
        else => return error.UnexpectedMethod,
    }

    const nack = Method{ .basic_nack = .{ .delivery_tag = 100, .multiple = false, .requeue = true } };
    const w_nack = try encodeMethod(&buf, nack);
    const dec_nack = try decodeMethod(buf[0..w_nack]);
    switch (dec_nack) {
        .basic_nack => |n| {
            try std.testing.expectEqual(@as(u64, 100), n.delivery_tag);
            try std.testing.expect(!n.multiple);
            try std.testing.expect(n.requeue);
        },
        else => return error.UnexpectedMethod,
    }
}

test "channel and connection lifecycle methods roundtrip" {
    var buf: [256]u8 = undefined;

    // Connection.Tune
    const tune = Method{ .connection_tune = .{ .channel_max = 100, .frame_max = 65536, .heartbeat = 30 } };
    const w_tune = try encodeMethod(&buf, tune);
    const dec_tune = try decodeMethod(buf[0..w_tune]);
    switch (dec_tune) {
        .connection_tune => |t| {
            try std.testing.expectEqual(@as(u16, 100), t.channel_max);
            try std.testing.expectEqual(@as(u32, 65536), t.frame_max);
            try std.testing.expectEqual(@as(u16, 30), t.heartbeat);
        },
        else => return error.UnexpectedMethod,
    }

    // Connection.Open
    const open = Method{ .connection_open = .{ .virtual_host = "test-vhost" } };
    const w_open = try encodeMethod(&buf, open);
    const dec_open = try decodeMethod(buf[0..w_open]);
    switch (dec_open) {
        .connection_open => |o| try std.testing.expectEqualStrings("test-vhost", o.virtual_host),
        else => return error.UnexpectedMethod,
    }

    // Connection.Close
    const close = Method{ .connection_close = .{ .reply_code = 320, .reply_text = "CONNECTION_FORCED", .class_id = 10, .method_id = 40 } };
    const w_close = try encodeMethod(&buf, close);
    const dec_close = try decodeMethod(buf[0..w_close]);
    switch (dec_close) {
        .connection_close => |c| {
            try std.testing.expectEqual(@as(u16, 320), c.reply_code);
            try std.testing.expectEqualStrings("CONNECTION_FORCED", c.reply_text);
        },
        else => return error.UnexpectedMethod,
    }

    // Channel.Open
    const ch_open = Method{ .channel_open = .{} };
    const w_cho = try encodeMethod(&buf, ch_open);
    const dec_cho = try decodeMethod(buf[0..w_cho]);
    switch (dec_cho) {
        .channel_open => |co| try std.testing.expectEqualStrings("", co.reserved_1),
        else => return error.UnexpectedMethod,
    }

    // Channel.Close
    const ch_close = Method{ .channel_close = .{ .reply_code = 404, .reply_text = "NOT_FOUND", .class_id = 50, .method_id = 10 } };
    const w_chc = try encodeMethod(&buf, ch_close);
    const dec_chc = try decodeMethod(buf[0..w_chc]);
    switch (dec_chc) {
        .channel_close => |cc| {
            try std.testing.expectEqual(@as(u16, 404), cc.reply_code);
            try std.testing.expectEqualStrings("NOT_FOUND", cc.reply_text);
        },
        else => return error.UnexpectedMethod,
    }
}

test "exchange declare, queue bind, purge and delete roundtrip" {
    var buf: [256]u8 = undefined;

    // Exchange.Declare
    const ex_dec = Method{
        .exchange_declare = .{
            .exchange = "events-topic",
            .type_name = "topic",
            .durable = true,
            .auto_delete = false,
        },
    };
    const w_ex = try encodeMethod(&buf, ex_dec);
    const dec_ex = try decodeMethod(buf[0..w_ex]);
    switch (dec_ex) {
        .exchange_declare => |e| {
            try std.testing.expectEqualStrings("events-topic", e.exchange);
            try std.testing.expectEqualStrings("topic", e.type_name);
            try std.testing.expect(e.durable);
        },
        else => return error.UnexpectedMethod,
    }

    // Queue.Bind
    const q_bind = Method{
        .queue_bind = .{
            .queue = "events-q",
            .exchange = "events-topic",
            .routing_key = "lead.*",
        },
    };
    const w_qb = try encodeMethod(&buf, q_bind);
    const dec_qb = try decodeMethod(buf[0..w_qb]);
    switch (dec_qb) {
        .queue_bind => |b| {
            try std.testing.expectEqualStrings("events-q", b.queue);
            try std.testing.expectEqualStrings("events-topic", b.exchange);
            try std.testing.expectEqualStrings("lead.*", b.routing_key);
        },
        else => return error.UnexpectedMethod,
    }

    // Queue.Purge
    const q_purge = Method{ .queue_purge = .{ .queue = "events-q" } };
    const w_qp = try encodeMethod(&buf, q_purge);
    const dec_qp = try decodeMethod(buf[0..w_qp]);
    switch (dec_qp) {
        .queue_purge => |p| try std.testing.expectEqualStrings("events-q", p.queue),
        else => return error.UnexpectedMethod,
    }

    // Queue.Delete
    const q_del = Method{ .queue_delete = .{ .queue = "events-q", .if_unused = true } };
    const w_qd = try encodeMethod(&buf, q_del);
    const dec_qd = try decodeMethod(buf[0..w_qd]);
    switch (dec_qd) {
        .queue_delete => |d| {
            try std.testing.expectEqualStrings("events-q", d.queue);
            try std.testing.expect(d.if_unused);
        },
        else => return error.UnexpectedMethod,
    }
}

test "basic qos, consume, deliver and reject roundtrip" {
    var buf: [256]u8 = undefined;

    // Basic.Qos
    const qos = Method{ .basic_qos = .{ .prefetch_size = 0, .prefetch_count = 50, .global = false } };
    const w_qos = try encodeMethod(&buf, qos);
    const dec_qos = try decodeMethod(buf[0..w_qos]);
    switch (dec_qos) {
        .basic_qos => |q| {
            try std.testing.expectEqual(@as(u16, 50), q.prefetch_count);
            try std.testing.expect(!q.global);
        },
        else => return error.UnexpectedMethod,
    }

    // Basic.Consume
    const consume = Method{
        .basic_consume = .{
            .queue = "events-q",
            .consumer_tag = "ctag-1",
            .no_ack = false,
            .exclusive = true,
        },
    };
    const w_c = try encodeMethod(&buf, consume);
    const dec_c = try decodeMethod(buf[0..w_c]);
    switch (dec_c) {
        .basic_consume => |c| {
            try std.testing.expectEqualStrings("events-q", c.queue);
            try std.testing.expectEqualStrings("ctag-1", c.consumer_tag);
            try std.testing.expect(!c.no_ack);
            try std.testing.expect(c.exclusive);
        },
        else => return error.UnexpectedMethod,
    }

    // Basic.Deliver
    const deliver = Method{
        .basic_deliver = .{
            .consumer_tag = "ctag-1",
            .delivery_tag = 999,
            .redelivered = true,
            .exchange = "events-topic",
            .routing_key = "lead.created",
        },
    };
    const w_d = try encodeMethod(&buf, deliver);
    const dec_d = try decodeMethod(buf[0..w_d]);
    switch (dec_d) {
        .basic_deliver => |d| {
            try std.testing.expectEqualStrings("ctag-1", d.consumer_tag);
            try std.testing.expectEqual(@as(u64, 999), d.delivery_tag);
            try std.testing.expect(d.redelivered);
            try std.testing.expectEqualStrings("events-topic", d.exchange);
            try std.testing.expectEqualStrings("lead.created", d.routing_key);
        },
        else => return error.UnexpectedMethod,
    }

    // Basic.Reject
    const reject = Method{ .basic_reject = .{ .delivery_tag = 999, .requeue = false } };
    const w_rej = try encodeMethod(&buf, reject);
    const dec_rej = try decodeMethod(buf[0..w_rej]);
    switch (dec_rej) {
        .basic_reject => |r| {
            try std.testing.expectEqual(@as(u64, 999), r.delivery_tag);
            try std.testing.expect(!r.requeue);
        },
        else => return error.UnexpectedMethod,
    }

    // Basic.Return
    const ret = Method{ .basic_return = .{
        .reply_code = 312,
        .reply_text = "NO_ROUTE",
        .exchange = "ex1",
        .routing_key = "rk1",
    } };
    const w_ret = try encodeMethod(&buf, ret);
    const dec_ret = try decodeMethod(buf[0..w_ret]);
    switch (dec_ret) {
        .basic_return => |r| {
            try std.testing.expectEqual(@as(u16, 312), r.reply_code);
            try std.testing.expectEqualStrings("NO_ROUTE", r.reply_text);
            try std.testing.expectEqualStrings("ex1", r.exchange);
            try std.testing.expectEqualStrings("rk1", r.routing_key);
        },
        else => return error.UnexpectedMethod,
    }

    // Basic.Get & GetOk & GetEmpty
    const get_m = Method{ .basic_get = .{ .queue = "q1", .no_ack = true } };
    const w_g = try encodeMethod(&buf, get_m);
    const dec_g = try decodeMethod(buf[0..w_g]);
    switch (dec_g) {
        .basic_get => |g| {
            try std.testing.expectEqualStrings("q1", g.queue);
            try std.testing.expect(g.no_ack);
        },
        else => return error.UnexpectedMethod,
    }

    const get_ok_m = Method{ .basic_get_ok = .{
        .delivery_tag = 42,
        .redelivered = false,
        .exchange = "ex2",
        .routing_key = "rk2",
        .message_count = 5,
    } };
    const w_gok = try encodeMethod(&buf, get_ok_m);
    const dec_gok = try decodeMethod(buf[0..w_gok]);
    switch (dec_gok) {
        .basic_get_ok => |gok| {
            try std.testing.expectEqual(@as(u64, 42), gok.delivery_tag);
            try std.testing.expect(!gok.redelivered);
            try std.testing.expectEqualStrings("ex2", gok.exchange);
            try std.testing.expectEqualStrings("rk2", gok.routing_key);
            try std.testing.expectEqual(@as(u32, 5), gok.message_count);
        },
        else => return error.UnexpectedMethod,
    }

    const get_emp = Method{ .basic_get_empty = .{} };
    const w_gemp = try encodeMethod(&buf, get_emp);
    const dec_gemp = try decodeMethod(buf[0..w_gemp]);
    switch (dec_gemp) {
        .basic_get_empty => {},
        else => return error.UnexpectedMethod,
    }

    // Connection.Blocked & Unblocked
    const blocked_m = Method{ .connection_blocked = .{ .reason = "low memory" } };
    const w_blk = try encodeMethod(&buf, blocked_m);
    const dec_blk = try decodeMethod(buf[0..w_blk]);
    switch (dec_blk) {
        .connection_blocked => |b| try std.testing.expectEqualStrings("low memory", b.reason),
        else => return error.UnexpectedMethod,
    }

    const unblk_m = Method{ .connection_unblocked = .{} };
    const w_unblk = try encodeMethod(&buf, unblk_m);
    const dec_unblk = try decodeMethod(buf[0..w_unblk]);
    switch (dec_unblk) {
        .connection_unblocked => {},
        else => return error.UnexpectedMethod,
    }
}
