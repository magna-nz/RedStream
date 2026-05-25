# RedStream — Specification

## 1. What RedStream gives you

A focused .NET library that turns Redis Streams from a low-level log into a production-ready message queue. You keep using `StackExchange.Redis` for the connection; RedStream adds the operational layer on top.

In plain English, you get:

- **Typed producer and consumer** — publish strongly-typed messages, handle them with `IMessageHandler<T>`. No `NameValueEntry[]` plumbing.
- **Hosted consumers** — register handlers via DI; RedStream runs them as `IHostedService` with proper graceful shutdown.
- **Automatic crash recovery** — if a consumer dies mid-message, a background reaper claims its in-flight work and redelivers it. No manually written `XAUTOCLAIM` loop.
- **Dead-letter queue** — poison messages move atomically to a DLQ stream after N attempts, with full error context and replay tooling.
- **No-double-process protection** — built-in middleware deduplicates redelivered messages on the consumer side; on Redis 8.6+ the producer also opts into native `IDMPAUTO` so duplicate publishes never reach the stream.
- **OpenTelemetry built in** — traces propagate from producer to consumer via the message envelope; metrics for lag, throughput, PEL depth, DLQ count.
- **In-memory test fake** — unit-test your handlers and reaper logic without spinning up a real Redis.

The pitch in one sentence: **Redis Streams with the production-readiness checklist already filled in — drop into any ASP.NET Core or worker app, no framework lock-in.**

## 2. Quickstart

```csharp
// Program.cs
var connection = await ConnectionMultiplexer.ConnectAsync("localhost:6379");

builder.Services.AddRedStream(connection)
    .AddProducer<OrderPlaced>("orders")
    .AddConsumer<OrderPlaced, OrderPlacedHandler>(o =>
    {
        o.Stream              = "orders";
        o.ConsumerGroup       = "fulfillment";
        o.MaxConcurrency      = 16;
        o.MaxDeliveryAttempts = 5;
        o.DeadLetterStream    = "orders:dlq";
    });

// Handler
public sealed class OrderPlacedHandler : IMessageHandler<OrderPlaced>
{
    public Task HandleAsync(MessageContext<OrderPlaced> ctx, CancellationToken ct)
    {
        // ... your business logic
        return Task.CompletedTask;
    }
}

// Producing
var producer = sp.GetRequiredService<IStreamProducer<OrderPlaced>>();
await producer.PublishAsync(new OrderPlaced(orderId: 42));
```

That's the entire surface area for the common case. Reaper, graceful shutdown, OTel, and (on Redis 8.6+) producer-side IDMP are wired automatically.

## 3. Why it exists

`StackExchange.Redis` exposes every Redis Streams command (`StreamAdd`, `StreamReadGroup`, `StreamAutoClaim`, etc.) but stops at the protocol boundary by design — it's a low-level command wrapper, not a workflow framework. Teams adopting Streams in production reinvent the same operational scaffolding (consumer loop, PEL recovery, DLQ, idempotency, observability) and most get at least one of those subtly wrong on the first iteration.

The wider .NET ecosystem fills adjacent gaps but not this one:

- **NRedisStack** is a Redis module wrapper (RedisJSON, RediSearch, RedisTimeSeries, RedisBloom). It adds essentially nothing on top of `StackExchange.Redis` for Streams, because Streams are a core data type, not a module.
- **MassTransit** uses Redis for saga storage; the Streams transport is community/experimental.
- **Wolverine** has a first-party Redis Streams transport, but it's framework-shaped — adopting it means buying the entire Wolverine hosting model.
- **Rebus / Foundatio** use Redis lists or pub/sub, not Streams.

There is no idiomatic, library-shaped, Streams-first option for .NET. RedStream fills exactly that gap.

## 4. Scope

### In scope

1. Typed producer and consumer with a versioned message envelope.
2. Hosted consumer service (`IHostedService`) for Generic Host / ASP.NET Core, plus a standalone API for non-host scenarios.
3. Handler pipeline / middleware (logging, idempotency, custom user middleware).
4. PEL reaper loop using `XAUTOCLAIM`.
5. Graceful shutdown that drains in-flight handlers and leaves uncompleted work safely in the PEL.
6. Dead-letter queue with atomic move (Lua), complete failure envelope, and replay tooling.
7. **Consumer-side** idempotency middleware with correct "process-then-mark" semantics.
8. Automatic use of **producer-side** Redis 8.6 `IDMPAUTO` when the connected server supports it.
9. Backpressure via `COUNT` + `MaxConcurrency` knob with sensible defaults.
10. OpenTelemetry instrumentation: W3C trace propagation through the envelope, messaging-semantic-convention spans, correctly computed lag metric.
11. Redis Cluster and reconnect awareness in the consumer loop.
12. In-process test fake (`InMemoryRedStream`) implementing the same interfaces, with a virtual clock for deterministic reaper / TTL tests.

### Explicit non-goals

- **Not a full bus.** No sagas, no request/response over messages, no cross-transport abstractions.
- **Not an object mapper.** Use Redis OM if you want that; RedStream is for messages, not entities.
- **Not a wrapper around every Streams command.** Low-level ops (`XINFO STREAM`, `XRANGE` scans, custom `XADD` shapes) stay on `IDatabase` from SE.Redis.
- **Not a replacement for Wolverine.** Complementary, library-shaped. If you want a full bus, use Wolverine.
- **No support for Redis < 6.2** (we require `XAUTOCLAIM`).
- **Not exactly-once delivery.** Streams provide at-least-once on the consumer side; RedStream provides idempotency helpers and clear documentation that handler-level idempotency is the primary safeguard.
- **v1 ships System.Text.Json only.** MessagePack / protobuf are deferred to optional packages.

## 5. Technical decisions

| | |
|---|---|
| Target frameworks | `net8.0`, `net10.0` (multi-target) |
| Root namespace | `RedStream` |
| License | MIT |
| Required Redis version | **6.2+** (for `XAUTOCLAIM`); **8.6+** unlocks producer-side IDMP |
| Required `StackExchange.Redis` | **≥ 2.11.0** (for `StreamIdempotentId` / `StreamConfigure`) |
| Default serializer | `System.Text.Json` |

### Package layout

| Package | Purpose | Dependencies |
|---|---|---|
| `RedStream` | Core: producer, consumer, envelope, middleware, reaper, DLQ, idempotency, hosted service. | `StackExchange.Redis (≥ 2.11.0)`, `Microsoft.Extensions.Hosting.Abstractions`, `Microsoft.Extensions.DependencyInjection.Abstractions`, `Microsoft.Extensions.Options`, `Microsoft.Extensions.Logging.Abstractions`, `System.Diagnostics.DiagnosticSource` |
| `RedStream.OpenTelemetry` | OTel instrumentation registration + metric definitions. Separate package so core has no hard OTel dependency. | `RedStream`, `OpenTelemetry.Api` |
| `RedStream.Testing` | `InMemoryRedStream` and virtual-clock primitives. | `RedStream` |

## 6. Message envelope

Stream entries are key/value pairs on the wire. RedStream uses a fixed envelope so that headers (type, version, trace context, IDs) survive serialization round-trips and are stable across language clients.

| Field | Required | Purpose |
|---|---|---|
| `v` | yes | Envelope schema version. `"1"` for v1. |
| `t` | yes | Message type identifier (defaults to `typeof(T).FullName`, overridable per type). |
| `id` | yes | Application-level message ID (Guid by default). Used as the idempotency key for **both** producer-side IDMP and consumer-side dedup. Distinct from the Redis stream entry ID. |
| `tp` | optional | W3C `traceparent`. Injected by producer, extracted by consumer. |
| `ts` | yes | RFC 3339 publish timestamp. |
| `corr` | optional | Correlation ID. |
| `caus` | optional | Causation ID. |
| `body` | yes | JSON payload. |
| `meta` | optional | JSON-encoded user headers. |

## 7. Public API sketch

> Final names will be refined during implementation. This is a directional sketch, not a frozen contract.

### Producer

```csharp
public interface IStreamProducer<T>
{
    Task<string> PublishAsync(
        T message,
        PublishOptions? options = null,
        CancellationToken ct = default);
}

public sealed class PublishOptions
{
    public string? MessageId { get; init; }       // override for idempotency keys
    public string? CorrelationId { get; init; }
    public string? CausationId { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
```

**IDMP behaviour.** On startup, RedStream detects the server version (`INFO server`). On Redis 8.6+ the producer:

- Sets a per-process `ProducerId` (configurable; defaults to `{MachineName}:{ProcessId}:{GuidSuffix}`).
- Passes the envelope `id` as `StreamIdempotentId` on `StreamAdd`, giving deterministic dedup keyed on the application-level ID.
- Calls `StreamConfigure` on first publish per stream to set sane `idmp-duration` and `idmp-maxsize` defaults (overridable).

On Redis < 8.6 the producer falls back to plain `StreamAdd`. Consumer-side dedup remains in either case.

### Consumer

```csharp
public interface IMessageHandler<T>
{
    Task HandleAsync(MessageContext<T> context, CancellationToken ct);
}

public sealed class MessageContext<T>
{
    public string MessageId { get; init; }     // envelope `id`
    public string StreamEntryId { get; init; } // Redis-assigned entry ID
    public T Payload { get; init; }
    public MessageHeaders Headers { get; init; }
    public int DeliveryCount { get; init; }    // from XPENDING
    public string Stream { get; init; }
    public string ConsumerGroup { get; init; }
    public string Consumer { get; init; }
}
```

### Middleware

```csharp
public interface IConsumerMiddleware
{
    Task InvokeAsync(
        MessageContext context,
        ConsumerDelegate next,
        CancellationToken ct);
}
```

Built-in middleware:
- `LoggingMiddleware` (default on)
- `IdempotencyMiddleware` (opt-in, default off)
- `OpenTelemetryMiddleware` (added by `RedStream.OpenTelemetry`)

### DI registration

```csharp
services.AddRedStream(connectionMultiplexer)
    .AddProducer<OrderPlaced>("orders")
    .AddConsumer<OrderPlaced, OrderPlacedHandler>(options =>
    {
        options.Stream              = "orders";
        options.ConsumerGroup       = "fulfillment";
        options.ConsumerName        = Environment.MachineName;
        options.BatchSize           = 10;
        options.MaxConcurrency      = 16;
        options.IdleReclaimAfter    = TimeSpan.FromSeconds(30);
        options.ReaperInterval      = TimeSpan.FromSeconds(15);
        options.MaxDeliveryAttempts = 5;
        options.DeadLetterStream    = "orders:dlq";
        options.DedupeWindow        = TimeSpan.FromHours(24);
    })
    .UseMiddleware<MyAuditMiddleware>();
```

### Permanent-failure signal

Handlers may throw `PermanentFailureException` to skip retries and send the message straight to the DLQ on first failure (validation errors, missing required fields, etc.).

## 8. Dead-letter queue

- Triggered when `DeliveryCount > MaxDeliveryAttempts` **or** the handler throws `PermanentFailureException`.
- The move from source stream to DLQ is **atomic via Lua**: `XADD` to DLQ + `XACK` of original in a single script. Avoids the "in both places" failure mode of a non-atomic move.
- DLQ envelope extends the original with `dlq.*` fields:

| Field | Purpose |
|---|---|
| `dlq.original_id` | Source Redis stream entry ID |
| `dlq.original_stream` | Source stream name |
| `dlq.original_group` | Source consumer group |
| `dlq.attempts` | Total delivery attempts |
| `dlq.first_seen` | First delivery timestamp |
| `dlq.last_seen` | Last failure timestamp |
| `dlq.error_type` | Exception type name |
| `dlq.error_message` | Exception message |
| `dlq.error_stack` | Truncated stack trace |

- Original payload and all original envelope fields are preserved.
- Replay API: `IDeadLetterReplay.ReplayAsync(dlqEntryId, targetStream?, ct)` — moves an entry back to the source (or chosen) stream. Strips `dlq.*`, bumps `replay.count`, preserves original `id` for idempotency.
- Inspection API: `IDeadLetterQuery.ListAsync(stream, since, limit, ct)` returning typed DLQ records.

## 9. Reaper (PEL recovery)

- Per-consumer background loop running `XAUTOCLAIM` against the configured stream/group with `MIN-IDLE-TIME = IdleReclaimAfter`.
- Claimed messages enter the normal handler pipeline; their `DeliveryCount` is automatically incremented by Redis.
- Default cadence: every `ReaperInterval` (15s). Default idle threshold: 30s.
- Runs as part of the same `IHostedService` that owns the consumer — no separate registration needed.

## 10. Graceful shutdown

On `IHostedService.StopAsync` (or `CancellationToken` cancellation in standalone mode):

1. Stop calling `XREADGROUP` (no new pulls).
2. Wait for in-flight handlers to complete, bounded by `ShutdownTimeout` (default 30s).
3. `XACK` everything that completed successfully.
4. Anything still in flight at the deadline is left in the PEL — the reaper on another instance (or this instance on restart) will pick it up.
5. Dispose the consumer.

## 11. Idempotency

Two different problems, two different solutions. RedStream addresses both, but it's important not to conflate them.

### 11.1 Producer-side (Redis 8.6 IDMP)

**Problem:** producer calls `XADD`, the network drops the response, the producer retries — without dedup you'd have two entries in the stream.

**Solved by:** Redis 8.6's native `IDMPAUTO` (or `IDMP` with explicit IDs), exposed via SE.Redis 2.11+ `StreamIdempotentId`. RedStream uses it automatically when the server supports it. Cost: 2–5% throughput overhead, <1.5% memory.

This is purely a wire-level guarantee. It does **not** prevent the consumer from seeing the same entry more than once via redelivery.

### 11.2 Consumer-side (RedStream middleware)

**Problem:** consumer reads entry `1234-0`, handler runs, process dies before `XACK`. The reaper (or another consumer) later picks up `1234-0` and delivers it again. The entry exists exactly once in the stream, but the handler has now run twice.

**Solved by:** `IdempotencyMiddleware`, which uses a "process-then-mark" pattern: handler runs, on success set `dedupe:{group}:{envelopeId} = 1 EX {DedupeWindow} NX`, then ACK. On redelivery, check the key; if present, ACK and skip.

`DedupeWindow` must be ≥ stream retention; we validate and warn at startup if it isn't.

**The race window is real and documented.** Between the existence check and the process-then-mark, two consumers could both decide to process the same message. The middleware is **defense in depth, not a primary safeguard** — the official guidance is that handlers should be idempotent at the business layer (`INSERT … ON CONFLICT (message_id)`, `UPDATE … WHERE version = X`). The middleware covers the common case where that isn't feasible, but it cannot promise exactly-once.

## 12. Backpressure

- Per-consumer `BatchSize` (`COUNT` on `XREADGROUP`) and `MaxConcurrency` (handler parallelism cap).
- Default: `BatchSize = 10`, `MaxConcurrency = Environment.ProcessorCount`.
- The pull loop awaits each batch's `Task.WhenAll` before reading the next — natural bounded concurrency.
- Per-handler-type concurrency overrides are out of scope for v1.

## 13. OpenTelemetry (`RedStream.OpenTelemetry`)

- `ActivitySource` name: `RedStream`.
- Producer activity: kind `Producer`, attributes per OTel messaging semantic conventions (`messaging.system = "redis"`, `messaging.operation = "publish"`, `messaging.destination.name = <stream>`, `messaging.message.id = <envelope id>`).
  - When IDMP is in use, add `redstream.producer.id = <pid>` and `redstream.idmp.mode = "auto" | "manual"`.
- `traceparent` (and `tracestate` if present) injected into envelope on publish.
- Consumer activity: kind `Consumer`, extracted from envelope, **span link** to the producer (not parent-child — async semantics).
- Batch reads: one activity per message (per OTel conventions), each linked to its producer.
- `Meter` name: `RedStream`.
- Metrics:
  - `redstream.publish.duration` — histogram, tagged by stream.
  - `redstream.consume.duration` — histogram, tagged by stream, group, handler type.
  - `redstream.consume.lag` — gauge, computed from `XINFO GROUPS` last-delivered-id vs. stream tail ID (parses the `{ms}-{seq}` format correctly).
  - `redstream.consume.pel_depth` — gauge.
  - `redstream.deadletter.count` — counter.
- Sampling decisions made upstream are respected.

## 14. Cluster and connection lifecycle

### Redis Cluster

Streams live on a single hash slot. A single `XREADGROUP` call can read multiple streams **only** if they share a slot.

- **v1 ships single-stream consumers** — one consumer instance per `(stream, group)`. This sidesteps the multi-slot problem entirely.
- For multi-stream tenancy patterns, document the hash-tag convention (`{tenant}:orders`, `{tenant}:invoices`) so users who want them get them voluntarily.
- The DLQ stream defaults to `{sourceStream}:dlq` so it lands on the same slot as the source — atomic Lua scripts require this.

### Connection lifecycle

`IConnectionMultiplexer` reconnects automatically but commands in flight during a disconnect throw `RedisConnectionException`. The consumer loop:

- Catches `RedisConnectionException` and `RedisTimeoutException`, logs with the structured `connection.state` from SE.Redis, and waits with exponential backoff (jittered, capped at 5s).
- Does **not** treat connection errors as message-processing failures — no DLQ, no delivery-count bump (the message was never claimed).
- On reconnect, resumes the `XREADGROUP` loop. Any messages claimed before the disconnect remain in the PEL and will either be re-read with `>` consumer-position semantics or reclaimed by the reaper.
- Health-check integration (see open questions) exposes connection state.

## 15. Testing (`RedStream.Testing`)

- `InMemoryRedStream` implements `IStreamProducer<T>` / consumer plumbing in-process.
- Models stream entries, consumer groups, PEL, and delivery counts faithfully enough for behavioural tests.
- `VirtualClock` lets tests drive reaper idle-threshold and dedupe TTL behaviour deterministically.
- Goal: behavioural fidelity for the operations RedStream uses, not 100% Redis Streams emulation. Integration tests against a real Redis (via Testcontainers) remain the source of truth for protocol-level concerns.

## 16. Quality bar

- Unit tests cover envelope encoding/decoding, middleware pipeline, idempotency middleware, reaper logic against the in-memory fake.
- Integration tests cover end-to-end producer → consumer → ACK, PEL recovery on consumer "death", DLQ atomicity, replay, OTel span/metric emission, and IDMP usage on Redis 8.6+. Run against real Redis 6.2 and 8.6 via Testcontainers.
- All public APIs have XML doc comments.
- README leads with the "what you get" pitch from §1 and the §2 quickstart, then covers: install, configuration, DLQ behaviour, the producer-vs-consumer idempotency split, OTel setup, in-memory testing.
- Sample app under `samples/` demonstrating producer + consumer + DLQ + OTel.

## 17. Open questions

The following decisions are intentionally deferred until they need answering during implementation:

1. **Serializer abstraction shape** — single `IMessageSerializer` interface? Per-type override? Fixed JSON in v1 and revisit when someone asks?
2. **`RedStream.Testing` packaging** — included in the core package under a `Testing` namespace (Microsoft-style) or a separate NuGet?
3. **Health-check integration** — ship a `Microsoft.Extensions.Diagnostics.HealthChecks` provider? Probably yes; v1.1 unless trivial.
4. **CLI tool** (`dotnet redstream`) for inspecting streams / groups / PEL / DLQ — parked, v2.
5. **Multi-stream consumers** — explicitly out of v1; revisit if user demand surfaces.

## 18. Out of scope for v1

- MessagePack / protobuf serializer packages.
- `dotnet redstream` CLI tool.
- Health-check provider (unless trivial during implementation).
- Per-handler-type concurrency overrides.
- Adaptive concurrency based on handler latency / error rate.
- Bloom-filter-backed idempotency option.
- Saga / orchestration helpers (explicitly not a goal — use Wolverine).
- Multi-stream-per-consumer reads.
