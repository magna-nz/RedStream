namespace RedStream;

/// <summary>
/// Wire-format envelope for a message on a Redis stream.
/// </summary>
/// <remarks>
/// Encoded as <see cref="StackExchange.Redis.NameValueEntry"/> pairs using the short
/// field names in <see cref="EnvelopeFieldNames"/> to minimise on-wire payload size.
/// </remarks>
public sealed record MessageEnvelope
{
    /// <summary>The current envelope schema version emitted by this version of RedStream.</summary>
    public const string CurrentVersion = "1";

    /// <summary>Envelope schema version. v1 emits <see cref="CurrentVersion"/>.</summary>
    public required string Version { get; init; }

    /// <summary>Message type identifier. Defaults to <c>typeof(T).FullName</c>; overridable per type.</summary>
    public required string TypeId { get; init; }

    /// <summary>
    /// Application-level message ID. Used as the idempotency key for both producer-side
    /// IDMP (on Redis 8.6+) and consumer-side dedup. Distinct from the Redis stream entry ID.
    /// </summary>
    public required string MessageId { get; init; }

    /// <summary>Publish timestamp.</summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>Serialised payload (JSON for the default serializer).</summary>
    public required string Body { get; init; }

    /// <summary>W3C <c>traceparent</c>. Injected by producer, extracted by consumer.</summary>
    public string? TraceParent { get; init; }

    /// <summary>Correlation ID, propagated across handlers.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Causation ID — the message that caused this one to be published.</summary>
    public string? CausationId { get; init; }

    /// <summary>Serialised user headers (JSON for the default serializer).</summary>
    public string? Headers { get; init; }
}

/// <summary>On-wire field names used to encode a <see cref="MessageEnvelope"/>.</summary>
public static class EnvelopeFieldNames
{
    /// <summary>Envelope schema version field.</summary>
    public const string Version = "v";

    /// <summary>Message type identifier field.</summary>
    public const string TypeId = "t";

    /// <summary>Application-level message ID field.</summary>
    public const string MessageId = "id";

    /// <summary>Timestamp field (ISO 8601 with offset).</summary>
    public const string Timestamp = "ts";

    /// <summary>Payload field.</summary>
    public const string Body = "body";

    /// <summary>W3C <c>traceparent</c> field.</summary>
    public const string TraceParent = "tp";

    /// <summary>Correlation ID field.</summary>
    public const string CorrelationId = "corr";

    /// <summary>Causation ID field.</summary>
    public const string CausationId = "caus";

    /// <summary>User-defined headers field.</summary>
    public const string Headers = "meta";
}
