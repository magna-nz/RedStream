namespace RedStream;

/// <summary>
/// Context for a single message flowing through the consumer pipeline.
/// Non-generic so middleware can be written once and reused across message types.
/// </summary>
public abstract class MessageContext
{
    /// <summary>Envelope <c>id</c> field — the application-level message id (idempotency key).</summary>
    public required string MessageId { get; init; }

    /// <summary>Redis-assigned stream entry id (e.g. <c>1714826400123-0</c>).</summary>
    public required string StreamEntryId { get; init; }

    /// <summary>Envelope <c>t</c> field — the message type identifier as seen on the wire.</summary>
    public required string TypeId { get; init; }

    /// <summary>Envelope <c>ts</c> field — when the producer published this message.</summary>
    public required DateTimeOffset PublishedAt { get; init; }

    /// <summary>Stream name this message was read from.</summary>
    public required string Stream { get; init; }

    /// <summary>Consumer group this message was delivered to.</summary>
    public required string ConsumerGroup { get; init; }

    /// <summary>This consumer's name within the group.</summary>
    public required string Consumer { get; init; }

    /// <summary>Total delivery attempts for this entry, including the current one (from XPENDING).</summary>
    public required int DeliveryCount { get; init; }

    /// <summary>Envelope <c>tp</c> field — W3C traceparent injected by the producer, if any.</summary>
    public string? TraceParent { get; init; }

    /// <summary>Envelope <c>corr</c> field — correlation id, if the producer set one.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Envelope <c>caus</c> field — causation id, if the producer set one.</summary>
    public string? CausationId { get; init; }

    /// <summary>User-defined headers from the envelope's <c>meta</c> field. Empty if none.</summary>
    public required IReadOnlyDictionary<string, string> Headers { get; init; }

    /// <summary>The payload boxed as <see cref="object"/> — useful for middleware that doesn't know T.</summary>
    public abstract object PayloadObject { get; }

    /// <summary>The CLR type of the payload.</summary>
    public abstract Type PayloadType { get; }
}

/// <summary>
/// Typed message context. The terminal handler in the pipeline receives this concrete shape;
/// middleware sees the non-generic <see cref="MessageContext"/> base.
/// </summary>
public sealed class MessageContext<T> : MessageContext
{
    /// <summary>Deserialised payload.</summary>
    public required T Payload { get; init; }

    /// <inheritdoc />
    public override object PayloadObject => Payload!;

    /// <inheritdoc />
    public override Type PayloadType => typeof(T);
}
