namespace RedStream;

/// <summary>
/// Per-call publish overrides.
/// </summary>
public sealed class PublishOptions
{
    /// <summary>
    /// Override the application-level message ID used for IDMP (on Redis 8.6+) and for
    /// consumer-side dedup. When null, a random Guid is generated.
    /// </summary>
    public string? MessageId { get; init; }

    /// <summary>Correlation ID propagated through the envelope's <c>corr</c> field.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Causation ID propagated through the envelope's <c>caus</c> field.</summary>
    public string? CausationId { get; init; }

    /// <summary>User-defined headers serialised into the envelope's <c>meta</c> field.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
