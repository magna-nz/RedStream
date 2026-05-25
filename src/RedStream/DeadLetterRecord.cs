namespace RedStream;

/// <summary>On-wire field names used to extend a stream entry with DLQ metadata.</summary>
public static class DeadLetterFieldNames
{
    /// <summary>Prefix for every DLQ-specific field.</summary>
    public const string Prefix = "dlq.";

    /// <summary>Source stream entry id field.</summary>
    public const string OriginalEntryId = "dlq.original_id";

    /// <summary>Source stream name field.</summary>
    public const string OriginalStream = "dlq.original_stream";

    /// <summary>Source consumer group field.</summary>
    public const string OriginalGroup = "dlq.original_group";

    /// <summary>Total delivery attempts field.</summary>
    public const string Attempts = "dlq.attempts";

    /// <summary>First-seen timestamp field (ISO 8601).</summary>
    public const string FirstSeen = "dlq.first_seen";

    /// <summary>Last-seen / failure timestamp field (ISO 8601).</summary>
    public const string LastSeen = "dlq.last_seen";

    /// <summary>Exception type name field.</summary>
    public const string ErrorType = "dlq.error_type";

    /// <summary>Exception message field.</summary>
    public const string ErrorMessage = "dlq.error_message";

    /// <summary>Truncated stack trace field.</summary>
    public const string ErrorStack = "dlq.error_stack";
}

/// <summary>Metadata attached to a DLQ entry — everything you need to debug or replay.</summary>
public sealed record DeadLetterMetadata
{
    /// <summary>Source Redis stream entry id (e.g. <c>1714826400123-0</c>).</summary>
    public required string OriginalStreamEntryId { get; init; }

    /// <summary>Source stream name.</summary>
    public required string OriginalStream { get; init; }

    /// <summary>Source consumer group name.</summary>
    public required string OriginalGroup { get; init; }

    /// <summary>Total delivery attempts before being dead-lettered.</summary>
    public required int Attempts { get; init; }

    /// <summary>Publish timestamp of the source message (envelope <c>ts</c> field).</summary>
    public required DateTimeOffset FirstSeen { get; init; }

    /// <summary>When the message was moved to the DLQ.</summary>
    public required DateTimeOffset LastSeen { get; init; }

    /// <summary>Exception type name (<c>typeof(ex).FullName</c>).</summary>
    public string? ErrorType { get; init; }

    /// <summary>Exception message.</summary>
    public string? ErrorMessage { get; init; }

    /// <summary>Stack trace (truncated to a sensible size).</summary>
    public string? ErrorStack { get; init; }
}

/// <summary>A DLQ entry — the original envelope plus the metadata about why it died.</summary>
public sealed record DeadLetterRecord
{
    /// <summary>Redis stream entry id within the DLQ stream.</summary>
    public required string DlqEntryId { get; init; }

    /// <summary>The original message envelope (replay-ready).</summary>
    public required MessageEnvelope OriginalEnvelope { get; init; }

    /// <summary>Why it died.</summary>
    public required DeadLetterMetadata Metadata { get; init; }
}
