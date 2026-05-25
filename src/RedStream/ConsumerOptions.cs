namespace RedStream;

/// <summary>
/// Per-consumer configuration.
/// </summary>
public sealed class ConsumerOptions
{
    /// <summary>Stream name. Required.</summary>
    public string Stream { get; set; } = "";

    /// <summary>Consumer group name. Required.</summary>
    public string ConsumerGroup { get; set; } = "";

    /// <summary>
    /// Name of this consumer within the group. Defaults to <see cref="Environment.MachineName"/>.
    /// Choose a value that's unique per consumer instance so XPENDING / XAUTOCLAIM behave correctly.
    /// </summary>
    public string ConsumerName { get; set; } = Environment.MachineName;

    /// <summary>Maximum number of entries pulled per <c>XREADGROUP</c> call (<c>COUNT</c>). Default 10.</summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>Maximum concurrent handler invocations in flight. Default <see cref="Environment.ProcessorCount"/>.</summary>
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount;

    /// <summary>How long an entry must sit in the PEL before the reaper considers it idle. Default 30s.</summary>
    public TimeSpan IdleReclaimAfter { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How often the reaper runs <c>XAUTOCLAIM</c>. Default 15s.</summary>
    public TimeSpan ReaperInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Delivery attempts allowed before a message moves to the DLQ. Default 5.</summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>
    /// Dead-letter stream name. When null, the convention <c>{Stream}:dlq</c> is used so
    /// the DLQ lives on the same hash slot as the source (required for the atomic Lua move).
    /// </summary>
    public string? DeadLetterStream { get; set; }

    /// <summary>
    /// TTL applied to the consumer-side dedupe key. Should be ≥ the stream's retention so
    /// a replayed entry is still recognised as a duplicate. Default 24 hours.
    /// </summary>
    public TimeSpan DedupeWindow { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// Pause between empty <c>XREADGROUP</c> polls. Lower means tighter latency at the cost
    /// of more idle work. Default 100ms.
    /// </summary>
    public TimeSpan PollIntervalWhenEmpty { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Maximum time graceful shutdown waits for in-flight handlers to finish. Default 30s.</summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Where a freshly-created consumer group starts reading from. Default <see cref="ConsumerStartPosition.LatestOnly"/>.</summary>
    public ConsumerStartPosition StartPosition { get; set; } = ConsumerStartPosition.LatestOnly;

    /// <summary>Throws <see cref="ArgumentException"/> with a precise reason for any invalid setting.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Stream))
        {
            throw new ArgumentException($"{nameof(Stream)} is required.", nameof(Stream));
        }
        if (string.IsNullOrWhiteSpace(ConsumerGroup))
        {
            throw new ArgumentException($"{nameof(ConsumerGroup)} is required.", nameof(ConsumerGroup));
        }
        if (string.IsNullOrWhiteSpace(ConsumerName))
        {
            throw new ArgumentException($"{nameof(ConsumerName)} is required.", nameof(ConsumerName));
        }
        if (BatchSize < 1)
        {
            throw new ArgumentException($"{nameof(BatchSize)} must be >= 1.", nameof(BatchSize));
        }
        if (MaxConcurrency < 1)
        {
            throw new ArgumentException($"{nameof(MaxConcurrency)} must be >= 1.", nameof(MaxConcurrency));
        }
        if (IdleReclaimAfter < TimeSpan.Zero)
        {
            throw new ArgumentException($"{nameof(IdleReclaimAfter)} must be >= 0.", nameof(IdleReclaimAfter));
        }
        if (ReaperInterval <= TimeSpan.Zero)
        {
            throw new ArgumentException($"{nameof(ReaperInterval)} must be > 0.", nameof(ReaperInterval));
        }
        if (MaxDeliveryAttempts < 1)
        {
            throw new ArgumentException($"{nameof(MaxDeliveryAttempts)} must be >= 1.", nameof(MaxDeliveryAttempts));
        }
        if (DedupeWindow <= TimeSpan.Zero)
        {
            throw new ArgumentException($"{nameof(DedupeWindow)} must be > 0.", nameof(DedupeWindow));
        }
        if (PollIntervalWhenEmpty < TimeSpan.Zero)
        {
            throw new ArgumentException($"{nameof(PollIntervalWhenEmpty)} must be >= 0.", nameof(PollIntervalWhenEmpty));
        }
        if (ShutdownTimeout < TimeSpan.Zero)
        {
            throw new ArgumentException($"{nameof(ShutdownTimeout)} must be >= 0.", nameof(ShutdownTimeout));
        }
    }

    /// <summary>Resolves the effective DLQ stream name, applying the <c>{Stream}:dlq</c> default.</summary>
    public string ResolveDeadLetterStream()
        => DeadLetterStream ?? $"{Stream}:dlq";
}
