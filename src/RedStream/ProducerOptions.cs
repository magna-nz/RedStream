namespace RedStream;

/// <summary>
/// Per-producer configuration.
/// </summary>
public sealed class ProducerOptions
{
    /// <summary>
    /// Producer ID used as the IDMP <c>pid</c> on Redis 8.6+. When null, the producer
    /// falls back to <see cref="RedStreamOptions.DefaultProducerIdFactory"/>, then to the
    /// built-in default of <c>{MachineName}:{ProcessId}:{guid8}</c>.
    /// </summary>
    public string? ProducerId { get; set; }

    /// <summary>
    /// IDMP retention for the producer's idempotent IDs (XCFGSET <c>idmp-duration</c>).
    /// Applied lazily on first publish per stream when targeting Redis 8.6+.
    /// Null means leave the server default in place.
    /// </summary>
    public TimeSpan? IdmpDuration { get; set; }

    /// <summary>
    /// IDMP max ID count per producer (XCFGSET <c>idmp-maxsize</c>). Null means leave
    /// the server default in place.
    /// </summary>
    public int? IdmpMaxSize { get; set; }
}
