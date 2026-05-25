namespace RedStream;

/// <summary>
/// Lazily detects the connected Redis server version and caches the result for the
/// lifetime of the connection multiplexer.
/// </summary>
public interface IRedisServerVersionCache
{
    /// <summary>Return the connected server's reported version, parsed from <c>INFO server</c>.</summary>
    ValueTask<Version> GetVersionAsync(CancellationToken ct = default);

    /// <summary>True iff the server is Redis 8.6+ (producer-side IDMP available).</summary>
    ValueTask<bool> IsIdmpSupportedAsync(CancellationToken ct = default);
}
