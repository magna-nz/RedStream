using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Atomically moves a failed message from its source stream to a DLQ stream.
/// The default implementation uses a Lua script to make the <c>XADD</c> (to DLQ)
/// and <c>XACK</c> (on source) a single atomic operation, eliminating the
/// "delivered to both places" failure mode of a non-atomic move.
/// </summary>
public interface IDeadLetter
{
    /// <summary>
    /// Append <paramref name="dlqEntries"/> to <paramref name="dlqStream"/> and ACK
    /// <paramref name="sourceEntryId"/> on (<paramref name="sourceStream"/>,
    /// <paramref name="sourceGroup"/>) atomically.
    /// </summary>
    /// <returns>The Redis entry id assigned to the new DLQ entry.</returns>
    Task<string> MoveAsync(
        string sourceStream,
        string sourceGroup,
        string sourceEntryId,
        string dlqStream,
        NameValueEntry[] dlqEntries,
        CancellationToken ct);
}
