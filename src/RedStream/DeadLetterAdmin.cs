using StackExchange.Redis;

namespace RedStream;

/// <summary>Replays DLQ entries back to a source (or chosen) stream.</summary>
public interface IDeadLetterReplay
{
    /// <summary>
    /// Replay <paramref name="dlqEntryId"/> from <paramref name="dlqStream"/> back to
    /// <paramref name="targetStream"/> (default: the original source stream recorded
    /// in the DLQ entry's <c>dlq.original_stream</c> metadata).
    /// </summary>
    /// <returns>The Redis entry id assigned by <paramref name="targetStream"/>.</returns>
    Task<string> ReplayAsync(
        string dlqStream,
        string dlqEntryId,
        string? targetStream = null,
        CancellationToken ct = default);
}

/// <summary>Queries the DLQ for inspection / admin tooling.</summary>
public interface IDeadLetterQuery
{
    /// <summary>List the most recent DLQ entries (newest first), up to <paramref name="count"/>.</summary>
    Task<IReadOnlyList<DeadLetterRecord>> ListAsync(string dlqStream, int count, CancellationToken ct = default);

    /// <summary>Get a specific DLQ entry, or null if it does not exist.</summary>
    Task<DeadLetterRecord?> GetAsync(string dlqStream, string dlqEntryId, CancellationToken ct = default);
}

/// <summary>
/// Default <see cref="IDeadLetterReplay"/> + <see cref="IDeadLetterQuery"/> implementation
/// over <see cref="IConnectionMultiplexer"/>. Replay preserves the original envelope's
/// <see cref="MessageEnvelope.MessageId"/> so consumer-side dedup recognises the message
/// as a replay rather than a new arrival.
/// </summary>
public sealed class RedisDeadLetterAdmin : IDeadLetterReplay, IDeadLetterQuery
{
    private readonly IConnectionMultiplexer _connection;

    /// <summary>Construct.</summary>
    public RedisDeadLetterAdmin(IConnectionMultiplexer connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
    }

    /// <inheritdoc />
    public async Task<string> ReplayAsync(
        string dlqStream,
        string dlqEntryId,
        string? targetStream = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dlqStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(dlqEntryId);

        var record = await GetAsync(dlqStream, dlqEntryId, ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"DLQ entry '{dlqEntryId}' not found in '{dlqStream}'.");

        var target = !string.IsNullOrWhiteSpace(targetStream)
            ? targetStream
            : record.Metadata.OriginalStream;

        if (string.IsNullOrWhiteSpace(target))
        {
            throw new InvalidOperationException(
                $"Cannot determine replay target for DLQ entry '{dlqEntryId}': no target supplied and the record has no original stream recorded.");
        }

        var entries = EnvelopeEncoder.Encode(record.OriginalEnvelope);
        var db = _connection.GetDatabase();
        var newEntryId = await db.StreamAddAsync(target, entries).ConfigureAwait(false);
        return newEntryId.ToString();
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<DeadLetterRecord>> ListAsync(string dlqStream, int count, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dlqStream);
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), count, "Must be > 0.");
        }

        var db = _connection.GetDatabase();
        var entries = await db.StreamRangeAsync(dlqStream, "-", "+", count, Order.Descending).ConfigureAwait(false);
        return entries.Select(DeadLetterEncoder.Decode).ToList();
    }

    /// <inheritdoc />
    public async Task<DeadLetterRecord?> GetAsync(string dlqStream, string dlqEntryId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dlqStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(dlqEntryId);

        var db = _connection.GetDatabase();
        var entries = await db.StreamRangeAsync(dlqStream, dlqEntryId, dlqEntryId, count: 1).ConfigureAwait(false);
        return entries.Length == 0 ? null : DeadLetterEncoder.Decode(entries[0]);
    }
}
