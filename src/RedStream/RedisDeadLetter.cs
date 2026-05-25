using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Default <see cref="IDeadLetter"/> implementation. Uses a Lua script so the
/// <c>XADD</c> to the DLQ and the <c>XACK</c> of the source entry happen in a
/// single atomic call. Source and DLQ streams must live on the same hash slot
/// (use a hash-tag like <c>{orders}</c> in Cluster mode); the default
/// <c>{Stream}:dlq</c> naming convention satisfies this automatically.
/// </summary>
public sealed class RedisDeadLetter : IDeadLetter
{
    // KEYS[1] = source stream
    // KEYS[2] = DLQ stream
    // KEYS[3] = source consumer group
    // ARGV[1] = source entry id (to ACK)
    // ARGV[2..N] = DLQ entry fields, alternating name/value
    private const string AtomicMoveScript = @"
local fields = {}
for i = 2, #ARGV do
    fields[i-1] = ARGV[i]
end
local dlqId = redis.call('XADD', KEYS[2], '*', unpack(fields))
redis.call('XACK', KEYS[1], KEYS[3], ARGV[1])
return dlqId
";

    private readonly IConnectionMultiplexer _connection;

    /// <summary>Construct using the supplied connection multiplexer.</summary>
    public RedisDeadLetter(IConnectionMultiplexer connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
    }

    /// <inheritdoc />
    public async Task<string> MoveAsync(
        string sourceStream,
        string sourceGroup,
        string sourceEntryId,
        string dlqStream,
        NameValueEntry[] dlqEntries,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceGroup);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceEntryId);
        ArgumentException.ThrowIfNullOrWhiteSpace(dlqStream);
        ArgumentNullException.ThrowIfNull(dlqEntries);

        var keys = new RedisKey[] { sourceStream, dlqStream, sourceGroup };
        var values = new RedisValue[1 + (dlqEntries.Length * 2)];
        values[0] = sourceEntryId;
        for (var i = 0; i < dlqEntries.Length; i++)
        {
            values[1 + (i * 2)] = dlqEntries[i].Name;
            values[2 + (i * 2)] = dlqEntries[i].Value;
        }

        var db = _connection.GetDatabase();
        var result = await db.ScriptEvaluateAsync(AtomicMoveScript, keys, values).ConfigureAwait(false);
        return result.ToString();
    }
}
