using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Default <see cref="IRedisServerVersionCache"/>. One instance per
/// <see cref="IConnectionMultiplexer"/>; the version is fetched once on the first call
/// and cached for the lifetime of the multiplexer.
/// </summary>
public sealed class RedisServerVersionCache : IRedisServerVersionCache
{
    private static readonly Version IdmpMinVersion = new(8, 6);

    private readonly IConnectionMultiplexer _connection;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private Version? _version;

    /// <summary>Construct a cache bound to <paramref name="connection"/>.</summary>
    public RedisServerVersionCache(IConnectionMultiplexer connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
    }

    /// <inheritdoc />
    public async ValueTask<Version> GetVersionAsync(CancellationToken ct = default)
    {
        if (_version is not null)
        {
            return _version;
        }

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_version is not null)
            {
                return _version;
            }

            var endpoint = _connection.GetEndPoints().FirstOrDefault()
                ?? throw new InvalidOperationException("Connection multiplexer has no endpoints.");
            var server = _connection.GetServer(endpoint);
            var info = await server.InfoAsync("server").ConfigureAwait(false);

            var versionStr = info
                .SelectMany(group => group)
                .FirstOrDefault(kvp => kvp.Key == "redis_version")
                .Value;

            if (string.IsNullOrEmpty(versionStr))
            {
                throw new InvalidOperationException("Could not determine Redis server version from INFO output.");
            }

            _version = RedisVersionParser.Parse(versionStr);
            return _version;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsIdmpSupportedAsync(CancellationToken ct = default)
    {
        var version = await GetVersionAsync(ct).ConfigureAwait(false);
        return version >= IdmpMinVersion;
    }
}
