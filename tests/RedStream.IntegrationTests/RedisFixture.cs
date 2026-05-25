using StackExchange.Redis;
using Testcontainers.Redis;

namespace RedStream.IntegrationTests;

/// <summary>
/// xUnit collection fixture that starts a single Redis container shared by every test
/// in the <see cref="RedisCollection"/>. Tests must use unique key / stream names
/// (e.g. a per-test Guid prefix) to avoid interfering with one another.
/// </summary>
public sealed class RedisFixture : IAsyncLifetime
{
    private const string RedisImage = "redis:8.6";

    private readonly RedisContainer _container = new RedisBuilder()
        .WithImage(RedisImage)
        .Build();

    /// <summary>Connection multiplexer pointing at the fixture's Redis instance.</summary>
    public IConnectionMultiplexer Connection { get; private set; } = null!;

    /// <summary>Raw <c>host:port</c> connection string.</summary>
    public string ConnectionString => _container.GetConnectionString();

    /// <inheritdoc />
    public async Task InitializeAsync()
    {
        await _container.StartAsync();

        var options = ConfigurationOptions.Parse(_container.GetConnectionString());
        options.AllowAdmin = true;
        options.ConnectTimeout = 5000;
        options.AbortOnConnectFail = false;

        Connection = await ConnectionMultiplexer.ConnectAsync(options);
    }

    /// <inheritdoc />
    public async Task DisposeAsync()
    {
        if (Connection is not null)
        {
            await Connection.CloseAsync();
            Connection.Dispose();
        }
        await _container.DisposeAsync();
    }
}
