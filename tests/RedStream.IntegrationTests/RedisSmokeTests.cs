namespace RedStream.IntegrationTests;

[Collection(RedisCollection.Name)]
public class RedisSmokeTests
{
    private readonly RedisFixture _fixture;

    public RedisSmokeTests(RedisFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Ping_returns_positive_latency()
    {
        var db = _fixture.Connection.GetDatabase();

        var latency = await db.PingAsync();

        latency.Should().BeGreaterThan(TimeSpan.Zero);
    }

    [Fact]
    public async Task Server_reports_version_eight_or_higher()
    {
        var endpoint = _fixture.Connection.GetEndPoints().First();
        var server = _fixture.Connection.GetServer(endpoint);

        var info = await server.InfoAsync("server");
        var version = info
            .SelectMany(group => group)
            .FirstOrDefault(kvp => kvp.Key == "redis_version")
            .Value;

        version.Should().NotBeNullOrEmpty();
        var major = int.Parse(version.Split('.')[0], System.Globalization.CultureInfo.InvariantCulture);
        major.Should().BeGreaterThanOrEqualTo(8, "RedStream requires Redis 6.2+ baseline; tests use 8.6+ for IDMP coverage");
    }
}
