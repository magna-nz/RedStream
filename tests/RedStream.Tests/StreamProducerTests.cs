using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace RedStream.Tests;

public class StreamProducerTests
{
    public sealed record OrderPlaced(int OrderId, string Customer);

    private const string Stream = "orders";

    [Fact]
    public async Task Plain_xadd_when_idmp_not_supported()
    {
        var ctx = NewContext(idmpSupported: false);

        await ctx.Producer.PublishAsync(new OrderPlaced(42, "ada"));

        await ctx.Database.ReceivedWithAnyArgs(1).StreamAddAsync(default, default!);
        await ctx.Database.DidNotReceive().ExecuteAsync(
            Arg.Is<string>(s => s == "XADD"),
            Arg.Any<object[]>());
    }

    [Fact]
    public async Task Raw_xadd_with_idmp_when_supported()
    {
        var ctx = NewContext(idmpSupported: true);

        await ctx.Producer.PublishAsync(new OrderPlaced(42, "ada"));

        await ctx.Database.Received(1).ExecuteAsync(
            "XADD",
            Arg.Is<object[]>(args =>
                (string)args[0] == Stream
                && (string)args[1] == "IDMP"
                && !string.IsNullOrEmpty((string)args[2])      // producer id
                && !string.IsNullOrEmpty((string)args[3])      // idempotent id
                && (string)args[4] == "*"));
    }

    [Fact]
    public async Task Idmp_iid_uses_envelope_message_id()
    {
        var ctx = NewContext(idmpSupported: true);
        const string explicitId = "test-msg-1";

        await ctx.Producer.PublishAsync(
            new OrderPlaced(42, "ada"),
            new PublishOptions { MessageId = explicitId });

        await ctx.Database.Received(1).ExecuteAsync(
            "XADD",
            Arg.Is<object[]>(args => (string)args[3] == explicitId));
    }

    [Fact]
    public async Task Idmp_pid_uses_explicit_producer_id()
    {
        var ctx = NewContext(idmpSupported: true, producerOptions: new ProducerOptions { ProducerId = "my-producer" });

        await ctx.Producer.PublishAsync(new OrderPlaced(42, "ada"));

        await ctx.Database.Received(1).ExecuteAsync(
            "XADD",
            Arg.Is<object[]>(args => (string)args[2] == "my-producer"));
    }

    [Fact]
    public async Task Idmp_pid_falls_back_to_global_factory()
    {
        var ctx = NewContext(
            idmpSupported: true,
            globalOptions: new RedStreamOptions { DefaultProducerIdFactory = () => "global-pid" });

        await ctx.Producer.PublishAsync(new OrderPlaced(42, "ada"));

        await ctx.Database.Received(1).ExecuteAsync(
            "XADD",
            Arg.Is<object[]>(args => (string)args[2] == "global-pid"));
    }

    [Fact]
    public async Task Default_producer_id_follows_machine_pid_pattern()
    {
        var ctx = NewContext(idmpSupported: true);

        ctx.Producer.ProducerId.Should().StartWith($"{Environment.MachineName}:{Environment.ProcessId}:");
        ctx.Producer.ProducerId.Length.Should().Be(
            Environment.MachineName.Length + 1 +
            Environment.ProcessId.ToString().Length + 1 + 8);
    }

    [Fact]
    public async Task Envelope_body_is_camel_case_json()
    {
        var ctx = NewContext(idmpSupported: true);

        await ctx.Producer.PublishAsync(new OrderPlaced(42, "ada"));

        await ctx.Database.Received(1).ExecuteAsync(
            "XADD",
            Arg.Is<object[]>(args => ExtractEnvelopeField(args, "body").Contains("\"orderId\":42")));
    }

    [Fact]
    public async Task Envelope_type_id_uses_full_name_by_default()
    {
        var ctx = NewContext(idmpSupported: true);

        await ctx.Producer.PublishAsync(new OrderPlaced(42, "ada"));

        await ctx.Database.Received(1).ExecuteAsync(
            "XADD",
            Arg.Is<object[]>(args => ExtractEnvelopeField(args, "t") == typeof(OrderPlaced).FullName));
    }

    [Fact]
    public async Task Envelope_carries_correlation_and_causation_ids()
    {
        var ctx = NewContext(idmpSupported: true);

        await ctx.Producer.PublishAsync(
            new OrderPlaced(42, "ada"),
            new PublishOptions { CorrelationId = "corr-1", CausationId = "caus-1" });

        await ctx.Database.Received(1).ExecuteAsync(
            "XADD",
            Arg.Is<object[]>(args =>
                ExtractEnvelopeField(args, "corr") == "corr-1"
                && ExtractEnvelopeField(args, "caus") == "caus-1"));
    }

    [Fact]
    public async Task Publish_null_throws()
    {
        var ctx = NewContext(idmpSupported: true);

        var act = () => ctx.Producer.PublishAsync(null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_validates_null_args()
    {
        var versionCache = Substitute.For<IRedisServerVersionCache>();
        var connection = Substitute.For<IConnectionMultiplexer>();
        connection.GetDatabase(Arg.Any<int>(), Arg.Any<object?>()).Returns(Substitute.For<IDatabase>());
        var serializer = new JsonMessageSerializer();
        var typeResolver = new DefaultMessageTypeResolver();
        var logger = NullLogger<StreamProducer<OrderPlaced>>.Instance;
        var producerOptions = new ProducerOptions();
        var globalOptions = new RedStreamOptions();

        FluentActions.Invoking(() => new StreamProducer<OrderPlaced>(
            null!, serializer, typeResolver, versionCache, "s", producerOptions, globalOptions, logger))
            .Should().Throw<ArgumentNullException>();

        FluentActions.Invoking(() => new StreamProducer<OrderPlaced>(
            connection, serializer, typeResolver, versionCache, "", producerOptions, globalOptions, logger))
            .Should().Throw<ArgumentException>();
    }

    private static string ExtractEnvelopeField(object[] xaddArgs, string fieldName)
    {
        // XADD args layout: [stream, "IDMP", pid, iid, "*", field, value, field, value, ...]
        for (var i = 5; i < xaddArgs.Length; i += 2)
        {
            if ((string)xaddArgs[i] == fieldName)
            {
                return (string)xaddArgs[i + 1];
            }
        }
        return "";
    }

    private static ProducerContext NewContext(
        bool idmpSupported,
        ProducerOptions? producerOptions = null,
        RedStreamOptions? globalOptions = null)
    {
        var db = Substitute.For<IDatabase>();
        db.StreamAddAsync(default, default!).ReturnsForAnyArgs((RedisValue)"1234-0");

        db.ExecuteAsync(Arg.Any<string>(), Arg.Any<object[]>())
            .Returns(RedisResult.Create((RedisValue)"1234-0"));

        var connection = Substitute.For<IConnectionMultiplexer>();
        connection.GetDatabase(Arg.Any<int>(), Arg.Any<object?>()).Returns(db);

        var versionCache = Substitute.For<IRedisServerVersionCache>();
        versionCache.IsIdmpSupportedAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<bool>(idmpSupported));

        var producer = new StreamProducer<OrderPlaced>(
            connection,
            new JsonMessageSerializer(),
            new DefaultMessageTypeResolver(),
            versionCache,
            Stream,
            producerOptions ?? new ProducerOptions(),
            globalOptions ?? new RedStreamOptions(),
            NullLogger<StreamProducer<OrderPlaced>>.Instance);

        return new ProducerContext(producer, connection, db, versionCache);
    }

    private sealed record ProducerContext(
        StreamProducer<OrderPlaced> Producer,
        IConnectionMultiplexer Connection,
        IDatabase Database,
        IRedisServerVersionCache VersionCache);
}
