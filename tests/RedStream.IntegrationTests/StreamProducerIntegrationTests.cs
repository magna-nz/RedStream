using Microsoft.Extensions.Logging.Abstractions;
using RedStream;

namespace RedStream.IntegrationTests;

[Collection(RedisCollection.Name)]
public class StreamProducerIntegrationTests
{
    public sealed record OrderPlaced(int OrderId, string Customer);

    private readonly RedisFixture _fixture;

    public StreamProducerIntegrationTests(RedisFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Publish_returns_a_stream_entry_id()
    {
        var stream = NewStreamName();
        var producer = NewProducer(stream);

        var entryId = await producer.PublishAsync(new OrderPlaced(1, "ada"));

        entryId.Should().NotBeNullOrEmpty();
        entryId.Should().MatchRegex(@"^\d+-\d+$");
    }

    [Fact]
    public async Task Idmp_dedupes_when_message_id_repeats()
    {
        var stream = NewStreamName();
        var producer = NewProducer(stream);
        const string id = "deterministic-id";
        var msg = new OrderPlaced(1, "ada");

        var first = await producer.PublishAsync(msg, new PublishOptions { MessageId = id });
        var second = await producer.PublishAsync(msg, new PublishOptions { MessageId = id });

        second.Should().Be(first, "IDMP should return the original entry id for a repeated (pid, iid)");

        var db = _fixture.Connection.GetDatabase();
        var length = await db.StreamLengthAsync(stream);
        length.Should().Be(1);
    }

    [Fact]
    public async Task Different_message_ids_create_distinct_entries()
    {
        var stream = NewStreamName();
        var producer = NewProducer(stream);
        var msg = new OrderPlaced(1, "ada");

        var first = await producer.PublishAsync(msg, new PublishOptions { MessageId = "a" });
        var second = await producer.PublishAsync(msg, new PublishOptions { MessageId = "b" });

        second.Should().NotBe(first);
        var db = _fixture.Connection.GetDatabase();
        var length = await db.StreamLengthAsync(stream);
        length.Should().Be(2);
    }

    [Fact]
    public async Task Envelope_round_trips_through_the_stream()
    {
        var stream = NewStreamName();
        var producer = NewProducer(stream);

        await producer.PublishAsync(
            new OrderPlaced(42, "ada"),
            new PublishOptions { MessageId = "envelope-test", CorrelationId = "corr-1" });

        var db = _fixture.Connection.GetDatabase();
        var entries = await db.StreamRangeAsync(stream, count: 1);
        entries.Should().HaveCount(1);

        var envelope = EnvelopeEncoder.Decode(entries[0].Values);
        envelope.MessageId.Should().Be("envelope-test");
        envelope.CorrelationId.Should().Be("corr-1");
        envelope.TypeId.Should().Be(typeof(OrderPlaced).FullName);
        envelope.Body.Should().Contain("\"orderId\":42");
    }

    private StreamProducer<OrderPlaced> NewProducer(string stream)
    {
        var versionCache = new RedisServerVersionCache(_fixture.Connection);
        return new StreamProducer<OrderPlaced>(
            _fixture.Connection,
            new JsonMessageSerializer(),
            new DefaultMessageTypeResolver(),
            versionCache,
            stream,
            new ProducerOptions(),
            new RedStreamOptions(),
            NullLogger<StreamProducer<OrderPlaced>>.Instance);
    }

    private static string NewStreamName() => $"test-stream:{Guid.NewGuid():N}";
}
