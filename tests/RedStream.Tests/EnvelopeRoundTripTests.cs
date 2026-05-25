using StackExchange.Redis;

namespace RedStream.Tests;

public class EnvelopeRoundTripTests
{
    [Fact]
    public void Round_trip_preserves_all_fields()
    {
        var original = new MessageEnvelope
        {
            Version = MessageEnvelope.CurrentVersion,
            TypeId = "MyApp.Orders.OrderPlaced",
            MessageId = "01HZX0Z3K8YJX0RKD4N9P0Q9X5",
            Timestamp = new DateTimeOffset(2026, 5, 25, 12, 0, 0, TimeSpan.Zero),
            Body = """{"orderId":42,"customer":"ada"}""",
            TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            CorrelationId = "corr-1",
            CausationId = "caus-1",
            Headers = """{"x-tenant":"acme"}""",
        };

        var entries = EnvelopeEncoder.Encode(original);
        var decoded = EnvelopeEncoder.Decode(entries);

        decoded.Should().BeEquivalentTo(original);
    }

    [Fact]
    public void Round_trip_with_only_required_fields_works()
    {
        var original = new MessageEnvelope
        {
            Version = MessageEnvelope.CurrentVersion,
            TypeId = "T",
            MessageId = "M",
            Timestamp = DateTimeOffset.UtcNow,
            Body = "{}",
        };

        var decoded = EnvelopeEncoder.Decode(EnvelopeEncoder.Encode(original));

        decoded.Should().BeEquivalentTo(original);
    }

    [Theory]
    [InlineData(EnvelopeFieldNames.Version)]
    [InlineData(EnvelopeFieldNames.TypeId)]
    [InlineData(EnvelopeFieldNames.MessageId)]
    [InlineData(EnvelopeFieldNames.Timestamp)]
    [InlineData(EnvelopeFieldNames.Body)]
    public void Missing_required_field_throws(string fieldToOmit)
    {
        var envelope = NewMinimalEnvelope();
        var entries = EnvelopeEncoder.Encode(envelope)
            .Where(e => (string?)e.Name != fieldToOmit)
            .ToArray();

        var act = () => EnvelopeEncoder.Decode(entries);

        act.Should().Throw<InvalidEnvelopeException>()
            .WithMessage($"*{fieldToOmit}*");
    }

    [Fact]
    public void Timestamp_preserves_tick_level_precision()
    {
        var ts = new DateTimeOffset(2026, 5, 25, 12, 34, 56, 789, TimeSpan.FromHours(2)).AddTicks(1234);
        var original = new MessageEnvelope
        {
            Version = MessageEnvelope.CurrentVersion,
            TypeId = "T",
            MessageId = "M",
            Timestamp = ts,
            Body = "{}",
        };

        var decoded = EnvelopeEncoder.Decode(EnvelopeEncoder.Encode(original));

        decoded.Timestamp.Should().Be(ts);
    }

    [Fact]
    public void Unknown_fields_are_tolerated_on_decode()
    {
        var entries = EnvelopeEncoder.Encode(NewMinimalEnvelope())
            .Append(new NameValueEntry("future-field", "future-value"))
            .ToArray();

        var act = () => EnvelopeEncoder.Decode(entries);

        act.Should().NotThrow();
    }

    [Fact]
    public void Encode_throws_on_null_envelope()
    {
        var act = () => EnvelopeEncoder.Encode(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Decode_throws_on_invalid_timestamp()
    {
        var entries = EnvelopeEncoder.Encode(NewMinimalEnvelope())
            .Select(e => (string?)e.Name == EnvelopeFieldNames.Timestamp
                ? new NameValueEntry(e.Name, "not-a-timestamp")
                : e)
            .ToArray();

        var act = () => EnvelopeEncoder.Decode(entries);

        act.Should().Throw<InvalidEnvelopeException>()
            .WithMessage("*Timestamp*not-a-timestamp*");
    }

    private static MessageEnvelope NewMinimalEnvelope() => new()
    {
        Version = MessageEnvelope.CurrentVersion,
        TypeId = "T",
        MessageId = "M",
        Timestamp = DateTimeOffset.UtcNow,
        Body = "{}",
    };
}
