using StackExchange.Redis;

namespace RedStream.Tests;

public class DeadLetterEncoderTests
{
    [Fact]
    public void Round_trip_preserves_envelope_and_metadata()
    {
        var envelope = new MessageEnvelope
        {
            Version = MessageEnvelope.CurrentVersion,
            TypeId = "MyApp.OrderPlaced",
            MessageId = "msg-1",
            Timestamp = new DateTimeOffset(2026, 5, 25, 12, 0, 0, TimeSpan.Zero),
            Body = """{"orderId":42}""",
            TraceParent = "00-trace-span-01",
            CorrelationId = "corr-1",
            CausationId = "caus-1",
            Headers = """{"x":"y"}""",
        };
        var meta = new DeadLetterMetadata
        {
            OriginalStreamEntryId = "100-0",
            OriginalStream = "orders",
            OriginalGroup = "fulfillment",
            Attempts = 5,
            FirstSeen = new DateTimeOffset(2026, 5, 25, 11, 0, 0, TimeSpan.Zero),
            LastSeen = new DateTimeOffset(2026, 5, 25, 12, 5, 0, TimeSpan.Zero),
            ErrorType = "System.InvalidOperationException",
            ErrorMessage = "boom",
            ErrorStack = "   at MyApp.Handler...",
        };

        var entries = DeadLetterEncoder.Encode(envelope, meta);
        var entry = new StreamEntry((RedisValue)"200-0", entries);
        var record = DeadLetterEncoder.Decode(entry);

        record.DlqEntryId.Should().Be("200-0");
        record.OriginalEnvelope.Should().BeEquivalentTo(envelope);
        record.Metadata.Should().BeEquivalentTo(meta);
    }

    [Fact]
    public void Round_trip_with_minimal_metadata()
    {
        var envelope = MinimalEnvelope();
        var meta = new DeadLetterMetadata
        {
            OriginalStreamEntryId = "1-0",
            OriginalStream = "s",
            OriginalGroup = "g",
            Attempts = 1,
            FirstSeen = DateTimeOffset.UtcNow,
            LastSeen = DateTimeOffset.UtcNow,
        };

        var record = DeadLetterEncoder.Decode(
            new StreamEntry((RedisValue)"99-0", DeadLetterEncoder.Encode(envelope, meta)));

        record.Metadata.ErrorType.Should().BeNull();
        record.Metadata.ErrorMessage.Should().BeNull();
        record.Metadata.ErrorStack.Should().BeNull();
        record.OriginalEnvelope.Should().BeEquivalentTo(envelope);
    }

    [Fact]
    public void Truncates_long_stack_traces()
    {
        var envelope = MinimalEnvelope();
        var hugeStack = new string('x', DeadLetterEncoder.StackTraceMaxLength * 2);
        var meta = new DeadLetterMetadata
        {
            OriginalStreamEntryId = "1-0",
            OriginalStream = "s",
            OriginalGroup = "g",
            Attempts = 1,
            FirstSeen = DateTimeOffset.UtcNow,
            LastSeen = DateTimeOffset.UtcNow,
            ErrorStack = hugeStack,
        };

        var record = DeadLetterEncoder.Decode(
            new StreamEntry((RedisValue)"1-0", DeadLetterEncoder.Encode(envelope, meta)));

        record.Metadata.ErrorStack!.Length.Should().Be(DeadLetterEncoder.StackTraceMaxLength);
    }

    [Fact]
    public void Encode_throws_on_null_envelope()
    {
        var meta = new DeadLetterMetadata
        {
            OriginalStreamEntryId = "1-0",
            OriginalStream = "s",
            OriginalGroup = "g",
            Attempts = 1,
            FirstSeen = DateTimeOffset.UtcNow,
            LastSeen = DateTimeOffset.UtcNow,
        };
        var act = () => DeadLetterEncoder.Encode(null!, meta);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Encode_throws_on_null_metadata()
    {
        var act = () => DeadLetterEncoder.Encode(MinimalEnvelope(), null!);
        act.Should().Throw<ArgumentNullException>();
    }

    private static MessageEnvelope MinimalEnvelope() => new()
    {
        Version = MessageEnvelope.CurrentVersion,
        TypeId = "T",
        MessageId = "m",
        Timestamp = DateTimeOffset.UtcNow,
        Body = "{}",
    };
}
