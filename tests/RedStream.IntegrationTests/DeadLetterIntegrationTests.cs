using Microsoft.Extensions.Logging.Abstractions;
using RedStream;
using StackExchange.Redis;

namespace RedStream.IntegrationTests;

[Collection(RedisCollection.Name)]
public class DeadLetterIntegrationTests
{
    public sealed record OrderPlaced(int OrderId, string Customer);

    private readonly RedisFixture _fixture;

    public DeadLetterIntegrationTests(RedisFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Poison_message_moves_to_dlq_after_max_attempts()
    {
        var stream = NewStreamName();
        var group = "test-group";
        const int maxAttempts = 3;

        var options = NewOptions(stream, group, "test-consumer", maxAttempts);
        var attempts = 0;
        var pipeline = BuildPipelineWithDlq(options, ctx =>
        {
            Interlocked.Increment(ref attempts);
            throw new InvalidOperationException("always fails");
        });

        var (consumer, reaper) = NewConsumerAndReaper(options, pipeline);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var tasks = Task.WhenAll(consumer.RunAsync(cts.Token), reaper.RunAsync(cts.Token));

        await NewProducer(stream).PublishAsync(new OrderPlaced(1, "ada"));

        var db = _fixture.Connection.GetDatabase();
        await EventuallyAsync(async () =>
        {
            var dlqLen = await db.StreamLengthAsync(options.ResolveDeadLetterStream());
            return dlqLen == 1;
        }, TimeSpan.FromSeconds(10));

        attempts.Should().BeGreaterThanOrEqualTo(maxAttempts);
        (await db.StreamPendingAsync(stream, group)).PendingMessageCount.Should().Be(0,
            "DLQ move atomically ACKs the source");

        var dlqEntries = await db.StreamRangeAsync(options.ResolveDeadLetterStream(), count: 5);
        dlqEntries.Should().HaveCount(1);
        var record = DeadLetterEncoder.Decode(dlqEntries[0]);
        record.Metadata.Attempts.Should().BeGreaterThanOrEqualTo(maxAttempts);
        record.Metadata.ErrorType.Should().Be(typeof(InvalidOperationException).FullName);
        record.Metadata.ErrorMessage.Should().Be("always fails");
        record.Metadata.OriginalStream.Should().Be(stream);
        record.OriginalEnvelope.Body.Should().Contain("\"orderId\":1");

        cts.Cancel();
        await tasks;
    }

    [Fact]
    public async Task Permanent_failure_moves_to_dlq_on_first_attempt()
    {
        var stream = NewStreamName();
        var group = "test-group";
        const int maxAttempts = 99;  // high — proves the permanent-failure path bypasses it

        var options = NewOptions(stream, group, "test-consumer", maxAttempts);
        var attempts = 0;
        var pipeline = BuildPipelineWithDlq(options, ctx =>
        {
            Interlocked.Increment(ref attempts);
            throw new PermanentFailureException("validation failed");
        });

        var (consumer, _) = NewConsumerAndReaper(options, pipeline);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var consumerTask = consumer.RunAsync(cts.Token);

        await NewProducer(stream).PublishAsync(new OrderPlaced(2, "ada"));

        var db = _fixture.Connection.GetDatabase();
        await EventuallyAsync(async () =>
            await db.StreamLengthAsync(options.ResolveDeadLetterStream()) == 1,
            TimeSpan.FromSeconds(5));

        attempts.Should().Be(1, "permanent failure should DLQ on first attempt");

        var dlqEntries = await db.StreamRangeAsync(options.ResolveDeadLetterStream(), count: 1);
        var record = DeadLetterEncoder.Decode(dlqEntries[0]);
        record.Metadata.ErrorType.Should().Be(typeof(PermanentFailureException).FullName);
        record.Metadata.Attempts.Should().Be(1);

        cts.Cancel();
        await consumerTask;
    }

    [Fact]
    public async Task Replay_writes_new_entry_to_source_stream_preserving_message_id()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var options = NewOptions(stream, group, "test-consumer", maxAttempts: 99);
        const string explicitId = "deterministic-id";

        // Put a message into the DLQ by failing it permanently
        var pipeline = BuildPipelineWithDlq(options, _ => throw new PermanentFailureException("dies"));
        var (consumer, _) = NewConsumerAndReaper(options, pipeline);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var consumerTask = consumer.RunAsync(cts.Token);

        await NewProducer(stream).PublishAsync(
            new OrderPlaced(3, "ada"),
            new PublishOptions { MessageId = explicitId });

        var db = _fixture.Connection.GetDatabase();
        await EventuallyAsync(async () =>
            await db.StreamLengthAsync(options.ResolveDeadLetterStream()) == 1,
            TimeSpan.FromSeconds(5));

        cts.Cancel();
        await consumerTask;

        // Replay
        var admin = new RedisDeadLetterAdmin(_fixture.Connection);
        var dlqEntries = await db.StreamRangeAsync(options.ResolveDeadLetterStream(), count: 1);
        var dlqEntryId = dlqEntries[0].Id.ToString();

        var newEntryId = await admin.ReplayAsync(options.ResolveDeadLetterStream(), dlqEntryId);

        // Verify the replayed entry exists on the source stream with the same MessageId
        var sourceEntries = await db.StreamRangeAsync(stream, newEntryId, newEntryId, count: 1);
        sourceEntries.Should().HaveCount(1);
        var envelope = EnvelopeEncoder.Decode(sourceEntries[0].Values);
        envelope.MessageId.Should().Be(explicitId, "replay preserves the application-level message id");
        envelope.Body.Should().Contain("\"orderId\":3");
    }

    [Fact]
    public async Task List_returns_newest_first()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var options = NewOptions(stream, group, "test-consumer", maxAttempts: 99);
        var pipeline = BuildPipelineWithDlq(options, _ => throw new PermanentFailureException("x"));
        var (consumer, _) = NewConsumerAndReaper(options, pipeline);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var consumerTask = consumer.RunAsync(cts.Token);

        var producer = NewProducer(stream);
        for (var i = 1; i <= 3; i++)
        {
            await producer.PublishAsync(new OrderPlaced(i, "ada"), new PublishOptions { MessageId = $"id-{i}" });
        }

        var db = _fixture.Connection.GetDatabase();
        await EventuallyAsync(async () =>
            await db.StreamLengthAsync(options.ResolveDeadLetterStream()) == 3,
            TimeSpan.FromSeconds(5));

        var admin = new RedisDeadLetterAdmin(_fixture.Connection);
        var list = await admin.ListAsync(options.ResolveDeadLetterStream(), count: 10);
        list.Should().HaveCount(3);
        list.Select(r => r.OriginalEnvelope.MessageId).Should().Equal("id-3", "id-2", "id-1");

        cts.Cancel();
        await consumerTask;
    }

    [Fact]
    public async Task Get_returns_null_for_missing_entry()
    {
        var stream = NewStreamName();
        var db = _fixture.Connection.GetDatabase();
        // Create the stream so the query doesn't fail
        await db.StreamAddAsync($"{stream}:dlq", new[] { new NameValueEntry("placeholder", "x") });

        var admin = new RedisDeadLetterAdmin(_fixture.Connection);
        var result = await admin.GetAsync($"{stream}:dlq", "999999999-0");

        result.Should().BeNull();
    }

    [Fact]
    public async Task Transient_failure_below_threshold_stays_in_pel()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var options = NewOptions(stream, group, "test-consumer", maxAttempts: 99);
        var pipeline = BuildPipelineWithDlq(options, _ => throw new InvalidOperationException("transient"));
        var (consumer, _) = NewConsumerAndReaper(options, pipeline);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var consumerTask = consumer.RunAsync(cts.Token);

        await NewProducer(stream).PublishAsync(new OrderPlaced(4, "ada"));

        // Wait briefly for the handler to fail
        await Task.Delay(500);

        var db = _fixture.Connection.GetDatabase();
        (await db.StreamPendingAsync(stream, group)).PendingMessageCount.Should().Be(1,
            "transient failure below maxAttempts should leave the entry in PEL");
        (await db.StreamLengthAsync(options.ResolveDeadLetterStream())).Should().Be(0,
            "no DLQ move should have happened");

        cts.Cancel();
        await consumerTask;
    }

    private ConsumerDelegate BuildPipelineWithDlq(ConsumerOptions options, Action<MessageContext> handler)
    {
        var dlq = new RedisDeadLetter(_fixture.Connection);
        var dlqMiddleware = new DeadLetterMiddleware<OrderPlaced>(
            dlq,
            new JsonMessageSerializer(),
            options,
            NullLogger<DeadLetterMiddleware<OrderPlaced>>.Instance);
        return new MiddlewarePipelineBuilder()
            .Use(dlqMiddleware)
            .Build((ctx, _) =>
            {
                handler(ctx);
                return Task.CompletedTask;
            });
    }

    private (Consumer<OrderPlaced> consumer, Reaper<OrderPlaced> reaper) NewConsumerAndReaper(
        ConsumerOptions options,
        ConsumerDelegate pipeline)
    {
        var consumer = new Consumer<OrderPlaced>(
            _fixture.Connection, new JsonMessageSerializer(), options, pipeline,
            NullLogger<Consumer<OrderPlaced>>.Instance);
        var reaper = new Reaper<OrderPlaced>(
            _fixture.Connection, new JsonMessageSerializer(), options, pipeline,
            NullLogger<Reaper<OrderPlaced>>.Instance);
        return (consumer, reaper);
    }

    private static ConsumerOptions NewOptions(string stream, string group, string consumer, int maxAttempts) => new()
    {
        Stream = stream,
        ConsumerGroup = group,
        ConsumerName = consumer,
        BatchSize = 10,
        MaxConcurrency = 1,
        MaxDeliveryAttempts = maxAttempts,
        IdleReclaimAfter = TimeSpan.FromMilliseconds(150),
        ReaperInterval = TimeSpan.FromMilliseconds(75),
        PollIntervalWhenEmpty = TimeSpan.FromMilliseconds(50),
    };

    private StreamProducer<OrderPlaced> NewProducer(string stream) => new(
        _fixture.Connection,
        new JsonMessageSerializer(),
        new DefaultMessageTypeResolver(),
        new RedisServerVersionCache(_fixture.Connection),
        stream,
        new ProducerOptions(),
        new RedStreamOptions(),
        NullLogger<StreamProducer<OrderPlaced>>.Instance);

    private static string NewStreamName() => $"test-dlq:{Guid.NewGuid():N}";

    private static async Task EventuallyAsync(Func<Task<bool>> predicate, TimeSpan within)
    {
        var deadline = DateTime.UtcNow + within;
        while (DateTime.UtcNow < deadline)
        {
            if (await predicate())
            {
                return;
            }
            await Task.Delay(50);
        }
        if (!await predicate())
        {
            throw new TimeoutException($"Condition was not met within {within}.");
        }
    }
}
