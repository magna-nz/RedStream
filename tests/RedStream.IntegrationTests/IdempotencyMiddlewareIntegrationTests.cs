using Microsoft.Extensions.Logging.Abstractions;
using RedStream;

namespace RedStream.IntegrationTests;

[Collection(RedisCollection.Name)]
public class IdempotencyMiddlewareIntegrationTests
{
    public sealed record OrderPlaced(int OrderId);

    private readonly RedisFixture _fixture;

    public IdempotencyMiddlewareIntegrationTests(RedisFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Same_message_id_skips_handler_on_redelivery()
    {
        var stream = NewStreamName();
        var group = "test-group";
        const string id = "same-id";
        var invocations = 0;

        var options = NewOptions(stream, group);
        var pipeline = BuildPipeline(options, _ =>
        {
            Interlocked.Increment(ref invocations);
            return Task.CompletedTask;
        });
        var consumer = NewConsumer(options, pipeline);

        // Ensure the group is at the head BEFORE publishing so the consumer sees the messages.
        await consumer.EnsureGroupExistsAsync();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var consumerTask = consumer.RunAsync(cts.Token);

        // Two distinct producer ids so producer-side IDMP doesn't dedupe at the wire —
        // both XADDs must land so consumer-side dedup is exercised.
        var producerA = NewProducer(stream, producerId: "producer-a");
        var producerB = NewProducer(stream, producerId: "producer-b");
        await producerA.PublishAsync(new OrderPlaced(1), new PublishOptions { MessageId = id });
        await producerB.PublishAsync(new OrderPlaced(2), new PublishOptions { MessageId = id });

        // Wait for both entries to land and the PEL to drain (both ACKed even though one was skipped).
        await EventuallyAsync(async () =>
        {
            var db = _fixture.Connection.GetDatabase();
            var len = await db.StreamLengthAsync(stream);
            var pending = (await db.StreamPendingAsync(stream, group)).PendingMessageCount;
            // Both entries must have entered the PEL (delivered to the consumer) then drained.
            // We assert handler ran exactly once below.
            return len == 2 && pending == 0 && invocations >= 1;
        }, TimeSpan.FromSeconds(5));

        invocations.Should().Be(1, "handler must only run once for the same MessageId");

        cts.Cancel();
        await consumerTask;
    }

    [Fact]
    public async Task Different_message_ids_are_each_processed()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var ids = new List<string>();
        var lockObj = new object();

        var options = NewOptions(stream, group);
        var pipeline = BuildPipeline(options, ctx =>
        {
            lock (lockObj) { ids.Add(ctx.MessageId); }
            return Task.CompletedTask;
        });
        var consumer = NewConsumer(options, pipeline);

        await consumer.EnsureGroupExistsAsync();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var consumerTask = consumer.RunAsync(cts.Token);

        var producer = NewProducer(stream);
        await producer.PublishAsync(new OrderPlaced(1), new PublishOptions { MessageId = "a" });
        await producer.PublishAsync(new OrderPlaced(2), new PublishOptions { MessageId = "b" });
        await producer.PublishAsync(new OrderPlaced(3), new PublishOptions { MessageId = "c" });

        await EventuallyAsync(() =>
        {
            lock (lockObj) { return Task.FromResult(ids.Count == 3); }
        }, TimeSpan.FromSeconds(5));

        ids.Should().BeEquivalentTo(["a", "b", "c"]);

        cts.Cancel();
        await consumerTask;
    }

    [Fact]
    public async Task Dedupe_key_persists_with_ttl()
    {
        var stream = NewStreamName();
        var group = "test-group";
        const string id = "persistent-id";

        var options = NewOptions(stream, group);
        var pipeline = BuildPipeline(options, _ => Task.CompletedTask);
        var consumer = NewConsumer(options, pipeline);

        await consumer.EnsureGroupExistsAsync();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var consumerTask = consumer.RunAsync(cts.Token);

        await NewProducer(stream).PublishAsync(
            new OrderPlaced(1), new PublishOptions { MessageId = id });

        var key = $"{IdempotencyMiddleware.KeyPrefix}{group}:{id}";
        var db = _fixture.Connection.GetDatabase();
        await EventuallyAsync(async () => await db.KeyExistsAsync(key), TimeSpan.FromSeconds(5));

        var ttl = await db.KeyTimeToLiveAsync(key);
        ttl.Should().NotBeNull();
        ttl!.Value.Should().BeGreaterThan(TimeSpan.Zero);
        ttl.Value.Should().BeLessThanOrEqualTo(options.DedupeWindow);

        cts.Cancel();
        await consumerTask;
    }

    private static ConsumerOptions NewOptions(string stream, string group) => new()
    {
        Stream = stream,
        ConsumerGroup = group,
        ConsumerName = "test-consumer",
        BatchSize = 10,
        MaxConcurrency = 1,
        DedupeWindow = TimeSpan.FromMinutes(5),
        PollIntervalWhenEmpty = TimeSpan.FromMilliseconds(50),
    };

    private ConsumerDelegate BuildPipeline(ConsumerOptions options, Func<MessageContext, Task> handler)
    {
        var middleware = new IdempotencyMiddleware(
            _fixture.Connection,
            options,
            NullLogger<IdempotencyMiddleware>.Instance);
        return new MiddlewarePipelineBuilder()
            .Use(middleware)
            .Build(async (ctx, _) => await handler(ctx));
    }

    private Consumer<OrderPlaced> NewConsumer(ConsumerOptions options, ConsumerDelegate pipeline)
        => new(
            _fixture.Connection,
            new JsonMessageSerializer(),
            options,
            pipeline,
            NullLogger<Consumer<OrderPlaced>>.Instance);

    private StreamProducer<OrderPlaced> NewProducer(string stream, string? producerId = null) => new(
        _fixture.Connection,
        new JsonMessageSerializer(),
        new DefaultMessageTypeResolver(),
        new RedisServerVersionCache(_fixture.Connection),
        stream,
        new ProducerOptions { ProducerId = producerId },
        new RedStreamOptions(),
        NullLogger<StreamProducer<OrderPlaced>>.Instance);

    private static string NewStreamName() => $"test-idempotency:{Guid.NewGuid():N}";

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
