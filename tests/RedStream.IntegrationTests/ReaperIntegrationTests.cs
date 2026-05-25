using Microsoft.Extensions.Logging.Abstractions;
using RedStream;
using StackExchange.Redis;

namespace RedStream.IntegrationTests;

[Collection(RedisCollection.Name)]
public class ReaperIntegrationTests
{
    public sealed record OrderPlaced(int OrderId, string Customer);

    private readonly RedisFixture _fixture;

    public ReaperIntegrationTests(RedisFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Reaper_redelivers_failed_handler_and_second_attempt_succeeds()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var attempts = 0;
        var successTcs = new TaskCompletionSource<MessageContext<OrderPlaced>>(TaskCreationOptions.RunContinuationsAsynchronously);

        var options = NewOptions(stream, group, "test-consumer");
        var pipeline = new MiddlewarePipelineBuilder().Build((ctx, _) =>
        {
            var typed = (MessageContext<OrderPlaced>)ctx;
            var attempt = Interlocked.Increment(ref attempts);
            if (attempt == 1)
            {
                throw new InvalidOperationException("first attempt fails");
            }
            successTcs.TrySetResult(typed);
            return Task.CompletedTask;
        });

        var consumer = new Consumer<OrderPlaced>(
            _fixture.Connection, new JsonMessageSerializer(), options, pipeline,
            NullLogger<Consumer<OrderPlaced>>.Instance);
        var reaper = new Reaper<OrderPlaced>(
            _fixture.Connection, new JsonMessageSerializer(), options, pipeline,
            NullLogger<Reaper<OrderPlaced>>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var consumerTask = consumer.RunAsync(cts.Token);
        var reaperTask = reaper.RunAsync(cts.Token);

        await NewProducer(stream).PublishAsync(new OrderPlaced(1, "ada"));

        var success = await successTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        success.Payload.OrderId.Should().Be(1);
        success.DeliveryCount.Should().BeGreaterThanOrEqualTo(2,
            "reaper redelivery should surface an incremented count");

        // Successful retry ACKs. PEL drains.
        await EventuallyAsync(async () =>
        {
            var pending = await _fixture.Connection.GetDatabase().StreamPendingAsync(stream, group);
            return pending.PendingMessageCount == 0;
        }, TimeSpan.FromSeconds(5));

        cts.Cancel();
        await Task.WhenAll(consumerTask, reaperTask);
    }

    [Fact]
    public async Task Reaper_claims_from_a_dead_consumer()
    {
        var stream = NewStreamName();
        var group = "test-group";
        const string deadConsumer = "dead-consumer-a";
        var processedTcs = new TaskCompletionSource<MessageContext<OrderPlaced>>(TaskCreationOptions.RunContinuationsAsynchronously);

        var db = _fixture.Connection.GetDatabase();
        await db.StreamCreateConsumerGroupAsync(stream, group, StreamPosition.NewMessages, createStream: true);

        await NewProducer(stream).PublishAsync(new OrderPlaced(7, "ada"));

        // "Dead" consumer reads but never ACKs.
        var entries = await db.StreamReadGroupAsync(stream, group, deadConsumer, StreamPosition.NewMessages, count: 10);
        entries.Should().HaveCount(1);

        // Wait past the idle threshold the reaper will use.
        await Task.Delay(300);

        var options = NewOptions(stream, group, "reaper-b");
        var pipeline = new MiddlewarePipelineBuilder().Build((ctx, _) =>
        {
            processedTcs.TrySetResult((MessageContext<OrderPlaced>)ctx);
            return Task.CompletedTask;
        });

        var reaper = new Reaper<OrderPlaced>(
            _fixture.Connection, new JsonMessageSerializer(), options, pipeline,
            NullLogger<Reaper<OrderPlaced>>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var reaperTask = reaper.RunAsync(cts.Token);

        var processed = await processedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        processed.Payload.OrderId.Should().Be(7);
        processed.Consumer.Should().Be("reaper-b");
        processed.DeliveryCount.Should().BeGreaterThanOrEqualTo(2);

        await EventuallyAsync(async () =>
        {
            var pending = await db.StreamPendingAsync(stream, group);
            return pending.PendingMessageCount == 0;
        }, TimeSpan.FromSeconds(5));

        cts.Cancel();
        await reaperTask;
    }

    [Fact]
    public async Task Reaper_is_a_noop_when_pel_is_empty()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var db = _fixture.Connection.GetDatabase();
        await db.StreamCreateConsumerGroupAsync(stream, group, StreamPosition.NewMessages, createStream: true);

        var handlerCalls = 0;
        var options = NewOptions(stream, group, "reaper");
        var pipeline = new MiddlewarePipelineBuilder().Build((_, _) =>
        {
            Interlocked.Increment(ref handlerCalls);
            return Task.CompletedTask;
        });

        var reaper = new Reaper<OrderPlaced>(
            _fixture.Connection, new JsonMessageSerializer(), options, pipeline,
            NullLogger<Reaper<OrderPlaced>>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        await reaper.RunAsync(cts.Token);

        handlerCalls.Should().Be(0, "no PEL entries should mean the handler is never invoked");
    }

    private static ConsumerOptions NewOptions(string stream, string group, string consumer) => new()
    {
        Stream = stream,
        ConsumerGroup = group,
        ConsumerName = consumer,
        BatchSize = 10,
        MaxConcurrency = 1,
        IdleReclaimAfter = TimeSpan.FromMilliseconds(200),
        ReaperInterval = TimeSpan.FromMilliseconds(100),
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

    private static string NewStreamName() => $"test-reaper:{Guid.NewGuid():N}";

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
