using Microsoft.Extensions.Logging.Abstractions;
using RedStream;

namespace RedStream.IntegrationTests;

[Collection(RedisCollection.Name)]
public class ConsumerIntegrationTests
{
    public sealed record OrderPlaced(int OrderId, string Customer);

    private readonly RedisFixture _fixture;

    public ConsumerIntegrationTests(RedisFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Producer_to_consumer_round_trip_acks_on_success()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var receivedTcs = new TaskCompletionSource<MessageContext<OrderPlaced>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var producer = NewProducer(stream);
        var consumer = NewConsumer(stream, group, ctx =>
        {
            receivedTcs.TrySetResult(ctx);
            return Task.CompletedTask;
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var consumerTask = consumer.RunAsync(cts.Token);

        await producer.PublishAsync(
            new OrderPlaced(42, "ada"),
            new PublishOptions { CorrelationId = "corr-x" });

        var received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        received.Payload.Should().Be(new OrderPlaced(42, "ada"));
        received.CorrelationId.Should().Be("corr-x");
        received.Stream.Should().Be(stream);
        received.ConsumerGroup.Should().Be(group);

        // ACK should have happened — XPENDING reports zero pending
        var db = _fixture.Connection.GetDatabase();
        var pending = await db.StreamPendingAsync(stream, group);
        pending.PendingMessageCount.Should().Be(0, "successful handler should ACK and clear the PEL");

        cts.Cancel();
        await consumerTask;
    }

    [Fact]
    public async Task Multiple_messages_all_processed()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var received = new List<int>();
        var lockObj = new object();
        var allReceivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        const int total = 5;

        var producer = NewProducer(stream);
        var consumer = NewConsumer(stream, group, ctx =>
        {
            lock (lockObj)
            {
                received.Add(ctx.Payload.OrderId);
                if (received.Count == total)
                {
                    allReceivedTcs.TrySetResult();
                }
            }
            return Task.CompletedTask;
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var consumerTask = consumer.RunAsync(cts.Token);

        for (var i = 1; i <= total; i++)
        {
            await producer.PublishAsync(new OrderPlaced(i, "ada"));
        }

        await allReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        received.Should().BeEquivalentTo(Enumerable.Range(1, total));

        cts.Cancel();
        await consumerTask;
    }

    [Fact]
    public async Task Handler_exception_leaves_entry_in_pel()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var attemptedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var producer = NewProducer(stream);
        var consumer = NewConsumer(stream, group, _ =>
        {
            attemptedTcs.TrySetResult();
            throw new InvalidOperationException("handler boom");
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var consumerTask = consumer.RunAsync(cts.Token);

        await producer.PublishAsync(new OrderPlaced(1, "ada"));
        await attemptedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Give it a moment in case ACK is delayed
        await Task.Delay(200);

        var db = _fixture.Connection.GetDatabase();
        var pending = await db.StreamPendingAsync(stream, group);
        pending.PendingMessageCount.Should().Be(1, "handler failed; entry should remain in PEL for reaper");

        cts.Cancel();
        await consumerTask;
    }

    [Fact]
    public async Task Cancellation_stops_consumer_cleanly()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var consumer = NewConsumer(stream, group, _ => Task.CompletedTask);

        using var cts = new CancellationTokenSource();
        var consumerTask = consumer.RunAsync(cts.Token);

        await Task.Delay(200);
        cts.Cancel();

        var completed = await Task.WhenAny(consumerTask, Task.Delay(2000));
        completed.Should().BeSameAs(consumerTask, "consumer should exit within ~1 poll interval of cancellation");
    }

    [Fact]
    public async Task Ensure_group_exists_is_idempotent()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var consumer = NewConsumer(stream, group, _ => Task.CompletedTask);

        await consumer.EnsureGroupExistsAsync();
        await consumer.EnsureGroupExistsAsync(); // second call must not throw

        var db = _fixture.Connection.GetDatabase();
        var groups = await db.StreamGroupInfoAsync(stream);
        groups.Should().ContainSingle(g => g.Name == group);
    }

    [Fact]
    public async Task Undecodeable_envelope_is_acked_and_skipped()
    {
        var stream = NewStreamName();
        var group = "test-group";
        var handlerInvokedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = NewConsumer(stream, group, _ =>
        {
            handlerInvokedTcs.TrySetResult();
            return Task.CompletedTask;
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var consumerTask = consumer.RunAsync(cts.Token);
        await Task.Delay(200); // let the group + stream be created

        var db = _fixture.Connection.GetDatabase();
        // Add a raw entry with no envelope fields — should be ACKed and skipped
        await db.StreamAddAsync(stream, new[] { new StackExchange.Redis.NameValueEntry("junk", "data") });

        // Wait briefly to give consumer a chance to process
        await Task.Delay(500);

        var pending = await db.StreamPendingAsync(stream, group);
        pending.PendingMessageCount.Should().Be(0, "undecodeable entry should be ACKed");
        handlerInvokedTcs.Task.IsCompleted.Should().BeFalse("handler should not run for undecodeable entries");

        cts.Cancel();
        await consumerTask;
    }

    private StreamProducer<OrderPlaced> NewProducer(string stream)
    {
        return new StreamProducer<OrderPlaced>(
            _fixture.Connection,
            new JsonMessageSerializer(),
            new DefaultMessageTypeResolver(),
            new RedisServerVersionCache(_fixture.Connection),
            stream,
            new ProducerOptions(),
            new RedStreamOptions(),
            NullLogger<StreamProducer<OrderPlaced>>.Instance);
    }

    private Consumer<OrderPlaced> NewConsumer(
        string stream,
        string group,
        Func<MessageContext<OrderPlaced>, Task> handler)
    {
        var pipeline = new MiddlewarePipelineBuilder()
            .Build(async (ctx, _) =>
            {
                var typed = (MessageContext<OrderPlaced>)ctx;
                await handler(typed);
            });

        var options = new ConsumerOptions
        {
            Stream = stream,
            ConsumerGroup = group,
            ConsumerName = "test-consumer",
            BatchSize = 10,
            MaxConcurrency = 4,
            PollIntervalWhenEmpty = TimeSpan.FromMilliseconds(50),
        };

        return new Consumer<OrderPlaced>(
            _fixture.Connection,
            new JsonMessageSerializer(),
            options,
            pipeline,
            NullLogger<Consumer<OrderPlaced>>.Instance);
    }

    private static string NewStreamName() => $"test-consumer:{Guid.NewGuid():N}";
}
