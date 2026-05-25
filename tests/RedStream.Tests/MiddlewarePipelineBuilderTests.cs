namespace RedStream.Tests;

public class MiddlewarePipelineBuilderTests
{
    [Fact]
    public async Task Empty_pipeline_invokes_terminal()
    {
        var calls = new List<string>();
        var pipeline = new MiddlewarePipelineBuilder()
            .Build((_, _) => { calls.Add("terminal"); return Task.CompletedTask; });

        await pipeline(NewContext(), CancellationToken.None);

        calls.Should().ContainSingle().Which.Should().Be("terminal");
    }

    [Fact]
    public async Task Middleware_runs_in_registration_order_then_terminal()
    {
        var calls = new List<string>();
        var pipeline = new MiddlewarePipelineBuilder()
            .Use(new RecordingMiddleware("a", calls))
            .Use(new RecordingMiddleware("b", calls))
            .Use(new RecordingMiddleware("c", calls))
            .Build((_, _) => { calls.Add("terminal"); return Task.CompletedTask; });

        await pipeline(NewContext(), CancellationToken.None);

        calls.Should().Equal("a:before", "b:before", "c:before", "terminal", "c:after", "b:after", "a:after");
    }

    [Fact]
    public async Task Short_circuit_middleware_skips_inner_pipeline()
    {
        var calls = new List<string>();
        var pipeline = new MiddlewarePipelineBuilder()
            .Use(new RecordingMiddleware("outer", calls))
            .Use(new ShortCircuitMiddleware(calls))
            .Use(new RecordingMiddleware("never", calls))
            .Build((_, _) => { calls.Add("terminal"); return Task.CompletedTask; });

        await pipeline(NewContext(), CancellationToken.None);

        calls.Should().Equal("outer:before", "short-circuit", "outer:after");
    }

    [Fact]
    public async Task Exception_in_middleware_propagates()
    {
        var calls = new List<string>();
        var pipeline = new MiddlewarePipelineBuilder()
            .Use(new RecordingMiddleware("outer", calls))
            .Use(new ThrowingMiddleware())
            .Build((_, _) => Task.CompletedTask);

        var act = () => pipeline(NewContext(), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("boom");
        calls.Should().Equal("outer:before");
    }

    [Fact]
    public async Task Cancellation_token_flows_through()
    {
        CancellationToken seenInside = default;
        var pipeline = new MiddlewarePipelineBuilder()
            .Use(new InlineMiddleware((ctx, next, ct) => next(ctx, ct)))
            .Build((_, ct) => { seenInside = ct; return Task.CompletedTask; });

        using var cts = new CancellationTokenSource();
        await pipeline(NewContext(), cts.Token);

        seenInside.Should().Be(cts.Token);
    }

    [Fact]
    public void Use_null_throws()
    {
        var builder = new MiddlewarePipelineBuilder();
        builder.Invoking(b => b.Use(null!)).Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Build_null_terminal_throws()
    {
        var builder = new MiddlewarePipelineBuilder();
        builder.Invoking(b => b.Build(null!)).Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Count_reflects_registered_middleware()
    {
        var builder = new MiddlewarePipelineBuilder();
        builder.Count.Should().Be(0);
        builder.Use(new InlineMiddleware((c, n, ct) => n(c, ct)));
        builder.Count.Should().Be(1);
    }

    private static MessageContext NewContext() => new MessageContext<string>
    {
        MessageId = "m",
        StreamEntryId = "1-0",
        TypeId = "T",
        PublishedAt = DateTimeOffset.UtcNow,
        Stream = "s",
        ConsumerGroup = "g",
        Consumer = "c",
        DeliveryCount = 1,
        Headers = new Dictionary<string, string>(),
        Payload = "p",
    };

    private sealed class RecordingMiddleware : IConsumerMiddleware
    {
        private readonly string _name;
        private readonly List<string> _calls;

        public RecordingMiddleware(string name, List<string> calls)
        {
            _name = name;
            _calls = calls;
        }

        public async Task InvokeAsync(MessageContext context, ConsumerDelegate next, CancellationToken ct)
        {
            _calls.Add($"{_name}:before");
            await next(context, ct);
            _calls.Add($"{_name}:after");
        }
    }

    private sealed class ShortCircuitMiddleware : IConsumerMiddleware
    {
        private readonly List<string> _calls;
        public ShortCircuitMiddleware(List<string> calls) => _calls = calls;

        public Task InvokeAsync(MessageContext context, ConsumerDelegate next, CancellationToken ct)
        {
            _calls.Add("short-circuit");
            return Task.CompletedTask; // intentionally does not call next
        }
    }

    private sealed class ThrowingMiddleware : IConsumerMiddleware
    {
        public Task InvokeAsync(MessageContext context, ConsumerDelegate next, CancellationToken ct)
            => throw new InvalidOperationException("boom");
    }

    private sealed class InlineMiddleware : IConsumerMiddleware
    {
        private readonly Func<MessageContext, ConsumerDelegate, CancellationToken, Task> _body;
        public InlineMiddleware(Func<MessageContext, ConsumerDelegate, CancellationToken, Task> body) => _body = body;
        public Task InvokeAsync(MessageContext context, ConsumerDelegate next, CancellationToken ct)
            => _body(context, next, ct);
    }
}
