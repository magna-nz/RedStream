using Microsoft.Extensions.Logging;

namespace RedStream.Tests;

public class LoggingMiddlewareTests
{
    [Fact]
    public async Task Logs_information_on_success_with_all_fields()
    {
        var logger = new TestLogger<LoggingMiddleware>();
        var middleware = new LoggingMiddleware(logger);

        await middleware.InvokeAsync(NewContext(), (_, _) => Task.CompletedTask, CancellationToken.None);

        logger.Entries.Should().ContainSingle();
        var entry = logger.Entries[0];
        entry.Level.Should().Be(LogLevel.Information);
        entry.Exception.Should().BeNull();
        entry.Message.Should().Contain("msg-1");
        entry.Message.Should().Contain("orders");
        entry.Message.Should().Contain("fulfillment");
        entry.Message.Should().Contain("delivery #1");
    }

    [Fact]
    public async Task Logs_error_with_exception_then_rethrows()
    {
        var logger = new TestLogger<LoggingMiddleware>();
        var middleware = new LoggingMiddleware(logger);
        var boom = new InvalidOperationException("kaboom");

        var act = () => middleware.InvokeAsync(NewContext(), (_, _) => throw boom, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("kaboom");
        logger.Entries.Should().ContainSingle();
        var entry = logger.Entries[0];
        entry.Level.Should().Be(LogLevel.Error);
        entry.Exception.Should().BeSameAs(boom);
        entry.Message.Should().Contain("msg-1");
        entry.Message.Should().Contain("orders");
    }

    [Fact]
    public async Task Includes_duration_in_message()
    {
        var logger = new TestLogger<LoggingMiddleware>();
        var middleware = new LoggingMiddleware(logger);

        await middleware.InvokeAsync(
            NewContext(),
            async (_, ct) => await Task.Delay(10, ct),
            CancellationToken.None);

        logger.Entries[0].Message.Should().MatchRegex(@"in \d+(\.\d+)?ms");
    }

    [Fact]
    public void Constructor_validates_null_logger()
    {
        var act = () => new LoggingMiddleware(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    private static MessageContext NewContext(int deliveryCount = 1) => new MessageContext<string>
    {
        MessageId = "msg-1",
        StreamEntryId = "1-0",
        TypeId = "T",
        PublishedAt = DateTimeOffset.UtcNow,
        Stream = "orders",
        ConsumerGroup = "fulfillment",
        Consumer = "consumer-1",
        DeliveryCount = deliveryCount,
        Headers = new Dictionary<string, string>(),
        Payload = "p",
    };
}
