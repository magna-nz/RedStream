using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace RedStream.Tests;

public class ConsumerTests
{
    public sealed record OrderPlaced(int OrderId);

    [Fact]
    public void Constructor_validates_null_args()
    {
        var connection = Substitute.For<IConnectionMultiplexer>();
        var serializer = new JsonMessageSerializer();
        var options = NewValidOptions();
        ConsumerDelegate pipeline = (_, _) => Task.CompletedTask;
        var logger = NullLogger<Consumer<OrderPlaced>>.Instance;

        FluentActions.Invoking(() => new Consumer<OrderPlaced>(null!, serializer, options, pipeline, logger))
            .Should().Throw<ArgumentNullException>();
        FluentActions.Invoking(() => new Consumer<OrderPlaced>(connection, null!, options, pipeline, logger))
            .Should().Throw<ArgumentNullException>();
        FluentActions.Invoking(() => new Consumer<OrderPlaced>(connection, serializer, null!, pipeline, logger))
            .Should().Throw<ArgumentNullException>();
        FluentActions.Invoking(() => new Consumer<OrderPlaced>(connection, serializer, options, null!, logger))
            .Should().Throw<ArgumentNullException>();
        FluentActions.Invoking(() => new Consumer<OrderPlaced>(connection, serializer, options, pipeline, null!))
            .Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_runs_options_validation()
    {
        var connection = Substitute.For<IConnectionMultiplexer>();
        var serializer = new JsonMessageSerializer();
        var invalid = new ConsumerOptions { Stream = "", ConsumerGroup = "g" }; // missing stream
        ConsumerDelegate pipeline = (_, _) => Task.CompletedTask;

        FluentActions.Invoking(() => new Consumer<OrderPlaced>(
                connection, serializer, invalid, pipeline, NullLogger<Consumer<OrderPlaced>>.Instance))
            .Should().Throw<ArgumentException>()
            .WithMessage("*Stream*");
    }

    private static ConsumerOptions NewValidOptions() => new()
    {
        Stream = "s",
        ConsumerGroup = "g",
    };
}
