namespace RedStream.Tests;

public class ConsumerOptionsTests
{
    [Fact]
    public void Defaults_pass_validation_when_required_fields_set()
    {
        var opts = NewValid();
        opts.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Theory]
    [InlineData(nameof(ConsumerOptions.Stream))]
    [InlineData(nameof(ConsumerOptions.ConsumerGroup))]
    [InlineData(nameof(ConsumerOptions.ConsumerName))]
    public void Missing_required_string_throws(string field)
    {
        var opts = NewValid();
        typeof(ConsumerOptions).GetProperty(field)!.SetValue(opts, "");

        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage($"*{field}*");
    }

    [Theory]
    [InlineData(nameof(ConsumerOptions.BatchSize), 0)]
    [InlineData(nameof(ConsumerOptions.BatchSize), -1)]
    [InlineData(nameof(ConsumerOptions.MaxConcurrency), 0)]
    [InlineData(nameof(ConsumerOptions.MaxConcurrency), -1)]
    [InlineData(nameof(ConsumerOptions.MaxDeliveryAttempts), 0)]
    [InlineData(nameof(ConsumerOptions.MaxDeliveryAttempts), -1)]
    public void Sub_one_int_throws(string field, int value)
    {
        var opts = NewValid();
        typeof(ConsumerOptions).GetProperty(field)!.SetValue(opts, value);

        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage($"*{field}*");
    }

    [Fact]
    public void Non_positive_dedupe_window_throws()
    {
        var opts = NewValid();
        opts.DedupeWindow = TimeSpan.Zero;

        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage($"*{nameof(ConsumerOptions.DedupeWindow)}*");
    }

    [Fact]
    public void Non_positive_reaper_interval_throws()
    {
        var opts = NewValid();
        opts.ReaperInterval = TimeSpan.Zero;

        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage($"*{nameof(ConsumerOptions.ReaperInterval)}*");
    }

    [Fact]
    public void Negative_idle_reclaim_throws()
    {
        var opts = NewValid();
        opts.IdleReclaimAfter = TimeSpan.FromSeconds(-1);

        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage($"*{nameof(ConsumerOptions.IdleReclaimAfter)}*");
    }

    [Fact]
    public void Negative_poll_interval_throws()
    {
        var opts = NewValid();
        opts.PollIntervalWhenEmpty = TimeSpan.FromMilliseconds(-1);

        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage($"*{nameof(ConsumerOptions.PollIntervalWhenEmpty)}*");
    }

    [Fact]
    public void Negative_shutdown_timeout_throws()
    {
        var opts = NewValid();
        opts.ShutdownTimeout = TimeSpan.FromSeconds(-1);

        opts.Invoking(o => o.Validate())
            .Should().Throw<ArgumentException>()
            .WithMessage($"*{nameof(ConsumerOptions.ShutdownTimeout)}*");
    }

    [Fact]
    public void Resolve_dlq_uses_convention_when_unset()
    {
        var opts = new ConsumerOptions { Stream = "orders", ConsumerGroup = "g" };
        opts.ResolveDeadLetterStream().Should().Be("orders:dlq");
    }

    [Fact]
    public void Resolve_dlq_uses_explicit_value_when_set()
    {
        var opts = new ConsumerOptions
        {
            Stream = "orders",
            ConsumerGroup = "g",
            DeadLetterStream = "custom:dlq",
        };
        opts.ResolveDeadLetterStream().Should().Be("custom:dlq");
    }

    [Fact]
    public void Defaults_are_sensible()
    {
        var opts = new ConsumerOptions();
        opts.BatchSize.Should().Be(10);
        opts.MaxConcurrency.Should().Be(Environment.ProcessorCount);
        opts.IdleReclaimAfter.Should().Be(TimeSpan.FromSeconds(30));
        opts.ReaperInterval.Should().Be(TimeSpan.FromSeconds(15));
        opts.MaxDeliveryAttempts.Should().Be(5);
        opts.DedupeWindow.Should().Be(TimeSpan.FromHours(24));
        opts.PollIntervalWhenEmpty.Should().Be(TimeSpan.FromMilliseconds(100));
        opts.ShutdownTimeout.Should().Be(TimeSpan.FromSeconds(30));
        opts.StartPosition.Should().Be(ConsumerStartPosition.LatestOnly);
        opts.ConsumerName.Should().Be(Environment.MachineName);
    }

    private static ConsumerOptions NewValid() => new()
    {
        Stream = "orders",
        ConsumerGroup = "fulfillment",
    };
}
