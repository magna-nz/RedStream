using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace RedStream;

/// <summary>
/// Built-in middleware that logs every handled message with structured fields
/// (<c>MessageId</c>, <c>Stream</c>, <c>ConsumerGroup</c>, <c>DeliveryCount</c>,
/// <c>DurationMs</c>) and re-throws on handler failure with an error-level log.
/// On by default; remove or replace via the DI builder.
/// </summary>
public sealed class LoggingMiddleware : IConsumerMiddleware
{
    private readonly ILogger<LoggingMiddleware> _logger;

    /// <summary>Construct with the injected logger.</summary>
    public LoggingMiddleware(ILogger<LoggingMiddleware> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task InvokeAsync(MessageContext context, ConsumerDelegate next, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);

        var stopwatch = Stopwatch.StartNew();
        try
        {
            await next(context, ct).ConfigureAwait(false);
            stopwatch.Stop();
            _logger.LogInformation(
                "Processed message {MessageId} on {Stream}/{ConsumerGroup} in {DurationMs}ms (delivery #{DeliveryCount})",
                context.MessageId,
                context.Stream,
                context.ConsumerGroup,
                stopwatch.Elapsed.TotalMilliseconds,
                context.DeliveryCount);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(
                ex,
                "Failed to process message {MessageId} on {Stream}/{ConsumerGroup} after {DurationMs}ms (delivery #{DeliveryCount})",
                context.MessageId,
                context.Stream,
                context.ConsumerGroup,
                stopwatch.Elapsed.TotalMilliseconds,
                context.DeliveryCount);
            throw;
        }
    }
}
