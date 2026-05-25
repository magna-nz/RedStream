using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Consumer-side idempotency: dedupes redelivered messages so the handler does not
/// run twice for the same application-level <see cref="MessageContext.MessageId"/>.
/// </summary>
/// <remarks>
/// Pattern: <strong>process then mark</strong>.
/// <list type="number">
///   <item>Pre-check: if <c>dedupe:{group}:{messageId}</c> exists, short-circuit
///     (the consumer will ACK without calling the handler).</item>
///   <item>Run the handler (call <c>next</c>).</item>
///   <item>On success, <c>SET dedupe:{group}:{messageId} 1 EX {DedupeWindow} NX</c>.</item>
/// </list>
/// <para>
/// This is <strong>defense in depth, not exactly-once</strong>. There is a real race
/// window between the pre-check and the post-handler mark: two consumers can both
/// pass the pre-check and both run the handler. Handlers should still be idempotent
/// at the business layer (e.g. <c>INSERT ... ON CONFLICT (message_id)</c>) for the
/// strongest guarantee. This middleware covers the common case where that isn't
/// feasible and dramatically narrows the window for duplicate processing.
/// </para>
/// <para>
/// <see cref="ConsumerOptions.DedupeWindow"/> must be ≥ the source stream's retention
/// or the dedupe key may expire before a delayed redelivery is recognised.
/// </para>
/// </remarks>
public sealed class IdempotencyMiddleware : IConsumerMiddleware
{
    /// <summary>The dedupe key prefix (<c>dedupe:</c>).</summary>
    public const string KeyPrefix = "dedupe:";

    private readonly IConnectionMultiplexer _connection;
    private readonly ConsumerOptions _options;
    private readonly ILogger<IdempotencyMiddleware> _logger;

    /// <summary>Construct.</summary>
    public IdempotencyMiddleware(
        IConnectionMultiplexer connection,
        ConsumerOptions options,
        ILogger<IdempotencyMiddleware> logger)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _connection = connection;
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task InvokeAsync(MessageContext context, ConsumerDelegate next, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);

        var db = _connection.GetDatabase();
        var key = BuildKey(context);

        if (await db.KeyExistsAsync(key).ConfigureAwait(false))
        {
            _logger.LogInformation(
                "Skipping already-processed message {MessageId} on {Stream}/{ConsumerGroup}",
                context.MessageId, context.Stream, context.ConsumerGroup);
            return; // short-circuit; consumer ACKs as if handler succeeded
        }

        await next(context, ct).ConfigureAwait(false);

        // Mark after the handler completes successfully. If we crash before this point,
        // the message will be redelivered (acceptable: handler will run again, and the
        // primary safeguard is handler-level idempotency).
        await db.StringSetAsync(key, "1", _options.DedupeWindow, when: When.NotExists).ConfigureAwait(false);
    }

    private static string BuildKey(MessageContext context)
        => $"{KeyPrefix}{context.ConsumerGroup}:{context.MessageId}";
}
