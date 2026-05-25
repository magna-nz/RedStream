using Microsoft.Extensions.Logging;

namespace RedStream;

/// <summary>
/// Middleware that decides when to move a failing message to the DLQ.
/// </summary>
/// <remarks>
/// Decision rules:
/// <list type="bullet">
///   <item><see cref="PermanentFailureException"/> → DLQ on first failure.</item>
///   <item>Any other exception with <see cref="MessageContext.DeliveryCount"/> ≥
///     <see cref="ConsumerOptions.MaxDeliveryAttempts"/> → DLQ.</item>
///   <item>Otherwise the exception is rethrown so the entry remains in the PEL for
///     <see cref="Reaper{T}"/> to redeliver later.</item>
/// </list>
/// On move, the original envelope is preserved (so replay round-trips) and
/// <c>dlq.*</c> metadata fields are appended with source IDs, attempt count,
/// timestamps, and the exception type/message/stack.
/// </remarks>
public sealed class DeadLetterMiddleware<T> : IConsumerMiddleware
{
    private readonly IDeadLetter _deadLetter;
    private readonly IMessageSerializer _serializer;
    private readonly ConsumerOptions _options;
    private readonly ILogger<DeadLetterMiddleware<T>> _logger;

    /// <summary>Construct.</summary>
    public DeadLetterMiddleware(
        IDeadLetter deadLetter,
        IMessageSerializer serializer,
        ConsumerOptions options,
        ILogger<DeadLetterMiddleware<T>> logger)
    {
        ArgumentNullException.ThrowIfNull(deadLetter);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _deadLetter = deadLetter;
        _serializer = serializer;
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task InvokeAsync(MessageContext context, ConsumerDelegate next, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);

        try
        {
            await next(context, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (PermanentFailureException ex)
        {
            await MoveAsync(context, ex, permanent: true, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (context.DeliveryCount >= _options.MaxDeliveryAttempts)
            {
                await MoveAsync(context, ex, permanent: false, ct).ConfigureAwait(false);
            }
            else
            {
                throw;
            }
        }
    }

    private async Task MoveAsync(MessageContext context, Exception exception, bool permanent, CancellationToken ct)
    {
        var typed = (MessageContext<T>)context;
        var body = _serializer.Serialize(typed.Payload!, typeof(T));
        var headers = SerialiseHeaders(context.Headers);

        var envelope = new MessageEnvelope
        {
            Version = MessageEnvelope.CurrentVersion,
            TypeId = context.TypeId,
            MessageId = context.MessageId,
            Timestamp = context.PublishedAt,
            Body = body,
            TraceParent = context.TraceParent,
            CorrelationId = context.CorrelationId,
            CausationId = context.CausationId,
            Headers = headers,
        };

        var metadata = new DeadLetterMetadata
        {
            OriginalStreamEntryId = context.StreamEntryId,
            OriginalStream = context.Stream,
            OriginalGroup = context.ConsumerGroup,
            Attempts = context.DeliveryCount,
            FirstSeen = context.PublishedAt,
            LastSeen = DateTimeOffset.UtcNow,
            ErrorType = exception.GetType().FullName,
            ErrorMessage = exception.Message,
            ErrorStack = exception.StackTrace,
        };

        var dlqEntries = DeadLetterEncoder.Encode(envelope, metadata);
        var dlqStream = _options.ResolveDeadLetterStream();

        var dlqEntryId = await _deadLetter.MoveAsync(
            context.Stream,
            context.ConsumerGroup,
            context.StreamEntryId,
            dlqStream,
            dlqEntries,
            ct).ConfigureAwait(false);

        _logger.LogWarning(
            exception,
            "Moved message {MessageId} to DLQ {DlqStream} (new entry {DlqEntryId}) after {Attempts} attempts (permanent: {Permanent})",
            context.MessageId, dlqStream, dlqEntryId, context.DeliveryCount, permanent);
    }

    private string? SerialiseHeaders(IReadOnlyDictionary<string, string> headers)
    {
        if (headers.Count == 0)
        {
            return null;
        }
        return _serializer.Serialize(headers, typeof(IReadOnlyDictionary<string, string>));
    }
}
