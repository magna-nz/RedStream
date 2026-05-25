using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Per-entry processing shared by <see cref="Consumer{T}"/> (fresh reads) and
/// <see cref="Reaper{T}"/> (XAUTOCLAIM redeliveries). Decodes the envelope,
/// deserialises the payload, runs the pipeline, and ACKs on success. Failure
/// modes mirror those documented on <see cref="Consumer{T}"/>.
/// </summary>
internal sealed class MessageProcessor<T>
{
    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders = new Dictionary<string, string>();

    private readonly IMessageSerializer _serializer;
    private readonly ConsumerOptions _options;
    private readonly ConsumerDelegate _pipeline;
    private readonly ILogger _logger;

    public MessageProcessor(
        IMessageSerializer serializer,
        ConsumerOptions options,
        ConsumerDelegate pipeline,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(logger);

        _serializer = serializer;
        _options = options;
        _pipeline = pipeline;
        _logger = logger;
    }

    public async Task ProcessAsync(IDatabase db, StreamEntry entry, int deliveryCount, CancellationToken ct)
    {
        MessageEnvelope envelope;
        try
        {
            envelope = EnvelopeEncoder.Decode(entry.Values);
        }
        catch (InvalidEnvelopeException ex)
        {
            _logger.LogWarning(
                ex,
                "Could not decode envelope on entry {EntryId} from {Stream}; ACKing so it does not block the stream",
                entry.Id, _options.Stream);
            await db.StreamAcknowledgeAsync(_options.Stream, _options.ConsumerGroup, entry.Id).ConfigureAwait(false);
            return;
        }

        T payload;
        try
        {
            payload = (T)_serializer.Deserialize(envelope.Body, typeof(T));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Could not deserialise payload of type {TypeId} on entry {EntryId}; ACKing so it does not block the stream",
                envelope.TypeId, entry.Id);
            await db.StreamAcknowledgeAsync(_options.Stream, _options.ConsumerGroup, entry.Id).ConfigureAwait(false);
            return;
        }

        var context = BuildContext(envelope, entry, payload, deliveryCount);

        try
        {
            await _pipeline(context, ct).ConfigureAwait(false);
            await db.StreamAcknowledgeAsync(_options.Stream, _options.ConsumerGroup, entry.Id).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Handler failed for entry {EntryId} on {Stream} (delivery #{DeliveryCount}); entry remains in PEL for reaper",
                entry.Id, _options.Stream, deliveryCount);
        }
    }

    private MessageContext<T> BuildContext(MessageEnvelope envelope, StreamEntry entry, T payload, int deliveryCount)
    {
        var headers = DeserialiseHeaders(envelope.Headers);
        return new MessageContext<T>
        {
            MessageId = envelope.MessageId,
            StreamEntryId = entry.Id.ToString(),
            TypeId = envelope.TypeId,
            PublishedAt = envelope.Timestamp,
            Stream = _options.Stream,
            ConsumerGroup = _options.ConsumerGroup,
            Consumer = _options.ConsumerName,
            DeliveryCount = deliveryCount,
            TraceParent = envelope.TraceParent,
            CorrelationId = envelope.CorrelationId,
            CausationId = envelope.CausationId,
            Headers = headers,
            Payload = payload,
        };
    }

    private IReadOnlyDictionary<string, string> DeserialiseHeaders(string? headersJson)
    {
        if (string.IsNullOrEmpty(headersJson))
        {
            return EmptyHeaders;
        }
        try
        {
            return (IReadOnlyDictionary<string, string>)_serializer.Deserialize(
                headersJson,
                typeof(IReadOnlyDictionary<string, string>));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not deserialise message headers; treating as empty");
            return EmptyHeaders;
        }
    }
}
