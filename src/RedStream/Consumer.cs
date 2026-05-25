using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Pull loop for a single (<see cref="ConsumerOptions.Stream"/>, <see cref="ConsumerOptions.ConsumerGroup"/>) pair.
/// </summary>
/// <remarks>
/// Reads batches via <c>XREADGROUP</c>, decodes the envelope, deserialises the payload,
/// runs the supplied pipeline, and ACKs on success. Handler exceptions are logged and
/// the entry is left in the PEL for the reaper (Wave 3.1) to redeliver. Malformed
/// envelopes / payloads are ACKed so they do not block the stream.
///
/// This wave does not handle: PEL reaper, dead-letter queue, graceful shutdown drain,
/// connection-loss backoff. Those land in later waves.
/// </remarks>
public sealed class Consumer<T>
{
    private readonly IConnectionMultiplexer _connection;
    private readonly IMessageSerializer _serializer;
    private readonly ConsumerOptions _options;
    private readonly ConsumerDelegate _pipeline;
    private readonly ILogger<Consumer<T>> _logger;

    /// <summary>
    /// Construct a consumer. <paramref name="pipeline"/> is the fully-composed middleware
    /// pipeline (built via <see cref="MiddlewarePipelineBuilder"/>) whose terminal step
    /// invokes the user's handler. <paramref name="options"/> is validated immediately.
    /// </summary>
    /// <param name="connection">Redis connection multiplexer.</param>
    /// <param name="serializer">Payload serializer.</param>
    /// <param name="options">Consumer configuration.</param>
    /// <param name="pipeline">Composed middleware pipeline terminating in the user's handler.</param>
    /// <param name="logger">Logger.</param>
    public Consumer(
        IConnectionMultiplexer connection,
        IMessageSerializer serializer,
        ConsumerOptions options,
        ConsumerDelegate pipeline,
        ILogger<Consumer<T>> logger)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(logger);
        options.Validate();

        _connection = connection;
        _serializer = serializer;
        _options = options;
        _pipeline = pipeline;
        _logger = logger;
    }

    /// <summary>
    /// Idempotently create the consumer group, then run the pull loop until
    /// <paramref name="ct"/> is cancelled.
    /// </summary>
    public async Task RunAsync(CancellationToken ct)
    {
        await EnsureGroupExistsAsync(ct).ConfigureAwait(false);

        var db = _connection.GetDatabase();
        using var semaphore = new SemaphoreSlim(_options.MaxConcurrency, _options.MaxConcurrency);

        while (!ct.IsCancellationRequested)
        {
            StreamEntry[] entries;
            try
            {
                entries = await db.StreamReadGroupAsync(
                    _options.Stream,
                    _options.ConsumerGroup,
                    _options.ConsumerName,
                    StreamPosition.NewMessages,
                    count: _options.BatchSize).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (entries is null || entries.Length == 0)
            {
                try
                {
                    await Task.Delay(_options.PollIntervalWhenEmpty, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                continue;
            }

            await Task.WhenAll(entries.Select(entry => ProcessEntryAsync(db, entry, semaphore, ct))).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Idempotently create the consumer group, ignoring the "group already exists" error.
    /// </summary>
    public async Task EnsureGroupExistsAsync(CancellationToken ct = default)
    {
        var db = _connection.GetDatabase();
        var position = _options.StartPosition == ConsumerStartPosition.LatestOnly
            ? StreamPosition.NewMessages
            : StreamPosition.Beginning;

        try
        {
            await db.StreamCreateConsumerGroupAsync(
                _options.Stream,
                _options.ConsumerGroup,
                position,
                createStream: true).ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYGROUP", StringComparison.Ordinal))
        {
            // Group already exists — fine.
        }
    }

    private async Task ProcessEntryAsync(IDatabase db, StreamEntry entry, SemaphoreSlim semaphore, CancellationToken ct)
    {
        await semaphore.WaitAsync(ct).ConfigureAwait(false);
        try
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

            var context = BuildContext(envelope, entry, payload);

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
                    "Handler failed for entry {EntryId} on {Stream}; entry remains in PEL for reaper",
                    entry.Id, _options.Stream);
                // Intentionally do NOT ACK. Reaper (Wave 3.1) will redeliver.
            }
        }
        finally
        {
            semaphore.Release();
        }
    }

    private MessageContext<T> BuildContext(MessageEnvelope envelope, StreamEntry entry, T payload)
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
            DeliveryCount = 1, // refined by reaper / XPENDING in Wave 3
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

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();
}
