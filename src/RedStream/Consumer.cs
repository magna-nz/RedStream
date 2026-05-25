using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Pull loop for a single (<see cref="ConsumerOptions.Stream"/>, <see cref="ConsumerOptions.ConsumerGroup"/>) pair.
/// </summary>
/// <remarks>
/// Reads batches via <c>XREADGROUP</c>, then delegates per-entry processing to
/// <see cref="MessageProcessor{T}"/>. Bounded concurrency via <see cref="SemaphoreSlim"/>
/// sized to <see cref="ConsumerOptions.MaxConcurrency"/>. ACK on success; handler
/// exceptions leave the entry in the PEL for <see cref="Reaper{T}"/>.
///
/// Out of scope for this wave: graceful shutdown drain (Wave 4.1), connection-loss
/// backoff (Wave 3.4).
/// </remarks>
public sealed class Consumer<T>
{
    private readonly IConnectionMultiplexer _connection;
    private readonly MessageProcessor<T> _processor;
    private readonly ConsumerOptions _options;

    /// <summary>
    /// Construct a consumer. <paramref name="options"/> is validated immediately.
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
        _options = options;
        _processor = new MessageProcessor<T>(serializer, options, pipeline, logger);
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

            await Task.WhenAll(entries.Select(entry => ProcessWithSemaphoreAsync(db, entry, semaphore, ct))).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Idempotently create the consumer group, ignoring "group already exists".
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

    private async Task ProcessWithSemaphoreAsync(IDatabase db, StreamEntry entry, SemaphoreSlim semaphore, CancellationToken ct)
    {
        await semaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _processor.ProcessAsync(db, entry, deliveryCount: 1, ct).ConfigureAwait(false);
        }
        finally
        {
            semaphore.Release();
        }
    }
}
