using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Background loop that periodically runs <c>XAUTOCLAIM</c> on
/// (<see cref="ConsumerOptions.Stream"/>, <see cref="ConsumerOptions.ConsumerGroup"/>),
/// reclaiming idle PEL entries to this consumer and redelivering them through the
/// same handler pipeline as <see cref="Consumer{T}"/>.
/// </summary>
/// <remarks>
/// One iteration per <see cref="ConsumerOptions.ReaperInterval"/>. Within an iteration the
/// reaper sweeps from the start of the PEL cursor through to the end, so a backlog is
/// fully drained per cycle. Delivery counts are read from <c>XPENDING</c> after each
/// claim and surfaced on <see cref="MessageContext.DeliveryCount"/> so middleware
/// (e.g. DLQ logic in Wave 3.2) can react.
/// </remarks>
public sealed class Reaper<T>
{
    private const string PelCursorStart = "0";
    private const string PelCursorEnd = "0-0";

    private readonly IConnectionMultiplexer _connection;
    private readonly MessageProcessor<T> _processor;
    private readonly ConsumerOptions _options;
    private readonly ILogger<Reaper<T>> _logger;

    /// <summary>
    /// Construct a reaper. <paramref name="options"/> is validated immediately.
    /// </summary>
    /// <param name="connection">Redis connection multiplexer.</param>
    /// <param name="serializer">Payload serializer.</param>
    /// <param name="options">Consumer configuration (shared with the paired <see cref="Consumer{T}"/>).</param>
    /// <param name="pipeline">Composed middleware pipeline terminating in the user's handler.</param>
    /// <param name="logger">Logger.</param>
    public Reaper(
        IConnectionMultiplexer connection,
        IMessageSerializer serializer,
        ConsumerOptions options,
        ConsumerDelegate pipeline,
        ILogger<Reaper<T>> logger)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(logger);
        options.Validate();

        _connection = connection;
        _options = options;
        _logger = logger;
        _processor = new MessageProcessor<T>(serializer, options, pipeline, logger);
    }

    /// <summary>Run the reaper loop until <paramref name="ct"/> is cancelled.</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        var db = _connection.GetDatabase();

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await SweepAsync(db, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Reaper cycle failed for {Stream}/{Group}; will retry after {Interval}",
                    _options.Stream, _options.ConsumerGroup, _options.ReaperInterval);
            }

            try
            {
                await Task.Delay(_options.ReaperInterval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task SweepAsync(IDatabase db, CancellationToken ct)
    {
        var idleMs = (long)_options.IdleReclaimAfter.TotalMilliseconds;
        var cursor = PelCursorStart;

        while (!ct.IsCancellationRequested)
        {
            var result = await db.StreamAutoClaimAsync(
                _options.Stream,
                _options.ConsumerGroup,
                _options.ConsumerName,
                idleMs,
                cursor,
                count: _options.BatchSize).ConfigureAwait(false);

            if (result.ClaimedEntries is { Length: > 0 })
            {
                var deliveryCounts = await GetDeliveryCountsAsync(db, result.ClaimedEntries).ConfigureAwait(false);
                foreach (var entry in result.ClaimedEntries)
                {
                    ct.ThrowIfCancellationRequested();
                    var count = deliveryCounts.TryGetValue(entry.Id.ToString(), out var dc) ? dc : 1;
                    await _processor.ProcessAsync(db, entry, count, ct).ConfigureAwait(false);
                }
            }

            var nextCursor = result.NextStartId.ToString();
            if (string.IsNullOrEmpty(nextCursor) || nextCursor == PelCursorEnd)
            {
                break;
            }
            cursor = nextCursor;
        }
    }

    private async Task<Dictionary<string, int>> GetDeliveryCountsAsync(IDatabase db, StreamEntry[] entries)
    {
        var pending = await db.StreamPendingMessagesAsync(
            _options.Stream,
            _options.ConsumerGroup,
            count: entries.Length,
            consumerName: _options.ConsumerName).ConfigureAwait(false);

        var map = new Dictionary<string, int>(pending.Length);
        foreach (var p in pending)
        {
            map[p.MessageId.ToString()] = p.DeliveryCount;
        }
        return map;
    }
}
