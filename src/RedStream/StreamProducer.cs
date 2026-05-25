using System.Diagnostics;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Default <see cref="IStreamProducer{T}"/> implementation.
/// </summary>
/// <remarks>
/// On Redis 8.6+ the producer uses XADD with the <c>IDMP</c> option, supplying a per-process
/// producer id and the envelope's <see cref="MessageEnvelope.MessageId"/> as the idempotent id.
/// This makes <c>XADD</c> retries safe end-to-end (the server returns the original entry id
/// instead of creating a duplicate). On older servers the producer falls back to plain
/// <c>XADD</c> and consumer-side dedup is the only safeguard.
/// </remarks>
public sealed class StreamProducer<T> : IStreamProducer<T>
{
    private readonly IConnectionMultiplexer _connection;
    private readonly IMessageSerializer _serializer;
    private readonly IMessageTypeResolver _typeResolver;
    private readonly IRedisServerVersionCache _versionCache;
    private readonly string _stream;
    private readonly string _producerId;
    private readonly ProducerOptions _producerOptions;
    private readonly ILogger<StreamProducer<T>> _logger;
    private int _streamConfigured;

    /// <summary>Construct a producer bound to <paramref name="stream"/>.</summary>
    public StreamProducer(
        IConnectionMultiplexer connection,
        IMessageSerializer serializer,
        IMessageTypeResolver typeResolver,
        IRedisServerVersionCache versionCache,
        string stream,
        ProducerOptions producerOptions,
        RedStreamOptions globalOptions,
        ILogger<StreamProducer<T>> logger)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(typeResolver);
        ArgumentNullException.ThrowIfNull(versionCache);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(producerOptions);
        ArgumentNullException.ThrowIfNull(globalOptions);
        ArgumentNullException.ThrowIfNull(logger);

        _connection = connection;
        _serializer = serializer;
        _typeResolver = typeResolver;
        _versionCache = versionCache;
        _stream = stream;
        _producerOptions = producerOptions;
        _logger = logger;
        _producerId = ResolveProducerId(producerOptions, globalOptions);
    }

    /// <summary>The resolved producer id used as the IDMP <c>pid</c>.</summary>
    public string ProducerId => _producerId;

    /// <inheritdoc />
    public async Task<string> PublishAsync(T message, PublishOptions? options = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        var envelope = BuildEnvelope(message, options);
        var entries = EnvelopeEncoder.Encode(envelope);
        var db = _connection.GetDatabase();

        var idmpSupported = await _versionCache.IsIdmpSupportedAsync(ct).ConfigureAwait(false);
        if (idmpSupported)
        {
            await EnsureStreamConfiguredAsync(db).ConfigureAwait(false);
            return await PublishWithIdmpAsync(db, entries, envelope.MessageId).ConfigureAwait(false);
        }

        var entryId = await db.StreamAddAsync(_stream, entries).ConfigureAwait(false);
        return entryId.ToString();
    }

    private MessageEnvelope BuildEnvelope(T message, PublishOptions? options)
    {
        var body = _serializer.Serialize(message!, typeof(T));
        var typeId = _typeResolver.GetTypeId(typeof(T));
        var messageId = options?.MessageId ?? Guid.NewGuid().ToString("N");
        var headers = options?.Headers is not null
            ? _serializer.Serialize(options.Headers, typeof(IReadOnlyDictionary<string, string>))
            : null;

        return new MessageEnvelope
        {
            Version = MessageEnvelope.CurrentVersion,
            TypeId = typeId,
            MessageId = messageId,
            Timestamp = DateTimeOffset.UtcNow,
            Body = body,
            TraceParent = GetW3CTraceParent(),
            CorrelationId = options?.CorrelationId,
            CausationId = options?.CausationId,
            Headers = headers,
        };
    }

    private static string? GetW3CTraceParent()
    {
        var activity = Activity.Current;
        if (activity is null)
        {
            return null;
        }
        return activity.IdFormat == ActivityIdFormat.W3C ? activity.Id : null;
    }

    private async Task EnsureStreamConfiguredAsync(IDatabase db)
    {
        if (Interlocked.CompareExchange(ref _streamConfigured, 1, 0) != 0)
        {
            return;
        }
        if (_producerOptions.IdmpDuration is null && _producerOptions.IdmpMaxSize is null)
        {
            return;
        }

        var args = new List<object> { _stream };
        if (_producerOptions.IdmpDuration is { } duration)
        {
            args.Add("idmp-duration");
            args.Add((long)duration.TotalSeconds);
        }
        if (_producerOptions.IdmpMaxSize is { } maxSize)
        {
            args.Add("idmp-maxsize");
            args.Add(maxSize);
        }

        try
        {
            _ = await db.ExecuteAsync("XCFGSET", args.ToArray()).ConfigureAwait(false);
        }
        catch (RedisServerException ex)
        {
            Interlocked.Exchange(ref _streamConfigured, 0);
            _logger.LogWarning(
                ex,
                "XCFGSET failed for stream {Stream}; IDMP retention defaults remain in place",
                _stream);
        }
    }

    private async Task<string> PublishWithIdmpAsync(IDatabase db, NameValueEntry[] entries, string idempotentId)
    {
        // XADD key IDMP pid iid * field value [field value ...]
        var args = new List<object>(5 + (entries.Length * 2))
        {
            _stream,
            "IDMP",
            _producerId,
            idempotentId,
            "*",
        };
        foreach (var entry in entries)
        {
            args.Add(entry.Name.ToString());
            args.Add(entry.Value.ToString());
        }

        var result = await db.ExecuteAsync("XADD", args.ToArray()).ConfigureAwait(false);
        return result.ToString();
    }

    private static string ResolveProducerId(ProducerOptions producerOptions, RedStreamOptions globalOptions)
    {
        if (!string.IsNullOrWhiteSpace(producerOptions.ProducerId))
        {
            return producerOptions.ProducerId;
        }
        if (globalOptions.DefaultProducerIdFactory is { } factory)
        {
            var id = factory();
            if (!string.IsNullOrWhiteSpace(id))
            {
                return id;
            }
        }
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return $"{Environment.MachineName}:{Environment.ProcessId}:{suffix}";
    }
}
