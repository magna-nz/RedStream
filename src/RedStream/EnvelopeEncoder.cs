using System.Globalization;
using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Converts <see cref="MessageEnvelope"/> values to and from the
/// <see cref="NameValueEntry"/> arrays used by StackExchange.Redis.
/// </summary>
public static class EnvelopeEncoder
{
    /// <summary>Encode an envelope to a stream-entry payload.</summary>
    public static NameValueEntry[] Encode(MessageEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var entries = new List<NameValueEntry>(9)
        {
            new(EnvelopeFieldNames.Version, envelope.Version),
            new(EnvelopeFieldNames.TypeId, envelope.TypeId),
            new(EnvelopeFieldNames.MessageId, envelope.MessageId),
            new(EnvelopeFieldNames.Timestamp, envelope.Timestamp.ToString("O", CultureInfo.InvariantCulture)),
            new(EnvelopeFieldNames.Body, envelope.Body),
        };

        if (envelope.TraceParent is not null)
        {
            entries.Add(new NameValueEntry(EnvelopeFieldNames.TraceParent, envelope.TraceParent));
        }
        if (envelope.CorrelationId is not null)
        {
            entries.Add(new NameValueEntry(EnvelopeFieldNames.CorrelationId, envelope.CorrelationId));
        }
        if (envelope.CausationId is not null)
        {
            entries.Add(new NameValueEntry(EnvelopeFieldNames.CausationId, envelope.CausationId));
        }
        if (envelope.Headers is not null)
        {
            entries.Add(new NameValueEntry(EnvelopeFieldNames.Headers, envelope.Headers));
        }

        return entries.ToArray();
    }

    /// <summary>Decode a stream-entry payload into an envelope.</summary>
    /// <exception cref="InvalidEnvelopeException">A required field is missing or unparseable.</exception>
    public static MessageEnvelope Decode(NameValueEntry[] entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        string? version = null;
        string? typeId = null;
        string? messageId = null;
        DateTimeOffset? timestamp = null;
        string? body = null;
        string? traceParent = null;
        string? correlationId = null;
        string? causationId = null;
        string? headers = null;

        foreach (var entry in entries)
        {
            var name = (string?)entry.Name;
            var value = (string?)entry.Value;
            if (name is null || value is null)
            {
                continue;
            }

            switch (name)
            {
                case EnvelopeFieldNames.Version:
                    version = value;
                    break;
                case EnvelopeFieldNames.TypeId:
                    typeId = value;
                    break;
                case EnvelopeFieldNames.MessageId:
                    messageId = value;
                    break;
                case EnvelopeFieldNames.Timestamp:
                    if (!DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed))
                    {
                        throw new InvalidEnvelopeException(
                            $"Field '{EnvelopeFieldNames.Timestamp}' is not a valid ISO 8601 timestamp: '{value}'.");
                    }
                    timestamp = parsed;
                    break;
                case EnvelopeFieldNames.Body:
                    body = value;
                    break;
                case EnvelopeFieldNames.TraceParent:
                    traceParent = value;
                    break;
                case EnvelopeFieldNames.CorrelationId:
                    correlationId = value;
                    break;
                case EnvelopeFieldNames.CausationId:
                    causationId = value;
                    break;
                case EnvelopeFieldNames.Headers:
                    headers = value;
                    break;
                default:
                    // Forward-compat: unknown fields are tolerated and discarded.
                    break;
            }
        }

        if (version is null)
        {
            throw new InvalidEnvelopeException($"Missing required field '{EnvelopeFieldNames.Version}'.");
        }
        if (typeId is null)
        {
            throw new InvalidEnvelopeException($"Missing required field '{EnvelopeFieldNames.TypeId}'.");
        }
        if (messageId is null)
        {
            throw new InvalidEnvelopeException($"Missing required field '{EnvelopeFieldNames.MessageId}'.");
        }
        if (timestamp is null)
        {
            throw new InvalidEnvelopeException($"Missing required field '{EnvelopeFieldNames.Timestamp}'.");
        }
        if (body is null)
        {
            throw new InvalidEnvelopeException($"Missing required field '{EnvelopeFieldNames.Body}'.");
        }

        return new MessageEnvelope
        {
            Version = version,
            TypeId = typeId,
            MessageId = messageId,
            Timestamp = timestamp.Value,
            Body = body,
            TraceParent = traceParent,
            CorrelationId = correlationId,
            CausationId = causationId,
            Headers = headers,
        };
    }
}
