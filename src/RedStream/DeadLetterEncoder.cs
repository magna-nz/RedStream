using System.Globalization;
using StackExchange.Redis;

namespace RedStream;

/// <summary>
/// Encodes a (<see cref="MessageEnvelope"/>, <see cref="DeadLetterMetadata"/>) pair to and
/// from the <see cref="NameValueEntry"/> form stored in the DLQ stream. The DLQ entry
/// contains every envelope field plus the <c>dlq.*</c> metadata fields.
/// </summary>
internal static class DeadLetterEncoder
{
    public const int StackTraceMaxLength = 4000;

    public static NameValueEntry[] Encode(MessageEnvelope envelope, DeadLetterMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(metadata);

        var envelopeEntries = EnvelopeEncoder.Encode(envelope);

        var dlqEntries = new List<NameValueEntry>(9)
        {
            new(DeadLetterFieldNames.OriginalEntryId, metadata.OriginalStreamEntryId),
            new(DeadLetterFieldNames.OriginalStream, metadata.OriginalStream),
            new(DeadLetterFieldNames.OriginalGroup, metadata.OriginalGroup),
            new(DeadLetterFieldNames.Attempts, metadata.Attempts.ToString(CultureInfo.InvariantCulture)),
            new(DeadLetterFieldNames.FirstSeen, metadata.FirstSeen.ToString("O", CultureInfo.InvariantCulture)),
            new(DeadLetterFieldNames.LastSeen, metadata.LastSeen.ToString("O", CultureInfo.InvariantCulture)),
        };

        if (metadata.ErrorType is not null)
        {
            dlqEntries.Add(new NameValueEntry(DeadLetterFieldNames.ErrorType, metadata.ErrorType));
        }
        if (metadata.ErrorMessage is not null)
        {
            dlqEntries.Add(new NameValueEntry(DeadLetterFieldNames.ErrorMessage, metadata.ErrorMessage));
        }
        if (metadata.ErrorStack is not null)
        {
            dlqEntries.Add(new NameValueEntry(DeadLetterFieldNames.ErrorStack, Truncate(metadata.ErrorStack, StackTraceMaxLength)));
        }

        var combined = new NameValueEntry[envelopeEntries.Length + dlqEntries.Count];
        envelopeEntries.CopyTo(combined, 0);
        for (var i = 0; i < dlqEntries.Count; i++)
        {
            combined[envelopeEntries.Length + i] = dlqEntries[i];
        }
        return combined;
    }

    public static DeadLetterRecord Decode(StreamEntry entry)
    {
        var envelopeEntries = new List<NameValueEntry>();
        string? originalId = null;
        string? originalStream = null;
        string? originalGroup = null;
        var attempts = 0;
        DateTimeOffset firstSeen = default;
        DateTimeOffset lastSeen = default;
        string? errorType = null;
        string? errorMessage = null;
        string? errorStack = null;

        foreach (var nv in entry.Values)
        {
            var name = (string?)nv.Name;
            var value = (string?)nv.Value;
            if (name is null || value is null)
            {
                continue;
            }

            switch (name)
            {
                case DeadLetterFieldNames.OriginalEntryId:
                    originalId = value;
                    break;
                case DeadLetterFieldNames.OriginalStream:
                    originalStream = value;
                    break;
                case DeadLetterFieldNames.OriginalGroup:
                    originalGroup = value;
                    break;
                case DeadLetterFieldNames.Attempts:
                    int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out attempts);
                    break;
                case DeadLetterFieldNames.FirstSeen:
                    DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out firstSeen);
                    break;
                case DeadLetterFieldNames.LastSeen:
                    DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out lastSeen);
                    break;
                case DeadLetterFieldNames.ErrorType:
                    errorType = value;
                    break;
                case DeadLetterFieldNames.ErrorMessage:
                    errorMessage = value;
                    break;
                case DeadLetterFieldNames.ErrorStack:
                    errorStack = value;
                    break;
                default:
                    envelopeEntries.Add(nv);
                    break;
            }
        }

        var envelope = EnvelopeEncoder.Decode(envelopeEntries.ToArray());
        var metadata = new DeadLetterMetadata
        {
            OriginalStreamEntryId = originalId ?? string.Empty,
            OriginalStream = originalStream ?? string.Empty,
            OriginalGroup = originalGroup ?? string.Empty,
            Attempts = attempts,
            FirstSeen = firstSeen,
            LastSeen = lastSeen,
            ErrorType = errorType,
            ErrorMessage = errorMessage,
            ErrorStack = errorStack,
        };

        return new DeadLetterRecord
        {
            DlqEntryId = entry.Id.ToString(),
            OriginalEnvelope = envelope,
            Metadata = metadata,
        };
    }

    private static string Truncate(string value, int maxLength)
        => value.Length <= maxLength ? value : value[..maxLength];
}
