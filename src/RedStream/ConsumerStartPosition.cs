namespace RedStream;

/// <summary>Where a freshly-created consumer group starts reading from.</summary>
public enum ConsumerStartPosition
{
    /// <summary>Start at the end of the stream — older entries are not delivered to a new group.</summary>
    LatestOnly,

    /// <summary>Start from the earliest entry in the stream — replay everything from the beginning.</summary>
    FromBeginning,
}
