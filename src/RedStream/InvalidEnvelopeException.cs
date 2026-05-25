namespace RedStream;

/// <summary>
/// Thrown when a stream entry cannot be decoded into a valid <see cref="MessageEnvelope"/>.
/// </summary>
public sealed class InvalidEnvelopeException : Exception
{
    /// <summary>Initialise with a message.</summary>
    public InvalidEnvelopeException(string message)
        : base(message)
    {
    }

    /// <summary>Initialise with a message and inner exception.</summary>
    public InvalidEnvelopeException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
