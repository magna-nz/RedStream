namespace RedStream;

/// <summary>
/// Thrown by a handler to signal that the failure is unrecoverable. The
/// <see cref="DeadLetterMiddleware{T}"/> moves the message straight to the DLQ on first
/// failure rather than waiting for <see cref="ConsumerOptions.MaxDeliveryAttempts"/>
/// to be reached. Use for validation errors, missing required fields, schema mismatches,
/// or any other "this will never succeed no matter how many times we retry" condition.
/// </summary>
public sealed class PermanentFailureException : Exception
{
    /// <summary>Initialise with a message.</summary>
    public PermanentFailureException(string message)
        : base(message)
    {
    }

    /// <summary>Initialise with a message and inner exception.</summary>
    public PermanentFailureException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
