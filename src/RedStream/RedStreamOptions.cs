namespace RedStream;

/// <summary>
/// Process-wide defaults that apply to every producer and consumer unless overridden.
/// </summary>
public sealed class RedStreamOptions
{
    /// <summary>
    /// Factory that produces the default producer ID used by every producer that doesn't
    /// set its own. When null, the built-in default <c>{MachineName}:{ProcessId}:{guid8}</c>
    /// is used.
    /// </summary>
    public Func<string>? DefaultProducerIdFactory { get; set; }
}
