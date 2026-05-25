namespace RedStream;

/// <summary>
/// Publishes typed messages to a Redis stream.
/// </summary>
/// <typeparam name="T">Message payload type.</typeparam>
public interface IStreamProducer<T>
{
    /// <summary>
    /// Publish <paramref name="message"/> to the producer's configured stream.
    /// </summary>
    /// <returns>The Redis stream entry ID assigned to the new entry.</returns>
    Task<string> PublishAsync(T message, PublishOptions? options = null, CancellationToken ct = default);
}
