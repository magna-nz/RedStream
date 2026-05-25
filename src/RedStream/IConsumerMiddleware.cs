namespace RedStream;

/// <summary>
/// Wraps a single step in the consumer pipeline. Same shape as ASP.NET request
/// middleware: call <c>next</c> to continue the pipeline, or short-circuit by not
/// calling it.
/// </summary>
public interface IConsumerMiddleware
{
    /// <summary>Invoke this middleware for <paramref name="context"/>.</summary>
    Task InvokeAsync(MessageContext context, ConsumerDelegate next, CancellationToken ct);
}
