namespace RedStream;

/// <summary>
/// Composes an ordered list of <see cref="IConsumerMiddleware"/> with a terminal handler
/// into a single <see cref="ConsumerDelegate"/>. Middleware registered first runs first.
/// </summary>
public sealed class MiddlewarePipelineBuilder
{
    private readonly List<IConsumerMiddleware> _middlewares = [];

    /// <summary>Append a middleware to the pipeline. Middleware runs in registration order.</summary>
    public MiddlewarePipelineBuilder Use(IConsumerMiddleware middleware)
    {
        ArgumentNullException.ThrowIfNull(middleware);
        _middlewares.Add(middleware);
        return this;
    }

    /// <summary>The number of middlewares currently registered.</summary>
    public int Count => _middlewares.Count;

    /// <summary>
    /// Compose the pipeline. <paramref name="terminal"/> is the innermost delegate that runs
    /// after every middleware has called <c>next</c>.
    /// </summary>
    public ConsumerDelegate Build(ConsumerDelegate terminal)
    {
        ArgumentNullException.ThrowIfNull(terminal);

        var current = terminal;
        for (var i = _middlewares.Count - 1; i >= 0; i--)
        {
            var middleware = _middlewares[i];
            var next = current;
            current = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }
        return current;
    }
}
