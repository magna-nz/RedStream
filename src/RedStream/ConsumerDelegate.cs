namespace RedStream;

/// <summary>
/// The delegate signature that the consumer pipeline ultimately invokes per message.
/// Middleware receives this delegate as <c>next</c>.
/// </summary>
public delegate Task ConsumerDelegate(MessageContext context, CancellationToken ct);
