namespace RedStream;

/// <summary>
/// Maps CLR types to the type identifier written to the envelope's
/// <see cref="MessageEnvelope.TypeId"/> field.
/// </summary>
public interface IMessageTypeResolver
{
    /// <summary>
    /// Get the type identifier for <paramref name="type"/>.
    /// Defaults to <c>type.FullName</c> when no explicit mapping is registered.
    /// </summary>
    string GetTypeId(Type type);
}
