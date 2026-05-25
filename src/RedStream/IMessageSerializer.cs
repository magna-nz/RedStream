namespace RedStream;

/// <summary>
/// Serialises message payloads to and from the string form carried in the
/// envelope's <see cref="MessageEnvelope.Body"/> field.
/// </summary>
/// <remarks>
/// The default implementation is <see cref="JsonMessageSerializer"/>.
/// Replace it via DI to switch encodings (e.g. MessagePack) in a future version.
/// </remarks>
public interface IMessageSerializer
{
    /// <summary>Serialise <paramref name="value"/> to its string form.</summary>
    string Serialize(object value, Type type);

    /// <summary>Deserialise <paramref name="value"/> into an instance of <paramref name="type"/>.</summary>
    object Deserialize(string value, Type type);
}
