using System.Text.Json;

namespace RedStream;

/// <summary>
/// Default <see cref="IMessageSerializer"/> backed by <see cref="System.Text.Json"/>.
/// </summary>
public sealed class JsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>Use the default web JSON options (camelCase, case-insensitive on read).</summary>
    public JsonMessageSerializer()
        : this(new JsonSerializerOptions(JsonSerializerDefaults.Web))
    {
    }

    /// <summary>Use the supplied <paramref name="options"/>.</summary>
    public JsonMessageSerializer(JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    public string Serialize(object value, Type type)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(type);
        return JsonSerializer.Serialize(value, type, _options);
    }

    /// <inheritdoc />
    public object Deserialize(string value, Type type)
    {
        ArgumentException.ThrowIfNullOrEmpty(value);
        ArgumentNullException.ThrowIfNull(type);
        return JsonSerializer.Deserialize(value, type, _options)
            ?? throw new InvalidOperationException($"Deserialization returned null for type {type.FullName}.");
    }
}
