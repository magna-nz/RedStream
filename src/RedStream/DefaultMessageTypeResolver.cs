using System.Collections.Concurrent;

namespace RedStream;

/// <summary>
/// Default <see cref="IMessageTypeResolver"/>. Falls back to <c>type.FullName</c>
/// unless an explicit mapping has been registered via <see cref="Register{T}(string)"/>.
/// </summary>
public sealed class DefaultMessageTypeResolver : IMessageTypeResolver
{
    private readonly ConcurrentDictionary<Type, string> _overrides = new();

    /// <summary>Register an explicit type identifier for <typeparamref name="T"/>.</summary>
    public void Register<T>(string typeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(typeId);
        _overrides[typeof(T)] = typeId;
    }

    /// <summary>Register an explicit type identifier for <paramref name="type"/>.</summary>
    public void Register(Type type, string typeId)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentException.ThrowIfNullOrEmpty(typeId);
        _overrides[type] = typeId;
    }

    /// <inheritdoc />
    public string GetTypeId(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (_overrides.TryGetValue(type, out var id))
        {
            return id;
        }
        return type.FullName
            ?? throw new InvalidOperationException(
                $"Type {type} has no FullName; register an explicit type id with Register<T>(typeId).");
    }
}
