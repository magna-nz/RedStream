namespace RedStream.IntegrationTests;

/// <summary>
/// xUnit collection that wires <see cref="RedisFixture"/> as a shared
/// fixture across every test class that opts in via <c>[Collection(Name)]</c>.
/// </summary>
[CollectionDefinition(Name)]
public sealed class RedisCollection : ICollectionFixture<RedisFixture>
{
    /// <summary>The name to pass to <c>[Collection(...)]</c> on test classes.</summary>
    public const string Name = "redis";
}
