namespace RedStream;

/// <summary>
/// Parses a Redis <c>INFO server</c> <c>redis_version</c> value into a <see cref="Version"/>,
/// tolerating pre-release suffixes like <c>8.6.0-rc1</c>.
/// </summary>
internal static class RedisVersionParser
{
    public static Version Parse(string versionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(versionString);
        var clean = versionString.Split('-', 2)[0];
        return Version.Parse(clean);
    }
}
