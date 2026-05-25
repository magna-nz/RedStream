namespace RedStream.Tests;

public class RedisVersionParserTests
{
    [Theory]
    [InlineData("8.6.0", 8, 6, 0)]
    [InlineData("7.4.2", 7, 4, 2)]
    [InlineData("6.2.0", 6, 2, 0)]
    public void Parses_release_version(string input, int major, int minor, int build)
    {
        var version = RedisVersionParser.Parse(input);

        version.Major.Should().Be(major);
        version.Minor.Should().Be(minor);
        version.Build.Should().Be(build);
    }

    [Theory]
    [InlineData("8.6.0-rc1")]
    [InlineData("8.6.0-beta")]
    [InlineData("8.6.0-alpha.3")]
    public void Strips_pre_release_suffix(string input)
    {
        var version = RedisVersionParser.Parse(input);

        version.Should().Be(new Version(8, 6, 0));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void Throws_on_null_or_blank(string? input)
    {
        var act = () => RedisVersionParser.Parse(input!);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Throws_on_unparseable()
    {
        var act = () => RedisVersionParser.Parse("not.a.version");
        act.Should().Throw<FormatException>();
    }
}
