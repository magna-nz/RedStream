namespace RedStream.Tests;

public class JsonMessageSerializerTests
{
    private readonly JsonMessageSerializer _sut = new();

    public sealed record OrderPlaced(int OrderId, string Customer);

    [Fact]
    public void Round_trip_simple_record()
    {
        var msg = new OrderPlaced(42, "ada");
        var json = _sut.Serialize(msg, typeof(OrderPlaced));
        var decoded = (OrderPlaced)_sut.Deserialize(json, typeof(OrderPlaced));
        decoded.Should().BeEquivalentTo(msg);
    }

    [Fact]
    public void Uses_web_defaults_camel_case()
    {
        var msg = new OrderPlaced(1, "ada");
        var json = _sut.Serialize(msg, typeof(OrderPlaced));
        json.Should().Contain("\"orderId\"").And.NotContain("\"OrderId\"");
    }

    [Fact]
    public void Serialize_throws_on_null_value()
    {
        var act = () => _sut.Serialize(null!, typeof(OrderPlaced));
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Deserialize_throws_on_empty_string()
    {
        var act = () => _sut.Deserialize("", typeof(OrderPlaced));
        act.Should().Throw<ArgumentException>();
    }
}
