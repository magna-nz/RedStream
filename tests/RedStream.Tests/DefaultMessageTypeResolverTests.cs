namespace RedStream.Tests;

public class DefaultMessageTypeResolverTests
{
    public sealed class TestMessage;

    [Fact]
    public void Default_is_full_name()
    {
        var resolver = new DefaultMessageTypeResolver();
        resolver.GetTypeId(typeof(TestMessage))
            .Should().Be(typeof(TestMessage).FullName);
    }

    [Fact]
    public void Generic_register_overrides_default()
    {
        var resolver = new DefaultMessageTypeResolver();
        resolver.Register<TestMessage>("orders/placed");
        resolver.GetTypeId(typeof(TestMessage)).Should().Be("orders/placed");
    }

    [Fact]
    public void Type_register_overrides_default()
    {
        var resolver = new DefaultMessageTypeResolver();
        resolver.Register(typeof(TestMessage), "orders/placed");
        resolver.GetTypeId(typeof(TestMessage)).Should().Be("orders/placed");
    }

    [Fact]
    public void Latest_registration_wins()
    {
        var resolver = new DefaultMessageTypeResolver();
        resolver.Register<TestMessage>("a");
        resolver.Register<TestMessage>("b");
        resolver.GetTypeId(typeof(TestMessage)).Should().Be("b");
    }

    [Fact]
    public void Register_throws_on_empty_typeid()
    {
        var resolver = new DefaultMessageTypeResolver();
        var act = () => resolver.Register<TestMessage>("");
        act.Should().Throw<ArgumentException>();
    }
}
