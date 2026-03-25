using FluentAssertions;
using MongoBus.Models.Saga;
using Xunit;

namespace MongoBus.Tests.Saga;

public class SagaConcurrencyExceptionTests
{
    [Fact]
    public void Constructor_SetsCorrelationIdAndVersion()
    {
        var ex = new SagaConcurrencyException("corr-123", 5);

        ex.CorrelationId.Should().Be("corr-123");
        ex.ExpectedVersion.Should().Be(5);
        ex.Message.Should().Contain("corr-123");
        ex.Message.Should().Contain("5");
    }

    [Fact]
    public void Constructor_WithInnerException_PreservesInner()
    {
        var inner = new InvalidOperationException("inner");

        var ex = new SagaConcurrencyException("corr-456", 3, inner);

        ex.CorrelationId.Should().Be("corr-456");
        ex.ExpectedVersion.Should().Be(3);
        ex.InnerException.Should().Be(inner);
        ex.Message.Should().Contain("corr-456");
        ex.Message.Should().Contain("3");
    }

    [Fact]
    public void Constructor_MessageContainsConcurrencyContext()
    {
        var ex = new SagaConcurrencyException("order-789", 10);

        ex.Message.Should().Contain("order-789");
        ex.Message.Should().Contain("10");
        ex.Message.Should().Contain("concurrently");
    }

    [Fact]
    public void Constructor_InheritsFromException()
    {
        var ex = new SagaConcurrencyException("test", 1);

        ex.Should().BeAssignableTo<Exception>();
    }
}
