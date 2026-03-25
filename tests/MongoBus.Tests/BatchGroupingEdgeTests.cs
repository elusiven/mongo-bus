using FluentAssertions;
using MongoBus.Abstractions;
using MongoBus.Models;
using MongoDB.Bson;
using Xunit;

namespace MongoBus.Tests;

public class BatchGroupingEdgeTests
{
    [Fact]
    public void None_ReturnsAllGroupKey()
    {
        var strategy = BatchGrouping.None;
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, null, "src", "id");
        strategy.GetGroupKey(new object(), ctx).Should().Be("__all__");
    }

    [Fact]
    public void None_ReturnsAllGroupKey_ForAnyMessage()
    {
        var strategy = BatchGrouping.None;
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, "subj", "src", "id", "corr", "caus");
        strategy.GetGroupKey("any-string", ctx).Should().Be("__all__");
        strategy.GetGroupKey(42, ctx).Should().Be("__all__");
    }

    [Fact]
    public void ByMetadata_UsesConsumeContext()
    {
        var strategy = BatchGrouping.ByMetadata(ctx => ctx.Subject ?? "__none__");
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, "my-subject", "src", "id");
        strategy.GetGroupKey(new object(), ctx).Should().Be("my-subject");
    }

    [Fact]
    public void ByMetadata_NullSubject_FallsBackToSelector()
    {
        var strategy = BatchGrouping.ByMetadata(ctx => ctx.Subject ?? "fallback");
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, null, "src", "id");
        strategy.GetGroupKey(new object(), ctx).Should().Be("fallback");
    }

    [Fact]
    public void ByMetadata_NullKey_ReturnsNone()
    {
        var strategy = BatchGrouping.ByMetadata(_ => null!);
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, null, "src", "id");
        strategy.GetGroupKey(new object(), ctx).Should().Be("__none__");
    }

    [Fact]
    public void ByMetadata_EmptyKey_ReturnsNone()
    {
        var strategy = BatchGrouping.ByMetadata(_ => "");
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, null, "src", "id");
        strategy.GetGroupKey(new object(), ctx).Should().Be("__none__");
    }

    [Fact]
    public void ByMetadata_WhitespaceKey_ReturnsNone()
    {
        var strategy = BatchGrouping.ByMetadata(_ => "   ");
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, null, "src", "id");
        strategy.GetGroupKey(new object(), ctx).Should().Be("__none__");
    }

    [Fact]
    public void ByMetadata_UsesEndpointId()
    {
        var strategy = BatchGrouping.ByMetadata(ctx => ctx.EndpointId);
        var ctx = new ConsumeContext("my-endpoint", "type", ObjectId.Empty, 0, null, "src", "id");
        strategy.GetGroupKey(new object(), ctx).Should().Be("my-endpoint");
    }

    [Fact]
    public void ByMessage_TypeMismatch_ThrowsInvalidOperation()
    {
        var strategy = BatchGrouping.ByMessage<string>(s => s);
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, null, "src", "id");
        var act = () => strategy.GetGroupKey(42, ctx);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*String*Int32*");
    }

    [Fact]
    public void ByMessage_NullKey_ReturnsNone()
    {
        var strategy = BatchGrouping.ByMessage<string>(_ => null!);
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, null, "src", "id");
        strategy.GetGroupKey("test", ctx).Should().Be("__none__");
    }

    [Fact]
    public void ByMessage_EmptyKey_ReturnsNone()
    {
        var strategy = BatchGrouping.ByMessage<string>(_ => "");
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, null, "src", "id");
        strategy.GetGroupKey("test", ctx).Should().Be("__none__");
    }

    [Fact]
    public void ByMessage_ValidKey_ReturnsKey()
    {
        var strategy = BatchGrouping.ByMessage<string>(s => s.ToUpperInvariant());
        var ctx = new ConsumeContext("ep", "type", ObjectId.Empty, 0, null, "src", "id");
        strategy.GetGroupKey("hello", ctx).Should().Be("HELLO");
    }
}
