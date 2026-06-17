using System.Text;
using FluentAssertions;
using MongoBus.Internal.ClaimCheck;
using Xunit;

namespace MongoBus.Tests;

public class LimitedReadStreamTests
{
    [Fact]
    public async Task ReadAsync_WithinLimit_ReturnsAllBytes()
    {
        var payload = Encoding.UTF8.GetBytes("hello world");
        using var inner = new MemoryStream(payload);
        await using var limited = new LimitedReadStream(inner, maxBytes: 100);

        using var result = new MemoryStream();
        await limited.CopyToAsync(result);

        result.ToArray().Should().Equal(payload);
    }

    [Fact]
    public async Task ReadAsync_ExceedingLimit_Throws()
    {
        var payload = new byte[1000];
        using var inner = new MemoryStream(payload);
        await using var limited = new LimitedReadStream(inner, maxBytes: 100);

        using var result = new MemoryStream();
        var act = () => limited.CopyToAsync(result);

        await act.Should().ThrowAsync<InvalidDataException>();
    }

    [Fact]
    public async Task ReadAsync_ExactlyAtLimit_DoesNotThrow()
    {
        var payload = new byte[100];
        using var inner = new MemoryStream(payload);
        await using var limited = new LimitedReadStream(inner, maxBytes: 100);

        using var result = new MemoryStream();
        var act = () => limited.CopyToAsync(result);

        await act.Should().NotThrowAsync();
    }
}
