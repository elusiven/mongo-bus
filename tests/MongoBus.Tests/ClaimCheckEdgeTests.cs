using FluentAssertions;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;
using Xunit;

namespace MongoBus.Tests;

public class ClaimCheckEdgeTests
{
    #region ClaimCheckOptions defaults

    [Fact]
    public void ClaimCheckOptions_DefaultValues()
    {
        var options = new ClaimCheckOptions();

        options.Enabled.Should().BeFalse();
        options.ThresholdBytes.Should().Be(256 * 1024);
        options.ProviderName.Should().BeNull();
    }

    [Fact]
    public void ClaimCheckCleanupOptions_DefaultValues()
    {
        var cleanup = new ClaimCheckCleanupOptions();

        cleanup.Enabled.Should().BeTrue();
        cleanup.Interval.Should().Be(TimeSpan.FromHours(6));
        cleanup.MinimumAge.Should().Be(TimeSpan.FromDays(8));
    }

    [Fact]
    public void ClaimCheckCompressionOptions_DefaultValues()
    {
        var compression = new ClaimCheckCompressionOptions();

        compression.Enabled.Should().BeFalse();
        compression.Algorithm.Should().Be("gzip");
    }

    [Fact]
    public void ClaimCheckOptions_SetProperties()
    {
        var options = new ClaimCheckOptions
        {
            Enabled = true,
            ThresholdBytes = 1024,
            ProviderName = "s3"
        };

        options.Enabled.Should().BeTrue();
        options.ThresholdBytes.Should().Be(1024);
        options.ProviderName.Should().Be("s3");
    }

    [Fact]
    public void ClaimCheckCleanupOptions_SetProperties()
    {
        var cleanup = new ClaimCheckCleanupOptions
        {
            Enabled = false,
            Interval = TimeSpan.FromMinutes(30),
            MinimumAge = TimeSpan.FromDays(14)
        };

        cleanup.Enabled.Should().BeFalse();
        cleanup.Interval.Should().Be(TimeSpan.FromMinutes(30));
        cleanup.MinimumAge.Should().Be(TimeSpan.FromDays(14));
    }

    #endregion

    #region ClaimCheckProviderResolver

    private sealed class FakeClaimCheckProvider(string name) : IClaimCheckProvider
    {
        public string Name => name;

        public Task<ClaimCheckReference> PutAsync(ClaimCheckWriteRequest request, CancellationToken ct) =>
            throw new NotImplementedException();

        public Task<Stream> OpenReadAsync(ClaimCheckReference reference, CancellationToken ct) =>
            throw new NotImplementedException();

        public Task DeleteAsync(ClaimCheckReference reference, CancellationToken ct) =>
            throw new NotImplementedException();

        public IAsyncEnumerable<ClaimCheckReference> ListAsync(CancellationToken ct) =>
            throw new NotImplementedException();
    }

    [Fact]
    public void GetProvider_UnknownName_Throws()
    {
        var resolver = new ClaimCheckProviderResolver([new FakeClaimCheckProvider("memory")]);

        var act = () => resolver.GetProvider("nonexistent");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*'nonexistent'*not registered*");
    }

    [Fact]
    public void GetProvider_KnownName_ReturnsProvider()
    {
        var provider = new FakeClaimCheckProvider("memory");
        var resolver = new ClaimCheckProviderResolver([provider]);

        var result = resolver.GetProvider("memory");

        result.Should().BeSameAs(provider);
    }

    [Fact]
    public void GetProvider_CaseInsensitive_ReturnsProvider()
    {
        var provider = new FakeClaimCheckProvider("Memory");
        var resolver = new ClaimCheckProviderResolver([provider]);

        var result = resolver.GetProvider("MEMORY");

        result.Should().BeSameAs(provider);
    }

    [Fact]
    public void GetProvider_NullName_SingleProvider_ReturnsSingleProvider()
    {
        var provider = new FakeClaimCheckProvider("memory");
        var resolver = new ClaimCheckProviderResolver([provider]);

        var result = resolver.GetProvider(null);

        result.Should().BeSameAs(provider);
    }

    [Fact]
    public void GetProvider_NullName_MultipleProviders_Throws()
    {
        var resolver = new ClaimCheckProviderResolver(
        [
            new FakeClaimCheckProvider("memory"),
            new FakeClaimCheckProvider("s3")
        ]);

        var act = () => resolver.GetProvider(null);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*multiple providers*");
    }

    [Fact]
    public void GetProviderForReference_UnknownProvider_Throws()
    {
        var resolver = new ClaimCheckProviderResolver([new FakeClaimCheckProvider("memory")]);

        var reference = new ClaimCheckReference("nonexistent", "container", "key", 100);
        var act = () => resolver.GetProviderForReference(reference);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*'nonexistent'*not registered*");
    }

    [Fact]
    public void GetProviderForReference_KnownProvider_ReturnsProvider()
    {
        var provider = new FakeClaimCheckProvider("memory");
        var resolver = new ClaimCheckProviderResolver([provider]);

        var reference = new ClaimCheckReference("memory", "container", "key", 100);
        var result = resolver.GetProviderForReference(reference);

        result.Should().BeSameAs(provider);
    }

    [Fact]
    public void GetProvider_NoProviders_NullName_Throws()
    {
        var resolver = new ClaimCheckProviderResolver([]);

        var act = () => resolver.GetProvider(null);

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void GetProvider_EmptyName_SingleProvider_ReturnsSingleProvider()
    {
        var provider = new FakeClaimCheckProvider("memory");
        var resolver = new ClaimCheckProviderResolver([provider]);

        // Empty string treated same as null/whitespace
        var result = resolver.GetProvider("");

        result.Should().BeSameAs(provider);
    }

    #endregion

    #region OutboxOptions defaults

    [Fact]
    public void OutboxOptions_DefaultValues()
    {
        var options = new OutboxOptions();

        options.Enabled.Should().BeFalse();
        options.ProcessedMessageTtl.Should().Be(TimeSpan.FromDays(7));
        options.PollingInterval.Should().Be(TimeSpan.FromMilliseconds(250));
        options.LockTime.Should().Be(TimeSpan.FromSeconds(30));
        options.MaxAttempts.Should().Be(10);
    }

    #endregion

    #region MongoBusOptions defaults

    [Fact]
    public void MongoBusOptions_DefaultValues()
    {
        var options = new MongoBusOptions();

        options.DatabaseName.Should().Be("mongo_bus");
        options.DefaultSource.Should().BeNull();
        options.ProcessedMessageTtl.Should().Be(TimeSpan.FromDays(7));
        options.ClaimCheck.Should().NotBeNull();
        options.Outbox.Should().NotBeNull();
        options.UseOutboxForTypeId.Should().BeNull();
    }

    #endregion
}
