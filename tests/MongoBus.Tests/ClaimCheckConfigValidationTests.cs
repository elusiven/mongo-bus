using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.DependencyInjection;
using Xunit;

namespace MongoBus.Tests;

public class ClaimCheckConfigValidationTests
{
    [Fact]
    public async Task Startup_Should_Fail_When_Cleanup_Runs_With_An_Interval_Of_Zero()
    {
        var act = () => StartValidationAsync(opt =>
        {
            opt.ClaimCheck.Enabled = false;
            opt.ClaimCheck.Cleanup.Interval = TimeSpan.Zero;
        });

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Cleanup.Interval*", "cleanup runs whenever a provider is registered, not only when ClaimCheck.Enabled is true");
    }

    [Fact]
    public async Task Startup_Should_Fail_When_Cleanup_Runs_With_A_Negative_Minimum_Age()
    {
        var act = () => StartValidationAsync(opt =>
        {
            opt.ClaimCheck.Enabled = false;
            opt.ClaimCheck.Cleanup.MinimumAge = TimeSpan.FromSeconds(-1);
        });

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*Cleanup.MinimumAge*");
    }

    [Fact]
    public async Task Startup_Should_Fail_When_No_Payload_May_Be_Read()
    {
        var act = () => StartValidationAsync(opt => opt.ClaimCheck.Compression.MaxDecompressedBytes = 0);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*MaxDecompressedBytes*", "a limit of zero rejects every claim-check payload a consumer reads");
    }

    [Fact]
    public async Task Startup_Should_Allow_An_Unused_Cleanup_Interval_When_Cleanup_Is_Disabled()
    {
        var act = () => StartValidationAsync(opt =>
        {
            opt.ClaimCheck.Cleanup.Enabled = false;
            opt.ClaimCheck.Cleanup.Interval = TimeSpan.Zero;
        });

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Startup_Should_Allow_A_Retention_Window_Longer_Than_The_Cleanup_Minimum_Age_Without_A_Provider()
    {
        var act = () => StartValidationAsync(
            opt => opt.ProcessedMessageTtl = TimeSpan.FromDays(14),
            registerProvider: false);

        await act.Should().NotThrowAsync("without a claim-check provider nothing cleans payloads up, so its settings do not apply");
    }

    private static async Task StartValidationAsync(Action<MongoBusOptions> configure, bool registerProvider = true)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = "mongodb://localhost:27017";
            opt.DatabaseName = "claim_check_validation";
            configure(opt);
        });
        if (registerProvider)
            services.AddMongoBusInMemoryClaimCheck();

        var provider = services.BuildServiceProvider();
        var validationService = typeof(MongoBusOptions).Assembly.GetType("MongoBus.Internal.MongoBusValidationHostedService")!;
        await ((IHostedService)provider.GetRequiredService(validationService)).StartAsync(CancellationToken.None);
    }
}
