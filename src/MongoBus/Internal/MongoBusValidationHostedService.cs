using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;

namespace MongoBus.Internal;

internal sealed class MongoBusValidationHostedService : IHostedService
{
    private readonly MongoBusOptions _options;
    private readonly IEnumerable<IConsumerDefinition> _definitions;
    private readonly bool _hasClaimCheckProvider;

    public MongoBusValidationHostedService(
        MongoBusOptions options,
        IEnumerable<IConsumerDefinition> definitions,
        IEnumerable<IClaimCheckProvider> claimCheckProviders)
    {
        _options = options;
        _definitions = definitions;
        _hasClaimCheckProvider = claimCheckProviders.Any();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        MongoBusConfigValidator.ValidateOptions(_options, _hasClaimCheckProvider);
        MongoBusConfigValidator.ValidateDefinitions(_definitions);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
