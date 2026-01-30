using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;

namespace MongoBus.Internal;

internal sealed class MongoBusValidationHostedService : IHostedService
{
    private readonly MongoBusOptions _options;
    private readonly IEnumerable<IConsumerDefinition> _definitions;

    public MongoBusValidationHostedService(MongoBusOptions options, IEnumerable<IConsumerDefinition> definitions)
    {
        _options = options;
        _definitions = definitions;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        MongoBusConfigValidator.ValidateOptions(_options);
        MongoBusConfigValidator.ValidateDefinitions(_definitions);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
