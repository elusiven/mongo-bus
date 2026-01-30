using MongoBus.Abstractions;
using MongoBus.DependencyInjection;

namespace MongoBus.Internal;

internal static class MongoBusConfigValidator
{
    public static void ValidateOptions(MongoBusOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            throw new InvalidOperationException("MongoBusOptions.ConnectionString is required.");

        if (string.IsNullOrWhiteSpace(options.DatabaseName))
            throw new InvalidOperationException("MongoBusOptions.DatabaseName is required.");

        if (options.ProcessedMessageTtl <= TimeSpan.Zero)
            throw new InvalidOperationException("MongoBusOptions.ProcessedMessageTtl must be > 0.");

        ValidateClaimCheck(options);
    }

    public static void ValidateDefinitions(IEnumerable<IConsumerDefinition> definitions)
    {
        var list = definitions.ToList();
        if (list.Count == 0)
            return;

        var registrations = new HashSet<(string Endpoint, string TypeId)>();
        foreach (var def in list)
        {
            ValidateDefinition(def);

            var key = (def.EndpointName, def.TypeId);
            if (!registrations.Add(key))
                throw new InvalidOperationException($"Duplicate consumer registration for endpoint '{def.EndpointName}' and type '{def.TypeId}'.");
        }

        var batchDefs = list.OfType<IBatchConsumerDefinition>().ToList();
        if (batchDefs.Count > 0)
        {
            foreach (var batchDef in batchDefs)
            {
                batchDef.BatchOptions.EnsureValid();
                if (batchDef.GroupingStrategy is null)
                    throw new InvalidOperationException($"Batch consumer '{batchDef.ConsumerType.Name}' must define a GroupingStrategy.");
            }
        }
    }

    private static void ValidateDefinition(IConsumerDefinition def)
    {
        if (string.IsNullOrWhiteSpace(def.TypeId))
            throw new InvalidOperationException($"Consumer '{def.ConsumerType.Name}' must define a non-empty TypeId.");

        if (string.IsNullOrWhiteSpace(def.EndpointName))
            throw new InvalidOperationException($"Consumer '{def.ConsumerType.Name}' must define a non-empty EndpointName.");

        if (def.ConcurrencyLimit < 1)
            throw new InvalidOperationException($"Consumer '{def.ConsumerType.Name}' ConcurrencyLimit must be >= 1.");

        if (def.PrefetchCount < 1)
            throw new InvalidOperationException($"Consumer '{def.ConsumerType.Name}' PrefetchCount must be >= 1.");

        if (def.LockTime <= TimeSpan.Zero)
            throw new InvalidOperationException($"Consumer '{def.ConsumerType.Name}' LockTime must be > 0.");

        if (def.MaxAttempts < 1)
            throw new InvalidOperationException($"Consumer '{def.ConsumerType.Name}' MaxAttempts must be >= 1.");
    }

    private static void ValidateClaimCheck(MongoBusOptions options)
    {
        var cc = options.ClaimCheck;
        if (!cc.Enabled)
            return;

        if (cc.ThresholdBytes <= 0)
            throw new InvalidOperationException("ClaimCheck.ThresholdBytes must be > 0 when claim-check is enabled.");

        if (string.IsNullOrWhiteSpace(cc.ProviderName))
            throw new InvalidOperationException("ClaimCheck.ProviderName is required when claim-check is enabled.");

        if (cc.Compression.Enabled && string.IsNullOrWhiteSpace(cc.Compression.Algorithm))
            throw new InvalidOperationException("ClaimCheck.Compression.Algorithm is required when compression is enabled.");

        if (cc.Cleanup.Interval <= TimeSpan.Zero)
            throw new InvalidOperationException("ClaimCheck.Cleanup.Interval must be > 0 when cleanup is enabled.");

        if (cc.Cleanup.MinimumAge < TimeSpan.Zero)
            throw new InvalidOperationException("ClaimCheck.Cleanup.MinimumAge must be >= 0 when cleanup is enabled.");

        if (cc.Cleanup.MinimumAge > TimeSpan.Zero && cc.Cleanup.MinimumAge <= options.ProcessedMessageTtl)
            throw new InvalidOperationException("ClaimCheck.Cleanup.MinimumAge must be greater than ProcessedMessageTtl when set.");
    }
}
