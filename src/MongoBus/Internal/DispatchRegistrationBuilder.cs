using MongoBus.Abstractions;
using MongoBus.Models;

namespace MongoBus.Internal;

internal static class DispatchRegistrationBuilder
{
    public static IReadOnlyDictionary<(string EndpointId, string TypeId), DispatchRegistration> BuildDispatchMap(IEnumerable<IConsumerDefinition> definitions)
    {
        var dispatch = new Dictionary<(string EndpointId, string TypeId), DispatchRegistration>();
        foreach (var def in definitions)
        {
            var key = (def.EndpointName, def.TypeId);
            if (dispatch.ContainsKey(key))
                throw new InvalidOperationException($"Duplicate (endpoint,type) registration: {def.EndpointName} / {def.TypeId}");

            dispatch[key] = CreateRegistration(def);
        }

        return dispatch;
    }

    public static IReadOnlyDictionary<string, EndpointRuntimeConfig> BuildEndpointConfigs(IEnumerable<IConsumerDefinition> definitions)
    {
        var endpointCfg = new Dictionary<string, EndpointRuntimeConfig>();
        foreach (var def in definitions)
        {
            if (!endpointCfg.TryGetValue(def.EndpointName, out var existing))
            {
                endpointCfg[def.EndpointName] = CreateEndpointConfig(def);
                continue;
            }

            endpointCfg[def.EndpointName] = existing with
            {
                Concurrency = Math.Max(existing.Concurrency, Math.Max(1, def.ConcurrencyLimit)),
                Prefetch = Math.Max(existing.Prefetch, Math.Max(def.PrefetchCount, def.ConcurrencyLimit)),
                LockTime = existing.LockTime > def.LockTime ? existing.LockTime : def.LockTime,
                MaxAttempts = Math.Max(existing.MaxAttempts, def.MaxAttempts),
                IdempotencyEnabled = existing.IdempotencyEnabled || def.IdempotencyEnabled
            };
        }

        return endpointCfg;
    }

    private static DispatchRegistration CreateRegistration(IConsumerDefinition def)
    {
        var handlerInterface = typeof(IMessageHandler<>).MakeGenericType(def.MessageType);
        var method = handlerInterface.GetMethod(nameof(IMessageHandler<object>.HandleAsync))!;

        Task HandlerDelegate(object handler, object data, ConsumeContext ctx, CancellationToken ct) =>
            (Task)method.Invoke(handler, [data, ctx, ct])!;

        return new DispatchRegistration(def.EndpointName, def.TypeId, def.MessageType, def.ConsumerType, HandlerDelegate);
    }

    private static EndpointRuntimeConfig CreateEndpointConfig(IConsumerDefinition def) =>
        new(
            def.EndpointName,
            Math.Max(1, def.ConcurrencyLimit),
            Math.Max(def.PrefetchCount, def.ConcurrencyLimit),
            def.LockTime,
            def.MaxAttempts,
            def.IdempotencyEnabled);
}