using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Driver;

namespace MongoBus.Internal;

internal sealed record DispatchRegistration(
    string EndpointId,
    string TypeId,
    Type MessageClrType,
    Type HandlerInterface,
    Func<object, object, ConsumeContext, CancellationToken, Task> HandlerDelegate);
internal sealed record EndpointRuntimeConfig(string EndpointId, int Concurrency, int Prefetch, TimeSpan LockTime, int MaxAttempts, bool IdempotencyEnabled);

public sealed class MongoBusRuntime : BackgroundService
{
    private readonly IMongoCollection<InboxMessage> _inbox;
    private readonly ITopologyManager _topology;
    private readonly IMessageDispatcher _dispatcher;
    private readonly IMessagePump _pump;
    private readonly ILogger<MongoBusRuntime> _log;
    private readonly IReadOnlyList<IConsumerDefinition> _definitions;

    public MongoBusRuntime(
        IMongoDatabase db,
        IEnumerable<IConsumerDefinition> definitions,
        ITopologyManager topology,
        IMessageDispatcher dispatcher,
        IMessagePump pump,
        ILogger<MongoBusRuntime> log)
    {
        _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        _topology = topology;
        _dispatcher = dispatcher;
        _pump = pump;
        _log = log;
        _definitions = definitions.ToList();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_definitions.Count == 0)
        {
            _log.LogWarning("MongoBusRuntime started with 0 consumer definitions registered.");
            await Task.Delay(Timeout.Infinite, stoppingToken);
            return;
        }

        // Bind topology
        foreach (var def in _definitions)
            await _topology.BindAsync(def.EndpointName, def.TypeId, stoppingToken);

        // Build dispatch + endpoint configs
        var dispatch = new Dictionary<(string EndpointId, string TypeId), DispatchRegistration>();
        var endpointCfg = new Dictionary<string, EndpointRuntimeConfig>();

        foreach (var def in _definitions)
        {
            var key = (def.EndpointName, def.TypeId);
            var handlerInterface = typeof(IMessageHandler<>).MakeGenericType(def.MessageType);
            var handlerType = def.ConsumerType;

            if (dispatch.ContainsKey(key))
                throw new InvalidOperationException($"Duplicate (endpoint,type) registration: {def.EndpointName} / {def.TypeId}");

            var method = handlerInterface.GetMethod(nameof(IMessageHandler<object>.HandleAsync))!;
            
            Task HandlerDelegate(object handler, object data, ConsumeContext ctx, CancellationToken ct) =>
                (Task)method.Invoke(handler, [data, ctx, ct])!;

            dispatch[key] = new DispatchRegistration(def.EndpointName, def.TypeId, def.MessageType, handlerType, HandlerDelegate);

            if (!endpointCfg.TryGetValue(def.EndpointName, out var existing))
            {
                endpointCfg[def.EndpointName] = new EndpointRuntimeConfig(
                    def.EndpointName,
                    Math.Max(1, def.ConcurrencyLimit),
                    Math.Max(def.PrefetchCount, def.ConcurrencyLimit),
                    def.LockTime,
                    def.MaxAttempts,
                    def.IdempotencyEnabled);
            }
            else
            {
                endpointCfg[def.EndpointName] = existing with
                {
                    Concurrency = Math.Max(existing.Concurrency, Math.Max(1, def.ConcurrencyLimit)),
                    Prefetch = Math.Max(existing.Prefetch, Math.Max(def.PrefetchCount, def.ConcurrencyLimit)),
                    LockTime = existing.LockTime > def.LockTime ? existing.LockTime : def.LockTime,
                    MaxAttempts = Math.Max(existing.MaxAttempts, def.MaxAttempts),
                    IdempotencyEnabled = existing.IdempotencyEnabled || def.IdempotencyEnabled
                };
            }
        }

        var pumps = endpointCfg.Values.Select(cfg => RunEndpointPumpAsync(cfg, dispatch, stoppingToken)).ToArray();
        await Task.WhenAll(pumps);
    }

    private async Task RunEndpointPumpAsync(
        EndpointRuntimeConfig cfg,
        IReadOnlyDictionary<(string EndpointId, string TypeId), DispatchRegistration> dispatch,
        CancellationToken ct)
    {
        var pumpId = $"{Environment.MachineName}:{Guid.NewGuid():N}:{cfg.EndpointId}";
        _log.LogInformation("Starting endpoint '{Endpoint}' concurrency={C} prefetch={P}", cfg.EndpointId, cfg.Concurrency, cfg.Prefetch);

        var channel = Channel.CreateBounded<InboxMessage>(new BoundedChannelOptions(cfg.Prefetch)
        {
            SingleWriter = true,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait
        });

        var fetchTask = FetchLoopAsync(cfg, pumpId, channel.Writer, ct);
        var workers = Enumerable.Range(0, cfg.Concurrency)
            .Select(i => WorkerLoopAsync(cfg, pumpId, i, channel.Reader, ct))
            .ToArray();

        await Task.WhenAll(workers.Prepend(fetchTask));
    }

    private async Task FetchLoopAsync(EndpointRuntimeConfig cfg, string pumpId, ChannelWriter<InboxMessage> writer, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var msg = await _pump.TryLockOneAsync(cfg.EndpointId, cfg.LockTime, pumpId, ct);
                if (msg is null)
                {
                    await Task.Delay(50, ct);
                    continue;
                }
                await writer.WriteAsync(msg, ct);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            writer.TryComplete();
        }
    }

    private async Task WorkerLoopAsync(
        EndpointRuntimeConfig cfg,
        string pumpId,
        int workerIndex,
        ChannelReader<InboxMessage> reader,
        CancellationToken ct)
    {
        await foreach (var msg in reader.ReadAllAsync(ct))
        {
            try
            {
                // Parse CloudEvent basic metadata for the context
                using var doc = JsonDocument.Parse(msg.PayloadJson);
                var root = doc.RootElement;
                
                var cloudEventId = root.TryGetProperty("id", out var idEl) ? idEl.GetString() ?? "" : "";
                var source = root.TryGetProperty("source", out var srcEl) ? srcEl.GetString() ?? "" : "";
                string? subject = root.TryGetProperty("subject", out var subjEl) && subjEl.ValueKind == JsonValueKind.String 
                    ? subjEl.GetString() 
                    : null;
                
                var correlationId = root.TryGetProperty("correlationId", out var corrEl) ? corrEl.GetString() : null;
                var causationId = root.TryGetProperty("causationId", out var causEl) ? causEl.GetString() : null;

                if (cfg.IdempotencyEnabled && !string.IsNullOrEmpty(cloudEventId))
                {
                    var alreadyProcessed = await _inbox.Find(x => 
                        x.EndpointId == cfg.EndpointId && 
                        x.CloudEventId == cloudEventId && 
                        x.Status == "Processed" &&
                        x.Id != msg.Id).AnyAsync(ct);

                    if (alreadyProcessed)
                    {
                        _log.LogInformation("Message {CloudEventId} already processed by endpoint {EndpointId}. Skipping.", cloudEventId, cfg.EndpointId);
                        
                        await _inbox.UpdateOneAsync(
                            x => x.Id == msg.Id,
                            Builders<InboxMessage>.Update
                                .Set(x => x.Status, "Processed")
                                .Set(x => x.ProcessedUtc, DateTime.UtcNow)
                                .Set(x => x.LockOwner, null)
                                .Set(x => x.LockedUntilUtc, null)
                                .Set(x => x.LastError, "Skipped due to idempotency"),
                            cancellationToken: ct);
                        continue;
                    }
                }

                var ctx = new ConsumeContext(msg.EndpointId, msg.TypeId, msg.Id, msg.Attempt, subject, source, cloudEventId, correlationId, causationId);
                
                // Use the dispatcher
                await _dispatcher.DispatchAsync(msg, ctx, ct);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Unexpected error in WorkerLoop for endpoint {Endpoint}", cfg.EndpointId);
            }
        }
    }

}
