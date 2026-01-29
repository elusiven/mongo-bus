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

        await BindTopologyAsync(stoppingToken);

        _ = DispatchRegistrationBuilder.BuildDispatchMap(_definitions);
        var endpointCfg = DispatchRegistrationBuilder.BuildEndpointConfigs(_definitions);

        var pumps = endpointCfg.Values.Select(cfg => RunEndpointPumpAsync(cfg, stoppingToken)).ToArray();
        await Task.WhenAll(pumps);
    }

    private async Task BindTopologyAsync(CancellationToken ct)
    {
        foreach (var def in _definitions)
            await _topology.BindAsync(def.EndpointName, def.TypeId, ct);
    }

    private async Task RunEndpointPumpAsync(EndpointRuntimeConfig cfg, CancellationToken ct)
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
                
                var ctx = BuildConsumeContext(msg, root);

                if (cfg.IdempotencyEnabled && !string.IsNullOrEmpty(ctx.CloudEventId))
                {
                    var shouldSkip = await TrySkipIdempotentAsync(cfg.EndpointId, msg, ctx.CloudEventId, ct);
                    if (shouldSkip) continue;
                }

                await _dispatcher.DispatchAsync(msg, ctx, ct);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Unexpected error in WorkerLoop for endpoint {Endpoint}", cfg.EndpointId);
            }
        }
    }

    private static ConsumeContext BuildConsumeContext(InboxMessage msg, JsonElement root)
    {
        var cloudEventId = root.TryGetProperty("id", out var idEl) ? idEl.GetString() ?? "" : "";
        var source = root.TryGetProperty("source", out var srcEl) ? srcEl.GetString() ?? "" : "";
        string? subject = root.TryGetProperty("subject", out var subjEl) && subjEl.ValueKind == JsonValueKind.String
            ? subjEl.GetString()
            : null;

        var correlationId = root.TryGetProperty("correlationId", out var corrEl) ? corrEl.GetString() : null;
        var causationId = root.TryGetProperty("causationId", out var causEl) ? causEl.GetString() : null;

        return new ConsumeContext(msg.EndpointId, msg.TypeId, msg.Id, msg.Attempt, subject, source, cloudEventId, correlationId, causationId);
    }

    private async Task<bool> TrySkipIdempotentAsync(string endpointId, InboxMessage msg, string cloudEventId, CancellationToken ct)
    {
        var alreadyProcessed = await _inbox.Find(x =>
            x.EndpointId == endpointId &&
            x.CloudEventId == cloudEventId &&
            x.Status == InboxStatus.Processed &&
            x.Id != msg.Id).AnyAsync(ct);

        if (!alreadyProcessed)
            return false;

        _log.LogInformation("Message {CloudEventId} already processed by endpoint {EndpointId}. Skipping.", cloudEventId, endpointId);

        await _inbox.UpdateOneAsync(
            x => x.Id == msg.Id,
            Builders<InboxMessage>.Update
                .Set(x => x.Status, InboxStatus.Processed)
                .Set(x => x.ProcessedUtc, DateTime.UtcNow)
                .Set(x => x.LockOwner, null)
                .Set(x => x.LockedUntilUtc, null)
                .Set(x => x.LastError, "Skipped due to idempotency"),
            cancellationToken: ct);

        return true;
    }

}
