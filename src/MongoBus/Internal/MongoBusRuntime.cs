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

internal sealed class MongoBusRuntime : BackgroundService
{
    private readonly IMongoCollection<InboxMessage> _inbox;
    private readonly ITopologyManager _topology;
    private readonly IMessageDispatcher _dispatcher;
    private readonly IBatchMessageDispatcher _batchDispatcher;
    private readonly IMessagePump _pump;
    private readonly ILogger<MongoBusRuntime> _log;
    private readonly IReadOnlyList<IConsumerDefinition> _definitions;

    public MongoBusRuntime(
        IMongoDatabase db,
        IEnumerable<IConsumerDefinition> definitions,
        ITopologyManager topology,
        IMessageDispatcher dispatcher,
        IBatchMessageDispatcher batchDispatcher,
        IMessagePump pump,
        ILogger<MongoBusRuntime> log)
    {
        _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        _topology = topology;
        _dispatcher = dispatcher;
        _batchDispatcher = batchDispatcher;
        _pump = pump;
        _log = log;
        _definitions = definitions.ToList();
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_definitions.Count > 0)
            await BindTopologyAsync(cancellationToken);

        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_definitions.Count == 0)
        {
            _log.LogWarning("MongoBusRuntime started with 0 consumer definitions registered.");
            await Task.Delay(Timeout.Infinite, stoppingToken);
            return;
        }

        var batchDefinitions = _definitions.OfType<IBatchConsumerDefinition>().ToList();
        var singleDefinitions = _definitions.Where(d => d is not IBatchConsumerDefinition).ToList();

        _ = DispatchRegistrationBuilder.BuildDispatchMap(singleDefinitions);
        _ = DispatchRegistrationBuilder.BuildBatchDispatchMap(batchDefinitions);

        var endpointCfg = DispatchRegistrationBuilder.BuildEndpointConfigs(singleDefinitions);
        var batchCfg = DispatchRegistrationBuilder.BuildBatchRuntimeConfigs(batchDefinitions);

        var singlePumps = endpointCfg.Values.Select(cfg => RunEndpointPumpAsync(cfg, stoppingToken)).ToArray();
        var batchPumps = batchCfg.Select(cfg => RunBatchPumpAsync(cfg, stoppingToken)).ToArray();

        await Task.WhenAll(singlePumps.Concat(batchPumps));
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
                var msg = await _pump.TryLockOneAsync(cfg.EndpointId, cfg.TypeIds, cfg.LockTime, pumpId, ct);
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

    private async Task RunBatchPumpAsync(BatchRuntimeConfig cfg, CancellationToken ct)
    {
        var pumpId = $"{Environment.MachineName}:{Guid.NewGuid():N}:{cfg.EndpointId}:{cfg.TypeId}";
        _log.LogInformation(
            "Starting batch consumer endpoint '{Endpoint}' type '{Type}' concurrency={C} batch={Min}-{Max} maxWait={MaxWait} idleWait={IdleWait}",
            cfg.EndpointId,
            cfg.TypeId,
            cfg.Concurrency,
            cfg.Options.MinBatchSize,
            cfg.Options.MaxBatchSize,
            cfg.Options.MaxBatchWaitTime,
            cfg.Options.MaxBatchIdleTime);

        var limiter = cfg.MaxInFlightBatches > 0
            ? new SemaphoreSlim(cfg.MaxInFlightBatches, cfg.MaxInFlightBatches)
            : null;

        var workers = Enumerable.Range(0, cfg.Concurrency)
            .Select(_ => BatchWorkerLoopAsync(cfg, pumpId, limiter, ct))
            .ToArray();

        await Task.WhenAll(workers);
    }

    private async Task BatchWorkerLoopAsync(BatchRuntimeConfig cfg, string pumpId, SemaphoreSlim? limiter, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var batchStart = DateTime.UtcNow;
            var firstMsg = await _pump.TryLockOneAsync(cfg.EndpointId, new[] { cfg.TypeId }, cfg.LockTime, pumpId, ct);
            if (firstMsg is null)
            {
                await Task.Delay(50, ct);
                continue;
            }

            var messages = new List<InboxMessage> { firstMsg };
            var lastReceived = DateTime.UtcNow;

            while (messages.Count < cfg.Options.MaxBatchSize)
            {
                var now = DateTime.UtcNow;
                var elapsed = now - batchStart;
                var idle = now - lastReceived;

                if (cfg.Options.FlushMode == BatchFlushMode.SinceFirstMessage)
                {
                    if (elapsed >= cfg.Options.MaxBatchWaitTime)
                        break;
                }
                else
                {
                    if (messages.Count >= cfg.Options.MinBatchSize && idle >= cfg.Options.MaxBatchIdleTime)
                        break;
                }

                var next = await _pump.TryLockOneAsync(cfg.EndpointId, new[] { cfg.TypeId }, cfg.LockTime, pumpId, ct);
                if (next is null)
                {
                    await Task.Delay(20, ct);
                    continue;
                }

                messages.Add(next);
                lastReceived = DateTime.UtcNow;
            }

            await DispatchBatchWithBackpressureAsync(cfg, limiter, messages, batchStart, ct);
        }
    }

    private async Task DispatchBatchWithBackpressureAsync(
        BatchRuntimeConfig cfg,
        SemaphoreSlim? limiter,
        IReadOnlyList<InboxMessage> messages,
        DateTime batchStart,
        CancellationToken ct)
    {
        if (limiter is null)
        {
            await DispatchBatchAsync(cfg, messages, batchStart, ct);
            return;
        }

        await limiter.WaitAsync(ct);
        try
        {
            await DispatchBatchAsync(cfg, messages, batchStart, ct);
        }
        finally
        {
            limiter.Release();
        }
    }

    private async Task DispatchBatchAsync(BatchRuntimeConfig cfg, IReadOnlyList<InboxMessage> messages, DateTime batchStart, CancellationToken ct)
    {
        try
        {
            var ctxList = new List<ConsumeContext>(messages.Count);
            var filteredMessages = new List<InboxMessage>(messages.Count);

            foreach (var msg in messages)
            {
                using var doc = JsonDocument.Parse(msg.PayloadJson);
                var root = doc.RootElement;

                var ctx = BuildConsumeContext(msg, root);

                if (cfg.IdempotencyEnabled && !string.IsNullOrEmpty(ctx.CloudEventId))
                {
                    var shouldSkip = await TrySkipIdempotentAsync(cfg.EndpointId, msg, ctx.CloudEventId, ct);
                    if (shouldSkip) continue;
                }

                filteredMessages.Add(msg);
                ctxList.Add(ctx);
            }

            if (filteredMessages.Count == 0)
                return;

            var batchContext = new BatchConsumeContext(cfg.EndpointId, cfg.TypeId, ctxList, batchStart, DateTime.UtcNow);
            await _batchDispatcher.DispatchBatchAsync(filteredMessages, batchContext, ct);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Unexpected error in BatchWorkerLoop for endpoint {Endpoint} type {Type}", cfg.EndpointId, cfg.TypeId);
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
