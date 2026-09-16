using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Bson;
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
    private readonly MessageLockRenewer _lockRenewer;

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
        _lockRenewer = new MessageLockRenewer(_inbox, log);
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
        var backoff = new FailureBackoff();
        Task<InboxMessage?> LockNext() => _pump.TryLockOneAsync(cfg.EndpointId, cfg.TypeIds, cfg.LockTime, LockOwnerFor(cfg, pumpId), ct);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var msg = await TryLockNextAsync(LockNext, backoff, cfg.EndpointId, ct);
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

    /// <summary>
    /// A renewing endpoint gives each lock its own owner: its fetch loop can re-lock a message whose lock lapsed while an
    /// older copy still waits in the channel, and distinct owners let the dispatch-time re-claim skip the stale copy.
    /// Other endpoints keep the pump id, so the first of their overlapping copies to finish still records the outcome.
    /// </summary>
    private static string LockOwnerFor(EndpointRuntimeConfig cfg, string pumpId) =>
        cfg.RenewLock ? $"{pumpId}:{ObjectId.GenerateNewId()}" : pumpId;

    /// <returns>The locked message, or null when none is available or locking failed.</returns>
    private async Task<InboxMessage?> TryLockNextAsync(
        Func<Task<InboxMessage?>> lockNext,
        FailureBackoff backoff,
        string endpointId,
        CancellationToken ct)
    {
        try
        {
            var message = await lockNext();
            backoff.Reset();
            return message;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            var retryDelay = backoff.NextDelay();
            _log.LogError(ex, "Could not lock the next message for endpoint {Endpoint}; retrying in {RetryDelay}", endpointId, retryDelay);
            await Task.Delay(retryDelay, ct);
            return null;
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
        var backoff = new FailureBackoff();
        Task<InboxMessage?> LockNext() => _pump.TryLockOneAsync(cfg.EndpointId, new[] { cfg.TypeId }, cfg.LockTime, pumpId, ct);

        while (!ct.IsCancellationRequested)
        {
            // The slot is taken before the first message is locked. A worker that waited for it afterwards would
            // hold its batch's locks for as long as the wait lasted, which nothing bounds, until they lapsed and a
            // competing consumer took the messages.
            if (limiter is not null)
                await limiter.WaitAsync(ct);

            try
            {
                var batchStart = DateTime.UtcNow;
                var firstMsg = await TryLockNextAsync(LockNext, backoff, cfg.EndpointId, ct);
                if (firstMsg is null)
                {
                    await Task.Delay(50, ct);
                    continue;
                }

                var messages = new List<InboxMessage> { firstMsg };
                var lastReceived = DateTime.UtcNow;
                var assemblyDeadline = batchStart.Add(AssemblyWindowFor(cfg.LockTime));

                while (messages.Count < cfg.Options.MaxBatchSize)
                {
                    var now = DateTime.UtcNow;
                    var elapsed = now - batchStart;
                    var idle = now - lastReceived;

                    if (now >= assemblyDeadline)
                        break;

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

                    var next = await TryLockNextAsync(LockNext, backoff, cfg.EndpointId, ct);
                    if (next is null)
                    {
                        await Task.Delay(20, ct);
                        continue;
                    }

                    messages.Add(next);
                    lastReceived = DateTime.UtcNow;
                }

                await DispatchBatchAsync(cfg, messages, batchStart, ct);
            }
            finally
            {
                limiter?.Release();
            }
        }
    }

    /// <summary>
    /// A batch holds every message's lock for as long as it is being assembled, so assembly stops halfway through the
    /// lock and leaves the rest of it for the handler. Without this bound a batch that never reaches its minimum size
    /// waits for messages that may never arrive: its own messages' locks lapse, the worker locks them again, and the
    /// handler is given the same message several times over in one batch.
    /// </summary>
    private static TimeSpan AssemblyWindowFor(TimeSpan lockTime) => lockTime / 2;

    private async Task DispatchBatchAsync(BatchRuntimeConfig cfg, IReadOnlyList<InboxMessage> messages, DateTime batchStart, CancellationToken ct)
    {
        try
        {
            var ctxList = new List<ConsumeContext>(messages.Count);
            var filteredMessages = new List<InboxMessage>(messages.Count);

            foreach (var msg in messages)
            {
                var ctx = BuildConsumeContext(msg);

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
                var ctx = BuildConsumeContext(msg);

                if (cfg.IdempotencyEnabled && !string.IsNullOrEmpty(ctx.CloudEventId))
                {
                    var shouldSkip = await TrySkipIdempotentAsync(cfg.EndpointId, msg, ctx.CloudEventId, ct);
                    if (shouldSkip) continue;
                }

                if (cfg.RenewLock)
                    await DispatchUnderLeaseAsync(cfg, msg, ctx, ct);
                else
                    await _dispatcher.DispatchAsync(msg, ctx, ct);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Unexpected error in WorkerLoop for endpoint {Endpoint}", cfg.EndpointId);
            }
        }
    }

    private async Task DispatchUnderLeaseAsync(EndpointRuntimeConfig cfg, InboxMessage msg, ConsumeContext ctx, CancellationToken ct)
    {
        await using var lease = await _lockRenewer.TryAcquireLeaseAsync(msg, cfg.LockTime, ct);
        if (lease is null)
        {
            _log.LogInformation(
                "Message {MessageId} on endpoint {Endpoint} was re-locked while waiting to be dispatched; skipping this copy.",
                msg.Id, cfg.EndpointId);
            return;
        }

        using var dispatchCancellation = CancellationTokenSource.CreateLinkedTokenSource(ct, lease.LockLost);
        await _dispatcher.DispatchAsync(msg, ctx, dispatchCancellation.Token);
    }

    /// <summary>
    /// Builds the consume context from the CloudEvent envelope. When the envelope cannot be read, the context
    /// falls back to what the inbox document records, so the message still reaches the dispatcher, whose
    /// failure handling retries and eventually dead-letters it, instead of staying locked and retried forever.
    /// </summary>
    private static ConsumeContext BuildConsumeContext(InboxMessage msg)
    {
        try
        {
            using var doc = JsonDocument.Parse(msg.PayloadJson);
            return BuildConsumeContext(msg, doc.RootElement);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            return new ConsumeContext(msg.EndpointId, msg.TypeId, msg.Id, msg.Attempt, null, "", msg.CloudEventId ?? "", msg.CorrelationId, msg.CausationId);
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
