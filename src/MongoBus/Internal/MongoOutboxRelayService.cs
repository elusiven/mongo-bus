using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoDB.Driver;

namespace MongoBus.Internal;

internal sealed class MongoOutboxRelayService : BackgroundService
{
    private readonly IMongoCollection<OutboxMessage> _outbox;
    private readonly IMongoCollection<InboxMessage> _inbox;
    private readonly IMongoCollection<Binding> _bindings;
    private readonly MongoBusOptions _options;
    private readonly ILogger<MongoOutboxRelayService> _log;

    public MongoOutboxRelayService(
        IMongoDatabase db,
        MongoBusOptions options,
        ILogger<MongoOutboxRelayService> log)
    {
        _outbox = db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);
        _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        _bindings = db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);
        _options = options;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Outbox.Enabled)
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
            return;
        }

        var lockOwner = $"{Environment.MachineName}:{Guid.NewGuid():N}:outbox";
        var backoff = new FailureBackoff();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var relayed = await TryRelayNextAsync(lockOwner, stoppingToken);
                backoff.Reset();

                if (!relayed)
                    await Task.Delay(_options.Outbox.PollingInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var retryDelay = backoff.NextDelay();
                _log.LogError(ex, "Outbox relay failed; retrying in {RetryDelay}", retryDelay);
                await Task.Delay(retryDelay, stoppingToken);
            }
        }
    }

    /// <returns>Whether a message was available to relay.</returns>
    private async Task<bool> TryRelayNextAsync(string lockOwner, CancellationToken ct)
    {
        var message = await TryLockOneAsync(lockOwner, ct);
        if (message is null)
            return false;

        await RelayOneAsync(message, lockOwner, ct);
        return true;
    }

    private async Task<OutboxMessage?> TryLockOneAsync(string lockOwner, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var filter = Builders<OutboxMessage>.Filter.And(
            Builders<OutboxMessage>.Filter.Eq(x => x.Status, OutboxStatus.Pending),
            Builders<OutboxMessage>.Filter.Lte(x => x.VisibleUtc, now),
            Builders<OutboxMessage>.Filter.Or(
                Builders<OutboxMessage>.Filter.Eq(x => x.LockedUntilUtc, null),
                Builders<OutboxMessage>.Filter.Lte(x => x.LockedUntilUtc, now)));

        var update = Builders<OutboxMessage>.Update
            .Set(x => x.LockOwner, lockOwner)
            .Set(x => x.LockedUntilUtc, now.Add(_options.Outbox.LockTime));

        var opts = new FindOneAndUpdateOptions<OutboxMessage>
        {
            Sort = Builders<OutboxMessage>.Sort.Ascending(x => x.VisibleUtc),
            ReturnDocument = ReturnDocument.After
        };

        return await _outbox.FindOneAndUpdateAsync(filter, update, opts, ct);
    }

    private async Task RelayOneAsync(OutboxMessage message, string lockOwner, CancellationToken ct)
    {
        try
        {
            var routes = await _bindings
                .Find(x => x.Topic == message.Topic)
                .ToListAsync(ct);

            if (routes.Count > 0)
            {
                var docs = await BuildInboxMessagesAsync(message, routes, ct);
                if (docs.Count == 1)
                {
                    await _inbox.InsertOneAsync(docs[0], cancellationToken: ct);
                }
                else if (docs.Count > 1)
                {
                    await _inbox.InsertManyAsync(docs, cancellationToken: ct);
                }
            }

            await _outbox.UpdateOneAsync(
                x => x.Id == message.Id && x.LockOwner == lockOwner,
                Builders<OutboxMessage>.Update
                    .Set(x => x.Status, OutboxStatus.Published)
                    .Set(x => x.PublishedUtc, DateTime.UtcNow)
                    .Set(x => x.LockOwner, null)
                    .Set(x => x.LockedUntilUtc, null)
                    .Set(x => x.LastError, null),
                cancellationToken: ct);
        }
        catch (Exception ex)
        {
            await HandleRelayFailureAsync(message, lockOwner, ex, ct);
        }
    }

    private async Task<List<InboxMessage>> BuildInboxMessagesAsync(OutboxMessage message, IReadOnlyList<Binding> routes, CancellationToken ct)
    {
        var endpointIds = routes.Select(x => x.EndpointId).Distinct().ToArray();
        HashSet<string> alreadyPublishedEndpoints = [];

        if (!string.IsNullOrWhiteSpace(message.CloudEventId))
        {
            var existingFilter = Builders<InboxMessage>.Filter.And(
                Builders<InboxMessage>.Filter.Eq(x => x.CloudEventId, message.CloudEventId),
                Builders<InboxMessage>.Filter.In(x => x.EndpointId, endpointIds));

            var existingEndpointIds = await _inbox
                .Find(existingFilter)
                .Project(x => x.EndpointId)
                .ToListAsync(ct);
            alreadyPublishedEndpoints = existingEndpointIds.ToHashSet(StringComparer.Ordinal);
        }

        var now = DateTime.UtcNow;
        var docs = new List<InboxMessage>(endpointIds.Length);

        foreach (var endpointId in endpointIds)
        {
            if (alreadyPublishedEndpoints.Contains(endpointId))
                continue;

            docs.Add(new InboxMessage
            {
                EndpointId = endpointId,
                Topic = message.Topic,
                TypeId = message.TypeId,
                PayloadJson = message.PayloadJson,
                CreatedUtc = now,
                VisibleUtc = now,
                Attempt = 0,
                Status = InboxStatus.Pending,
                CorrelationId = message.CorrelationId,
                CausationId = message.CausationId,
                CloudEventId = message.CloudEventId
            });
        }

        return docs;
    }

    private async Task HandleRelayFailureAsync(OutboxMessage message, string lockOwner, Exception ex, CancellationToken ct)
    {
        _log.LogError(ex, "Outbox relay failed for message {OutboxMessageId}", message.Id);

        var nextAttempt = message.Attempt + 1;
        if (nextAttempt >= _options.Outbox.MaxAttempts)
        {
            await _outbox.UpdateOneAsync(
                x => x.Id == message.Id && x.LockOwner == lockOwner,
                Builders<OutboxMessage>.Update
                    .Set(x => x.Status, OutboxStatus.Dead)
                    .Set(x => x.Attempt, nextAttempt)
                    .Set(x => x.LastError, ErrorMessageFormatter.Describe(ex))
                    .Set(x => x.LockOwner, null)
                    .Set(x => x.LockedUntilUtc, null),
                cancellationToken: ct);
            return;
        }

        var delay = TimeSpan.FromSeconds(Math.Pow(2, nextAttempt));
        await _outbox.UpdateOneAsync(
            x => x.Id == message.Id && x.LockOwner == lockOwner,
            Builders<OutboxMessage>.Update
                .Set(x => x.Status, OutboxStatus.Pending)
                .Set(x => x.Attempt, nextAttempt)
                .Set(x => x.VisibleUtc, DateTime.UtcNow.Add(delay))
                .Set(x => x.LastError, ErrorMessageFormatter.Describe(ex))
                .Set(x => x.LockOwner, null)
                .Set(x => x.LockedUntilUtc, null),
            cancellationToken: ct);
    }
}
