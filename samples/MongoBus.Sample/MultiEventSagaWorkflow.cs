// =============================================================================
// Multi-event saga workflow — durable step-by-step external orchestration.
//
// Why this sample exists
// ----------------------
// A saga state machine saves its instance state ONCE at the end of an event
// handler — after every activity (Then/ThenAsync/Publish/Send/...) in the
// chain has run. If the handler crashes mid-chain, intermediate writes to the
// saga instance are lost and the message redelivers from the start.
//
// For workflows that orchestrate multiple non-idempotent external calls
// (e.g. uploading media to a third-party API, then attaching metadata, then
// publishing it live), that semantic isn't enough on its own: a mid-chain
// crash can re-issue an external call that already succeeded, producing
// duplicates the external system can't unwind.
//
// The pattern shown here splits the long workflow into one *event per
// external call*. Each event becomes its own saga transition with its own
// durable checkpoint. Workers (regular consumers) make the external calls;
// the saga only coordinates state. A stable idempotency token passed to the
// external API closes the residual window where a worker could complete the
// call but crash before publishing the result event.
//
// What gets demonstrated
// ----------------------
// - Three sequential external steps modeled as three saga states.
// - Each external step is performed by its own consumer, not inside the saga.
// - A stubbed external client (FakeMetaClient) that fails the first attempt
//   of each operation to prove the retry path is safe.
// - Idempotency tokens derived from (correlationId, step) so a retried worker
//   call returns the same result instead of double-applying.
// =============================================================================

using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.Models;
using MongoBus.Models.Saga;

namespace MongoBus.Sample.MultiEventSaga;

// --- Step commands the saga emits to drive workers ---

public record StartMediaUpload(string PostId, string MediaUrl);
public record StartMetadataAttach(string PostId, string ContainerId, string Caption);
public record StartMediaPublish(string PostId, string ContainerId);

// --- Result events workers publish back to the saga ---

public record MediaUploaded(string ContainerId);
public record MetadataAttached(string ContainerId);
public record MediaPublished(string PublishedMediaId);

// --- Initial trigger from the application ---

public record PublishToMetaRequested(string PostId, string MediaUrl, string Caption);

// --- Saga instance: every property mutation between transitions is durable ---

public class MetaPublishSagaState : ISagaInstance
{
    public string CorrelationId { get; set; } = default!;
    public string CurrentState { get; set; } = default!;
    public int Version { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime LastModifiedUtc { get; set; }

    public string? PostId { get; set; }
    public string? MediaUrl { get; set; }
    public string? Caption { get; set; }

    public string? ContainerId { get; set; }
    public string? PublishedMediaId { get; set; }
}

// --- State machine: one state per external step ---

public class MetaPublishStateMachine : MongoBusStateMachine<MetaPublishSagaState>
{
    public SagaState UploadingMedia { get; private set; }
    public SagaState AttachingMetadata { get; private set; }
    public SagaState Publishing { get; private set; }
    public SagaState Completed { get; private set; }

    public SagaEvent<PublishToMetaRequested> Requested { get; private set; }
    public SagaEvent<MediaUploaded> Uploaded { get; private set; }
    public SagaEvent<MetadataAttached> Attached { get; private set; }
    public SagaEvent<MediaPublished> Published { get; private set; }

    public MetaPublishStateMachine()
    {
        Event(() => Requested, "meta.publish.requested",
            e => e.CorrelateById(ctx => ctx.CorrelationId!));
        Event(() => Uploaded, "meta.media.uploaded",
            e => e.CorrelateById(ctx => ctx.CorrelationId!));
        Event(() => Attached, "meta.metadata.attached",
            e => e.CorrelateById(ctx => ctx.CorrelationId!));
        Event(() => Published, "meta.media.published",
            e => e.CorrelateById(ctx => ctx.CorrelationId!));

        InstanceState(x => x.CurrentState);

        Initially(
            When(Requested)
                .Then(ctx =>
                {
                    ctx.Saga.PostId = ctx.Message.PostId;
                    ctx.Saga.MediaUrl = ctx.Message.MediaUrl;
                    ctx.Saga.Caption = ctx.Message.Caption;
                })
                .Publish("meta.upload.start", ctx => new StartMediaUpload(
                    ctx.Saga.PostId!, ctx.Saga.MediaUrl!))
                .TransitionTo(UploadingMedia));

        During(UploadingMedia,
            When(Uploaded)
                .Then(ctx => ctx.Saga.ContainerId = ctx.Message.ContainerId)
                .Publish("meta.metadata.start", ctx => new StartMetadataAttach(
                    ctx.Saga.PostId!, ctx.Saga.ContainerId!, ctx.Saga.Caption!))
                .TransitionTo(AttachingMetadata));

        During(AttachingMetadata,
            When(Attached)
                .Publish("meta.publish.start", ctx => new StartMediaPublish(
                    ctx.Saga.PostId!, ctx.Saga.ContainerId!))
                .TransitionTo(Publishing));

        During(Publishing,
            When(Published)
                .Then(ctx => ctx.Saga.PublishedMediaId = ctx.Message.PublishedMediaId)
                .TransitionTo(Completed)
                .Finalize());

        SetCompletedWhenFinalized();
    }
}

// --- Stubbed external client.
//
// First attempt of every operation throws to demonstrate retry safety.
// Subsequent attempts with the same idempotency key return the SAME recorded
// result rather than performing the side effect again — this is how real
// idempotent APIs (Stripe, Meta Graph API with client_request_id, etc.)
// behave, and the property the saga relies on for at-most-once externally.
public sealed class FakeMetaClient(ILogger<FakeMetaClient> logger)
{
    private readonly Dictionary<string, string> _results = new();
    private readonly HashSet<string> _seenIdempotencyKeys = new();
    private readonly object _gate = new();

    public Task<string> UploadAsync(string idempotencyKey, string mediaUrl)
        => SimulateAsync(idempotencyKey, op: "upload",
            produce: () => $"container-{Guid.NewGuid():N}");

    public Task<string> AttachMetadataAsync(string idempotencyKey, string containerId, string caption)
        => SimulateAsync(idempotencyKey, op: "attach",
            produce: () => containerId);

    public Task<string> PublishAsync(string idempotencyKey, string containerId)
        => SimulateAsync(idempotencyKey, op: "publish",
            produce: () => $"published-{Guid.NewGuid():N}");

    private Task<string> SimulateAsync(string idempotencyKey, string op, Func<string> produce)
    {
        lock (_gate)
        {
            if (_results.TryGetValue(idempotencyKey, out var prior))
            {
                logger.LogInformation(
                    "FakeMeta[{Op}] idempotent hit for key {Key} → returning prior result {Result}",
                    op, idempotencyKey, prior);
                return Task.FromResult(prior);
            }

            if (_seenIdempotencyKeys.Add(idempotencyKey))
            {
                logger.LogWarning(
                    "FakeMeta[{Op}] simulated transient failure for key {Key} (first attempt)",
                    op, idempotencyKey);
                throw new InvalidOperationException($"transient failure on first {op} attempt");
            }

            var value = produce();
            _results[idempotencyKey] = value;
            logger.LogInformation(
                "FakeMeta[{Op}] succeeded for key {Key} → {Result}",
                op, idempotencyKey, value);
            return Task.FromResult(value);
        }
    }
}

// --- Workers: one consumer per external step. ---
//
// Each worker computes its idempotency key from (correlationId, step). That
// key is what makes the *external* call at-most-once even when the worker
// retries after publishing the result but crashing before the inbox is
// marked Processed.

public sealed class UploadMediaWorker(
    FakeMetaClient meta,
    IMessageBus bus,
    ILogger<UploadMediaWorker> logger)
    : IMessageHandler<StartMediaUpload>
{
    public async Task HandleAsync(StartMediaUpload message, ConsumeContext context, CancellationToken ct)
    {
        var key = $"{context.CorrelationId}:upload";
        var containerId = await meta.UploadAsync(key, message.MediaUrl);

        logger.LogInformation("Upload complete for post {PostId} → {ContainerId}", message.PostId, containerId);

        await bus.PublishAsync("meta.media.uploaded", new MediaUploaded(containerId),
            correlationId: context.CorrelationId,
            causationId: context.CloudEventId,
            ct: ct);
    }
}

public sealed class AttachMetadataWorker(
    FakeMetaClient meta,
    IMessageBus bus,
    ILogger<AttachMetadataWorker> logger)
    : IMessageHandler<StartMetadataAttach>
{
    public async Task HandleAsync(StartMetadataAttach message, ConsumeContext context, CancellationToken ct)
    {
        var key = $"{context.CorrelationId}:attach";
        var containerId = await meta.AttachMetadataAsync(key, message.ContainerId, message.Caption);

        logger.LogInformation("Metadata attached for post {PostId}", message.PostId);

        await bus.PublishAsync("meta.metadata.attached", new MetadataAttached(containerId),
            correlationId: context.CorrelationId,
            causationId: context.CloudEventId,
            ct: ct);
    }
}

public sealed class PublishMediaWorker(
    FakeMetaClient meta,
    IMessageBus bus,
    ILogger<PublishMediaWorker> logger)
    : IMessageHandler<StartMediaPublish>
{
    public async Task HandleAsync(StartMediaPublish message, ConsumeContext context, CancellationToken ct)
    {
        var key = $"{context.CorrelationId}:publish";
        var publishedId = await meta.PublishAsync(key, message.ContainerId);

        logger.LogInformation("Post {PostId} published as {PublishedId}", message.PostId, publishedId);

        await bus.PublishAsync("meta.media.published", new MediaPublished(publishedId),
            correlationId: context.CorrelationId,
            causationId: context.CloudEventId,
            ct: ct);
    }
}

// --- Consumer definitions ---

public sealed class StartMediaUploadDefinition : ConsumerDefinition<UploadMediaWorker, StartMediaUpload>
{
    public override string TypeId => "meta.upload.start";
    public override string EndpointName => "meta-upload-worker";
    public override int MaxAttempts => 5;
    public override bool IdempotencyEnabled => true;
}

public sealed class StartMetadataAttachDefinition : ConsumerDefinition<AttachMetadataWorker, StartMetadataAttach>
{
    public override string TypeId => "meta.metadata.start";
    public override string EndpointName => "meta-metadata-worker";
    public override int MaxAttempts => 5;
    public override bool IdempotencyEnabled => true;
}

public sealed class StartMediaPublishDefinition : ConsumerDefinition<PublishMediaWorker, StartMediaPublish>
{
    public override string TypeId => "meta.publish.start";
    public override string EndpointName => "meta-publish-worker";
    public override int MaxAttempts => 5;
    public override bool IdempotencyEnabled => true;
}
