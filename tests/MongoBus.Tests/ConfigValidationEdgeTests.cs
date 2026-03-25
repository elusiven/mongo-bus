using FluentAssertions;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Internal;
using Xunit;

namespace MongoBus.Tests;

public class ConfigValidationEdgeTests
{
    #region ClaimCheck validation edges

    [Fact]
    public void ClaimCheck_Enabled_NoProviderName_Throws()
    {
        var options = ValidOptions();
        options.ClaimCheck.Enabled = true;
        options.ClaimCheck.ProviderName = null;

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*ProviderName*");
    }

    [Fact]
    public void ClaimCheck_Enabled_ThresholdZero_Throws()
    {
        var options = ValidOptions();
        options.ClaimCheck.Enabled = true;
        options.ClaimCheck.ProviderName = "memory";
        options.ClaimCheck.ThresholdBytes = 0;

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*ThresholdBytes*");
    }

    [Fact]
    public void ClaimCheck_Compression_Enabled_NoAlgorithm_Throws()
    {
        var options = ValidOptions();
        options.ClaimCheck.Enabled = true;
        options.ClaimCheck.ProviderName = "memory";
        options.ClaimCheck.Compression.Enabled = true;
        options.ClaimCheck.Compression.Algorithm = "";

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Compression.Algorithm*");
    }

    [Fact]
    public void ClaimCheck_Cleanup_IntervalZero_Throws()
    {
        var options = ValidOptions();
        options.ClaimCheck.Enabled = true;
        options.ClaimCheck.ProviderName = "memory";
        options.ClaimCheck.Cleanup.Interval = TimeSpan.Zero;

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Cleanup.Interval*");
    }

    [Fact]
    public void ClaimCheck_Cleanup_MinimumAgeNegative_Throws()
    {
        var options = ValidOptions();
        options.ClaimCheck.Enabled = true;
        options.ClaimCheck.ProviderName = "memory";
        options.ClaimCheck.Cleanup.MinimumAge = TimeSpan.FromSeconds(-1);

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Cleanup.MinimumAge*");
    }

    [Fact]
    public void ClaimCheck_Cleanup_MinimumAgeLessThanTtl_Throws()
    {
        var options = ValidOptions();
        options.ClaimCheck.Enabled = true;
        options.ClaimCheck.ProviderName = "memory";
        options.ProcessedMessageTtl = TimeSpan.FromDays(7);
        options.ClaimCheck.Cleanup.MinimumAge = TimeSpan.FromDays(5); // less than ProcessedMessageTtl

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*MinimumAge*ProcessedMessageTtl*");
    }

    [Fact]
    public void ClaimCheck_Disabled_SkipsValidation()
    {
        var options = ValidOptions();
        options.ClaimCheck.Enabled = false;
        options.ClaimCheck.ProviderName = null;
        options.ClaimCheck.ThresholdBytes = 0;

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().NotThrow();
    }

    #endregion

    #region Outbox validation edges

    [Fact]
    public void Outbox_Enabled_PollingIntervalZero_Throws()
    {
        var options = ValidOptions();
        options.Outbox.Enabled = true;
        options.Outbox.PollingInterval = TimeSpan.Zero;

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*PollingInterval*");
    }

    [Fact]
    public void Outbox_Enabled_LockTimeZero_Throws()
    {
        var options = ValidOptions();
        options.Outbox.Enabled = true;
        options.Outbox.LockTime = TimeSpan.Zero;

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*LockTime*");
    }

    [Fact]
    public void Outbox_Enabled_MaxAttemptsZero_Throws()
    {
        var options = ValidOptions();
        options.Outbox.Enabled = true;
        options.Outbox.MaxAttempts = 0;

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*MaxAttempts*");
    }

    [Fact]
    public void Outbox_Enabled_ProcessedMessageTtlZero_Throws()
    {
        var options = ValidOptions();
        options.Outbox.Enabled = true;
        options.Outbox.ProcessedMessageTtl = TimeSpan.Zero;

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*ProcessedMessageTtl*");
    }

    [Fact]
    public void Outbox_Disabled_SkipsValidation()
    {
        var options = ValidOptions();
        options.Outbox.Enabled = false;
        options.Outbox.PollingInterval = TimeSpan.Zero;
        options.Outbox.MaxAttempts = 0;

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().NotThrow();
    }

    #endregion

    #region Definition validation edges

    public sealed class DummyMessage { }

    public sealed class DummyHandler : IMessageHandler<DummyMessage>
    {
        public Task HandleAsync(DummyMessage message, Models.ConsumeContext context, CancellationToken ct) =>
            Task.CompletedTask;
    }

    public sealed class DummyDefinition : ConsumerDefinition<DummyHandler, DummyMessage>
    {
        public override string TypeId => "dummy.message";
        public override string EndpointName => "dummy-endpoint";
    }

    public sealed class DuplicateDefinition : ConsumerDefinition<DummyHandler, DummyMessage>
    {
        public override string TypeId => "dummy.message";
        public override string EndpointName => "dummy-endpoint";
    }

    [Fact]
    public void DuplicateConsumerRegistration_Throws()
    {
        var definitions = new IConsumerDefinition[]
        {
            new DummyDefinition(),
            new DuplicateDefinition()
        };

        var act = () => MongoBusConfigValidator.ValidateDefinitions(definitions);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Duplicate*dummy-endpoint*dummy.message*");
    }

    [Fact]
    public void EmptyDefinitions_DoesNotThrow()
    {
        var act = () => MongoBusConfigValidator.ValidateDefinitions(Array.Empty<IConsumerDefinition>());
        act.Should().NotThrow();
    }

    public sealed class NullGroupingBatchHandler : IBatchMessageHandler<DummyMessage>
    {
        public Task HandleBatchAsync(IReadOnlyList<DummyMessage> messages, Models.BatchConsumeContext context, CancellationToken ct) =>
            Task.CompletedTask;
    }

    public sealed class NullGroupingBatchDefinition : BatchConsumerDefinition<NullGroupingBatchHandler, DummyMessage>
    {
        public override string TypeId => "batch.null.grouping";
        public override string EndpointName => "batch-null-grouping";
        public override IBatchGroupingStrategy GroupingStrategy => null!;
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = 10,
            MaxBatchWaitTime = TimeSpan.FromSeconds(1),
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage
        };
    }

    [Fact]
    public void BatchConsumer_NullGroupingStrategy_Throws()
    {
        var definitions = new IConsumerDefinition[]
        {
            new NullGroupingBatchDefinition()
        };

        var act = () => MongoBusConfigValidator.ValidateDefinitions(definitions);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*GroupingStrategy*");
    }

    public sealed class InvalidBatchOptionsHandler : IBatchMessageHandler<DummyMessage>
    {
        public Task HandleBatchAsync(IReadOnlyList<DummyMessage> messages, Models.BatchConsumeContext context, CancellationToken ct) =>
            Task.CompletedTask;
    }

    public sealed class InvalidBatchOptionsDefinition : BatchConsumerDefinition<InvalidBatchOptionsHandler, DummyMessage>
    {
        public override string TypeId => "batch.invalid.options";
        public override string EndpointName => "batch-invalid-options";
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 0, // invalid: must be >= 1
            MaxBatchSize = 10,
            MaxBatchWaitTime = TimeSpan.FromSeconds(1),
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage
        };
    }

    [Fact]
    public void BatchConsumer_InvalidBatchOptions_MinBatchSizeZero_Throws()
    {
        var definitions = new IConsumerDefinition[]
        {
            new InvalidBatchOptionsDefinition()
        };

        var act = () => MongoBusConfigValidator.ValidateDefinitions(definitions);

        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithMessage("*MinBatchSize*");
    }

    public sealed class MaxBatchSizeZeroDefinition : BatchConsumerDefinition<InvalidBatchOptionsHandler, DummyMessage>
    {
        public override string TypeId => "batch.maxsize.zero";
        public override string EndpointName => "batch-maxsize-zero";
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = 0, // invalid
            MaxBatchWaitTime = TimeSpan.FromSeconds(1),
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage
        };
    }

    [Fact]
    public void BatchConsumer_MaxBatchSizeZero_Throws()
    {
        var definitions = new IConsumerDefinition[]
        {
            new MaxBatchSizeZeroDefinition()
        };

        var act = () => MongoBusConfigValidator.ValidateDefinitions(definitions);

        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithMessage("*MaxBatchSize*");
    }

    #endregion

    #region ProcessedMessageTtl validation

    [Fact]
    public void ProcessedMessageTtl_Zero_Throws()
    {
        var options = ValidOptions();
        options.ProcessedMessageTtl = TimeSpan.Zero;

        var act = () => MongoBusConfigValidator.ValidateOptions(options);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*ProcessedMessageTtl*");
    }

    #endregion

    private static MongoBusOptions ValidOptions() => new()
    {
        ConnectionString = "mongodb://localhost:27017",
        DatabaseName = "test_validation"
    };
}
