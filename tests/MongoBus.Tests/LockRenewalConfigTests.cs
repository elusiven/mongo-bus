using FluentAssertions;
using MongoBus.Abstractions;
using MongoBus.Internal;
using MongoBus.Models;
using Xunit;

namespace MongoBus.Tests;

public class LockRenewalConfigTests
{
    public sealed class RenewedMessage { }

    public sealed class PlainMessage { }

    public sealed class RenewedHandler : IMessageHandler<RenewedMessage>
    {
        public Task HandleAsync(RenewedMessage message, ConsumeContext context, CancellationToken ct) => Task.CompletedTask;
    }

    public sealed class PlainHandler : IMessageHandler<PlainMessage>
    {
        public Task HandleAsync(PlainMessage message, ConsumeContext context, CancellationToken ct) => Task.CompletedTask;
    }

    public sealed class RenewedDefinition : ConsumerDefinition<RenewedHandler, RenewedMessage>
    {
        public override string TypeId => "renewal.config.renewed";
        public override string EndpointName => "renewal-config-endpoint";
        public override bool RenewLock => true;
    }

    public sealed class PlainDefinition : ConsumerDefinition<PlainHandler, PlainMessage>
    {
        public override string TypeId => "renewal.config.plain";
        public override string EndpointName => "renewal-config-endpoint";
    }

    public sealed class ShortRenewedDefinition : ConsumerDefinition<RenewedHandler, RenewedMessage>
    {
        public override string TypeId => "renewal.config.short";
        public override TimeSpan LockTime => TimeSpan.FromMilliseconds(500);
        public override bool RenewLock => true;
    }

    public sealed class OneSecondRenewedDefinition : ConsumerDefinition<RenewedHandler, RenewedMessage>
    {
        public override string TypeId => "renewal.config.one-second";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(1);
        public override bool RenewLock => true;
    }

    [Fact]
    public void ConsumerDefinition_DoesNotRenewLocksByDefault()
    {
        new PlainDefinition().RenewLock.Should().BeFalse();
    }

    [Fact]
    public void Endpoint_RenewsLocks_WhenAnyDefinitionOnItOptsIn()
    {
        var configs = DispatchRegistrationBuilder.BuildEndpointConfigs(
            new IConsumerDefinition[] { new PlainDefinition(), new RenewedDefinition() });

        configs["renewal-config-endpoint"].RenewLock.Should().BeTrue();
    }

    [Fact]
    public void Endpoint_DoesNotRenewLocks_WhenNoDefinitionOptsIn()
    {
        var configs = DispatchRegistrationBuilder.BuildEndpointConfigs(
            new IConsumerDefinition[] { new PlainDefinition() });

        configs["renewal-config-endpoint"].RenewLock.Should().BeFalse();
    }

    [Fact]
    public void RenewingDefinition_WithLockTimeUnderOneSecond_IsRejected()
    {
        var validate = () => MongoBusConfigValidator.ValidateDefinitions(
            new IConsumerDefinition[] { new ShortRenewedDefinition() });

        validate.Should().Throw<InvalidOperationException>().WithMessage("*RenewLock*");
    }

    [Fact]
    public void RenewingDefinition_WithOneSecondLockTime_IsAccepted()
    {
        var validate = () => MongoBusConfigValidator.ValidateDefinitions(
            new IConsumerDefinition[] { new OneSecondRenewedDefinition() });

        validate.Should().NotThrow();
    }
}
