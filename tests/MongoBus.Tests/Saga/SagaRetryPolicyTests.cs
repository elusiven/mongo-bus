using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.DependencyInjection;
using Xunit;

namespace MongoBus.Tests.Saga;

public class SagaRetryPolicyTests
{
    [Fact]
    public void DenyList_ExceptionInList_ShouldNotRetry()
    {
        var options = new SagaOptions
        {
            RetryMode = ExceptionRetryMode.DenyList,
            NoRetryExceptions = [typeof(InvalidOperationException), typeof(ArgumentException)]
        };

        options.ShouldRetry(new InvalidOperationException("test")).Should().BeFalse();
        options.ShouldRetry(new ArgumentException("test")).Should().BeFalse();
    }

    [Fact]
    public void DenyList_ExceptionNotInList_ShouldRetry()
    {
        var options = new SagaOptions
        {
            RetryMode = ExceptionRetryMode.DenyList,
            NoRetryExceptions = [typeof(InvalidOperationException)]
        };

        options.ShouldRetry(new TimeoutException("test")).Should().BeTrue();
        options.ShouldRetry(new Exception("test")).Should().BeTrue();
    }

    [Fact]
    public void DenyList_SubtypeMatch_ShouldNotRetry()
    {
        var options = new SagaOptions
        {
            RetryMode = ExceptionRetryMode.DenyList,
            NoRetryExceptions = [typeof(ArgumentException)]
        };

        // ArgumentNullException inherits from ArgumentException
        options.ShouldRetry(new ArgumentNullException("param")).Should().BeFalse();
    }

    [Fact]
    public void AllowList_ExceptionInList_ShouldRetry()
    {
        var options = new SagaOptions
        {
            RetryMode = ExceptionRetryMode.AllowList,
            RetryExceptions = [typeof(TimeoutException)]
        };

        options.ShouldRetry(new TimeoutException("test")).Should().BeTrue();
    }

    [Fact]
    public void AllowList_ExceptionNotInList_ShouldNotRetry()
    {
        var options = new SagaOptions
        {
            RetryMode = ExceptionRetryMode.AllowList,
            RetryExceptions = [typeof(TimeoutException)]
        };

        options.ShouldRetry(new InvalidOperationException("test")).Should().BeFalse();
        options.ShouldRetry(new ArgumentException("test")).Should().BeFalse();
    }

    [Fact]
    public void Default_NoConfig_ShouldRetryEverything()
    {
        var options = new SagaOptions();

        options.ShouldRetry(new Exception("test")).Should().BeTrue();
        options.ShouldRetry(new InvalidOperationException("test")).Should().BeTrue();
        options.ShouldRetry(new TimeoutException("test")).Should().BeTrue();
    }
}
