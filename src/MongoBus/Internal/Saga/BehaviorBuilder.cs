using MongoBus.Abstractions.Saga;
using MongoBus.Internal.Saga.Activities;
using MongoBus.Models.Saga;

namespace MongoBus.Internal.Saga;

/// <summary>
/// Fluent builder for constructing a saga behavior chain (list of activities).
/// Each method appends an activity to the chain and returns the builder for chaining.
/// </summary>
public sealed class BehaviorBuilder<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    private readonly List<ISagaActivity<TInstance, TMessage>> _activities = [];

    internal IReadOnlyList<ISagaActivity<TInstance, TMessage>> Build() => _activities.AsReadOnly();

    // --- Core Activities ---

    public BehaviorBuilder<TInstance, TMessage> Then(
        Action<SagaConsumeContext<TInstance, TMessage>> action)
    {
        _activities.Add(new ThenActivity<TInstance, TMessage>(action));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> ThenAsync(
        Func<SagaConsumeContext<TInstance, TMessage>, Task> action)
    {
        _activities.Add(new ThenAsyncActivity<TInstance, TMessage>(action));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> TransitionTo(SagaState state)
    {
        _activities.Add(new TransitionToActivity<TInstance, TMessage>(state));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> Finalize()
    {
        _activities.Add(new FinalizeActivity<TInstance, TMessage>());
        return this;
    }

    // --- Messaging Activities ---

    public BehaviorBuilder<TInstance, TMessage> Publish<TPublish>(
        string typeId,
        Func<SagaConsumeContext<TInstance, TMessage>, TPublish> factory)
    {
        _activities.Add(new PublishActivity<TInstance, TMessage, TPublish>(typeId, factory));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> PublishAsync<TPublish>(
        string typeId,
        Func<SagaConsumeContext<TInstance, TMessage>, Task<TPublish>> factory)
    {
        _activities.Add(new PublishAsyncActivity<TInstance, TMessage, TPublish>(typeId, factory));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> Send(
        string endpointName,
        string typeId,
        Func<SagaConsumeContext<TInstance, TMessage>, object> factory)
    {
        _activities.Add(new SendActivity<TInstance, TMessage>(endpointName, typeId, factory));
        return this;
    }

    // --- Scheduling ---

    public BehaviorBuilder<TInstance, TMessage> Schedule<TTimeout>(
        SagaSchedule<TInstance, TTimeout> schedule,
        Func<SagaConsumeContext<TInstance, TMessage>, TTimeout> factory,
        Action<TInstance, string?>? tokenSetter = null,
        TimeSpan? delay = null)
    {
        if (tokenSetter != null)
            _activities.Add(new ScheduleWithTokenActivity<TInstance, TMessage, TTimeout>(schedule, factory, tokenSetter, delay));
        else
            _activities.Add(new ScheduleActivity<TInstance, TMessage, TTimeout>(schedule, factory, delay));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> Unschedule<TTimeout>(
        Action<TInstance, string?> tokenSetter)
    {
        _activities.Add(new UnscheduleActivity<TInstance, TMessage, TTimeout>(tokenSetter));
        return this;
    }

    // --- Request/Response ---

    public BehaviorBuilder<TInstance, TMessage> Request<TRequest, TResponse>(
        SagaRequest<TInstance, TRequest, TResponse> request,
        Func<SagaConsumeContext<TInstance, TMessage>, TRequest> factory,
        Action<TInstance, string?> requestIdSetter)
    {
        _activities.Add(new RequestActivity<TInstance, TMessage, TRequest, TResponse>(request, factory, requestIdSetter));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> Respond<TResponse>(
        string typeId,
        Func<SagaConsumeContext<TInstance, TMessage>, TResponse> factory)
    {
        _activities.Add(new RespondActivity<TInstance, TMessage, TResponse>(typeId, factory));
        return this;
    }

    // --- Logic Gates ---

    public BehaviorBuilder<TInstance, TMessage> If(
        Func<SagaConsumeContext<TInstance, TMessage>, bool> condition,
        Action<BehaviorBuilder<TInstance, TMessage>> thenBranch)
    {
        var builder = new BehaviorBuilder<TInstance, TMessage>();
        thenBranch(builder);
        _activities.Add(new IfActivity<TInstance, TMessage>(condition, builder.Build()));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> IfAsync(
        Func<SagaConsumeContext<TInstance, TMessage>, Task<bool>> condition,
        Action<BehaviorBuilder<TInstance, TMessage>> thenBranch)
    {
        var builder = new BehaviorBuilder<TInstance, TMessage>();
        thenBranch(builder);
        _activities.Add(new IfAsyncActivity<TInstance, TMessage>(condition, builder.Build()));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> IfElse(
        Func<SagaConsumeContext<TInstance, TMessage>, bool> condition,
        Action<BehaviorBuilder<TInstance, TMessage>> thenBranch,
        Action<BehaviorBuilder<TInstance, TMessage>> elseBranch)
    {
        var thenBuilder = new BehaviorBuilder<TInstance, TMessage>();
        var elseBuilder = new BehaviorBuilder<TInstance, TMessage>();
        thenBranch(thenBuilder);
        elseBranch(elseBuilder);
        _activities.Add(new IfElseActivity<TInstance, TMessage>(condition, thenBuilder.Build(), elseBuilder.Build()));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> IfElseAsync(
        Func<SagaConsumeContext<TInstance, TMessage>, Task<bool>> condition,
        Action<BehaviorBuilder<TInstance, TMessage>> thenBranch,
        Action<BehaviorBuilder<TInstance, TMessage>> elseBranch)
    {
        var thenBuilder = new BehaviorBuilder<TInstance, TMessage>();
        var elseBuilder = new BehaviorBuilder<TInstance, TMessage>();
        thenBranch(thenBuilder);
        elseBranch(elseBuilder);
        _activities.Add(new IfElseAsyncActivity<TInstance, TMessage>(condition, thenBuilder.Build(), elseBuilder.Build()));
        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> Switch(
        Func<SagaConsumeContext<TInstance, TMessage>, string> selector,
        Action<SwitchCaseBuilder<TInstance, TMessage>> casesBuilder)
    {
        var builder = new SwitchCaseBuilder<TInstance, TMessage>();
        casesBuilder(builder);
        var (cases, defaultBranch) = builder.Build();
        _activities.Add(new SwitchActivity<TInstance, TMessage>(selector, cases, defaultBranch));
        return this;
    }

    // --- Exception Handling ---

    public BehaviorBuilder<TInstance, TMessage> Catch<TException>(
        Action<ExceptionBehaviorBuilder<TInstance, TMessage, TException>> handler)
        where TException : Exception
    {
        var exBuilder = new ExceptionBehaviorBuilder<TInstance, TMessage, TException>();
        handler(exBuilder);

        // Wrap all preceding activities in the catch
        var guarded = new List<ISagaActivity<TInstance, TMessage>>(_activities);
        _activities.Clear();

        var (catchActivities, rethrow) = exBuilder.Build();
        _activities.Add(new CatchActivity<TInstance, TMessage, TException>(guarded, catchActivities, rethrow));

        return this;
    }

    public BehaviorBuilder<TInstance, TMessage> CatchAll(
        Action<ExceptionBehaviorBuilder<TInstance, TMessage, Exception>> handler)
    {
        return Catch(handler);
    }
}

/// <summary>
/// Builder for switch-case branches.
/// </summary>
public sealed class SwitchCaseBuilder<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    private readonly Dictionary<string, IReadOnlyList<ISagaActivity<TInstance, TMessage>>> _cases = new();
    private IReadOnlyList<ISagaActivity<TInstance, TMessage>>? _defaultBranch;

    public SwitchCaseBuilder<TInstance, TMessage> Case(
        string value,
        Action<BehaviorBuilder<TInstance, TMessage>> branch)
    {
        var builder = new BehaviorBuilder<TInstance, TMessage>();
        branch(builder);
        _cases[value] = builder.Build();
        return this;
    }

    public SwitchCaseBuilder<TInstance, TMessage> Default(
        Action<BehaviorBuilder<TInstance, TMessage>> branch)
    {
        var builder = new BehaviorBuilder<TInstance, TMessage>();
        branch(builder);
        _defaultBranch = builder.Build();
        return this;
    }

    internal (IReadOnlyDictionary<string, IReadOnlyList<ISagaActivity<TInstance, TMessage>>> Cases,
        IReadOnlyList<ISagaActivity<TInstance, TMessage>>? DefaultBranch) Build()
    {
        return (_cases, _defaultBranch);
    }
}

/// <summary>
/// Builder for exception handler branches inside Catch activities.
/// </summary>
public sealed class ExceptionBehaviorBuilder<TInstance, TMessage, TException>
    where TInstance : class, ISagaInstance
    where TException : Exception
{
    private readonly List<ISagaExceptionActivity<TInstance, TMessage, TException>> _activities = [];
    private bool _rethrow;

    public ExceptionBehaviorBuilder<TInstance, TMessage, TException> Then(
        Action<SagaExceptionContext<TInstance, TMessage, TException>> action)
    {
        _activities.Add(new ExceptionThenActivity<TInstance, TMessage, TException>(action));
        return this;
    }

    public ExceptionBehaviorBuilder<TInstance, TMessage, TException> ThenAsync(
        Func<SagaExceptionContext<TInstance, TMessage, TException>, Task> action)
    {
        _activities.Add(new ExceptionThenAsyncActivity<TInstance, TMessage, TException>(action));
        return this;
    }

    public ExceptionBehaviorBuilder<TInstance, TMessage, TException> TransitionTo(SagaState state)
    {
        _activities.Add(new ExceptionTransitionToActivity<TInstance, TMessage, TException>(state));
        return this;
    }

    public ExceptionBehaviorBuilder<TInstance, TMessage, TException> Finalize()
    {
        _activities.Add(new ExceptionFinalizeActivity<TInstance, TMessage, TException>());
        return this;
    }

    public ExceptionBehaviorBuilder<TInstance, TMessage, TException> Publish<TPublish>(
        string typeId,
        Func<SagaExceptionContext<TInstance, TMessage, TException>, TPublish> factory)
    {
        _activities.Add(new ExceptionPublishActivity<TInstance, TMessage, TException, TPublish>(typeId, factory));
        return this;
    }

    public ExceptionBehaviorBuilder<TInstance, TMessage, TException> Rethrow()
    {
        _rethrow = true;
        return this;
    }

    internal (IReadOnlyList<ISagaExceptionActivity<TInstance, TMessage, TException>> Activities, bool Rethrow) Build()
    {
        return (_activities.AsReadOnly(), _rethrow);
    }
}
