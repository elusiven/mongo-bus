using System.Linq.Expressions;
using System.Reflection;
using MongoBus.Internal.Saga;
using MongoBus.Models;
using MongoBus.Models.Saga;

namespace MongoBus.Abstractions.Saga;

public abstract class MongoBusStateMachine<TInstance>
    where TInstance : class, ISagaInstance, new()
{
    public SagaState Initial { get; } = new("Initial");
    public SagaState Final { get; } = new("Final");

    private readonly Dictionary<(string StateName, Type MessageType), object> _behaviors = new();
    private readonly Dictionary<string, SagaEventRegistration> _eventRegistrations = new();
    private readonly Dictionary<Type, int> _compositeEventBitPositions = new();
    private readonly List<CompositeEventConfig> _compositeEvents = [];
    private readonly Dictionary<string, Func<TInstance, IMessageBus, Models.ConsumeContext, CancellationToken, Task>> _compositeBehaviors = new();
    private readonly HashSet<(string StateName, Type MessageType)> _ignoredEvents = [];

    private Func<TInstance, bool>? _completedPredicate;

    protected MongoBusStateMachine()
    {
        AutoInitializeStates();
    }

    private void AutoInitializeStates()
    {
        var stateProps = GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(SagaState) && p.CanWrite && p.GetSetMethod(true)?.IsPublic == false);

        foreach (var prop in stateProps)
        {
            prop.SetValue(this, new SagaState(prop.Name));
        }
    }

    // --- Event Declaration ---

    protected void Event<TMessage>(
        Expression<Func<SagaEvent<TMessage>>> propertyExpression,
        string typeId,
        Action<EventCorrelationConfigurator<TInstance, TMessage>>? configure = null)
    {
        var memberExpr = (MemberExpression)propertyExpression.Body;
        var prop = (PropertyInfo)memberExpr.Member;

        var sagaEvent = new SagaEvent<TMessage>(prop.Name, typeId);

        if (configure != null)
        {
            var configurator = new EventCorrelationConfigurator<TInstance, TMessage>(sagaEvent.Correlation);
            configure(configurator);
        }

        prop.SetValue(this, sagaEvent);

        _eventRegistrations[typeId] = new SagaEventRegistration(
            typeId,
            typeof(TMessage),
            sagaEvent.Correlation.CorrelateByIdExpression,
            sagaEvent.Correlation.CorrelateByProperty,
            sagaEvent.Correlation.SelectIdExpression,
            sagaEvent.Correlation.MissingInstanceBehavior,
            sagaEvent.Correlation.Partition);
    }

    protected void InstanceState(Expression<Func<TInstance, string>> stateProperty)
    {
        // Validation/documentation marker. CurrentState is always used via ISagaInstance.
    }

    // --- Behavior Declaration ---

    protected void Initially(params SagaWhenClause<TInstance>[] clauses)
    {
        foreach (var clause in clauses)
        {
            if (clause.IsIgnore)
                _ignoredEvents.Add((Initial.Name, clause.MessageType));
            else
                RegisterBehavior(Initial.Name, clause);
        }
    }

    protected void During(SagaState state, params SagaWhenClause<TInstance>[] clauses)
    {
        foreach (var clause in clauses)
        {
            if (clause.IsIgnore)
                _ignoredEvents.Add((state.Name, clause.MessageType));
            else
                RegisterBehavior(state.Name, clause);
        }
    }

    protected void DuringAny(params SagaWhenClause<TInstance>[] clauses)
    {
        foreach (var clause in clauses)
        {
            if (clause.IsIgnore)
                _ignoredEvents.Add(("*", clause.MessageType));
            else
                RegisterBehavior("*", clause);
        }
    }

    protected SagaWhenClause<TInstance, TMessage> When<TMessage>(SagaEvent<TMessage> @event)
    {
        var builder = new BehaviorBuilder<TInstance, TMessage>();
        return new SagaWhenClause<TInstance, TMessage>(builder, @event.TypeId);
    }

    protected SagaWhenClause<TInstance> Ignore<TMessage>(SagaEvent<TMessage> @event)
    {
        return new SagaWhenClause<TInstance>(typeof(TMessage), null, @event.TypeId, isIgnore: true);
    }

    protected CompositeWhenClause<TInstance> When(SagaEvent @event)
    {
        return new CompositeWhenClause<TInstance>(@event.Name, this);
    }

    // --- Composite Events ---

    protected void CompositeEvent(
        Expression<Func<SagaEvent>> propertyExpression,
        Expression<Func<TInstance, int>> flagsProperty,
        params object[] requiredEvents)
    {
        var memberExpr = (MemberExpression)propertyExpression.Body;
        var prop = (PropertyInfo)memberExpr.Member;
        var compositeEvent = new SagaEvent(prop.Name);
        prop.SetValue(this, compositeEvent);

        var flagsPropInfo = GetPropertyInfo(flagsProperty);

        var config = new CompositeEventConfig
        {
            EventName = prop.Name,
            FlagsPropertyName = flagsPropInfo.Name,
            RequiredEventTypeIds = new List<string>()
        };

        var bit = 0;
        foreach (var reqEvent in requiredEvents)
        {
            var typeIdProp = reqEvent.GetType().GetProperty("TypeId");
            if (typeIdProp != null)
            {
                var typeId = (string)typeIdProp.GetValue(reqEvent)!;
                config.RequiredEventTypeIds.Add(typeId);
                _compositeEventBitPositions[reqEvent.GetType()] = bit;
                bit++;
            }
        }

        config.RequiredBitmask = (1 << bit) - 1;
        _compositeEvents.Add(config);
    }

    // --- Completion ---

    protected void SetCompletedWhenFinalized()
    {
        _completedPredicate = instance => instance.CurrentState == Final.Name;
    }

    protected void SetCompleted(Func<TInstance, bool> predicate)
    {
        _completedPredicate = predicate;
    }

    // --- Internal API ---

    internal IReadOnlyList<ISagaActivity<TInstance, TMessage>>? GetBehavior<TMessage>(string currentState)
    {
        if (_behaviors.TryGetValue((currentState, typeof(TMessage)), out var behavior))
            return (IReadOnlyList<ISagaActivity<TInstance, TMessage>>)behavior;

        if (_behaviors.TryGetValue(("*", typeof(TMessage)), out var anyBehavior))
            return (IReadOnlyList<ISagaActivity<TInstance, TMessage>>)anyBehavior;

        return null;
    }

    internal bool IsIgnored<TMessage>(string currentState)
    {
        return _ignoredEvents.Contains((currentState, typeof(TMessage)))
            || _ignoredEvents.Contains(("*", typeof(TMessage)));
    }

    internal bool IsCompleted(TInstance instance) => _completedPredicate?.Invoke(instance) ?? false;

    internal IReadOnlyDictionary<string, SagaEventRegistration> GetEventRegistrations() => _eventRegistrations;

    internal IReadOnlyList<CompositeEventConfig> GetCompositeEvents() => _compositeEvents;

    internal Func<TInstance, IMessageBus, Models.ConsumeContext, CancellationToken, Task>? GetCompositeBehavior(string eventName)
    {
        return _compositeBehaviors.GetValueOrDefault(eventName);
    }

    internal void RegisterCompositeBehavior(
        string eventName,
        Func<TInstance, IMessageBus, Models.ConsumeContext, CancellationToken, Task> behavior)
    {
        _compositeBehaviors[eventName] = behavior;
    }

    // --- Private Helpers ---

    private void RegisterBehavior(string stateName, SagaWhenClause<TInstance> clause)
    {
        var activities = clause.BuildActivities();
        if (activities != null)
            _behaviors[(stateName, clause.MessageType)] = activities;
    }

    private static PropertyInfo GetPropertyInfo<TProperty>(Expression<Func<TInstance, TProperty>> expression)
    {
        if (expression.Body is MemberExpression memberExpr && memberExpr.Member is PropertyInfo propInfo)
            return propInfo;

        throw new ArgumentException("Expression must be a property access expression.", nameof(expression));
    }
}

/// <summary>
/// Non-generic base for When clause results. Used by Initially/During/DuringAny params arrays.
/// </summary>
public class SagaWhenClause<TInstance>
    where TInstance : class, ISagaInstance
{
    internal Type MessageType { get; }
    internal string TypeId { get; }
    internal bool IsIgnore { get; }
    private readonly object? _builder;

    internal SagaWhenClause(Type messageType, object? builder, string typeId, bool isIgnore = false)
    {
        MessageType = messageType;
        TypeId = typeId;
        IsIgnore = isIgnore;
        _builder = builder;
    }

    internal virtual object? BuildActivities()
    {
        if (_builder == null) return null;
        var buildMethod = _builder.GetType().GetMethod("Build", BindingFlags.Instance | BindingFlags.NonPublic);
        return buildMethod?.Invoke(_builder, null);
    }
}

/// <summary>
/// Typed When clause that provides a fluent API for building behavior chains.
/// Implicitly converts to the non-generic base for use in Initially/During/DuringAny.
/// </summary>
public sealed class SagaWhenClause<TInstance, TMessage> : SagaWhenClause<TInstance>
    where TInstance : class, ISagaInstance
{
    private readonly BehaviorBuilder<TInstance, TMessage> _builder;

    internal SagaWhenClause(BehaviorBuilder<TInstance, TMessage> builder, string typeId)
        : base(typeof(TMessage), builder, typeId)
    {
        _builder = builder;
    }

    internal override object? BuildActivities() => _builder.Build();

    public SagaWhenClause<TInstance, TMessage> Then(Action<SagaConsumeContext<TInstance, TMessage>> action)
    {
        _builder.Then(action);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> ThenAsync(Func<SagaConsumeContext<TInstance, TMessage>, Task> action)
    {
        _builder.ThenAsync(action);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> TransitionTo(SagaState state)
    {
        _builder.TransitionTo(state);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> Finalize()
    {
        _builder.Finalize();
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> Publish<TPublish>(
        string typeId,
        Func<SagaConsumeContext<TInstance, TMessage>, TPublish> factory)
    {
        _builder.Publish(typeId, factory);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> PublishAsync<TPublish>(
        string typeId,
        Func<SagaConsumeContext<TInstance, TMessage>, Task<TPublish>> factory)
    {
        _builder.PublishAsync(typeId, factory);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> Send(
        string endpointName,
        string typeId,
        Func<SagaConsumeContext<TInstance, TMessage>, object> factory)
    {
        _builder.Send(endpointName, typeId, factory);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> If(
        Func<SagaConsumeContext<TInstance, TMessage>, bool> condition,
        Action<BehaviorBuilder<TInstance, TMessage>> thenBranch)
    {
        _builder.If(condition, thenBranch);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> IfAsync(
        Func<SagaConsumeContext<TInstance, TMessage>, Task<bool>> condition,
        Action<BehaviorBuilder<TInstance, TMessage>> thenBranch)
    {
        _builder.IfAsync(condition, thenBranch);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> IfElse(
        Func<SagaConsumeContext<TInstance, TMessage>, bool> condition,
        Action<BehaviorBuilder<TInstance, TMessage>> thenBranch,
        Action<BehaviorBuilder<TInstance, TMessage>> elseBranch)
    {
        _builder.IfElse(condition, thenBranch, elseBranch);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> IfElseAsync(
        Func<SagaConsumeContext<TInstance, TMessage>, Task<bool>> condition,
        Action<BehaviorBuilder<TInstance, TMessage>> thenBranch,
        Action<BehaviorBuilder<TInstance, TMessage>> elseBranch)
    {
        _builder.IfElseAsync(condition, thenBranch, elseBranch);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> Switch(
        Func<SagaConsumeContext<TInstance, TMessage>, string> selector,
        Action<SwitchCaseBuilder<TInstance, TMessage>> casesBuilder)
    {
        _builder.Switch(selector, casesBuilder);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> Catch<TException>(
        Action<ExceptionBehaviorBuilder<TInstance, TMessage, TException>> handler)
        where TException : Exception
    {
        _builder.Catch(handler);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> CatchAll(
        Action<ExceptionBehaviorBuilder<TInstance, TMessage, Exception>> handler)
    {
        _builder.CatchAll(handler);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> Respond<TResponse>(
        string typeId,
        Func<SagaConsumeContext<TInstance, TMessage>, TResponse> factory)
    {
        _builder.Respond(typeId, factory);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> Schedule<TTimeout>(
        SagaSchedule<TInstance, TTimeout> schedule,
        Func<SagaConsumeContext<TInstance, TMessage>, TTimeout> factory,
        Action<TInstance, string?>? tokenSetter = null,
        TimeSpan? delay = null)
    {
        _builder.Schedule(schedule, factory, tokenSetter, delay);
        return this;
    }

    public SagaWhenClause<TInstance, TMessage> Unschedule<TTimeout>(
        Action<TInstance, string?> tokenSetter)
    {
        _builder.Unschedule<TTimeout>(tokenSetter);
        return this;
    }
}

/// <summary>
/// When clause for composite (non-generic) events.
/// Stores behavior as a delegate since composite events have no associated message type.
/// </summary>
public sealed class CompositeWhenClause<TInstance>
    where TInstance : class, ISagaInstance, new()
{
    private readonly string _eventName;
    private readonly MongoBusStateMachine<TInstance> _stateMachine;
    private readonly List<Func<TInstance, IMessageBus, Models.ConsumeContext, CancellationToken, Task>> _actions = [];

    internal CompositeWhenClause(string eventName, MongoBusStateMachine<TInstance> stateMachine)
    {
        _eventName = eventName;
        _stateMachine = stateMachine;
    }

    public CompositeWhenClause<TInstance> Then(Action<TInstance> action)
    {
        _actions.Add((instance, _, _, _) => { action(instance); return Task.CompletedTask; });
        return this;
    }

    public CompositeWhenClause<TInstance> ThenAsync(Func<TInstance, Task> action)
    {
        _actions.Add((instance, _, _, _) => action(instance));
        return this;
    }

    public CompositeWhenClause<TInstance> TransitionTo(SagaState state)
    {
        _actions.Add((instance, _, _, _) => { instance.CurrentState = state.Name; return Task.CompletedTask; });
        return this;
    }

    public CompositeWhenClause<TInstance> Finalize()
    {
        _actions.Add((instance, _, _, _) => { instance.CurrentState = "Final"; return Task.CompletedTask; });
        return this;
    }

    public CompositeWhenClause<TInstance> Publish<TPublish>(
        string typeId,
        Func<TInstance, TPublish> factory)
    {
        _actions.Add(async (instance, bus, context, ct) =>
        {
            var data = factory(instance);
            await bus.PublishAsync(typeId, data!,
                correlationId: instance.CorrelationId,
                causationId: context.CloudEventId,
                ct: ct);
        });
        return this;
    }

    /// <summary>
    /// Builds and registers the composite behavior on the state machine.
    /// Must be called to finalize the When clause (typically via implicit conversion in DuringAny/During).
    /// </summary>
    internal void Register()
    {
        var actions = _actions.ToList();
        _stateMachine.RegisterCompositeBehavior(_eventName, async (instance, bus, context, ct) =>
        {
            foreach (var action in actions)
                await action(instance, bus, context, ct);
        });
    }

    /// <summary>
    /// Implicit conversion to SagaWhenClause for use in Initially/During/DuringAny.
    /// Registers the composite behavior as a side effect.
    /// </summary>
    public static implicit operator SagaWhenClause<TInstance>(CompositeWhenClause<TInstance> clause)
    {
        clause.Register();
        // Return a no-op clause — actual behavior is stored separately
        return new SagaWhenClause<TInstance>(typeof(object), null, "__composite__" + clause._eventName);
    }
}

// --- Supporting types ---

internal sealed record SagaEventRegistration(
    string TypeId,
    Type MessageType,
    Func<ConsumeContext, string>? CorrelateByIdExpression,
    CorrelateByConfig? CorrelateByProperty,
    Func<ConsumeContext, string>? SelectIdExpression,
    MissingInstanceBehavior? MissingInstanceBehavior,
    PartitionConfig? Partition);

internal sealed class CompositeEventConfig
{
    public string EventName { get; set; } = default!;
    public string FlagsPropertyName { get; set; } = default!;
    public List<string> RequiredEventTypeIds { get; set; } = [];
    public int RequiredBitmask { get; set; }
}

public sealed class EventCorrelationConfigurator<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    private readonly EventCorrelation<TMessage> _correlation;

    internal EventCorrelationConfigurator(EventCorrelation<TMessage> correlation)
    {
        _correlation = correlation;
    }

    public void CorrelateById(Func<ConsumeContext, string> correlationExpression)
    {
        _correlation.CorrelateByIdExpression = correlationExpression;
    }

    public void CorrelateBy(
        Expression<Func<TInstance, string>> instanceProperty,
        Func<ConsumeContext, string> messageProperty)
    {
        var memberExpr = (MemberExpression)instanceProperty.Body;
        var propInfo = (PropertyInfo)memberExpr.Member;

        _correlation.CorrelateByProperty = new CorrelateByConfig
        {
            InstancePropertyName = propInfo.Name,
            MessageValueSelector = messageProperty
        };
    }

    public void SelectId(Func<ConsumeContext, string> idSelector)
    {
        _correlation.SelectIdExpression = idSelector;
    }

    public void OnMissingInstance(Action<MissingInstanceConfigurator<TMessage>> configure)
    {
        var configurator = new MissingInstanceConfigurator<TMessage>();
        configure(configurator);
        _correlation.MissingInstanceBehavior = configurator.Build();
    }

    public void UsePartitioner(int partitionCount, Func<ConsumeContext, string> keySelector)
    {
        _correlation.Partition = new PartitionConfig
        {
            PartitionCount = partitionCount,
            KeySelector = keySelector
        };
    }
}

public sealed class MissingInstanceConfigurator<TMessage>
{
    private MissingInstanceAction _action = MissingInstanceAction.Fault;
    private Func<ConsumeContext, Task>? _asyncHandler;

    public void Discard() => _action = MissingInstanceAction.Discard;
    public void Fault() => _action = MissingInstanceAction.Fault;

    public void Execute(Action<ConsumeContext> action)
    {
        _action = MissingInstanceAction.Execute;
        _asyncHandler = ctx => { action(ctx); return Task.CompletedTask; };
    }

    public void ExecuteAsync(Func<ConsumeContext, Task> action)
    {
        _action = MissingInstanceAction.Execute;
        _asyncHandler = action;
    }

    internal MissingInstanceBehavior Build() => new()
    {
        Action = _action,
        AsyncHandler = _asyncHandler
    };
}
