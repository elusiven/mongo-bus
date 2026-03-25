using MongoBus.Abstractions.Saga;

namespace MongoBus.Internal.Saga.Activities;

internal sealed class IfActivity<TInstance, TMessage>(
    Func<SagaConsumeContext<TInstance, TMessage>, bool> condition,
    IReadOnlyList<ISagaActivity<TInstance, TMessage>> thenBranch)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        if (condition(context))
        {
            foreach (var activity in thenBranch)
                await activity.ExecuteAsync(context);
        }
    }
}

internal sealed class IfAsyncActivity<TInstance, TMessage>(
    Func<SagaConsumeContext<TInstance, TMessage>, Task<bool>> condition,
    IReadOnlyList<ISagaActivity<TInstance, TMessage>> thenBranch)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        if (await condition(context))
        {
            foreach (var activity in thenBranch)
                await activity.ExecuteAsync(context);
        }
    }
}

internal sealed class IfElseActivity<TInstance, TMessage>(
    Func<SagaConsumeContext<TInstance, TMessage>, bool> condition,
    IReadOnlyList<ISagaActivity<TInstance, TMessage>> thenBranch,
    IReadOnlyList<ISagaActivity<TInstance, TMessage>> elseBranch)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var branch = condition(context) ? thenBranch : elseBranch;
        foreach (var activity in branch)
            await activity.ExecuteAsync(context);
    }
}

internal sealed class IfElseAsyncActivity<TInstance, TMessage>(
    Func<SagaConsumeContext<TInstance, TMessage>, Task<bool>> condition,
    IReadOnlyList<ISagaActivity<TInstance, TMessage>> thenBranch,
    IReadOnlyList<ISagaActivity<TInstance, TMessage>> elseBranch)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var branch = await condition(context) ? thenBranch : elseBranch;
        foreach (var activity in branch)
            await activity.ExecuteAsync(context);
    }
}

internal sealed class SwitchActivity<TInstance, TMessage>(
    Func<SagaConsumeContext<TInstance, TMessage>, string> selector,
    IReadOnlyDictionary<string, IReadOnlyList<ISagaActivity<TInstance, TMessage>>> cases,
    IReadOnlyList<ISagaActivity<TInstance, TMessage>>? defaultBranch)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var key = selector(context);

        IReadOnlyList<ISagaActivity<TInstance, TMessage>>? branch = null;

        if (key != null)
            cases.TryGetValue(key, out branch);

        branch ??= defaultBranch;

        if (branch != null)
        {
            foreach (var activity in branch)
                await activity.ExecuteAsync(context);
        }
    }
}
