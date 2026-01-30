using MongoBus.Models;

namespace MongoBus.Abstractions;

public static class BatchGrouping
{
    public static IBatchGroupingStrategy None { get; } = new NoGroupingStrategy();

    public static IBatchGroupingStrategy ByMessage<T>(Func<T, string> keySelector) =>
        new MessageGroupingStrategy<T>(keySelector);

    public static IBatchGroupingStrategy ByMetadata(Func<ConsumeContext, string> keySelector) =>
        new MetadataGroupingStrategy(keySelector);

    private sealed class NoGroupingStrategy : IBatchGroupingStrategy
    {
        public string GetGroupKey(object message, ConsumeContext context) => "__all__";
    }

    private sealed class MessageGroupingStrategy<T>(Func<T, string> selector) : IBatchGroupingStrategy
    {
        public string GetGroupKey(object message, ConsumeContext context)
        {
            if (message is not T typed)
                throw new InvalidOperationException($"Expected message type {typeof(T).Name} but received {message.GetType().Name}.");

            var key = selector(typed);
            return string.IsNullOrWhiteSpace(key) ? "__none__" : key;
        }
    }

    private sealed class MetadataGroupingStrategy(Func<ConsumeContext, string> selector) : IBatchGroupingStrategy
    {
        public string GetGroupKey(object message, ConsumeContext context)
        {
            var key = selector(context);
            return string.IsNullOrWhiteSpace(key) ? "__none__" : key;
        }
    }
}
