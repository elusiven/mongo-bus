using MongoBus.Models;

namespace MongoBus.Abstractions;

public interface IBatchGroupingStrategy
{
    string GetGroupKey(object message, ConsumeContext context);
}
