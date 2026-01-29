namespace MongoBus.Abstractions;

public interface IBatchConsumerDefinition : IConsumerDefinition
{
    BatchConsumerOptions BatchOptions { get; }
}
