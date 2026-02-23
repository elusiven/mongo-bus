namespace MongoBus.Internal;

internal static class OutboxStatus
{
    public const string Pending = "Pending";
    public const string Published = "Published";
    public const string Dead = "Dead";
}
