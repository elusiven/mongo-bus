namespace MongoBus.Infrastructure;

public static class MongoBusConstants
{
    public const string InboxCollectionName = "bus_inbox";
    public const string BindingsCollectionName = "bus_bindings";
    public const string OutboxCollectionName = "bus_outbox";
    public const string InboxDedupCollectionName = "bus_inbox_dedup";
}
