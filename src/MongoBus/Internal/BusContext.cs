namespace MongoBus.Internal;

using MongoBus.Models;

internal static class BusContext
{
    private static readonly AsyncLocal<ConsumeContext?> CurrentContext = new();

    public static ConsumeContext? Current
    {
        get => CurrentContext.Value;
        set => CurrentContext.Value = value;
    }
}
