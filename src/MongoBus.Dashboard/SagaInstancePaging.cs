namespace MongoBus.Dashboard;

/// <summary>
/// Paging limits for the saga instances endpoint. Without an upper bound, <c>take=0</c> (which MongoDB treats as
/// "no limit") or a very large page would return every instance in the collection.
/// </summary>
internal static class SagaInstancePaging
{
    public const int DefaultSkip = 0;
    public const int DefaultTake = 50;
    public const int MaxTake = 200;

    public static string ValidationMessage => $"skip must be at least 0, and take between 1 and {MaxTake}.";

    public static bool IsValid(int? skip, int? take) =>
        (skip ?? DefaultSkip) >= 0 && (take ?? DefaultTake) is >= 1 and <= MaxTake;
}
