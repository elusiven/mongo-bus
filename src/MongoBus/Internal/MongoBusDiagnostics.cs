using System.Diagnostics;

namespace MongoBus.Internal;

public static class MongoBusDiagnostics
{
    public const string ActivitySourceName = "MongoBus";
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
