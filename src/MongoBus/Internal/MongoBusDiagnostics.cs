using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace MongoBus.Internal;

public static class MongoBusDiagnostics
{
    public const string ActivitySourceName = "MongoBus";
    public const string MeterName = "MongoBus";
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    public static readonly Meter Meter = new(MeterName);
}
