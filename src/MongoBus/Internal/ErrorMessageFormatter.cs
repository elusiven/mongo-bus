using System.Reflection;

namespace MongoBus.Internal;

internal static class ErrorMessageFormatter
{
    /// <summary>
    /// Produces a concise error message suitable for persisting to the inbox/outbox and
    /// surfacing in the dashboard. Full exception details (including the stack trace) are
    /// always written to the logger; only the message is stored, to avoid disclosing
    /// internal implementation details. Reflection wrappers are unwrapped so the real
    /// cause is shown instead of "Exception has been thrown by the target of an invocation."
    /// </summary>
    public static string Describe(Exception ex)
    {
        var current = ex;
        while (current is TargetInvocationException && current.InnerException is not null)
            current = current.InnerException;
        return current.Message;
    }
}
