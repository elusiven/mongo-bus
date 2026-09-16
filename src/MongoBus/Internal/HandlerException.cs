using System.Reflection;

namespace MongoBus.Internal;

internal static class HandlerException
{
    /// <summary>
    /// Handlers are invoked through reflection, so one that throws synchronously arrives wrapped in a
    /// <see cref="TargetInvocationException"/>. Anything that cares about what actually went wrong — the error shown
    /// on the dashboard, or whether a consumer treats the failure as retryable — has to look past that wrapper.
    /// </summary>
    public static Exception Unwrap(Exception exception)
    {
        var current = exception;
        while (current is TargetInvocationException && current.InnerException is not null)
            current = current.InnerException;

        return current;
    }
}
