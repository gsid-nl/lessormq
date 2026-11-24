using System.Globalization;
using System.Text;

namespace LessorMQ;

/// <summary>How serious a <see cref="LogEvent"/> is.</summary>
public enum LogSeverity
{
    /// <summary>Detail for debugging.</summary>
    Debug,

    /// <summary>Something worth knowing, such as a failed message and what the server did with it.</summary>
    Information,

    /// <summary>A retry, or a lease lost.</summary>
    Warning,

    /// <summary>A call that failed after its retries.</summary>
    Error,
}

/// <summary>
/// One thing the client or a worker has to say. <see cref="Template"/> and
/// <see cref="Arguments"/> are a message template with named holes, as
/// <c>Microsoft.Extensions.Logging</c> takes it; <see cref="ToString"/> fills them in.
/// The core package logs nothing by itself, so it needs no logging library.
/// </summary>
public sealed class LogEvent
{
    internal LogEvent(LogSeverity severity, string template, object?[] arguments, Exception? exception)
    {
        Severity = severity;
        Template = template;
        Arguments = arguments;
        Exception = exception;
    }

    /// <summary>How serious it is.</summary>
    public LogSeverity Severity { get; }

    /// <summary>The message template, such as <c>Claim from {Queue} failed</c>.</summary>
    public string Template { get; }

    /// <summary>The values of the template's holes, in order.</summary>
    public IReadOnlyList<object?> Arguments { get; }

    /// <summary>The exception it is about, if any.</summary>
    public Exception? Exception { get; }

    /// <summary>The template with its holes filled in.</summary>
    public override string ToString()
    {
        var text = new StringBuilder(Template.Length + 32);
        var next = 0;
        for (var i = 0; i < Template.Length; i++)
        {
            var close = Template[i] == '{' ? Template.IndexOf('}', i) : -1;
            if (close < 0 || next >= Arguments.Count)
            {
                text.Append(Template[i]);
                continue;
            }

            text.Append(Convert.ToString(Arguments[next++], CultureInfo.InvariantCulture));
            i = close;
        }

        return text.ToString();
    }
}
