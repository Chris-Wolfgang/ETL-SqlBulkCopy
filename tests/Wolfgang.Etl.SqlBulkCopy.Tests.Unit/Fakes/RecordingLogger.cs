using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;

/// <summary>
/// Test spy for <see cref="ILogger"/> that captures every formatted message.
/// Lets tests assert on log-only behaviour — values that never reach a public
/// property or callback (e.g. the validation-failure position) and therefore
/// cannot be pinned any other way.
/// </summary>
internal sealed class RecordingLogger : ILogger
{
    /// <summary>
    /// Every formatted message logged, in order.
    /// </summary>
    public List<string> Entries { get; } = new();



    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull => NullScope.Instance;



    public bool IsEnabled(LogLevel logLevel) => true;



    public void Log<TState>
    (
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter
    )
    {
        Entries.Add(formatter(state, exception));
    }



    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
            // No-op
        }
    }
}
