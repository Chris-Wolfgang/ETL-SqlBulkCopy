using System;
using Microsoft.Extensions.Logging;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Covers the <see cref="ILogger"/> members <see cref="RecordingLogger"/> must
/// implement but that the code under test never drives.
/// <para>
/// The loader logs messages but never opens a logging scope, so
/// <see cref="RecordingLogger.BeginScope{TState}"/> and the scope object it
/// returns are reached by no other test. They are not dead code — the interface
/// requires them, and a caller that did open a scope would depend on them not
/// throwing — so the fake's contract is asserted directly here rather than the
/// type being excluded from the coverage gate. See the "Fakes are deliberately
/// NOT excluded" note in coverlet.runsettings.
/// </para>
/// </summary>
public class RecordingLoggerTests
{
    [Fact]
    public void BeginScope_returns_a_disposable_that_can_be_disposed_without_throwing()
    {
        var sut = new RecordingLogger();

        var scope = sut.BeginScope("state");

        Assert.NotNull(scope);
        scope.Dispose();
    }



    [Fact]
    public void BeginScope_when_disposed_twice_does_not_throw()
    {
        var sut = new RecordingLogger();
        var scope = sut.BeginScope(42);

        scope.Dispose();

        Assert.Null(Record.Exception(scope.Dispose));
    }



    [Fact]
    public void BeginScope_when_called_repeatedly_never_returns_null()
    {
        var sut = new RecordingLogger();

        var first = sut.BeginScope("a");
        var second = sut.BeginScope("b");

        Assert.NotNull(first);
        Assert.NotNull(second);
    }



    [Fact]
    public void BeginScope_does_not_record_an_entry()
    {
        var sut = new RecordingLogger();

        using (sut.BeginScope("scope-state"))
        {
            // Opening a scope is not a log call; only Log adds entries.
        }

        Assert.Empty(sut.Entries);
    }



    [Fact]
    public void Log_records_the_formatted_message()
    {
        var sut = new RecordingLogger();

        sut.Log
        (
            LogLevel.Information,
            new EventId(1),
            "state",
            exception: null,
            formatter: (state, _) => $"formatted:{state}"
        );

        Assert.Equal
        (
            new[] { "formatted:state" },
            sut.Entries
        );
    }



    [Fact]
    public void IsEnabled_returns_true_for_every_level()
    {
        var sut = new RecordingLogger();

        // Cast the Array rather than `foreach (LogLevel level in ...)`, which
        // unboxes each element and trips CS8605 under nullable analysis on the
        // older TFMs. Enum.GetValues<T>() is net5.0+, so it is not an option here.
        var levels = (LogLevel[])Enum.GetValues(typeof(LogLevel));

        foreach (var level in levels)
        {
            Assert.True(sut.IsEnabled(level));
        }
    }
}
