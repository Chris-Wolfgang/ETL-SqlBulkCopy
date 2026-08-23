#pragma warning disable S125 // narrative header describes concurrency invariant, not commented-out code
// Coyote systematic-concurrency driver for TypeMap's cache.
//
// TypeMap.Cache is a ConcurrentDictionary keyed by (type, schema, table);
// TypeMap.Create builds a map and publishes it via GetOrAdd. Concurrent
// first-time callers for the same key race on that GetOrAdd — the invariant
// is that every caller ends up with the SAME TypeMap instance (reference-equal
// via the cache), never a duplicate or partially-built one.
//
// Coyote's TestingEngine takes control of the scheduler and explores many
// interleavings of the concurrent calls — paths a "run it 10,000 times" stress
// loop would rarely reproduce. Iterations come from COYOTE_ITERATIONS (small
// per-PR, large on the scheduled workflow). No IL rewriting is required because
// the engine drives the test action directly.
//
// Refs #89.
#pragma warning restore S125

using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;
using JetBrains.Annotations;
using Microsoft.Coyote;
using Microsoft.Coyote.SystematicTesting;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Concurrency;

[ExcludeFromCodeCoverage]
[UsedImplicitly(ImplicitUseKindFlags.Default, ImplicitUseTargetFlags.WithMembers)]
[Table("concurrent_probe")]
internal sealed class ConcurrentProbe
{
    public int Id { get; set; }

    public string Value { get; set; } = string.Empty;
}



public class TypeMapCacheConcurrencyTests
{
    private static int Iterations =>
        int.TryParse(Environment.GetEnvironmentVariable("COYOTE_ITERATIONS"), out var n) && n > 0
            ? n
            : 100;



    /// <summary>
    /// Under any interleaving of N concurrent <c>TypeMap.Create</c> callers for
    /// the same type, every caller must observe the SAME cached instance. A
    /// racy GetOrAdd would surface as two callers holding different maps.
    /// </summary>
    [Fact]
    [Trait("Category", "Concurrency")]
    public void TypeMap_Create_is_race_free_under_concurrent_access()
    {
        RunUnderCoyote(() =>
        {
            const int workers = 4;
            var results = new TypeMap[workers];
            var tasks = new Task[workers];

            for (var i = 0; i < workers; i++)
            {
                var idx = i;
                tasks[i] = Task.Run(() =>
                {
                    results[idx] = TypeMap.Create(typeof(ConcurrentProbe));
                });
            }

#pragma warning disable VSTHRD002 // Coyote schedules cooperatively; sync-wait IS the test
            Task.WaitAll(tasks);
#pragma warning restore VSTHRD002

            var first = results[0];
            for (var i = 1; i < workers; i++)
            {
                Microsoft.Coyote.Specifications.Specification.Assert(
                    ReferenceEquals(results[i], first),
                    "Worker {0} received a different TypeMap instance than worker 0 — the cache GetOrAdd raced.",
                    i);
            }
        });
    }



    private static void RunUnderCoyote(Action body)
    {
        var config = Configuration.Create()
            .WithTestingIterations((uint)Iterations)
            .WithMaxSchedulingSteps(500)
            .WithVerbosityEnabled();

        using var engine = TestingEngine.Create(config, body);
        engine.Run();

        var report = engine.TestReport;
        Assert.True(
            report.NumOfFoundBugs == 0,
            $"Coyote found {report.NumOfFoundBugs} bug(s). " +
            $"First: {(report.BugReports.Count > 0 ? report.BugReports.First() : "(no repro)")}");
    }
}
