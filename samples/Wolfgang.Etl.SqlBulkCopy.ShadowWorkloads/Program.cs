using BenchmarkDotNet.Running;

namespace Wolfgang.Etl.SqlBulkCopy.ShadowWorkloads;

/// <summary>
/// Entry point for the SqlBulkCopy shadow-workload benchmarks (issue #82).
/// </summary>
/// <remarks>
/// These are realistic consumer workloads — bulk-loading production-shaped
/// record counts into a real SQL Server (spun up via Testcontainers) — rather
/// than the micro-benchmarks in the <c>benchmarks/</c> project. The nightly
/// <c>shadow.yaml</c> workflow replays them against a baseline release and the
/// current build, comparing latency and allocations to catch regressions.
///
/// Run all workloads:      dotnet run -c Release --
/// Filter a single one:    dotnet run -c Release -- --filter '*LoadFlat*'
///
/// Requires a reachable Docker daemon (Testcontainers pulls the SQL Server
/// image). Without Docker the run fails fast in <c>[GlobalSetup]</c>.
/// </remarks>
internal static class Program
{
    private static void Main(string[] args) =>
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
