#if NETCOREAPP3_1
using System;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Stand-in for <c>[SkippableFact]</c> on <c>netcoreapp3.1</c> only. Xunit.SkippableFact probes
/// <c>System.Runtime.Versioning.SupportedOSPlatformAttribute</c> while building every skippable test case; that type
/// first shipped in .NET 5, so on 3.1 the lookup yields <see langword="null"/> and the case fails during
/// initialisation before the test body (or its own <c>Skip.IfNot</c> guard) can run. The affected tests still run
/// on the other twelve target frameworks; on 3.1 they are reported as skipped instead of failed.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
internal sealed class NetCoreApp31SkippedFactAttribute : FactAttribute
{
    public NetCoreApp31SkippedFactAttribute()
    {
        Skip = "Xunit.SkippableFact cannot build test cases on netcoreapp3.1 (SupportedOSPlatformAttribute is .NET 5+); covered on the other target frameworks.";
    }
}



/// <summary>
/// <c>[SkippableTheory]</c> counterpart of <see cref="NetCoreApp31SkippedFactAttribute"/>.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
internal sealed class NetCoreApp31SkippedTheoryAttribute : TheoryAttribute
{
    public NetCoreApp31SkippedTheoryAttribute()
    {
        Skip = "Xunit.SkippableFact cannot build test cases on netcoreapp3.1 (SupportedOSPlatformAttribute is .NET 5+); covered on the other target frameworks.";
    }
}
#endif
