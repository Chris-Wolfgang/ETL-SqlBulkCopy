// GC.GetAllocatedBytesForCurrentThread() does not exist on the older .NET
// Framework reference assemblies (net462/net47/net471 — CS0117), so this whole
// file compiles only on .NET Core / .NET 5+ targets, where the API is
// guaranteed. The allocation behavior it guards is runtime-independent, so
// running it on the modern TFMs (netcoreapp3.1 + net5.0–net10.0) is sufficient.
// The entire file — usings included — is inside the guard so excluded TFMs
// don't trip the unused-using analyzer under warnings-as-errors.
#if NETCOREAPP
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Guards that hot-path methods which have deliberately chosen a zero-allocation
/// shape keep it. A method that "looks identical but allocates" silently
/// regresses perf-critical consumers, and neither the compiler nor the normal
/// test suite would catch it.
/// </summary>
public class AllocationTests
{
    // Consume the result through a non-inlinable sink so the JIT cannot elide
    // the call (and thus the measurement) as dead code.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void Consume(object value)
    {
        _ = value;
    }



    [Fact]
    public void SliceList_full_span_fast_path_allocates_nothing()
    {
        // The full-span case (offset == 0 && count == source.Count) returns the
        // source reference unchanged — the batching loop hits it on every batch
        // that isn't the final short one, so it must not allocate.
        IReadOnlyList<object> source = new object[] { 1, 2, 3, 4, 5 };

        // Warm up: force JIT compilation of SliceList and Consume before we
        // start counting, so first-call codegen allocations aren't measured.
        for (var i = 0; i < 200; i++)
        {
            Consume(SqlBulkCopyLoader<TestRecord>.SliceList(source, 0, source.Count));
        }

        var before = System.GC.GetAllocatedBytesForCurrentThread();

        for (var i = 0; i < 10_000; i++)
        {
            Consume(SqlBulkCopyLoader<TestRecord>.SliceList(source, 0, source.Count));
        }

        var allocated = System.GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(0, allocated);
    }
}
#endif
