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
