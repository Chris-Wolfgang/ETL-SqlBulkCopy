using System.Collections.Generic;
using System.Linq;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Pins the per-cell memo in <c>TypeMapReader</c>: <c>IsDBNull</c> followed by
/// <c>GetValue</c> for the same cell must invoke the underlying property getter
/// <em>once</em>, not twice.
/// </summary>
/// <remarks>
/// <c>SqlBulkCopy</c> uses exactly that access pattern for nullable columns, so
/// the duplicate invocation doubled the per-row getter cost (and the boxing for
/// value types) on the hottest path in the library. Counting invocations is a
/// deterministic proof — unlike a timing benchmark, it cannot be washed out by
/// run-to-run noise, and it fails loudly if the memo is ever removed. Refs #47.
/// </remarks>
public class TypeMapReaderCellCachingTests
{
    [Fact]
    public void IsDBNull_then_GetValue_for_the_same_cell_invokes_the_getter_once()
    {
        GetterCountingRecord.ResetCount();

        var batch = new List<object> { new GetterCountingRecord { Id = 1 } };
        using var reader = new TypeMapReader(batch, TypeMap.Create(typeof(GetterCountingRecord)));

        var ordinal = OrdinalOf(reader, nameof(GetterCountingRecord.Counted));

        Assert.True(reader.Read());
        _ = reader.IsDBNull(ordinal);
        _ = reader.GetValue(ordinal);

        Assert.Equal(1, GetterCountingRecord.GetCount);
    }



    [Fact]
    public void Reading_the_same_cell_across_rows_re_invokes_the_getter_per_row()
    {
        // The memo is keyed on the row index as well as the ordinal, so advancing
        // the cursor must invalidate it. If it did not, row 2 would read row 1's
        // value — a correctness bug far worse than the duplicate call it replaces.
        GetterCountingRecord.ResetCount();

        var batch = new List<object>
        {
            new GetterCountingRecord { Id = 1, Counted = "first" },
            new GetterCountingRecord { Id = 2, Counted = "second" }
        };

        using var reader = new TypeMapReader(batch, TypeMap.Create(typeof(GetterCountingRecord)));
        var ordinal = OrdinalOf(reader, nameof(GetterCountingRecord.Counted));

        Assert.True(reader.Read());
        var first = reader.GetValue(ordinal);

        Assert.True(reader.Read());
        var second = reader.GetValue(ordinal);

        Assert.Equal("first", first);
        Assert.Equal("second", second);
        Assert.Equal(2, GetterCountingRecord.GetCount);
    }



    [Fact]
    public void Alternating_ordinals_on_one_row_still_returns_each_column_value()
    {
        // A single-cell memo means alternating ordinals are all misses. Correctness
        // must not depend on the access order.
        GetterCountingRecord.ResetCount();

        var batch = new List<object> { new GetterCountingRecord { Id = 42, Counted = "value" } };
        using var reader = new TypeMapReader(batch, TypeMap.Create(typeof(GetterCountingRecord)));

        var idOrdinal = OrdinalOf(reader, nameof(GetterCountingRecord.Id));
        var countedOrdinal = OrdinalOf(reader, nameof(GetterCountingRecord.Counted));

        Assert.True(reader.Read());

        Assert.Equal(42, reader.GetValue(idOrdinal));
        Assert.Equal("value", reader.GetValue(countedOrdinal));
        Assert.Equal(42, reader.GetValue(idOrdinal));
        Assert.Equal("value", reader.GetValue(countedOrdinal));
    }



    private static int OrdinalOf(TypeMapReader reader, string columnName)
    {
        return Enumerable.Range(0, reader.FieldCount)
            .Single(i => string.Equals(reader.GetName(i), columnName, System.StringComparison.Ordinal));
    }
}
