using System.Collections.Generic;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class SliceListTests
{
    [Fact]
    public void SliceList_when_slice_covers_entire_source_returns_same_reference()
    {
        var source = new object[] { 1, 2, 3 };

        var result = SqlBulkCopyLoader<TestRecord>.SliceList(source, offset: 0, count: source.Length);

        Assert.Same(source, result);
    }



    [Fact]
    public void SliceList_when_offset_is_nonzero_returns_partial_copy()
    {
        IReadOnlyList<object> source = new object[] { "a", "b", "c", "d", "e" };

        var result = SqlBulkCopyLoader<TestRecord>.SliceList(source, offset: 2, count: 2);

        Assert.NotSame(source, result);
        Assert.Equal(new object[] { "c", "d" }, result);
    }



    [Fact]
    public void SliceList_when_count_is_less_than_source_returns_partial_copy()
    {
        IReadOnlyList<object> source = new object[] { 10, 20, 30, 40 };

        var result = SqlBulkCopyLoader<TestRecord>.SliceList(source, offset: 0, count: 2);

        Assert.NotSame(source, result);
        Assert.Equal(new object[] { 10, 20 }, result);
    }
}
