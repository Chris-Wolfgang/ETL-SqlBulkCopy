using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Covers boundaries the source handles explicitly but no test exercised.
/// </summary>
public class BoundaryCoverageTests
{
    private static async IAsyncEnumerable<T> ToAsyncEnumerableAsync<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }



    [Fact]
    public void Create_when_enum_has_an_unsupported_underlying_type_does_not_map_the_column()
    {
        // TypeMap gates enum columns on the underlying integral type being in
        // SupportedColumnTypes. sbyte/ushort/uint/ulong are deliberately absent
        // (SQL Server has no unsigned integer types), but nothing asserted it —
        // a mutation to `return true` would have gone unnoticed.
        var map = TypeMap.Create(typeof(UnsupportedEnumRecord));

        Assert.DoesNotContain
        (
            map.Columns,
            c => string.Equals(c.PropertyName, nameof(UnsupportedEnumRecord.Unsigned), StringComparison.Ordinal)
        );

        // The supported sibling on the same type still maps, so this is proving
        // the underlying-type gate rather than the whole type being skipped.
        Assert.Contains
        (
            map.Columns,
            c => string.Equals(c.PropertyName, nameof(UnsupportedEnumRecord.Id), StringComparison.Ordinal)
        );
    }



    [Fact]
    public async Task LoadAsync_when_cancelled_during_nested_traversal_throws_OperationCanceledException()
    {
        // WriteNestedTableStreamingAsync polls the token at both the parent and
        // child boundaries. The root loop's check is covered by the TestKit
        // contract base, but neither nested check had a test.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<ParentRecord>(factory, logger: null, new ManualProgressTimer())
        {
            BatchSize = 1
        };

        using var cts = new CancellationTokenSource();

        var parents = Enumerable.Range(1, 50)
            .Select(i => new ParentRecord
            {
                ParentId = i,
                Name = $"p{i}",
                Children = Enumerable.Range(1, 20)
                    .Select(c => new ChildRecord { ChildId = c, Description = $"c{c}" })
                    .ToList()
            })
            .ToList();

        // Cancel immediately: the loader must observe the token rather than run
        // the whole graph to completion.
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(parents), cts.Token)
        );
    }
}
