using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Behavioural tests that pin batch-flush boundaries, logger pass-through and
/// nested element-type detection. Added under #163 to kill surviving Stryker
/// mutants that the existing (mostly total-count / build-shape) assertions did
/// not distinguish — e.g. flipping <c>batch.Count &gt;= _batchSize</c> to
/// <c>true</c>, dropping <c>buffer.Clear()</c>, or ignoring a supplied logger.
/// </summary>
public class LoaderMutationHardeningTests
{
    private static async IAsyncEnumerable<T> ToAsyncEnumerableAsync<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }



    private static IReadOnlyList<TestRecord> CreateTestItems(int count)
    {
        return Enumerable.Range(1, count)
            .Select(i => new TestRecord { Id = i, Name = $"Item{i}", Amount = i * 10m })
            .ToList();
    }



    // Every batch flushed to the server, in flush order, across all wrappers.
    // Each chunk write creates its own wrapper whose BatchRowCounts holds the
    // single chunk it wrote, so flattening in creation order reconstructs the
    // exact flush sequence.
    private static IReadOnlyList<int> FlushSizes(FakeSqlBulkCopyWrapperFactory factory, string destinationTable)
    {
        return factory.CreatedWrappers
            .Where(w => string.Equals(w.DestinationTableName, destinationTable, StringComparison.Ordinal))
            .SelectMany(w => w.BatchRowCounts)
            .ToList();
    }



    [Fact]
    public async Task LoadAsync_when_item_count_below_BatchSize_flushes_exactly_one_batch()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, new ManualProgressTimer())
        {
            BatchSize = 100
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(3)));

        // A mutant that forces the flush guard to `true` would flush after every
        // add ([1,1,1]); the real guard flushes once at end-of-stream ([3]).
        Assert.Equal(new[] { 3 }, FlushSizes(factory, "[dbo].[TestRecords]"));
    }



    [Fact]
    public async Task LoadAsync_when_item_count_exceeds_BatchSize_flushes_at_boundary_with_remainder()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, new ManualProgressTimer())
        {
            BatchSize = 2
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(5)));

        // Pins both the >= boundary flush and the batch.Clear() between flushes:
        // a dropped Clear() would grow the batch ([2,4,...]); a `true` guard
        // would flush every item ([1,1,1,1,1]).
        Assert.Equal(new[] { 2, 2, 1 }, FlushSizes(factory, "[dbo].[TestRecords]"));
    }



    [Fact]
    public async Task LoadAsync_when_item_count_is_exact_multiple_of_BatchSize_writes_no_empty_trailing_batch()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, new ManualProgressTimer())
        {
            BatchSize = 2
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(4)));

        var flushes = FlushSizes(factory, "[dbo].[TestRecords]");
        Assert.Equal(new[] { 2, 2 }, flushes);
        Assert.DoesNotContain(0, flushes);
    }



    [Fact]
    public async Task LoadAsync_when_nested_children_exceed_BatchSize_flushes_child_batches_with_remainder()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<ParentRecord>(factory, logger: null, new ManualProgressTimer())
        {
            BatchSize = 2
        };

        var parent = new ParentRecord
        {
            ParentId = 1,
            Name = "p",
            Children = new List<ChildRecord>
            {
                new() { ChildId = 10, Description = "a" },
                new() { ChildId = 11, Description = "b" },
                new() { ChildId = 12, Description = "c" }
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(new[] { parent }));

        // Nested buffer flushes at _batchSize (2) then a final flush of the
        // remainder (1). A dropped final `buffer.Count > 0` flush loses the last
        // child; a `>= 0` mutant appends an empty ([2,1,0]) batch.
        Assert.Equal(new[] { 2, 1 }, FlushSizes(factory, "[ChildRecords]"));
    }



    [Fact]
    public async Task LoadAsync_writes_to_the_supplied_logger_not_a_null_logger()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var logger = new RecordingLogger();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger, new ManualProgressTimer());

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)));

        // A `logger ?? NullLogger.Instance` mutant that drops the left operand
        // would route all logging to NullLogger and this spy would stay empty.
        Assert.NotEmpty(logger.Entries);
    }
}



/// <summary>
/// Mutation-hardening tests for <see cref="TypeMap"/> element-type detection and
/// override normalization (#163).
/// </summary>
public class TypeMapMutationHardeningTests
{
    [Fact]
    public void Create_when_property_is_directly_typed_IEnumerable_detects_nested_table()
    {
        // The type is declared as IEnumerable<ChildRecord> directly. Type.GetInterfaces()
        // does NOT include the type itself, so the direct IsGenericType/IEnumerable<>
        // check is the only path that finds the element type — dropping it would
        // mis-detect this property as a non-collection and lose the nested table.
        var map = TypeMap.Create(typeof(ParentWithIEnumerableChildren));

        var nested = Assert.Single(map.NestedTables);
        Assert.Equal("ChildRecords", nested.ChildTypeMap.TableName);
    }



    [Fact]
    public void Create_when_schema_and_table_overrides_are_whitespace_shares_cache_instance_with_null()
    {
        // Whitespace/empty overrides are normalized to null so ("   "), ("") and
        // (null) collapse to a single cache key. A mutant that skips the
        // normalization keys the cache on the raw whitespace string, producing a
        // distinct instance.
        var whitespace = TypeMap.Create(typeof(SimpleRecord), "   ", "   ");
        var none = TypeMap.Create(typeof(SimpleRecord));

        Assert.Same(none, whitespace);
    }
}
