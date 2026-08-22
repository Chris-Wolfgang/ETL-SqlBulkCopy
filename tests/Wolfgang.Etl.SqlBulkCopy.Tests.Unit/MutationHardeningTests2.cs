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
/// Second batch of mutation-hardening tests (#163). Each test pins a specific
/// surviving mutant that the first batch and the pre-existing suite did not
/// distinguish — nested exact-multiple flush boundaries, skip/max-item counting,
/// validation-position arithmetic, descriptor override resolution, and nested
/// element-type filtering.
/// </summary>
public class LoaderMutationHardeningTests2
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



    private static IReadOnlyList<int> FlushSizes(FakeSqlBulkCopyWrapperFactory factory, string destinationTable)
    {
        return factory.CreatedWrappers
            .Where(w => string.Equals(w.DestinationTableName, destinationTable, StringComparison.Ordinal))
            .SelectMany(w => w.BatchRowCounts)
            .ToList();
    }



    [Fact]
    public async Task LoadAsync_when_nested_children_are_exact_multiple_of_BatchSize_writes_no_empty_trailing_batch()
    {
        // Pins the nested `buffer.Count > 0` guard: with children an exact
        // multiple of BatchSize the buffer is empty at the end, so a `>= 0`
        // mutant would append a third, empty batch ([2,2,0]).
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
                new() { ChildId = 12, Description = "c" },
                new() { ChildId = 13, Description = "d" }
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(new[] { parent }));

        var flushes = FlushSizes(factory, "[ChildRecords]");
        Assert.Equal(new[] { 2, 2 }, flushes);
        Assert.DoesNotContain(0, flushes);
    }



    [Fact]
    public async Task LoadAsync_when_SkipItemCount_set_skips_leading_items_and_writes_only_the_remainder()
    {
        // Pins the skip branch: the skipped items must not reach the server and
        // must be counted as skipped rather than loaded.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, new ManualProgressTimer())
        {
            SkipItemCount = 2
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(5)));

        Assert.Equal(3, sut.CurrentItemCount);
        Assert.Equal(2, sut.CurrentSkippedItemCount);
        Assert.Equal(new[] { 3 }, FlushSizes(factory, "[dbo].[TestRecords]"));
    }



    [Fact]
    public async Task LoadAsync_when_MaximumItemCount_reached_stops_writing_further_items()
    {
        // Pins the max-item break: only MaximumItemCount rows may reach the
        // server even though the source yields more.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, new ManualProgressTimer())
        {
            MaximumItemCount = 2
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(10)));

        Assert.Equal(2, sut.CurrentItemCount);
        Assert.Equal(2, FlushSizes(factory, "[dbo].[TestRecords]").Sum());
    }



    [Fact]
    public async Task LoadAsync_when_skipped_items_precede_a_validation_failure_position_counts_both()
    {
        // The failure position is (CurrentItemCount + CurrentSkippedItemCount).
        // With 1 skipped item before the first (invalid) processed item, the
        // '+' and '-' forms differ in sign/'value, so the arithmetic mutant dies.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var captured = new List<ValidatableRecord>();
        var sut = new SqlBulkCopyLoader<ValidatableRecord>(factory, logger: null, new ManualProgressTimer())
        {
            EnableDataValidation = true,
            ValidationFailureBehavior = ValidationFailureBehavior.Skip,
            SkipItemCount = 1,
            OnValidationFailed = (item, _) => captured.Add(item)
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "skipped-by-SkipItemCount", Quantity = 5 },
            new ValidatableRecord { Id = 2, Name = "", Quantity = 5 } // Required fails
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        var failed = Assert.Single(captured);
        Assert.Equal(2, failed.Id);

        // Two distinct skip reasons both feed CurrentSkippedItemCount: the
        // leading SkipItemCount item, and the validation-failed item.
        Assert.Equal(2, sut.CurrentSkippedItemCount);
        Assert.Equal(0, sut.CurrentItemCount);
    }



    [Fact]
    public async Task LoadAsync_when_dry_run_reads_every_row_without_writing()
    {
        // Pins the dry-run branch AND the DrainReaderAsync read loop: no batch
        // may be written, but the pipeline still enumerates every item.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, new ManualProgressTimer())
        {
            IsDryRun = true
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(4)));

        Assert.Equal(4, sut.CurrentItemCount);
        Assert.Empty(FlushSizes(factory, "[dbo].[TestRecords]"));
    }
}



/// <summary>
/// Second batch of <see cref="TypeMap"/> / <see cref="ColumnMap"/>
/// mutation-hardening tests (#163).
/// </summary>
public class TypeMapMutationHardeningTests2
{
    [Fact]
    public void Create_when_collection_element_type_is_NotMapped_excludes_the_nested_table()
    {
        // Pins the `pair.ElementType.GetCustomAttribute<NotMappedAttribute>() is null`
        // filter: a `true` mutant would map NotMappedChild as a child table.
        var map = TypeMap.Create(typeof(ParentWithNotMappedChildren));

        Assert.Empty(map.NestedTables);
    }



    [Fact]
    public void Create_from_descriptor_when_overrides_are_whitespace_falls_back_to_descriptor_names()
    {
        // BuildFromDescriptor resolves overrides with IsNullOrWhiteSpace: a
        // whitespace override must NOT win over the descriptor's own names.
        var whitespaceOverride = TypeMap.Create(typeof(BulkCopyableFixture), schemaName: "   ", tableName: "   ");
        var noOverride = TypeMap.Create(typeof(BulkCopyableFixture));

        Assert.Equal(noOverride.TableName, whitespaceOverride.TableName);
        Assert.Equal(noOverride.SchemaName, whitespaceOverride.SchemaName);
    }



    [Fact]
    public void Create_from_descriptor_when_overrides_are_provided_uses_the_overrides()
    {
        // The complementary direction: a real override must beat the descriptor,
        // so a conditional-to-false mutant (always descriptor) dies.
        var map = TypeMap.Create(typeof(BulkCopyableFixture), schemaName: "custom", tableName: "Overridden");

        Assert.Equal("Overridden", map.TableName);
        Assert.Equal("custom", map.SchemaName);
    }



    [Fact]
    public void Create_from_descriptor_is_mapped_to_table()
    {
        // Pins isMappedToTable: true on the descriptor path — a false mutant
        // makes QualifiedTableName throw.
        var map = TypeMap.Create(typeof(BulkCopyableFixture));

        Assert.True(map.IsMappedToTable);
        Assert.False(string.IsNullOrEmpty(map.QualifiedTableName));
    }
}
