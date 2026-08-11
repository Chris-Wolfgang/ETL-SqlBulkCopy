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
/// Third batch of mutation-hardening tests (#163), pinning behaviour that is
/// observable only through the logger. Values such as the validation-failure
/// position and the root-vs-nested batch message never reach a public property
/// or callback, so a spy logger is the only way to kill those mutants.
/// </summary>
public class LoggerObservableMutationHardeningTests
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
    public async Task LoadAsync_when_validation_fails_after_skips_logs_position_as_processed_plus_skipped()
    {
        // The logged position is (CurrentItemCount + CurrentSkippedItemCount).
        // Arrange 2 SkipItemCount skips then 1 processed item followed by an
        // invalid one: at failure time processed=1, skipped=2 => position "3".
        // The '-' mutant yields "-1", so the mutant dies on this assertion.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var logger = new RecordingLogger();
        var sut = new SqlBulkCopyLoader<ValidatableRecord>(factory, logger, new ManualProgressTimer())
        {
            EnableDataValidation = true,
            ValidationFailureBehavior = ValidationFailureBehavior.Skip,
            SkipItemCount = 2
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "skipped", Quantity = 5 },
            new ValidatableRecord { Id = 2, Name = "skipped", Quantity = 5 },
            new ValidatableRecord { Id = 3, Name = "valid",   Quantity = 5 },
            new ValidatableRecord { Id = 4, Name = "",        Quantity = 5 } // Required fails
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        var validationEntry = Assert.Single(logger.Entries.Where(e => e.Contains("alidation", StringComparison.Ordinal)));
        Assert.Contains("3", validationEntry, StringComparison.Ordinal);
        Assert.DoesNotContain("-1", validationEntry, StringComparison.Ordinal);
    }



    [Fact]
    public async Task LoadAsync_when_writing_root_and_nested_tables_logs_distinct_messages_per_level()
    {
        // WriteRecursiveAsync branches on isRoot to log either the root
        // batch message or the nested-table message (which carries the child's
        // qualified table name). Negating isRoot swaps them, so asserting that
        // the child table name appears in a nested message - and that a root
        // message also exists - kills the negation.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var logger = new RecordingLogger();
        var sut = new SqlBulkCopyLoader<ParentRecord>(factory, logger, new ManualProgressTimer());

        var parent = new ParentRecord
        {
            ParentId = 1,
            Name = "p",
            Children = new List<ChildRecord>
            {
                new() { ChildId = 10, Description = "a" }
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(new[] { parent }));

        // The nested-table message names the child table; the root message does not.
        Assert.Contains(logger.Entries, e => e.Contains("[ChildRecords]", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, e => !e.Contains("[ChildRecords]", StringComparison.Ordinal));
    }



    [Fact]
    public async Task LoadAsync_when_MaximumItemCount_reached_logs_the_limit()
    {
        // Pins the max-item-count log statement (a statement-removal mutant
        // would drop it silently while the counts still look correct).
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var logger = new RecordingLogger();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger, new ManualProgressTimer())
        {
            MaximumItemCount = 2
        };

        var items = Enumerable.Range(1, 5)
            .Select(i => new TestRecord { Id = i, Name = $"Item{i}", Amount = i })
            .ToList();

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Contains(logger.Entries, e => e.Contains("aximum", StringComparison.Ordinal));
    }



    [Fact]
    public async Task LoadAsync_when_SkipItemCount_set_logs_each_skipped_item()
    {
        // Pins the skipped-item log statement.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var logger = new RecordingLogger();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger, new ManualProgressTimer())
        {
            SkipItemCount = 2
        };

        var items = Enumerable.Range(1, 4)
            .Select(i => new TestRecord { Id = i, Name = $"Item{i}", Amount = i })
            .ToList();

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        // Match the per-item message ("Skipped item N of M.") specifically — the
        // completion summary also mentions "items skipped" and would otherwise
        // inflate the count.
        Assert.Equal(2, logger.Entries.Count(e => e.StartsWith("Skipped item ", StringComparison.Ordinal)));
        Assert.Contains("Skipped item 1 of 2.", logger.Entries);
        Assert.Contains("Skipped item 2 of 2.", logger.Entries);
    }
}
