using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class SqlBulkCopyLoaderTests
{
    // Microsoft.Data.SqlClient 6.x's SqlPerformanceCounters cctor depends on
    // Windows-only PerformanceCounter and on perf-counter categories that may
    // not exist on every runner. When the cctor throws TypeInitializationException
    // (Linux, locked-down Windows boxes), tests that need a real SqlConnection
    // instance skip rather than fail. Determine this once per process.
    private static readonly Lazy<bool> _sqlConnectionConstructible = new(IsSqlConnectionConstructible, LazyThreadSafetyMode.PublicationOnly);

    private static bool IsSqlConnectionConstructible()
    {
        try
        {
            using var probe = new Microsoft.Data.SqlClient.SqlConnection("Server=.;");
            return true;
        }
        catch (TypeInitializationException)
        {
            return false;
        }
    }

    private static void SkipUnlessSqlConnectionConstructible()
    {
        Skip.IfNot
        (
            _sqlConnectionConstructible.Value,
            "Microsoft.Data.SqlClient cannot initialize on this runner (SqlPerformanceCounters cctor failed); ctor test covered by environments where SqlConnection is constructible."
        );
    }

    private static SqlBulkCopyLoader<TestRecord> CreateSut()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        return new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer);
    }



    private static SqlBulkCopyLoader<TestRecord> CreateSut(FakeSqlBulkCopyWrapperFactory factory)
    {
        var timer = new ManualProgressTimer();
        return new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer);
    }



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



    // --- Constructor tests ---

    [Fact]
    public void Constructor_when_connection_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyLoader<TestRecord>((Microsoft.Data.SqlClient.SqlConnection)null!)
        );
    }



    [SkippableFact]
    public void Constructor_with_connection_only_succeeds_with_closed_connection()
    {
        SkipUnlessSqlConnectionConstructible();

        // A closed SqlConnection is enough to construct — the loader does not
        // open it until LoadAsync runs. Covers the public (SqlConnection)
        // constructor body which integration tests would otherwise be the
        // only callers of.
        using var connection = new Microsoft.Data.SqlClient.SqlConnection("Server=.;");

        var sut = new SqlBulkCopyLoader<TestRecord>(connection);

        Assert.Equal(10_000, sut.BatchSize);
    }



    [SkippableFact]
    public void Constructor_with_connection_and_logger_when_logger_is_null_throws_ArgumentNullException()
    {
        SkipUnlessSqlConnectionConstructible();

        using var connection = new Microsoft.Data.SqlClient.SqlConnection("Server=.;");

        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyLoader<TestRecord>(connection, (Microsoft.Extensions.Logging.ILogger<SqlBulkCopyLoader<TestRecord>>)null!)
        );
    }



    [Fact]
    public void Constructor_with_connection_and_logger_when_connection_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyLoader<TestRecord>
            (
                (Microsoft.Data.SqlClient.SqlConnection)null!,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<SqlBulkCopyLoader<TestRecord>>.Instance
            )
        );
    }



    [SkippableFact]
    public void Constructor_with_connection_and_logger_succeeds()
    {
        SkipUnlessSqlConnectionConstructible();

        using var connection = new Microsoft.Data.SqlClient.SqlConnection("Server=.;");

        var sut = new SqlBulkCopyLoader<TestRecord>
        (
            connection,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<SqlBulkCopyLoader<TestRecord>>.Instance
        );

        Assert.Equal(10_000, sut.BatchSize);
    }



    [Fact]
    public void Constructor_full_when_connection_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyLoader<TestRecord>
            (
                (Microsoft.Data.SqlClient.SqlConnection)null!,
                Microsoft.Data.SqlClient.SqlBulkCopyOptions.Default,
                transaction: null
            )
        );
    }



    [SkippableFact]
    public void Constructor_full_succeeds_without_logger()
    {
        SkipUnlessSqlConnectionConstructible();

        using var connection = new Microsoft.Data.SqlClient.SqlConnection("Server=.;");

        var sut = new SqlBulkCopyLoader<TestRecord>
        (
            connection,
            Microsoft.Data.SqlClient.SqlBulkCopyOptions.Default,
            transaction: null
        );

        Assert.Equal(10_000, sut.BatchSize);
    }



    [SkippableFact]
    public void Constructor_full_succeeds_with_logger()
    {
        SkipUnlessSqlConnectionConstructible();

        using var connection = new Microsoft.Data.SqlClient.SqlConnection("Server=.;");

        var sut = new SqlBulkCopyLoader<TestRecord>
        (
            connection,
            Microsoft.Data.SqlClient.SqlBulkCopyOptions.Default,
            transaction: null,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<SqlBulkCopyLoader<TestRecord>>.Instance
        );

        Assert.Equal(10_000, sut.BatchSize);
    }



    // --- Property tests ---

    [Fact]
    public void BatchSize_default_is_10000()
    {
        var sut = CreateSut();

        Assert.Equal(10_000, sut.BatchSize);
    }



    [Fact]
    public void BatchSize_when_set_to_valid_value_updates()
    {
        var sut = CreateSut();

        sut.BatchSize = 500;

        Assert.Equal(500, sut.BatchSize);
    }



    [Fact]
    public void BatchSize_when_set_to_zero_throws_ArgumentOutOfRangeException()
    {
        var sut = CreateSut();

        Assert.Throws<ArgumentOutOfRangeException>
        (
            () => sut.BatchSize = 0
        );
    }



    [Fact]
    public void BatchSize_when_set_to_negative_throws_ArgumentOutOfRangeException()
    {
        var sut = CreateSut();

        Assert.Throws<ArgumentOutOfRangeException>
        (
            () => sut.BatchSize = -1
        );
    }



    [Fact]
    public void BulkCopyTimeout_default_is_30()
    {
        var sut = CreateSut();

        Assert.Equal(30, sut.BulkCopyTimeout);
    }



    [Fact]
    public void BulkCopyTimeout_when_set_to_zero_succeeds()
    {
        var sut = CreateSut();

        sut.BulkCopyTimeout = 0;

        Assert.Equal(0, sut.BulkCopyTimeout);
    }



    [Fact]
    public void BulkCopyTimeout_when_set_to_negative_throws_ArgumentOutOfRangeException()
    {
        var sut = CreateSut();

        Assert.Throws<ArgumentOutOfRangeException>
        (
            () => sut.BulkCopyTimeout = -1
        );
    }



    // --- Loading behavior tests ---

    [Fact]
    public async Task LoadAsync_writes_all_items_in_single_batch_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);
        var items = CreateTestItems(5);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Single(factory.CreatedWrappers);
        Assert.Equal(5, factory.CreatedWrappers[0].BatchRowCounts[0]);
    }



    [Fact]
    public async Task LoadAsync_when_items_exceed_BatchSize_creates_multiple_batches_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);
        sut.BatchSize = 3;
        var items = CreateTestItems(7);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        // 7 items / batch size 3 = 3 batches (3+3+1)
        // Each batch creates a wrapper for main table
        Assert.Equal(3, factory.CreatedWrappers.Count);
        Assert.Equal(3, factory.CreatedWrappers[0].BatchRowCounts[0]);
        Assert.Equal(3, factory.CreatedWrappers[1].BatchRowCounts[0]);
        Assert.Equal(1, factory.CreatedWrappers[2].BatchRowCounts[0]);
    }



    [Fact]
    public async Task LoadAsync_sets_column_mappings_on_wrapper_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);
        var items = CreateTestItems(1);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        var wrapper = factory.CreatedWrappers[0];
        Assert.Contains(wrapper.ColumnMappings, m => string.Equals(m.Source, "Id", StringComparison.Ordinal) && string.Equals(m.Destination, "Id", StringComparison.Ordinal));
        Assert.Contains(wrapper.ColumnMappings, m => string.Equals(m.Source, "FullName", StringComparison.Ordinal) && string.Equals(m.Destination, "FullName", StringComparison.Ordinal));
        Assert.Contains(wrapper.ColumnMappings, m => string.Equals(m.Source, "Amount", StringComparison.Ordinal) && string.Equals(m.Destination, "Amount", StringComparison.Ordinal));
    }



    [Fact]
    public async Task LoadAsync_sets_destination_table_name_on_wrapper_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);
        var items = CreateTestItems(1);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal("[dbo].[TestRecords]", factory.CreatedWrappers[0].DestinationTableName);
    }



    [Fact]
    public async Task LoadAsync_when_DestinationTableName_override_uses_override_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);
        sut.DestinationTableName = "CustomTable";
        sut.DestinationSchemaName = "custom";
        var items = CreateTestItems(1);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal("[custom].[CustomTable]", factory.CreatedWrappers[0].DestinationTableName);
    }



    [Fact]
    public async Task LoadAsync_with_empty_source_does_not_write_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);

        await sut.LoadAsync(ToAsyncEnumerableAsync(Array.Empty<TestRecord>()));

        Assert.Empty(factory.CreatedWrappers);
    }



    [Fact]
    public async Task LoadAsync_increments_CurrentItemCount_Async()
    {
        var sut = CreateSut();
        var items = CreateTestItems(5);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal(5, sut.CurrentItemCount);
    }



    // --- Validation tests ---

    [Fact]
    public async Task LoadAsync_when_validation_enabled_skips_invalid_items_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ValidatableRecord>(factory, logger: null, timer)
        {
            EnableDataValidation = true,
            ValidationFailureBehavior = ValidationFailureBehavior.Skip
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "Valid", Quantity = 5 },
            new ValidatableRecord { Id = 2, Name = "", Quantity = 5 },     // Required fails
            new ValidatableRecord { Id = 3, Name = "Valid", Quantity = 5000 } // Range fails
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal(1, sut.CurrentItemCount);
        Assert.Equal(2, sut.CurrentSkippedItemCount);
    }



    [Fact]
    public async Task LoadAsync_when_validation_enabled_invokes_callback_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var capturedErrors = new List<ICollection<ValidationResult>>();
        var sut = new SqlBulkCopyLoader<ValidatableRecord>(factory, logger: null, timer)
        {
            EnableDataValidation = true,
            ValidationFailureBehavior = ValidationFailureBehavior.Skip,
            OnValidationFailed = (_, errors) => capturedErrors.Add(errors)
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "", Quantity = 5 } // Required fails
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Single(capturedErrors);
        Assert.NotEmpty(capturedErrors[0]);
    }



    // --- Nested-child validation tests (Issue #27) ---

    [Fact]
    public async Task LoadAsync_when_validation_enabled_skips_invalid_nested_children_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ParentWithValidatableChildren>(factory, logger: null, timer)
        {
            EnableDataValidation = true,
            ValidationFailureBehavior = ValidationFailureBehavior.Skip
        };

        var items = new[]
        {
            new ParentWithValidatableChildren
            {
                Id = 1,
                Children =
                [
                    new ValidatableChild { Id = 10, Name = "ok",          Quantity = 5 },
                    new ValidatableChild { Id = 11, Name = "",            Quantity = 5 },   // Required fails
                    new ValidatableChild { Id = 12, Name = "out-of-range", Quantity = 9000 } // Range fails
                ]
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        // The child-table wrapper(s) should have received only the 1 valid child.
        // Wrappers are created lazily — find the one writing to the Children table.
        var childWrapper = factory.CreatedWrappers.Single
        (
            w => string.Equals(w.DestinationTableName, "[Children]", StringComparison.Ordinal)
        );
        Assert.Equal(1, childWrapper.BatchRowCounts.Sum());
    }



    [Fact]
    public async Task LoadAsync_when_validation_enabled_invokes_OnNestedValidationFailed_for_invalid_children_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var capturedChildren = new List<object>();
        var capturedErrorCounts = new List<int>();
        var sut = new SqlBulkCopyLoader<ParentWithValidatableChildren>(factory, logger: null, timer)
        {
            EnableDataValidation = true,
            ValidationFailureBehavior = ValidationFailureBehavior.Skip,
            OnNestedValidationFailed = (child, errors) =>
            {
                capturedChildren.Add(child);
                capturedErrorCounts.Add(errors.Count);
            }
        };

        var items = new[]
        {
            new ParentWithValidatableChildren
            {
                Id = 1,
                Children =
                [
                    new ValidatableChild { Id = 10, Name = "ok",   Quantity = 5 },
                    new ValidatableChild { Id = 11, Name = "",     Quantity = 5 },   // Required fails
                    new ValidatableChild { Id = 12, Name = "also", Quantity = 9000 } // Range fails
                ]
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal(2, capturedChildren.Count);
        Assert.Contains(capturedChildren, c => ((ValidatableChild)c).Id == 11);
        Assert.Contains(capturedChildren, c => ((ValidatableChild)c).Id == 12);
        Assert.All(capturedErrorCounts, count => Assert.True(count >= 1));
    }



    [Fact]
    public async Task LoadAsync_when_validation_enabled_validates_grandchildren_recursively_Async()
    {
        // Two levels deep: parent → child → grandchild. The grandchild has its
        // own [Required] Label; an empty Label must be dropped from the
        // grandchild table without affecting the parent or its valid child.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ParentWithValidatableChildren>(factory, logger: null, timer)
        {
            EnableDataValidation = true,
            ValidationFailureBehavior = ValidationFailureBehavior.Skip
        };

        var items = new[]
        {
            new ParentWithValidatableChildren
            {
                Id = 1,
                Children =
                [
                    new ValidatableChild
                    {
                        Id = 10,
                        Name = "ok",
                        Quantity = 5,
                        Grandchildren =
                        [
                            new ValidatableGrandchild { Id = 100, Label = "ok" },
                            new ValidatableGrandchild { Id = 101, Label = "" } // Required fails
                        ]
                    }
                ]
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        var grandchildWrapper = factory.CreatedWrappers.Single
        (
            w => string.Equals(w.DestinationTableName, "[Grandchildren]", StringComparison.Ordinal)
        );
        Assert.Equal(1, grandchildWrapper.BatchRowCounts.Sum());
    }



    [Fact]
    public async Task LoadAsync_when_validation_disabled_does_not_skip_invalid_nested_children_Async()
    {
        // Sanity: with EnableDataValidation = false, invalid children are
        // still written. This is the existing default behavior.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ParentWithValidatableChildren>(factory, logger: null, timer);

        var items = new[]
        {
            new ParentWithValidatableChildren
            {
                Id = 1,
                Children =
                [
                    new ValidatableChild { Id = 10, Name = "ok", Quantity = 5 },
                    new ValidatableChild { Id = 11, Name = "",   Quantity = 5 }
                ]
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        var childWrapper = factory.CreatedWrappers.Single
        (
            w => string.Equals(w.DestinationTableName, "[Children]", StringComparison.Ordinal)
        );
        Assert.Equal(2, childWrapper.BatchRowCounts.Sum());
    }



    // --- ValidationFailureBehavior.Throw (default) tests ---

    [Fact]
    public async Task LoadAsync_when_validation_enabled_default_behavior_throws_SqlBulkCopyValidationException_for_root_Async()
    {
        // Default ValidationFailureBehavior is Throw. A failing root item
        // should raise SqlBulkCopyValidationException carrying the item
        // and its ValidationResults.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ValidatableRecord>(factory, logger: null, timer)
        {
            EnableDataValidation = true
            // ValidationFailureBehavior left at default (Throw)
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "", Quantity = 5 } // Required fails
        };

        var ex = await Assert.ThrowsAsync<SqlBulkCopyValidationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(items))
        );

        var failing = Assert.IsType<ValidatableRecord>(ex.Item);
        Assert.Equal(1, failing.Id);
        Assert.NotEmpty(ex.ValidationResults);
    }



    [Fact]
    public async Task LoadAsync_when_validation_enabled_default_behavior_invokes_OnValidationFailed_before_throwing_Async()
    {
        // The OnValidationFailed callback must fire before the throw so a
        // single hook can log / inspect the failure regardless of mode.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var capturedBeforeThrow = false;
        var sut = new SqlBulkCopyLoader<ValidatableRecord>(factory, logger: null, timer)
        {
            EnableDataValidation = true,
            OnValidationFailed = (_, _) => capturedBeforeThrow = true
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "", Quantity = 5 }
        };

        await Assert.ThrowsAsync<SqlBulkCopyValidationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(items))
        );

        Assert.True(capturedBeforeThrow);
    }



    [Fact]
    public async Task LoadAsync_when_validation_enabled_default_behavior_throws_SqlBulkCopyValidationException_for_nested_child_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ParentWithValidatableChildren>(factory, logger: null, timer)
        {
            EnableDataValidation = true
        };

        var items = new[]
        {
            new ParentWithValidatableChildren
            {
                Id = 1,
                Children = [new ValidatableChild { Id = 10, Name = "", Quantity = 5 }] // Required fails
            }
        };

        var ex = await Assert.ThrowsAsync<SqlBulkCopyValidationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(items))
        );

        var failing = Assert.IsType<ValidatableChild>(ex.Item);
        Assert.Equal(10, failing.Id);
        Assert.NotEmpty(ex.ValidationResults);
    }



    // --- Nested table tests ---

    [Fact]
    public async Task LoadAsync_writes_nested_table_items_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ParentRecord>(factory, logger: null, timer);

        var items = new[]
        {
            new ParentRecord
            {
                ParentId = 1,
                Name = "Parent1",
                Children = new System.Collections.Generic.List<ChildRecord>
                {
                    new ChildRecord { ChildId = 10, Description = "Child10" },
                    new ChildRecord { ChildId = 11, Description = "Child11" }
                }
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        // Should create 2 wrappers: one for parent table, one for child table
        Assert.Equal(2, factory.CreatedWrappers.Count);

        // Parent wrapper
        Assert.Equal("[ParentRecords]", factory.CreatedWrappers[0].DestinationTableName);
        Assert.Equal(1, factory.CreatedWrappers[0].BatchRowCounts[0]);

        // Child wrapper
        Assert.Equal("[ChildRecords]", factory.CreatedWrappers[1].DestinationTableName);
        Assert.Equal(2, factory.CreatedWrappers[1].BatchRowCounts[0]);
    }



    // --- SkipItemCount + Validation interaction tests ---

    [Fact]
    public async Task LoadAsync_when_SkipItemCount_set_with_validation_skips_correctly_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ValidatableRecord>(factory, logger: null, timer)
        {
            SkipItemCount = 1,
            EnableDataValidation = true,
            ValidationFailureBehavior = ValidationFailureBehavior.Skip
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "Skipped", Quantity = 5 },
            new ValidatableRecord { Id = 2, Name = "", Quantity = 5 },       // invalid
            new ValidatableRecord { Id = 3, Name = "Valid", Quantity = 5 }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        // 1 skipped by SkipItemCount, 1 skipped by validation, 1 loaded
        Assert.Equal(1, sut.CurrentItemCount);
    }



    // --- PreAction / PostAction SQL orchestration tests (via FakeSqlCommandExecutor) ---

    [Fact]
    public async Task LoadAsync_when_PreAction_is_DeleteAllRecords_issues_DELETE_FROM_command_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var executor = new FakeSqlCommandExecutor();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer, executor)
        {
            PreAction = PreAction.DeleteAllRecords
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)));

        var cmd = Assert.Single(executor.ExecutedCommands);
        Assert.Equal("DELETE FROM [dbo].[TestRecords]", cmd.CommandText);
    }



    [Fact]
    public async Task LoadAsync_when_PreAction_is_TruncateTable_issues_TRUNCATE_TABLE_command_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var executor = new FakeSqlCommandExecutor();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer, executor)
        {
            PreAction = PreAction.TruncateTable
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)));

        var cmd = Assert.Single(executor.ExecutedCommands);
        Assert.Equal("TRUNCATE TABLE [dbo].[TestRecords]", cmd.CommandText);
    }



    [Fact]
    public async Task LoadAsync_when_PreAction_is_None_executor_is_not_called_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var executor = new FakeSqlCommandExecutor();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer, executor);
        // PreAction stays at default (None)

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)));

        Assert.Empty(executor.ExecutedCommands);
    }



    [Fact]
    public async Task LoadAsync_when_PreAction_command_uses_configured_BulkCopyTimeout_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var executor = new FakeSqlCommandExecutor();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer, executor)
        {
            PreAction = PreAction.DeleteAllRecords,
            BulkCopyTimeout = 120
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)));

        Assert.Equal(120, executor.ExecutedCommands[0].CommandTimeout);
    }



    [Fact]
    public async Task LoadAsync_when_PreAction_is_DeleteAllRecords_without_executor_throws_InvalidOperationException_Async()
    {
        // Internal test ctor without an ISqlCommandExecutor + a SQL-issuing
        // PreAction = clear configuration error rather than NRE.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer)
        {
            PreAction = PreAction.DeleteAllRecords
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );

        Assert.Contains("SqlConnection", ex.Message, StringComparison.Ordinal);
    }



    // --- ValidateActionConfiguration tests ---

    [Fact]
    public Task LoadAsync_when_PreAction_CustomAction_without_delegate_throws_Async()
    {
        var sut = CreateSut();
        sut.PreAction = PreAction.CustomAction;

        return Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );
    }



    [Fact]
    public Task LoadAsync_when_PostAction_CustomAction_without_delegate_throws_Async()
    {
        var sut = CreateSut();
        sut.PostAction = PostAction.CustomAction;

        return Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );
    }



    // --- EnsureConnectionAvailable tests ---

    [Fact]
    public Task LoadAsync_when_PreAction_DeleteAllRecords_without_connection_throws_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer)
        {
            PreAction = PreAction.DeleteAllRecords
        };

        return Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );
    }



    [Fact]
    public Task LoadAsync_when_PreAction_TruncateTable_without_connection_throws_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer)
        {
            PreAction = PreAction.TruncateTable
        };

        return Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );
    }



    [Fact]
    public Task LoadAsync_when_PreAction_CustomAction_without_connection_throws_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer)
        {
            PreAction = PreAction.CustomAction,
            PreLoadCustomAction = _ => Task.CompletedTask
        };

        return Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );
    }



    [Fact]
    public Task LoadAsync_when_PostAction_CustomAction_without_connection_throws_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer)
        {
            PostAction = PostAction.CustomAction,
            PostLoadCustomAction = _ => Task.CompletedTask
        };

        return Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );
    }



    // --- MaximumItemCount tests ---

    [Fact]
    public async Task LoadAsync_stops_at_MaximumItemCount_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);
        sut.MaximumItemCount = 3;
        var items = CreateTestItems(10);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal(3, sut.CurrentItemCount);
    }



    // --- SkipItemCount tests ---

    [Fact]
    public async Task LoadAsync_skips_items_up_to_SkipItemCount_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);
        sut.SkipItemCount = 3;
        var items = CreateTestItems(5);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal(2, sut.CurrentItemCount);
        Assert.Equal(3, sut.CurrentSkippedItemCount);
    }



    // --- BulkCopyTimeout propagation ---

    [Fact]
    public async Task LoadAsync_sets_BulkCopyTimeout_on_wrapper_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);
        sut.BulkCopyTimeout = 120;
        var items = CreateTestItems(1);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal(120, factory.CreatedWrappers[0].BulkCopyTimeout);
    }



    // --- Progress report ---

    [Fact]
    public async Task LoadAsync_with_progress_reports_batch_count_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer)
        {
            BatchSize = 2
        };
        var items = CreateTestItems(5);
        SqlBulkCopyReport? captured = null;
        var progress = new SynchronousProgress<SqlBulkCopyReport>(r => captured = r);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items), progress);

        Assert.NotNull(captured);
        Assert.Equal(5, captured!.CurrentItemCount);
        Assert.True(captured.BatchCount >= 1);
    }



    // --- CreateProgressTimer fallback path test ---

    [Fact]
    public async Task LoadAsync_with_progress_when_no_timer_injected_uses_base_timer_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer: null);
        var captured = new List<SqlBulkCopyReport>();
        var progress = new SynchronousProgress<SqlBulkCopyReport>(captured.Add);

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(3)), progress);

        // The fact that LoadAsync completed without throwing means
        // base.CreateProgressTimer(progress) was successfully invoked
        // and the resulting SystemProgressTimer was started and stopped.
        Assert.Equal(3, sut.CurrentItemCount);
    }



    [Fact]
    public async Task LoadAsync_called_twice_with_different_progress_swaps_Elapsed_handler_Async()
    {
        // Reusing a single loader across multiple LoadAsync calls must not
        // continue invoking the first IProgress after the second call has
        // wired a new one — the prior Elapsed handler must be detached and
        // replaced. Verify via reflection on the internal handler field
        // because ManualProgressTimer is disposed by each LoadAsync, so we
        // can't Fire() it post-call to observe routing directly.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer);

        var progressA = new SynchronousProgress<SqlBulkCopyReport>(_ => { });
        var progressB = new SynchronousProgress<SqlBulkCopyReport>(_ => { });

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(3)), progressA);
        var handlerField = typeof(SqlBulkCopyLoader<TestRecord>).GetField
        (
            "_progressTimerHandler",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic
        );
        Assert.NotNull(handlerField);
        var handlerAfterFirstCall = handlerField!.GetValue(sut);

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(3)), progressB);
        var handlerAfterSecondCall = handlerField.GetValue(sut);

        Assert.NotNull(handlerAfterFirstCall);
        Assert.NotNull(handlerAfterSecondCall);
        Assert.NotSame(handlerAfterFirstCall, handlerAfterSecondCall);
    }



    // --- Invalid enum value tests ---

    [Fact]
    public Task LoadAsync_when_PreAction_is_invalid_enum_value_throws_Async()
    {
        var sut = CreateSut();
        sut.PreAction = (PreAction)999;

        return Assert.ThrowsAsync<ArgumentOutOfRangeException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );
    }



    [Fact]
    public Task LoadAsync_when_PostAction_is_invalid_enum_value_throws_Async()
    {
        var sut = CreateSut();
        sut.PostAction = (PostAction)999;

        return Assert.ThrowsAsync<ArgumentOutOfRangeException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );
    }



    [Fact]
    public Task LoadAsync_when_PreAction_DeleteAllRecords_with_NotMapped_type_throws_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<NotMappedWithChildrenRecord>(factory, logger: null, timer)
        {
            PreAction = PreAction.DeleteAllRecords
        };

        var items = new[]
        {
            new NotMappedWithChildrenRecord { Id = 1 }
        };

        return Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(items))
        );
    }



    [Fact]
    public Task LoadAsync_when_PreAction_TruncateTable_with_NotMapped_type_throws_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<NotMappedWithChildrenRecord>(factory, logger: null, timer)
        {
            PreAction = PreAction.TruncateTable
        };

        var items = new[]
        {
            new NotMappedWithChildrenRecord { Id = 1 }
        };

        return Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(items))
        );
    }



    // --- EnableDataValidation false path ---

    [Fact]
    public async Task LoadAsync_when_validation_disabled_loads_all_items_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ValidatableRecord>(factory, logger: null, timer)
        {
            EnableDataValidation = false
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "", Quantity = 5000 } // would fail validation
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal(1, sut.CurrentItemCount);
        Assert.Equal(0, sut.CurrentSkippedItemCount);
    }



    // --- Additional constructor tests ---

    [Fact]
    public void Constructor_with_logger_when_connection_is_null_throws()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyLoader<TestRecord>
            (
                (Microsoft.Data.SqlClient.SqlConnection)null!,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<SqlBulkCopyLoader<TestRecord>>.Instance
            )
        );
    }



    [Fact]
    public void Constructor_full_when_connection_is_null_throws()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyLoader<TestRecord>
            (
                null!,
                Microsoft.Data.SqlClient.SqlBulkCopyOptions.Default,
                transaction: null
            )
        );
    }



    // --- Report tests ---

    [Fact]
    public void SqlBulkCopyReport_stores_values_correctly()
    {
        var report = new SqlBulkCopyReport(100, 5, 3);

        Assert.Equal(100, report.CurrentItemCount);
        Assert.Equal(5, report.CurrentSkippedItemCount);
        Assert.Equal(3, report.BatchCount);
    }



    // --- Deep nesting tests ---

    [Fact]
    public async Task LoadAsync_recurses_into_grandchild_collections_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<GrandparentRecord>(factory, logger: null, timer);

        var items = new[]
        {
            new GrandparentRecord
            {
                GrandparentId = 1,
                Children = new List<IntermediateRecord>
                {
                    new IntermediateRecord
                    {
                        IntermediateId = 10,
                        Grandchildren = new List<GrandchildRecord>
                        {
                            new GrandchildRecord { GrandchildId = 100, Note = "g1" },
                            new GrandchildRecord { GrandchildId = 101, Note = "g2" }
                        }
                    }
                }
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        // 3 wrappers expected: grandparent table, intermediate table, grandchild table
        Assert.Equal(3, factory.CreatedWrappers.Count);
        Assert.Equal("[GrandparentRecords]", factory.CreatedWrappers[0].DestinationTableName);
        Assert.Equal("[IntermediateRecords]", factory.CreatedWrappers[1].DestinationTableName);
        Assert.Equal("[GrandchildRecords]", factory.CreatedWrappers[2].DestinationTableName);
        Assert.Equal(2, factory.CreatedWrappers[2].BatchRowCounts[0]);
    }



    [Fact]
    public async Task LoadAsync_enforces_BatchSize_for_nested_tables_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ParentRecord>(factory, logger: null, timer)
        {
            BatchSize = 3
        };

        // Single parent with 7 children — should produce 3 nested batches (3+3+1)
        var items = new[]
        {
            new ParentRecord
            {
                ParentId = 1,
                Name = "P",
                Children = Enumerable.Range(1, 7)
                    .Select(i => new ChildRecord { ChildId = i, Description = $"C{i}" })
                    .ToList()
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        // 1 parent wrapper + 3 child wrappers
        Assert.Equal(4, factory.CreatedWrappers.Count);
        Assert.Equal(1, factory.CreatedWrappers[0].BatchRowCounts[0]);    // parent
        Assert.Equal(3, factory.CreatedWrappers[1].BatchRowCounts[0]);    // child chunk 1
        Assert.Equal(3, factory.CreatedWrappers[2].BatchRowCounts[0]);    // child chunk 2
        Assert.Equal(1, factory.CreatedWrappers[3].BatchRowCounts[0]);    // child chunk 3 (remainder)
    }



    [Fact]
    public async Task LoadAsync_BatchCount_includes_nested_writes_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<ParentRecord>(factory, logger: null, timer);

        var items = new[]
        {
            new ParentRecord
            {
                ParentId = 1,
                Name = "P",
                Children = new List<ChildRecord>
                {
                    new ChildRecord { ChildId = 10, Description = "C" }
                }
            }
        };

        SqlBulkCopyReport? captured = null;
        var progress = new SynchronousProgress<SqlBulkCopyReport>(r => captured = r);

        await sut.LoadAsync(ToAsyncEnumerableAsync(items), progress);

        // 1 parent batch + 1 nested child batch = 2
        Assert.NotNull(captured);
        Assert.Equal(2, captured!.BatchCount);
    }
}
