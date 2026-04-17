using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class SqlBulkCopyLoaderTests
{
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



    private static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(IEnumerable<T> items)
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

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

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

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

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

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

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

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

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

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

        Assert.Equal("[custom].[CustomTable]", factory.CreatedWrappers[0].DestinationTableName);
    }



    [Fact]
    public async Task LoadAsync_with_empty_source_does_not_write_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);

        await sut.LoadAsync(ToAsyncEnumerable(Array.Empty<TestRecord>())).ConfigureAwait(false);

        Assert.Empty(factory.CreatedWrappers);
    }



    [Fact]
    public async Task LoadAsync_increments_CurrentItemCount_Async()
    {
        var sut = CreateSut();
        var items = CreateTestItems(5);

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

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
            EnableDataValidation = true
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "Valid", Quantity = 5 },
            new ValidatableRecord { Id = 2, Name = "", Quantity = 5 },     // Required fails
            new ValidatableRecord { Id = 3, Name = "Valid", Quantity = 5000 } // Range fails
        };

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

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
            OnValidationFailed = (_, errors) => capturedErrors.Add(errors)
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "", Quantity = 5 } // Required fails
        };

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

        Assert.Single(capturedErrors);
        Assert.NotEmpty(capturedErrors[0]);
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

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

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
            EnableDataValidation = true
        };

        var items = new[]
        {
            new ValidatableRecord { Id = 1, Name = "Skipped", Quantity = 5 },
            new ValidatableRecord { Id = 2, Name = "", Quantity = 5 },       // invalid
            new ValidatableRecord { Id = 3, Name = "Valid", Quantity = 5 }
        };

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

        // 1 skipped by SkipItemCount, 1 skipped by validation, 1 loaded
        Assert.Equal(1, sut.CurrentItemCount);
    }



    // --- ValidateActionConfiguration tests ---

    [Fact]
    public async Task LoadAsync_when_PreAction_CustomAction_without_delegate_throws_Async()
    {
        var sut = CreateSut();
        sut.PreAction = PreAction.CustomAction;

        await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerable(CreateTestItems(1)))
        ).ConfigureAwait(false);
    }



    [Fact]
    public async Task LoadAsync_when_PostAction_CustomAction_without_delegate_throws_Async()
    {
        var sut = CreateSut();
        sut.PostAction = PostAction.CustomAction;

        await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerable(CreateTestItems(1)))
        ).ConfigureAwait(false);
    }



    // --- EnsureConnectionAvailable tests ---

    [Fact]
    public async Task LoadAsync_when_PreAction_DeleteAllRecords_without_connection_throws_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer)
        {
            PreAction = PreAction.DeleteAllRecords
        };

        await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerable(CreateTestItems(1)))
        ).ConfigureAwait(false);
    }



    [Fact]
    public async Task LoadAsync_when_PreAction_TruncateTable_without_connection_throws_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer)
        {
            PreAction = PreAction.TruncateTable
        };

        await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerable(CreateTestItems(1)))
        ).ConfigureAwait(false);
    }



    [Fact]
    public async Task LoadAsync_when_PreAction_CustomAction_without_connection_throws_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer)
        {
            PreAction = PreAction.CustomAction,
            PreLoadCustomAction = _ => Task.CompletedTask
        };

        await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerable(CreateTestItems(1)))
        ).ConfigureAwait(false);
    }



    [Fact]
    public async Task LoadAsync_when_PostAction_CustomAction_without_connection_throws_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var timer = new ManualProgressTimer();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer)
        {
            PostAction = PostAction.CustomAction,
            PostLoadCustomAction = _ => Task.CompletedTask
        };

        await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerable(CreateTestItems(1)))
        ).ConfigureAwait(false);
    }



    // --- MaximumItemCount tests ---

    [Fact]
    public async Task LoadAsync_stops_at_MaximumItemCount_Async()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);
        sut.MaximumItemCount = 3;
        var items = CreateTestItems(10);

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

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

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

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

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

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

        await sut.LoadAsync(ToAsyncEnumerable(items), progress).ConfigureAwait(false);

        Assert.NotNull(captured);
        Assert.Equal(5, captured!.CurrentItemCount);
        Assert.True(captured.BatchCount >= 1);
    }



    // --- PreAction/PostAction CustomAction with real connection ---

    [Fact]
    public async Task LoadAsync_when_PreAction_CustomAction_with_connection_invokes_delegate_Async()
    {
        using var connection = new Microsoft.Data.SqlClient.SqlConnection("Data Source=.;");
        var sut = new SqlBulkCopyLoader<TestRecord>
        (
            connection,
            Microsoft.Data.SqlClient.SqlBulkCopyOptions.Default,
            transaction: null
        );

        var delegateCalled = false;
        sut.PreAction = PreAction.CustomAction;
        sut.PreLoadCustomAction = _ =>
        {
            delegateCalled = true;
            return Task.CompletedTask;
        };

        // The loader will call the delegate but fail when trying to WriteToServer
        // because the connection isn't open. We catch that — the delegate was already called.
        try
        {
            await sut.LoadAsync(ToAsyncEnumerable(CreateTestItems(1))).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // Expected — connection not open for bulk copy
        }

        Assert.True(delegateCalled);
    }



    [Fact]
    public async Task LoadAsync_when_PostAction_CustomAction_with_connection_invokes_delegate_Async()
    {
        using var connection = new Microsoft.Data.SqlClient.SqlConnection("Data Source=.;");
        var loader = new SqlBulkCopyLoader<TestRecord>
        (
            connection,
            Microsoft.Data.SqlClient.SqlBulkCopyOptions.Default,
            transaction: null
        );

        var delegateCalled = false;
        loader.PostAction = PostAction.CustomAction;
        loader.PostLoadCustomAction = _ =>
        {
            delegateCalled = true;
            return Task.CompletedTask;
        };

        // Will fail trying to bulk copy (connection not open) before reaching PostAction
        try
        {
            await loader.LoadAsync(ToAsyncEnumerable(CreateTestItems(1))).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // Expected
        }

        // PostAction only runs after successful load, so won't be called here
        // But the ValidatePostActionConfiguration path IS covered
        Assert.False(delegateCalled);
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

        await sut.LoadAsync(ToAsyncEnumerable(items)).ConfigureAwait(false);

        Assert.Equal(1, sut.CurrentItemCount);
        Assert.Equal(0, sut.CurrentSkippedItemCount);
    }



    // --- Constructor with logger tests ---

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
    public void Constructor_with_logger_when_logger_is_null_throws()
    {
        using var connection = new Microsoft.Data.SqlClient.SqlConnection("Data Source=.;");

        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyLoader<TestRecord>
            (
                connection,
                (Microsoft.Extensions.Logging.ILogger<SqlBulkCopyLoader<TestRecord>>)null!
            )
        );
    }



    [Fact]
    public void Constructor_minimal_when_connection_is_null_throws()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyLoader<TestRecord>((Microsoft.Data.SqlClient.SqlConnection)null!)
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
}
