using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;
// ReSharper disable once RedundantUsingDirective -- required on older TFMs
// (net5.0/net6.0) that lack xunit's global usings; InspectCode is TFM-blind.
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Sixth batch of mutation-hardening tests (#414): the <c>CustomAction</c> configuration
/// checks and the post-action log line. The existing tests assert only the exception
/// type, and a loader built without a <c>SqlConnection</c> throws
/// <see cref="InvalidOperationException"/> either way, so these pin the message: the
/// validation message when the delegate is null, the missing-connection message when it
/// is set.
/// </summary>
public class MutationHardeningTests6
{
    private const string MissingPreConnection = "Cannot perform 'PreAction.CustomAction' without a SqlConnection. " +
                                                "Use a constructor that accepts a SqlConnection.";

    private const string MissingPostConnection = "Cannot perform 'PostAction.CustomAction' without a SqlConnection. " +
                                                 "Use a constructor that accepts a SqlConnection.";



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



    private static Task NoOpPreLoadActionAsync(PreLoadActionParameters parameters) => Task.CompletedTask;



    private static Task NoOpPostLoadActionAsync(PostLoadActionParameters parameters) => Task.CompletedTask;



    private static SqlBulkCopyLoader<TestRecord> CreateSut
    (
        FakeSqlBulkCopyWrapperFactory factory,
        SqlBulkCopyLoaderOptions<TestRecord> options,
        RecordingLogger? logger = null
    )
    {
        return new SqlBulkCopyLoader<TestRecord>(factory, new ManualProgressTimer(), logger: logger, options: options);
    }



    [Fact]
    public void NoOp_actions_complete_synchronously()
    {
        // The delegates are never invoked by the tests below: every loader here has no
        // SqlConnection, so the missing-connection guard throws before the action runs.
        Assert.Same(Task.CompletedTask, NoOpPreLoadActionAsync(null!));
        Assert.Same(Task.CompletedTask, NoOpPostLoadActionAsync(null!));
    }



    [Fact]
    public async Task LoadAsync_when_PreLoadCustomAction_is_set_passes_validation_and_reaches_the_connection_guard()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut
        (
            factory,
            new SqlBulkCopyLoaderOptions<TestRecord>
            {
                PreAction = PreAction.CustomAction,
                PreLoadCustomAction = NoOpPreLoadActionAsync
            }
        );

        var ex = await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );

        Assert.Equal(MissingPreConnection, ex.Message);
    }



    [Fact]
    public async Task LoadAsync_when_PreLoadCustomAction_is_null_throws_the_validation_message()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut
        (
            factory,
            new SqlBulkCopyLoaderOptions<TestRecord> { PreAction = PreAction.CustomAction }
        );

        var ex = await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );

        Assert.Equal("PreAction is CustomAction but PreLoadCustomAction is null.", ex.Message);
    }



    [Fact]
    public async Task LoadAsync_when_PostLoadCustomAction_is_set_writes_the_rows_then_reaches_the_connection_guard()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut
        (
            factory,
            new SqlBulkCopyLoaderOptions<TestRecord>
            {
                PostAction = PostAction.CustomAction,
                PostLoadCustomAction = NoOpPostLoadActionAsync
            }
        );

        var ex = await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(2)))
        );

        Assert.Equal(MissingPostConnection, ex.Message);

        // Validation passed and the load ran before the post-action guard.
        Assert.Equal(2, factory.CreatedWrappers.SelectMany(w => w.BatchRowCounts).Sum());
    }



    [Fact]
    public async Task LoadAsync_when_PostLoadCustomAction_is_null_throws_the_validation_message_before_writing()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut
        (
            factory,
            new SqlBulkCopyLoaderOptions<TestRecord> { PostAction = PostAction.CustomAction }
        );

        var ex = await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(2)))
        );

        Assert.Equal("PostAction is CustomAction but PostLoadCustomAction is null.", ex.Message);
        Assert.Empty(factory.CreatedWrappers);
    }



    [Fact]
    public async Task LoadAsync_when_PostAction_runs_logs_the_post_action()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var logger = new RecordingLogger();
        var sut = CreateSut
        (
            factory,
            new SqlBulkCopyLoaderOptions<TestRecord>
            {
                PostAction = PostAction.CustomAction,
                PostLoadCustomAction = NoOpPostLoadActionAsync
            },
            logger
        );

        await Assert.ThrowsAsync<InvalidOperationException>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1)))
        );

        Assert.Contains("Executing post-action: CustomAction.", logger.Entries);
    }
}
