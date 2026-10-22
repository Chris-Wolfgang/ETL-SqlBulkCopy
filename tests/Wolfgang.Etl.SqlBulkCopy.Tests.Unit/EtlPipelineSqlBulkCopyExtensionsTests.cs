using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Wolfgang.Etl.Abstractions;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;
#if NETCOREAPP3_1
using SkippableFactAttribute = Wolfgang.Etl.SqlBulkCopy.Tests.Unit.NetCoreApp31SkippedFactAttribute;
#endif

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class EtlPipelineSqlBulkCopyExtensionsTests
{
    // Same guard as SqlBulkCopyLoaderOptionsRecordTests: Microsoft.Data.SqlClient cannot
    // construct a SqlConnection on every leg (the net5.0-net7.0 and netcoreapp3.1 legs bind
    // its throwing netstandard2.0 asset), so the facts that need one skip there.
    private static readonly Lazy<bool> _sqlConnectionConstructible = new(IsSqlConnectionConstructible, LazyThreadSafetyMode.PublicationOnly);

    // Infrastructure check: which branch runs depends on the runner's
    // Microsoft.Data.SqlClient asset, not on the code under test.
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static bool IsSqlConnectionConstructible()
    {
        try
        {
            using var probe = new SqlConnection();
            return true;
        }
        catch (TypeInitializationException)
        {
            return false;
        }
        catch (PlatformNotSupportedException)
        {
            return false;
        }
    }

    private static SqlConnection NewConnection()
    {
        Skip.IfNot(_sqlConnectionConstructible.Value, "Microsoft.Data.SqlClient cannot construct a SqlConnection on this target framework.");
        return new SqlConnection();
    }



    [SkippableFact]
    public void SqlBulkCopyLoader_when_pipeline_is_null_throws_ArgumentNullException()
    {
        using var connection = NewConnection();

        var ex = Assert.Throws<ArgumentNullException>
        (
            () => EtlPipelineSqlBulkCopyExtensions.SqlBulkCopyLoader<TestRecord>(null!, connection)
        );

        Assert.Equal("pipeline", ex.ParamName);
    }



    [Fact]
    public void SqlBulkCopyLoader_when_connection_is_null_throws_ArgumentNullException()
    {
        var pipeline = EtlPipeline
            .Create()
            .From(RecordsAsync(1));

        var ex = Assert.Throws<ArgumentNullException>
        (
            () => pipeline.SqlBulkCopyLoader(null!)
        );

        Assert.Equal("connection", ex.ParamName);
    }



    [SkippableFact]
    public async Task RunAsync_when_terminated_by_SqlBulkCopyLoader_in_dry_run_loads_every_record()
    {
        // Dry run pulls every row through mapping without touching the server, so an
        // unopened connection is enough to prove the terminator wires the loader in.
        using var connection = NewConnection();
        var progress = new LastValueProgress();

        await EtlPipeline
            .Create()
            .From(RecordsAsync(3))
            .SqlBulkCopyLoader
            (
                connection,
                new SqlBulkCopyLoaderOptions<TestRecord> { IsDryRun = true }
            )
            .RunAsync(progress);

        Assert.NotNull(progress.Last);
        Assert.Equal(3, progress.Last!.LoadedItemCount);
    }



    private static async IAsyncEnumerable<TestRecord> RecordsAsync(int count)
    {
        for (var i = 1; i <= count; i++)
        {
            yield return new TestRecord { Id = i, Name = $"N{i}", Amount = i };
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }



    private sealed class LastValueProgress : IProgress<EtlPipelineProgress>
    {
        public EtlPipelineProgress? Last { get; private set; }

        public void Report(EtlPipelineProgress value) => Last = value;
    }
}
