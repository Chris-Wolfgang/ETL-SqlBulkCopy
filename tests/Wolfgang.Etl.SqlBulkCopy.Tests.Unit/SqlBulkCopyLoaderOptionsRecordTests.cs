using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Wolfgang.Etl.Abstractions;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// <see cref="SqlBulkCopyLoaderOptions{TRecord}"/> derives from the Abstractions base record (ADR-0009):
/// the shared and the loader's own settings are configured on the record and applied by the constructor.
/// </summary>
public class SqlBulkCopyLoaderOptionsRecordTests
{
    private static Func<ItemErrorContext, ItemErrorAction> AnyPolicy => _ => default;



    [Fact]
    public void SqlBulkCopyLoaderOptions_derives_from_LoaderOptions()
    {
        Assert.IsAssignableFrom<LoaderOptions>(new SqlBulkCopyLoaderOptions<TestRecord>());
    }



    [Fact]
    public void Constructor_when_given_options_applies_the_inherited_settings()
    {
        using var connection = new SqlConnection();
        var policy = AnyPolicy;
        var options = new SqlBulkCopyLoaderOptions<TestRecord>
        {
            ReportingInterval = 5,
            SkipItemCount = 2,
            MaximumItemCount = 3,
            ErrorPolicy = policy,
        };

        var sut = new SqlBulkCopyLoader<TestRecord>(connection, options);

        Assert.Equal(5, sut.ReportingInterval);
        Assert.Equal(2, sut.SkipItemCount);
        Assert.Equal(3, sut.MaximumItemCount);
        Assert.Same(policy, sut.ErrorPolicy);
    }



    [Fact]
    public void Constructor_when_given_options_applies_the_loaders_own_settings()
    {
        using var connection = new SqlConnection();
        Action<TestRecord, ICollection<ValidationResult>> onFailed = (_, _) => { };
        Action<object, ICollection<ValidationResult>> onNestedFailed = (_, _) => { };
        Func<PreLoadActionParameters, Task> pre = _ => Task.CompletedTask;
        Func<PostLoadActionParameters, Task> post = _ => Task.CompletedTask;
        var options = new SqlBulkCopyLoaderOptions<TestRecord>
        {
            BatchSize = 5_000,
            BulkCopyTimeout = 60,
            DestinationTableName = "Staging",
            DestinationSchemaName = "etl",
            EnableDataValidation = true,
            IsDryRun = true,
            ValidationFailureBehavior = ValidationFailureBehavior.Skip,
            OnValidationFailed = onFailed,
            OnNestedValidationFailed = onNestedFailed,
            PreAction = PreAction.CustomAction,
            PreLoadCustomAction = pre,
            PostAction = PostAction.CustomAction,
            PostLoadCustomAction = post,
        };

        var sut = new SqlBulkCopyLoader<TestRecord>(connection, options);

        Assert.Equal(5_000, sut.BatchSize);
        Assert.Equal(60, sut.BulkCopyTimeout);
        Assert.Equal("Staging", sut.DestinationTableName);
        Assert.Equal("etl", sut.DestinationSchemaName);
        Assert.True(sut.EnableDataValidation);
        Assert.True(sut.IsDryRun);
        Assert.Equal(ValidationFailureBehavior.Skip, sut.ValidationFailureBehavior);
        Assert.Same(onFailed, sut.OnValidationFailed);
        Assert.Same(onNestedFailed, sut.OnNestedValidationFailed);
        Assert.Equal(PreAction.CustomAction, sut.PreAction);
        Assert.Same(pre, sut.PreLoadCustomAction);
        Assert.Equal(PostAction.CustomAction, sut.PostAction);
        Assert.Same(post, sut.PostLoadCustomAction);
    }



    [Fact]
    public void Constructor_when_options_are_omitted_matches_an_empty_record()
    {
        using var connection = new SqlConnection();

        var without = new SqlBulkCopyLoader<TestRecord>(connection);
        var empty = new SqlBulkCopyLoader<TestRecord>(connection, new SqlBulkCopyLoaderOptions<TestRecord>());

        Assert.Equal(empty.BatchSize, without.BatchSize);
        Assert.Equal(empty.BulkCopyTimeout, without.BulkCopyTimeout);
        Assert.Equal(empty.ReportingInterval, without.ReportingInterval);
        Assert.Equal(empty.SkipItemCount, without.SkipItemCount);
        Assert.Equal(empty.MaximumItemCount, without.MaximumItemCount);
        Assert.Equal(empty.ValidationFailureBehavior, without.ValidationFailureBehavior);
        Assert.False(without.IsDryRun);
        Assert.Null(without.DestinationTableName);
    }



    [Fact]
    public void Record_defaults_match_the_loaders_defaults()
    {
        using var connection = new SqlConnection();
        var options = new SqlBulkCopyLoaderOptions<TestRecord>();

        var sut = new SqlBulkCopyLoader<TestRecord>(connection);

        Assert.Equal(sut.BatchSize, options.BatchSize);
        Assert.Equal(sut.BulkCopyTimeout, options.BulkCopyTimeout);
        Assert.Equal(sut.ValidationFailureBehavior, options.ValidationFailureBehavior);
        Assert.Equal(sut.PreAction, options.PreAction);
        Assert.Equal(sut.PostAction, options.PostAction);
        Assert.Equal(SqlBulkCopyOptions.Default, options.BulkCopyOptions);
    }



    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_when_BatchSize_on_the_record_is_below_one_throws(int batchSize)
    {
        using var connection = new SqlConnection();
        var options = new SqlBulkCopyLoaderOptions<TestRecord> { BatchSize = batchSize };

        Assert.Throws<ArgumentOutOfRangeException>(() => new SqlBulkCopyLoader<TestRecord>(connection, options));
    }



    [Fact]
    public void Constructor_when_BulkCopyTimeout_on_the_record_is_negative_throws()
    {
        using var connection = new SqlConnection();
        var options = new SqlBulkCopyLoaderOptions<TestRecord> { BulkCopyTimeout = -1 };

        Assert.Throws<ArgumentOutOfRangeException>(() => new SqlBulkCopyLoader<TestRecord>(connection, options));
    }



    [Fact]
    public void Internal_constructor_applies_the_record_too()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var options = new SqlBulkCopyLoaderOptions<TestRecord> { IsDryRun = true, BatchSize = 7, SkipItemCount = 1 };

        var sut = new SqlBulkCopyLoader<TestRecord>(factory, timer: null, options: options);

        Assert.True(sut.IsDryRun);
        Assert.Equal(7, sut.BatchSize);
        Assert.Equal(1, sut.SkipItemCount);
    }



    [Fact]
    public void Overload_resolution_positional_shapes_still_bind_as_before()
    {
        // Compile-time guards: the shipped (connection, logger) overload keeps winning a two-argument
        // call with a positional null (all arguments supplied beats default substitution), and the
        // three-argument null shape reaches the options constructor because null is not convertible
        // to the SqlBulkCopyOptions enum of the older four-parameter overload.
        using var connection = new SqlConnection();

        var viaLogger = new SqlBulkCopyLoader<TestRecord>(connection, null);
        var viaOptions = new SqlBulkCopyLoader<TestRecord>(connection, null, null);
        var viaTransaction = new SqlBulkCopyLoader<TestRecord>(connection, transaction: null);

        Assert.NotNull(viaLogger);
        Assert.NotNull(viaOptions);
        Assert.NotNull(viaTransaction);
    }
}
