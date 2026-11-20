using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Fourth batch of mutation-hardening tests (#414). Each test pins behaviour that a surviving
/// Stryker mutant of the current code left unobserved: log-only messages, empty-batch writes,
/// per-load state reset, custom-action dispatch and the exact guard-clause exceptions.
/// </summary>
public class MutationHardeningTests4
{
    private static async IAsyncEnumerable<T> ToAsyncEnumerableAsync<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }



    private static IReadOnlyList<TestRecord> CreateItems(int count)
    {
        return Enumerable.Range(1, count)
            .Select(i => new TestRecord { Id = i, Name = $"Item{i}", Amount = i * 10m })
            .ToList();
    }



    private static SqlBulkCopyLoader<TestRecord> CreateSut
    (
        FakeSqlBulkCopyWrapperFactory factory,
        RecordingLogger? logger = null,
        FakeSqlCommandExecutor? executor = null,
        SqlBulkCopyLoaderOptions<TestRecord>? options = null
    )
    {
        return new SqlBulkCopyLoader<TestRecord>
        (
            factory,
            new ManualProgressTimer(),
            commandExecutor: executor,
            logger: logger,
            options: options
        );
    }



    // --- Log messages (statement mutants that drop the logging call) ---

    [Fact]
    public async Task LoadAsync_logs_the_starting_operation_message()
    {
        var logger = new RecordingLogger();
        var sut = CreateSut(new FakeSqlBulkCopyWrapperFactory(), logger);

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateItems(1)));

        Assert.Contains("Starting SQL bulk copy loading of TestRecord.", logger.Entries);
    }



    [Fact]
    public async Task LoadAsync_logs_the_completed_message_with_totals()
    {
        var logger = new RecordingLogger();
        var sut = CreateSut
        (
            new FakeSqlBulkCopyWrapperFactory(),
            logger,
            options: new SqlBulkCopyLoaderOptions<TestRecord> { SkipItemCount = 1 }
        );

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateItems(4)));

        Assert.Contains("Bulk copy completed. 3 items loaded, 1 items skipped.", logger.Entries);
    }



    [Fact]
    public async Task LoadAsync_logs_the_pre_action_name_before_running_it()
    {
        var logger = new RecordingLogger();
        var executor = new FakeSqlCommandExecutor();
        var sut = CreateSut
        (
            new FakeSqlBulkCopyWrapperFactory(),
            logger,
            executor,
            new SqlBulkCopyLoaderOptions<TestRecord> { PreAction = PreAction.DeleteAllRecords }
        );

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateItems(1)));

        Assert.Contains("Executing pre-action: DeleteAllRecords.", logger.Entries);
        Assert.Single(executor.ExecutedCommands);
    }



    [Fact]
    public async Task LoadAsync_does_not_log_a_pre_or_post_action_when_both_are_None()
    {
        var logger = new RecordingLogger();
        var sut = CreateSut(new FakeSqlBulkCopyWrapperFactory(), logger);

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateItems(1)));

        Assert.DoesNotContain(logger.Entries, e => e.StartsWith("Executing pre-action", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Entries, e => e.StartsWith("Executing post-action", StringComparison.Ordinal));
    }



    [Fact]
    public async Task LoadAsync_when_a_nested_child_fails_validation_logs_the_nested_table_message()
    {
        var logger = new RecordingLogger();
        var sut = new SqlBulkCopyLoader<ParentWithValidatableChildren>
        (
            new FakeSqlBulkCopyWrapperFactory(),
            new ManualProgressTimer(),
            logger: logger,
            options: new SqlBulkCopyLoaderOptions<ParentWithValidatableChildren>
            {
                EnableDataValidation = true,
                ValidationFailureBehavior = ValidationFailureBehavior.Skip
            }
        );

        var items = new[]
        {
            new ParentWithValidatableChildren
            {
                Id = 1,
                Children = [new ValidatableChild { Id = 10, Name = "", Quantity = 5 }] // Required fails
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Contains
        (
            logger.Entries,
            e => e.StartsWith("Validation failed for nested-table '", StringComparison.Ordinal)
        );
    }



    // --- Empty batches are never written ---

    [Fact]
    public async Task LoadAsync_when_the_source_is_empty_writes_no_batch()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory);

        await sut.LoadAsync(ToAsyncEnumerableAsync(Array.Empty<TestRecord>()));

        Assert.Empty(factory.CreatedWrappers);
    }



    [Fact]
    public async Task LoadAsync_when_every_item_is_skipped_writes_no_batch()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = CreateSut(factory, options: new SqlBulkCopyLoaderOptions<TestRecord> { SkipItemCount = 5 });

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateItems(3)));

        Assert.Empty(factory.CreatedWrappers);
    }



    [Fact]
    public async Task LoadAsync_when_parents_have_no_children_writes_no_child_batch()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<ParentWithValidatableChildren>
        (
            factory,
            new ManualProgressTimer(),
            logger: null
        );

        await sut.LoadAsync
        (
            ToAsyncEnumerableAsync(new[] { new ParentWithValidatableChildren { Id = 1, Children = [] } })
        );

        Assert.Single(factory.CreatedWrappers);
    }



    // --- Per-load state ---

    [Fact]
    public async Task LoadAsync_when_the_loader_is_reused_numbers_batches_from_one_again()
    {
        var logger = new RecordingLogger();
        var sut = CreateSut
        (
            new FakeSqlBulkCopyWrapperFactory(),
            logger,
            options: new SqlBulkCopyLoaderOptions<TestRecord> { BatchSize = 2 }
        );

        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateItems(4)));
        logger.Entries.Clear();
        await sut.LoadAsync(ToAsyncEnumerableAsync(CreateItems(2)));

        Assert.Contains("Batch 1 written with 2 items.", logger.Entries);
        Assert.DoesNotContain(logger.Entries, e => e.StartsWith("Batch 3 ", StringComparison.Ordinal));
    }



    // --- Guard clauses: exact parameter and message ---

    // A property with only a setter cannot be written in C# without tripping analyzers, so the
    // fixture type is emitted at run time. The indexer comes from a BCL type.
    private static PropertyInfo CreateSetOnlyProperty()
    {
        var assembly = AssemblyBuilder.DefineDynamicAssembly
        (
            new AssemblyName("SetOnlyFixture"),
            AssemblyBuilderAccess.Run
        );
        var type = assembly
            .DefineDynamicModule("SetOnlyFixture")
            .DefineType("SetOnlySample", TypeAttributes.Public);
        var field = type.DefineField("_value", typeof(string), FieldAttributes.Private);
        var property = type.DefineProperty("SetOnly", PropertyAttributes.None, typeof(string), null);
        var setter = type.DefineMethod
        (
            "set_SetOnly",
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            null,
            new[] { typeof(string) }
        );
        var il = setter.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Stfld, field);
        il.Emit(OpCodes.Ret);
        property.SetSetMethod(setter);

        return type.CreateType()!.GetProperty("SetOnly")!;
    }



    [Fact]
    public void CompilePropertyGetter_when_property_has_no_getter_names_the_property_and_parameter()
    {
        var ex = Assert.Throws<ArgumentException>
        (
            () => ReflectionHelpers.CompilePropertyGetter(CreateSetOnlyProperty())
        );

        Assert.Equal("propertyInfo", ex.ParamName);
        Assert.Contains("'SetOnly' has no readable getter", ex.Message, StringComparison.Ordinal);
    }



    [Fact]
    public void CompilePropertyGetter_when_property_is_an_indexer_names_the_property_and_parameter()
    {
        var ex = Assert.Throws<ArgumentException>
        (
            () => ReflectionHelpers.CompilePropertyGetter(typeof(List<int>).GetProperty("Item")!)
        );

        Assert.Equal("propertyInfo", ex.ParamName);
        Assert.Contains("'Item' is an indexer", ex.Message, StringComparison.Ordinal);
    }



    [Fact]
    public void CompileEnumToUnderlyingConverter_when_type_is_not_an_enum_names_the_type_and_parameter()
    {
        var ex = Assert.Throws<ArgumentException>
        (
            () => ReflectionHelpers.CompileEnumToUnderlyingConverter(typeof(int))
        );

        Assert.Equal("enumType", ex.ParamName);
        Assert.Contains("'Int32' is not an enum", ex.Message, StringComparison.Ordinal);
    }
}
