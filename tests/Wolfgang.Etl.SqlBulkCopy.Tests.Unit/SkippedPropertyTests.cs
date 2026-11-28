using System;
using System.Collections.Generic;
#if NET5_0_OR_GREATER
using System.Linq;
#endif
using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;
// ReSharper disable once RedundantUsingDirective -- required on older TFMs
// (net5.0/net6.0) that lack xunit's global usings; InspectCode is TFM-blind.
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// #601: a public readable, non-<c>[NotMapped]</c> property whose type is not a supported
/// column type is still skipped, but no longer silently: the map records its name and the
/// loader logs one warning per record type.
/// </summary>
public class SkippedPropertyTests
{
    private const string WarningMarker = "not a supported column type";



    private static async IAsyncEnumerable<T> ToAsyncEnumerableAsync<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }



    private static bool IsSkipWarning(string entry)
    {
        return entry.IndexOf(WarningMarker, StringComparison.Ordinal) >= 0;
    }



    [Fact]
    public void Create_when_a_property_type_is_unsupported_lists_it_in_SkippedPropertyNames()
    {
        var map = TypeMap.Create(typeof(PlainSkippedPropertyFixture));

        Assert.Equal
        (
            new[] { "Counter", "Link", "Numbers" },
            map.SkippedPropertyNames
        );
    }



    [Fact]
    public void Create_when_every_property_maps_returns_empty_SkippedPropertyNames()
    {
        var map = TypeMap.Create(typeof(TestRecord));

        Assert.Empty(map.SkippedPropertyNames);
    }



    [Fact]
    public void Create_when_type_is_NotMapped_returns_empty_SkippedPropertyNames()
    {
        var map = TypeMap.Create(typeof(NotMappedContainerWithScalar));

        Assert.Empty(map.SkippedPropertyNames);
    }



    [Fact]
    public void Create_records_the_record_type()
    {
        var map = TypeMap.Create(typeof(PlainSkippedPropertyFixture));

        Assert.Equal(typeof(PlainSkippedPropertyFixture), map.RecordType);
    }



    [Fact]
    public void GeneratedTypeDescriptor_four_argument_constructor_has_no_skipped_properties()
    {
        var descriptor = new GeneratedTypeDescriptor
        (
            schemaName: null,
            tableName: "T",
            Array.Empty<GeneratedColumnDescriptor>(),
            Array.Empty<GeneratedNestedTableDescriptor>()
        );

        Assert.Empty(descriptor.SkippedPropertyNames);
    }



    [Fact]
    public void GeneratedTypeDescriptor_when_skippedPropertyNames_is_null_throws_ArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>
        (
            () => new GeneratedTypeDescriptor
            (
                schemaName: null,
                tableName: "T",
                Array.Empty<GeneratedColumnDescriptor>(),
                Array.Empty<GeneratedNestedTableDescriptor>(),
                skippedPropertyNames: null!
            )
        );

        Assert.Equal("skippedPropertyNames", ex.ParamName);
    }



    [Fact]
    public void GeneratedTypeDescriptor_keeps_the_supplied_skipped_property_names()
    {
        var names = new[] { "A", "B" };

        var descriptor = new GeneratedTypeDescriptor
        (
            schemaName: null,
            tableName: "T",
            Array.Empty<GeneratedColumnDescriptor>(),
            Array.Empty<GeneratedNestedTableDescriptor>(),
            names
        );

        Assert.Same(names, descriptor.SkippedPropertyNames);
    }



#if NET5_0_OR_GREATER
    [Fact]
    public void Generator_emits_the_skipped_property_names_in_the_descriptor()
    {
        Assert.True(GeneratedTypeMapRegistry.TryGet(typeof(BulkCopyableSkippedPropertyFixture), out var descriptor));

        Assert.Equal
        (
            new[] { "Counter", "Link", "Numbers" },
            descriptor.SkippedPropertyNames
        );
    }



    [Fact]
    public void Generated_descriptor_skipped_names_match_reflection()
    {
        var generated = TypeMap.Create(typeof(BulkCopyableSkippedPropertyFixture));
        var reflection = TypeMap.Create(typeof(PlainSkippedPropertyFixture));

        Assert.Equal(reflection.SkippedPropertyNames, generated.SkippedPropertyNames);
        Assert.Equal
        (
            reflection.Columns.Select(c => c.ColumnName),
            generated.Columns.Select(c => c.ColumnName)
        );
    }



    [Fact]
    public void Generator_emits_no_skipped_names_for_a_fully_mapped_type()
    {
        Assert.True(GeneratedTypeMapRegistry.TryGet(typeof(BulkCopyableFixture), out var descriptor));

        Assert.Empty(descriptor.SkippedPropertyNames);
    }
#endif



    [Fact]
    public async Task LoadAsync_when_a_property_is_skipped_logs_one_warning_naming_it()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var logger = new RecordingLogger();
        var sut = new SqlBulkCopyLoader<PlainSkippedPropertyFixture>(factory, new ManualProgressTimer(), logger: logger);

        await sut.LoadAsync(ToAsyncEnumerableAsync(new[] { new PlainSkippedPropertyFixture { Id = 1 } }));

        var warning = Assert.Single(logger.Entries, IsSkipWarning);
        Assert.Equal
        (
            "Type 'PlainSkippedPropertyFixture' has properties that are not loaded because their type is not a " +
            "supported column type: Counter, Link, Numbers. Mark them [NotMapped] to exclude them explicitly.",
            warning
        );
    }



    [Fact]
    public async Task LoadAsync_when_no_property_is_skipped_logs_no_warning()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var logger = new RecordingLogger();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, new ManualProgressTimer(), logger: logger);

        await sut.LoadAsync(ToAsyncEnumerableAsync(new[] { new TestRecord { Id = 1, Name = "a" } }));

        Assert.DoesNotContain(logger.Entries, IsSkipWarning);
    }



    [Fact]
    public async Task LoadAsync_when_a_nested_child_type_skips_a_property_logs_it_once()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var logger = new RecordingLogger();
        var sut = new SqlBulkCopyLoader<SkippedPropertyParent>(factory, new ManualProgressTimer(), logger: logger);

        await sut.LoadAsync(ToAsyncEnumerableAsync(new[] { new SkippedPropertyParent { Id = 1 } }));

        var warning = Assert.Single(logger.Entries, IsSkipWarning);
        Assert.Contains("'SkippedPropertyChild'", warning, StringComparison.Ordinal);
        Assert.Contains(": Big.", warning, StringComparison.Ordinal);
    }
}
