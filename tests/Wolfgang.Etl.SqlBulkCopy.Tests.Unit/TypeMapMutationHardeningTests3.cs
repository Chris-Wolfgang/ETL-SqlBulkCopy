using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Pins TypeMap behaviour that the Stryker run left unproven (issue #414): the
/// descriptor path, the NotMapped-type column skip, the nested-table candidate and
/// element-type filters, and the element-type discovery for collection shapes.
/// Each test names the mutant it kills. Line numbers are those of Stryker run
/// 45030074393, which mutated TypeMap.cs at commit 284b16a.
/// </summary>
public class TypeMapMutationHardeningTests3
{
    // Kills TypeMap.cs:207 (block removal of the descriptor branch): without it the
    // reflection path would map the type and the table name would be the type name.
    [Fact]
    public void Create_when_descriptor_is_registered_uses_the_descriptor_table_name()
    {
        // Register what the source generator would emit for a [BulkCopyable] type.
        GeneratedAccessorRegistry.Register
        (
            typeof(DescriptorOnlyProbe),
            nameof(DescriptorOnlyProbe.Id),
            obj => ((DescriptorOnlyProbe)obj).Id
        );
        GeneratedTypeMapRegistry.Register
        (
            typeof(DescriptorOnlyProbe),
            new GeneratedTypeDescriptor
            (
                schemaName: "desc",
                tableName: "DescriptorTable",
                columns: new List<GeneratedColumnDescriptor>
                {
                    new
                    (
                        nameof(DescriptorOnlyProbe.Id),
                        "Id",
                        typeof(int),
                        isNullable: false,
                        ordinal: 0
                    ),
                },
                nestedTables: new List<GeneratedNestedTableDescriptor>()
            )
        );

        var map = TypeMap.Create(typeof(DescriptorOnlyProbe));

        Assert.Equal("[desc].[DescriptorTable]", map.QualifiedTableName);

        // The column reads through the registered generated accessor.
        var column = Assert.Single(map.Columns);
        Assert.Equal(7, column.GetValue(new DescriptorOnlyProbe { Id = 7 }));
    }



    // Kills TypeMap.cs:260 (block removal of the nested-table loop body in
    // BuildFromDescriptor): the array slot would stay null.
    [Fact]
    public void Create_when_descriptor_has_nested_table_builds_the_nested_map()
    {
        // Register what the source generator would emit for a [BulkCopyable] type.
        GeneratedAccessorRegistry.Register
        (
            typeof(DescriptorParentProbe),
            nameof(DescriptorParentProbe.Id),
            obj => ((DescriptorParentProbe)obj).Id
        );
        GeneratedAccessorRegistry.Register
        (
            typeof(DescriptorParentProbe),
            nameof(DescriptorParentProbe.Children),
            obj => ((DescriptorParentProbe)obj).Children
        );
        GeneratedTypeMapRegistry.Register
        (
            typeof(DescriptorParentProbe),
            new GeneratedTypeDescriptor
            (
                schemaName: null,
                tableName: "DescriptorParents",
                columns: new List<GeneratedColumnDescriptor>
                {
                    new
                    (
                        nameof(DescriptorParentProbe.Id),
                        "Id",
                        typeof(int),
                        isNullable: false,
                        ordinal: 0
                    ),
                },
                nestedTables: new List<GeneratedNestedTableDescriptor>
                {
                    new(nameof(DescriptorParentProbe.Children), typeof(ChildRecord)),
                }
            )
        );

        var map = TypeMap.Create(typeof(DescriptorParentProbe));

        var nested = Assert.Single(map.NestedTables);
        Assert.NotNull(nested);
        Assert.Equal(nameof(DescriptorParentProbe.Children), nested.PropertyName);
        Assert.Same(TypeMap.Create(typeof(ChildRecord)), nested.ChildTypeMap);

        // The column and the nested table read through the registered generated accessors.
        var child = new ChildRecord();
        var parent = new DescriptorParentProbe { Id = 3, Children = new List<ChildRecord> { child } };
        Assert.Equal(3, Assert.Single(map.Columns).GetValue(parent));
        Assert.Same(child, Assert.Single(nested.GetValues(parent)));
    }



    // Kills TypeMap.cs:339 (conditional forced true): a NotMapped type must not get
    // columns even when it has unmarked scalar properties.
    [Fact]
    public void Create_when_type_is_NotMapped_maps_no_columns_for_its_scalar_properties()
    {
        var map = TypeMap.Create(typeof(NotMappedParentWithScalar));

        Assert.Empty(map.Columns);
        Assert.Single(map.NestedTables);
    }



    // Kills TypeMap.cs:512 (continue removed) and the four TypeMap.cs:544 logical
    // mutants: a [NotMapped] collection property must not become a nested table.
    [Fact]
    public void Create_when_collection_property_is_NotMapped_maps_no_nested_table()
    {
        var map = TypeMap.Create(typeof(ParentWithNotMappedCollectionProperty));

        Assert.Empty(map.NestedTables);
    }



    // Kills TypeMap.cs:561 (&& to ||): string is a class but a supported column type,
    // so a List<string> must not become a child table.
    [Fact]
    public void Create_when_collection_element_is_string_maps_no_nested_table()
    {
        var map = TypeMap.Create(typeof(ParentWithStringList));

        Assert.Empty(map.NestedTables);
    }



    // Kills TypeMap.cs:604 (&& to ||): an indexer has a getter but takes a parameter,
    // so it must not become a column.
    [Fact]
    public void Create_when_type_has_an_indexer_maps_no_column_for_it()
    {
        var map = TypeMap.Create(typeof(RecordWithIndexer));

        var column = Assert.Single(map.Columns);
        Assert.Equal(nameof(RecordWithIndexer.Id), column.PropertyName);
    }



    // Kills TypeMap.cs:628 and :638 (&& to ||, which call GetGenericTypeDefinition on a
    // non-generic type) and :634 (FirstOrDefault to First, which throws when no
    // IEnumerable<T> is implemented): an ArrayList property is skipped, not fatal.
    [Fact]
    public void Create_when_property_is_a_non_generic_collection_skips_it()
    {
        var map = TypeMap.Create(typeof(ParentWithArrayList));

        Assert.Empty(map.NestedTables);
        Assert.Single(map.Columns);
    }



    // Kills TypeMap.cs:639 (== to !=): the element type must come from IEnumerable<T>,
    // not from the first other generic interface the collection implements.
    [Fact]
    public void Create_when_collection_implements_another_generic_interface_uses_the_IEnumerable_element_type()
    {
        var map = TypeMap.Create(typeof(ParentWithTaggedChildren));

        var nested = Assert.Single(map.NestedTables);
        Assert.Same(TypeMap.Create(typeof(ChildRecord)), nested.ChildTypeMap);
    }



    [ExcludeFromCodeCoverage]
    private sealed class DescriptorOnlyProbe
    {
        public int Id { get; init; }
    }



    [ExcludeFromCodeCoverage]
    private sealed class DescriptorParentProbe
    {
        public int Id { get; init; }

        public List<ChildRecord> Children { get; init; } = new();
    }



    [NotMapped]
    [ExcludeFromCodeCoverage]
    private sealed class NotMappedParentWithScalar
    {
        public int Id { get; init; }

        public List<ChildRecord> Children { get; init; } = new();
    }



    [ExcludeFromCodeCoverage]
    private sealed class ParentWithNotMappedCollectionProperty
    {
        public int Id { get; init; }

        [NotMapped]
        public List<ChildRecord> Children { get; init; } = new();
    }



    [ExcludeFromCodeCoverage]
    private sealed class ParentWithStringList
    {
        public int Id { get; init; }

        public List<string> Tags { get; init; } = new();
    }



    [ExcludeFromCodeCoverage]
    private sealed class RecordWithIndexer
    {
        public int Id { get; init; }

        public string this[int index] => index.ToString(CultureInfo.InvariantCulture);
    }



    [ExcludeFromCodeCoverage]
    private sealed class ParentWithArrayList
    {
        public int Id { get; init; }

        public ArrayList Items { get; init; } = new();
    }



    [ExcludeFromCodeCoverage]
    private sealed class ParentWithTaggedChildren
    {
        public int Id { get; init; }

        public TaggedChildren Children { get; init; } = new();
    }



    // Implements a second generic interface whose type argument (SimpleRecord) differs
    // from the element type, so picking the wrong interface changes the child map.
    [ExcludeFromCodeCoverage]
    private sealed class TaggedChildren : IEnumerable<ChildRecord>, IProgress<SimpleRecord>
    {
        private readonly List<ChildRecord> _items = new();



        public IEnumerator<ChildRecord> GetEnumerator() => _items.GetEnumerator();



        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();



        public void Report(SimpleRecord value)
        {
            // Not used: the interface exists only to give the type a second generic interface.
        }
    }
}
