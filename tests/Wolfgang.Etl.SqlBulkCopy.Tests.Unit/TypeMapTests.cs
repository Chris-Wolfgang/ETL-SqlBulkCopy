using System;
using System.Linq;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class TypeMapTests
{
    [Fact]
    public void Create_when_type_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => TypeMap.Create(null!)
        );
    }



    [Fact]
    public void Create_when_Table_attribute_present_uses_attribute_name()
    {
        var map = TypeMap.Create(typeof(TestRecord));

        Assert.Equal("TestRecords", map.TableName);
    }



    [Fact]
    public void Create_when_Table_attribute_has_schema_uses_attribute_schema()
    {
        var map = TypeMap.Create(typeof(TestRecord));

        Assert.Equal("dbo", map.SchemaName);
    }



    [Fact]
    public void Create_when_no_Table_attribute_uses_type_name()
    {
        var map = TypeMap.Create(typeof(SimpleRecord));

        Assert.Equal("SimpleRecord", map.TableName);
    }



    [Fact]
    public void Create_when_tableName_override_provided_uses_override()
    {
        var map = TypeMap.Create(typeof(TestRecord), tableName: "OverriddenTable");

        Assert.Equal("OverriddenTable", map.TableName);
    }



    [Fact]
    public void Create_when_schemaName_override_provided_uses_override()
    {
        var map = TypeMap.Create(typeof(TestRecord), schemaName: "custom");

        Assert.Equal("custom", map.SchemaName);
    }



    [Fact]
    public void Create_excludes_NotMapped_properties()
    {
        var map = TypeMap.Create(typeof(TestRecord));

        Assert.DoesNotContain
        (
            map.Columns,
            c => c.PropertyName == "Ignored"
        );
    }



    [Fact]
    public void Create_maps_Column_attribute_to_column_name()
    {
        var map = TypeMap.Create(typeof(TestRecord));

        var nameColumn = map.Columns.Single(c => c.PropertyName == "Name");
        Assert.Equal("FullName", nameColumn.ColumnName);
    }



    [Fact]
    public void Create_assigns_sequential_ordinals()
    {
        var map = TypeMap.Create(typeof(TestRecord));

        for (var i = 0; i < map.Columns.Count; i++)
        {
            Assert.Equal(i, map.Columns[i].Ordinal);
        }
    }



    [Fact]
    public void IsMappedToTable_when_type_is_mapped_returns_true()
    {
        var map = TypeMap.Create(typeof(TestRecord));

        Assert.True(map.IsMappedToTable);
    }



    [Fact]
    public void QualifiedTableName_when_schema_present_returns_bracketed_format()
    {
        var map = TypeMap.Create(typeof(TestRecord));

        Assert.Equal("[dbo].[TestRecords]", map.QualifiedTableName);
    }



    [Fact]
    public void QualifiedTableName_when_no_schema_returns_table_only()
    {
        var map = TypeMap.Create(typeof(SimpleRecord));

        Assert.Equal("[SimpleRecord]", map.QualifiedTableName);
    }



    [Fact]
    public void Create_when_type_has_both_Table_and_NotMapped_throws()
    {
        Assert.Throws<InvalidOperationException>
        (
            () => TypeMap.Create(typeof(InvalidDualAttributeRecord))
        );
    }



    [Fact]
    public void Create_when_property_has_both_NotMapped_and_Column_throws()
    {
        Assert.Throws<InvalidOperationException>
        (
            () => TypeMap.Create(typeof(InvalidPropertyDualAttributeRecord))
        );
    }



    [Fact]
    public void Create_detects_nested_table_mappings()
    {
        var map = TypeMap.Create(typeof(ParentRecord));

        Assert.Single(map.NestedTables);
        Assert.Equal("Children", map.NestedTables[0].PropertyName);
    }



    [Fact]
    public void Create_nested_table_child_type_map_is_correct()
    {
        var map = TypeMap.Create(typeof(ParentRecord));

        var childMap = map.NestedTables[0].ChildTypeMap;
        Assert.Equal("ChildRecords", childMap.TableName);
        Assert.Equal(2, childMap.Columns.Count);
    }



    [Fact]
    public void Create_when_nullable_properties_maps_underlying_type()
    {
        var map = TypeMap.Create(typeof(NullablePropertiesRecord));

        var nullableIntCol = map.Columns.Single(c => c.PropertyName == "NullableInt");
        Assert.Equal(typeof(int), nullableIntCol.ClrType);
        Assert.True(nullableIntCol.IsNullable);
    }
}
