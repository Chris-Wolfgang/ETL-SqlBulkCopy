#if NET5_0_OR_GREATER
using System;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// The descriptor half of the ADR 0006 single-sourcing guard: for a
/// <c>[BulkCopyable]</c> type the generator emits a descriptor that
/// <c>TypeMap.Create</c> uses instead of reflection. These tests assert the
/// descriptor-built map is identical to the reflection-built map of a
/// structurally-identical plain type — so the generated (AOT-clean) path can't
/// diverge from the reflection fallback.
/// </summary>
public class DescriptorConformanceTests
{
    [Theory]
    [InlineData(typeof(BulkCopyableFixture), typeof(PlainFixture))]
    [InlineData(typeof(BulkCopyableEnumFixture), typeof(PlainEnumFixture))]
    [InlineData(typeof(BulkCopyableAttributedFixture), typeof(PlainAttributedFixture))]
    [InlineData(typeof(BulkCopyableParentFixture), typeof(PlainParentFixture))]
    public void Generated_descriptor_columns_match_reflection(Type generatedType, Type reflectionType)
    {
        var generated = TypeMap.Create(generatedType);
        var reflection = TypeMap.Create(reflectionType);

        Assert.Equal(reflection.Columns.Count, generated.Columns.Count);

        for (var i = 0; i < reflection.Columns.Count; i++)
        {
            var expected = reflection.Columns[i];
            var actual = generated.Columns[i];

            Assert.Equal(expected.PropertyName, actual.PropertyName);
            Assert.Equal(expected.ColumnName, actual.ColumnName);
            Assert.Equal(expected.ClrType, actual.ClrType);
            Assert.Equal(expected.IsNullable, actual.IsNullable);
            Assert.Equal(expected.Ordinal, actual.Ordinal);
        }
    }



    [Fact]
    public void Generated_descriptor_resolves_table_and_schema_from_attribute()
    {
        // Both types carry [Table("Widgets", Schema = "dbo")]; the marked one is
        // built from the descriptor, the plain one from reflection.
        var generated = TypeMap.Create(typeof(BulkCopyableAttributedFixture));
        var reflection = TypeMap.Create(typeof(PlainAttributedFixture));

        Assert.Equal(reflection.TableName, generated.TableName);
        Assert.Equal(reflection.SchemaName, generated.SchemaName);
        Assert.Equal("Widgets", generated.TableName);
        Assert.Equal("dbo", generated.SchemaName);
    }



    [Fact]
    public void Generated_descriptor_nested_tables_match_reflection()
    {
        var generated = TypeMap.Create(typeof(BulkCopyableParentFixture));
        var reflection = TypeMap.Create(typeof(PlainParentFixture));

        Assert.Equal(reflection.NestedTables.Count, generated.NestedTables.Count);
        Assert.NotEmpty(generated.NestedTables);

        for (var i = 0; i < reflection.NestedTables.Count; i++)
        {
            var expected = reflection.NestedTables[i];
            var actual = generated.NestedTables[i];

            Assert.Equal(expected.PropertyName, actual.PropertyName);
            Assert.Equal(expected.ChildTypeMap.TableName, actual.ChildTypeMap.TableName);
            Assert.Equal(expected.ChildTypeMap.Columns.Count, actual.ChildTypeMap.Columns.Count);

            for (var j = 0; j < expected.ChildTypeMap.Columns.Count; j++)
            {
                Assert.Equal(expected.ChildTypeMap.Columns[j].ColumnName, actual.ChildTypeMap.Columns[j].ColumnName);
                Assert.Equal(expected.ChildTypeMap.Columns[j].ClrType, actual.ChildTypeMap.Columns[j].ClrType);
            }
        }
    }



    [Fact]
    public void Generated_descriptor_applies_per_load_table_override()
    {
        // Overrides must resolve identically on both paths.
        var generated = TypeMap.Create(typeof(BulkCopyableFixture), schemaName: "custom", tableName: "Overridden");
        var reflection = TypeMap.Create(typeof(PlainFixture), schemaName: "custom", tableName: "Overridden");

        Assert.Equal("Overridden", generated.TableName);
        Assert.Equal("custom", generated.SchemaName);
        Assert.Equal(reflection.TableName, generated.TableName);
        Assert.Equal(reflection.SchemaName, generated.SchemaName);
    }
}
#endif
