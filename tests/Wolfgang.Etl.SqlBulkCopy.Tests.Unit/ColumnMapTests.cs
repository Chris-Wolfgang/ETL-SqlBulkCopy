using System;
using System.Reflection;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class ColumnMapTests
{
    [Fact]
    public void Constructor_when_propertyInfo_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new ColumnMap(null!, 0)
        );
    }



    [Fact]
    public void PropertyName_returns_property_name()
    {
        var property = typeof(TestRecord).GetProperty(nameof(TestRecord.Id))!;

        var sut = new ColumnMap(property, 0);

        Assert.Equal("Id", sut.PropertyName);
    }



    [Fact]
    public void ColumnName_when_no_ColumnAttribute_returns_property_name()
    {
        var property = typeof(TestRecord).GetProperty(nameof(TestRecord.Id))!;

        var sut = new ColumnMap(property, 0);

        Assert.Equal("Id", sut.ColumnName);
    }



    [Fact]
    public void ColumnName_when_ColumnAttribute_present_returns_attribute_name()
    {
        var property = typeof(TestRecord).GetProperty(nameof(TestRecord.Name))!;

        var sut = new ColumnMap(property, 0);

        Assert.Equal("FullName", sut.ColumnName);
    }



    [Fact]
    public void ClrType_returns_underlying_type_for_non_nullable()
    {
        var property = typeof(TestRecord).GetProperty(nameof(TestRecord.Id))!;

        var sut = new ColumnMap(property, 0);

        Assert.Equal(typeof(int), sut.ClrType);
    }



    [Fact]
    public void ClrType_returns_underlying_type_for_nullable()
    {
        var property = typeof(NullablePropertiesRecord).GetProperty(nameof(NullablePropertiesRecord.NullableInt))!;

        var sut = new ColumnMap(property, 0);

        Assert.Equal(typeof(int), sut.ClrType);
    }



    [Fact]
    public void IsNullable_when_value_type_returns_false()
    {
        var property = typeof(TestRecord).GetProperty(nameof(TestRecord.Id))!;

        var sut = new ColumnMap(property, 0);

        Assert.False(sut.IsNullable);
    }



    [Fact]
    public void IsNullable_when_nullable_value_type_returns_true()
    {
        var property = typeof(NullablePropertiesRecord).GetProperty(nameof(NullablePropertiesRecord.NullableInt))!;

        var sut = new ColumnMap(property, 0);

        Assert.True(sut.IsNullable);
    }



    [Fact]
    public void IsNullable_when_reference_type_returns_true()
    {
        var property = typeof(TestRecord).GetProperty(nameof(TestRecord.Name))!;

        var sut = new ColumnMap(property, 0);

        Assert.True(sut.IsNullable);
    }



    [Fact]
    public void Ordinal_returns_assigned_ordinal()
    {
        var property = typeof(TestRecord).GetProperty(nameof(TestRecord.Id))!;

        var sut = new ColumnMap(property, 7);

        Assert.Equal(7, sut.Ordinal);
    }



    [Fact]
    public void GetValue_returns_property_value()
    {
        var property = typeof(TestRecord).GetProperty(nameof(TestRecord.Id))!;
        var sut = new ColumnMap(property, 0);
        var record = new TestRecord { Id = 42 };

        var result = sut.GetValue(record);

        Assert.Equal(42, result);
    }



    [Fact]
    public void GetValue_when_null_value_returns_null()
    {
        var property = typeof(NullablePropertiesRecord).GetProperty(nameof(NullablePropertiesRecord.NullableString))!;
        var sut = new ColumnMap(property, 0);
        var record = new NullablePropertiesRecord { Id = 1, NullableString = null };

        var result = sut.GetValue(record);

        Assert.Null(result);
    }
}
