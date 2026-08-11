using System;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

[Collection(TestCollections.GeneratedAccessorRegistry)]
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



    [Fact]
    public void Constructor_descriptor_when_no_generated_getter_registered_throws_InvalidOperationException()
    {
        // The descriptor-based ctor requires a generated getter for the
        // (declaringType, propertyName) pair; a pair that was never registered
        // is a source-generator defect and must fail loudly.
        var ex = Assert.Throws<InvalidOperationException>
        (
            () => new ColumnMap
            (
                typeof(object),
                "__no_such_generated_getter__",
                "Col",
                typeof(int),
                isNullable: false,
                ordinal: 0
            )
        );

        Assert.Contains("No source-generated getter", ex.Message, StringComparison.Ordinal);
    }



    [Fact]
    public void Constructor_descriptor_when_enum_clrType_has_no_generated_converter_throws_InvalidOperationException()
    {
        // Register a getter so the ctor clears the getter check and reaches the
        // enum-converter check; the enum itself has no registered converter.
        GeneratedAccessorRegistry.Register
        (
            typeof(GeneratedAccessorProbeRecord),
            "__enum_probe__",
            _ => UnregisteredProbeEnum.One
        );

        var ex = Assert.Throws<InvalidOperationException>
        (
            () => new ColumnMap
            (
                typeof(GeneratedAccessorProbeRecord),
                "__enum_probe__",
                "Col",
                typeof(UnregisteredProbeEnum),
                isNullable: false,
                ordinal: 0
            )
        );

        Assert.Contains("No source-generated enum converter", ex.Message, StringComparison.Ordinal);
    }



    [Fact]
    public void Constructor_reflection_for_inherited_property_uses_generated_getter_under_mapped_type()
    {
        // An inherited property's DeclaringType is the *base* class, but the
        // generator registers getters under the mapped (derived) type. ColumnMap
        // must look up by the mapped type so inherited [BulkCopyable] properties
        // still use the generated getter instead of the reflection fallback.
        var inheritedProperty = typeof(InheritedGetterDerivedProbe)
            .GetProperty(nameof(InheritedGetterBaseProbe.Inherited))!;
        Assert.Equal(typeof(InheritedGetterBaseProbe), inheritedProperty.DeclaringType);

        // Sentinel getter registered under the DERIVED (mapped) type only.
        GeneratedAccessorRegistry.Register
        (
            typeof(InheritedGetterDerivedProbe),
            nameof(InheritedGetterBaseProbe.Inherited),
            _ => 999
        );

        var sut = new ColumnMap(inheritedProperty, ordinal: 0, mappedType: typeof(InheritedGetterDerivedProbe));

        // 999 (the sentinel) proves the mapped-type lookup hit; the real value (7)
        // would mean it missed and fell back to reflection.
        Assert.Equal(999, sut.GetValue(new InheritedGetterDerivedProbe { Inherited = 7 }));
    }



    [Fact]
    public void Constructor_reflection_inherited_property_without_mappedType_uses_ReflectedType()
    {
        // No mappedType passed: the getter is obtained via the derived type, so
        // PropertyInfo.ReflectedType is the derived (mapped) type. CreateGetter
        // must prefer ReflectedType over DeclaringType (the base) to still find
        // the generated getter registered under the derived type.
        var inheritedProperty = typeof(InheritedGetterDerivedProbe)
            .GetProperty(nameof(InheritedGetterBaseProbe.Inherited))!;
        Assert.Same(typeof(InheritedGetterDerivedProbe), inheritedProperty.ReflectedType);

        GeneratedAccessorRegistry.Register
        (
            typeof(InheritedGetterDerivedProbe),
            nameof(InheritedGetterBaseProbe.Inherited),
            _ => 999
        );

        var sut = new ColumnMap(inheritedProperty, ordinal: 0);

        Assert.Equal(999, sut.GetValue(new InheritedGetterDerivedProbe { Inherited = 7 }));
    }
}
