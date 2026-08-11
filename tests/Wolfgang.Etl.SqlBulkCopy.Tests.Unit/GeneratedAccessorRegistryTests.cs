using System;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

[Collection(TestCollections.GeneratedAccessorRegistry)]
public class GeneratedAccessorRegistryTests
{
    [Fact]
    public void ColumnMap_when_generated_getter_is_registered_prefers_it_over_reflection()
    {
        var property = typeof(GeneratedAccessorProbeRecord).GetProperty(nameof(GeneratedAccessorProbeRecord.Registered))!;

        // A sentinel getter that ignores the instance and returns a value the
        // real property could never produce. If ColumnMap read it back, the
        // generated-accessor path was taken instead of the reflection getter.
        GeneratedAccessorRegistry.Register
        (
            typeof(GeneratedAccessorProbeRecord),
            nameof(GeneratedAccessorProbeRecord.Registered),
            _ => 999
        );

        var columnMap = new ColumnMap(property, ordinal: 0);
        var instance = new GeneratedAccessorProbeRecord { Registered = 7 };

        Assert.Equal(999, columnMap.GetValue(instance));
    }



    [Fact]
    public void ColumnMap_when_no_generated_getter_is_registered_falls_back_to_reflection()
    {
        var property = typeof(GeneratedAccessorProbeRecord).GetProperty(nameof(GeneratedAccessorProbeRecord.Unregistered))!;

        // No registration for this property — the reflection-compiled getter
        // must read the actual property value.
        var columnMap = new ColumnMap(property, ordinal: 0);
        var instance = new GeneratedAccessorProbeRecord { Unregistered = 42 };

        Assert.Equal(42, columnMap.GetValue(instance));
    }



    [Fact]
    public void Register_when_declaringType_is_null_throws_ArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>
        (
            () => GeneratedAccessorRegistry.Register(null!, "Prop", _ => null)
        );

        Assert.Equal("declaringType", ex.ParamName);
    }



    [Fact]
    public void Register_when_propertyName_is_null_throws_ArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>
        (
            () => GeneratedAccessorRegistry.Register(typeof(GeneratedAccessorProbeRecord), null!, _ => null)
        );

        Assert.Equal("propertyName", ex.ParamName);
    }



    [Fact]
    public void Register_when_getter_is_null_throws_ArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>
        (
            () => GeneratedAccessorRegistry.Register(typeof(GeneratedAccessorProbeRecord), "Prop", null!)
        );

        Assert.Equal("getter", ex.ParamName);
    }



    [Fact]
    public void RegisterEnumConverter_when_enumType_is_null_throws_ArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>
        (
            () => GeneratedAccessorRegistry.RegisterEnumConverter(null!, _ => 0)
        );

        Assert.Equal("enumType", ex.ParamName);
    }



    [Fact]
    public void RegisterEnumConverter_when_converter_is_null_throws_ArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>
        (
            () => GeneratedAccessorRegistry.RegisterEnumConverter(typeof(Status), null!)
        );

        Assert.Equal("converter", ex.ParamName);
    }
}
