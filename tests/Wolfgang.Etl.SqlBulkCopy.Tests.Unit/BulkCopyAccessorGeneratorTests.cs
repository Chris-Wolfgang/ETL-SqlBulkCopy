using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// End-to-end coverage for the source generator: the generator runs over this
/// test assembly (referenced as an analyzer in the csproj), so a
/// <see cref="BulkCopyableFixture"/> marked <c>[BulkCopyable]</c> should have
/// its accessors registered in <see cref="GeneratedAccessorRegistry"/> by the
/// generated module initializer.
/// </summary>
/// <remarks>
/// The generated registration is emitted under <c>#if NET5_0_OR_GREATER</c>
/// because it relies on module initializers, so these assertions are scoped to
/// net5.0+. On earlier target frameworks the type falls back to the
/// runtime-compiled getter, which is covered by <c>GeneratedAccessorRegistryTests</c>.
/// </remarks>
public class BulkCopyAccessorGeneratorTests
{
#if NET5_0_OR_GREATER
    [Fact]
    public void Generator_registers_a_getter_for_each_mappable_property()
    {
        var idRegistered = GeneratedAccessorRegistry.TryGetGetter
        (
            typeof(BulkCopyableFixture),
            nameof(BulkCopyableFixture.Id),
            out _
        );

        var nameRegistered = GeneratedAccessorRegistry.TryGetGetter
        (
            typeof(BulkCopyableFixture),
            nameof(BulkCopyableFixture.Name),
            out _
        );

        Assert.True(idRegistered);
        Assert.True(nameRegistered);
    }



    [Fact]
    public void Generated_getter_reads_the_actual_property_value()
    {
        GeneratedAccessorRegistry.TryGetGetter
        (
            typeof(BulkCopyableFixture),
            nameof(BulkCopyableFixture.Name),
            out var getter
        );

        var value = getter(new BulkCopyableFixture { Name = "Ada" });

        Assert.Equal("Ada", value);
    }



    [Fact]
    public void ColumnMap_uses_the_generated_getter_for_a_BulkCopyable_type()
    {
        var property = typeof(BulkCopyableFixture).GetProperty(nameof(BulkCopyableFixture.Id))!;

        var columnMap = new ColumnMap(property, ordinal: 0);
        var value = columnMap.GetValue(new BulkCopyableFixture { Id = 123 });

        // The generated getter and the reflection fallback return the same
        // value; this asserts the read path is correct end-to-end. That the
        // generated path was taken is proven by
        // Generator_registers_a_getter_for_each_mappable_property.
        Assert.Equal(123, value);
    }
#endif
}
