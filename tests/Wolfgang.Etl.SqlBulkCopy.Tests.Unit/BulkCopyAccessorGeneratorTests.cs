using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
// ReSharper disable once RedundantUsingDirective -- required on older TFMs
// (net5.0/net6.0) that lack xunit's global usings; InspectCode is TFM-blind.
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
        var registered = GeneratedAccessorRegistry.TryGetGetter
        (
            typeof(BulkCopyableFixture),
            nameof(BulkCopyableFixture.Name),
            out var getter
        );

        Assert.True(registered, "A generated getter should be registered for BulkCopyableFixture.Name; a false result indicates a generator/registration regression (and avoids an NRE on getter(...) below).");

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



    [Fact]
    public void Generated_accessor_types_are_excluded_from_code_coverage()
    {
        var found = 0;

        foreach (var type in typeof(BulkCopyAccessorGeneratorTests).Assembly.GetTypes())
        {
            if (!string.Equals(type.Namespace, "Wolfgang.Etl.SqlBulkCopy.Generated", System.StringComparison.Ordinal)
                || !type.Name.StartsWith("BulkCopyAccessors_", System.StringComparison.Ordinal))
            {
                continue;
            }

            found++;

            Assert.True
            (
                type.IsDefined(typeof(System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute), inherit: false),
                $"{type.FullName} must carry [ExcludeFromCodeCoverage]. Without it, coverage tools instrument the generated thunks like hand-written code - here, and in every consumer assembly that marks a type [BulkCopyable]."
            );
        }

        // Without this the loop above passes vacuously when the generator has not run.
        Assert.True
        (
            found > 0,
            "No generated BulkCopyAccessors_* type was found in this assembly, so the attribute assertion proved nothing."
        );
    }
#endif
}
