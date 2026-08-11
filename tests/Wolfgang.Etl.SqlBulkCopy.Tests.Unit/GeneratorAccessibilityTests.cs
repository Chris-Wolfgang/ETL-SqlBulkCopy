using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Guards the generator's internal consistency invariant: if a type receives a
/// generated <see cref="GeneratedTypeDescriptor"/>, then every column in that
/// descriptor must have a matching getter registered in
/// <see cref="GeneratedAccessorRegistry"/>.
/// </summary>
/// <remarks>
/// Breaking the invariant is not a subtle mapping difference — it is a hard
/// failure on first load: <c>ColumnMap</c>'s descriptor constructor throws
/// <see cref="System.InvalidOperationException"/> ("No source-generated getter is
/// registered ... This indicates a source-generator defect"). A property whose
/// getter is <c>private</c> or <c>protected</c> used to hit exactly that path,
/// because the descriptor filter checked only mappability while the accessor
/// filter additionally required reachable accessibility. Refs #47.
/// </remarks>
public class GeneratorAccessibilityTests
{
#if NET5_0_OR_GREATER
    [Fact]
    public void Type_with_a_non_public_getter_does_not_get_a_generated_descriptor()
    {
        // The generator cannot emit an accessor for a private getter, so it must
        // decline to emit a descriptor for the whole type and let the reflection
        // path build the map instead.
        var hasDescriptor = GeneratedTypeMapRegistry.TryGet(typeof(NonPublicGetterProbe), out _);

        Assert.False
        (
            hasDescriptor,
            "A type with a property the generator cannot emit an accessor for must fall back " +
            "to the reflection map; emitting a descriptor would reference a getter that is " +
            "never registered and throw on first load."
        );
    }



    [Fact]
    public void Type_with_a_non_public_nested_collection_getter_does_not_get_a_generated_descriptor()
    {
        // The nested-table sibling of the scalar-column case above: a descriptor's
        // nested entry needs a registered getter for the COLLECTION property, and
        // NestedTableMap throws the same "source-generator defect" error as
        // ColumnMap when one is missing.
        var hasDescriptor = GeneratedTypeMapRegistry.TryGet(typeof(NonPublicNestedGetterProbe), out _);

        Assert.False
        (
            hasDescriptor,
            "A nested collection property the generator cannot emit an accessor for must force " +
            "the whole type back to the reflection map."
        );
    }



    [Fact]
    public void Every_generated_descriptor_column_has_a_registered_getter()
    {
        // The invariant itself, asserted over the fixtures that DO get a descriptor.
        foreach (var type in new[] { typeof(BulkCopyableFixture), typeof(BulkCopyableEnumFixture) })
        {
            // Assert rather than `continue`: these fixtures are expected to have
            // descriptors, so a silent skip here would let a descriptor-generation
            // regression pass the invariant check unnoticed.
            Assert.True
            (
                GeneratedTypeMapRegistry.TryGet(type, out var descriptor),
                $"Expected a generated descriptor for '{type.Name}'; without one this test would " +
                "vacuously pass and stop guarding the column/accessor invariant."
            );

            foreach (var column in descriptor.Columns)
            {
                var registered = GeneratedAccessorRegistry.TryGetGetter(type, column.PropertyName, out _);

                Assert.True
                (
                    registered,
                    $"Descriptor for '{type.Name}' declares column '{column.PropertyName}' but no " +
                    "generated getter is registered for it — ColumnMap would throw on first load."
                );
            }
        }
    }



    [Fact]
    public void Types_whose_names_collide_under_naive_mangling_are_generated_independently()
    {
        // MangleProbe_A.B and MangleProbe.A_B both collapse to the same identifier
        // when every non-alphanumeric character becomes '_'. Without a
        // disambiguating suffix the generator emits duplicate hint names and
        // duplicate class names, and this assembly does not compile at all — so
        // reaching this assertion already proves the collision is resolved. The
        // assertion additionally pins that BOTH types got their own accessors.
        var firstRegistered = GeneratedAccessorRegistry.TryGetGetter
        (
            typeof(TestModels.MangleProbe_A.B),
            nameof(TestModels.MangleProbe_A.B.Id),
            out _
        );

        var secondRegistered = GeneratedAccessorRegistry.TryGetGetter
        (
            typeof(TestModels.MangleProbe.A_B),
            nameof(TestModels.MangleProbe.A_B.Id),
            out _
        );

        Assert.True(firstRegistered, "MangleProbe_A.B should have a generated getter.");
        Assert.True(secondRegistered, "MangleProbe.A_B should have a generated getter.");
    }
#endif
}
