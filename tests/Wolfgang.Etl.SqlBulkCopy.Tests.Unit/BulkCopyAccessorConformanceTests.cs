#if NET5_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Reflection;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// The single-sourcing guard from ADR 0006: the source generator and the
/// runtime reflection path each read a type's properties through their own
/// front-end (Roslyn symbols vs <see cref="System.Reflection"/>), and those
/// front-ends cannot share code. These tests assert the two produce identical
/// results for a corpus of <c>[BulkCopyable]</c> types, so the generated fast
/// path can never silently diverge from the reflection fallback.
/// </summary>
/// <remarks>
/// Scoped to net5.0+ because the generated accessors are only emitted there
/// (they register via a module initializer). On earlier target frameworks
/// there is nothing generated to compare against.
/// </remarks>
public class BulkCopyAccessorConformanceTests
{
    private static IEnumerable<(Type Type, object Instance)> Corpus()
    {
        yield return
        (
            typeof(BulkCopyableFixture),
            new BulkCopyableFixture { Id = 42, Name = "Ada" }
        );

        yield return
        (
            typeof(BulkCopyableEnumFixture),
            new BulkCopyableEnumFixture
            {
                Id = 9,
                Priority = GeneratedPriority.High,
                Kind = GeneratedSmallKind.B,
                MaybePriority = GeneratedPriority.Low,
                Label = "widget",
            }
        );
    }



    [Fact]
    public void Generated_getter_matches_reflection_for_every_property()
    {
        foreach (var (type, instance) in Corpus())
        {
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (property.GetMethod is null || property.GetIndexParameters().Length != 0)
                {
                    continue;
                }

                Assert.True
                (
                    GeneratedAccessorRegistry.TryGetGetter(type, property.Name, out var generated),
                    $"No generated getter registered for {type.Name}.{property.Name}."
                );

                var reflection = ReflectionHelpers.CompilePropertyGetter(property);

                Assert.Equal(reflection(instance), generated(instance));
            }
        }
    }



    [Fact]
    public void Generated_enum_converter_matches_reflection()
    {
        var samples = new (Type EnumType, object Value)[]
        {
            (typeof(GeneratedPriority), GeneratedPriority.High),
            (typeof(GeneratedSmallKind), GeneratedSmallKind.B),
        };

        foreach (var (enumType, value) in samples)
        {
            Assert.True
            (
                GeneratedAccessorRegistry.TryGetEnumConverter(enumType, out var generated),
                $"No generated enum converter registered for {enumType.Name}."
            );

            var reflection = ReflectionHelpers.CompileEnumToUnderlyingConverter(enumType);

            Assert.Equal(reflection(value), generated(value));
        }
    }
}
#endif
