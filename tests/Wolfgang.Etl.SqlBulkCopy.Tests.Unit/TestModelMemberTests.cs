using System;
using System.Collections.Generic;
using System.Linq;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Most models under <c>TestModels</c> are only ever reflected over: the tests
/// hand <c>typeof(X)</c> to <see cref="TypeMap"/>, the descriptor conformance
/// checks or the generator and assert on the shape, so no test ever runs the
/// models' own members. The test assembly is held to 100% line coverage, so these
/// tests build each such model and read every member back, which also pins the
/// shape the reflection tests rely on (each property is settable and readable
/// with the type the probe expects).
/// </summary>
public class TestModelMemberTests
{
    [Fact]
    public void CircularNode_members_round_trip()
    {
        var child = new CircularNode { Id = 2 };
        var sut = new CircularNode { Id = 1, Children = new List<CircularNode> { child } };

        Assert.Equal(1, sut.Id);
        Assert.Same(child, Assert.Single(sut.Children));
        Assert.Empty(child.Children);
    }



    [Fact]
    public void DescriptorConformance_plain_fixtures_members_round_trip()
    {
        var plain = new PlainFixture { Id = 1, Name = "a" };
        var plainEnum = new PlainEnumFixture
        {
            Id = 2,
            Priority = GeneratedPriority.High,
            Kind = GeneratedSmallKind.B,
            MaybePriority = GeneratedPriority.Low,
            Label = "b"
        };
        var plainAttributed = new PlainAttributedFixture { Id = 3, Name = "c" };
        var plainChild = new PlainChildFixture { ParentId = 4, Value = "d" };
        var plainParent = new PlainParentFixture { Id = 4, Children = new[] { plainChild } };

        Assert.Equal((1, "a"), (plain.Id, plain.Name));
        Assert.Equal
        (
            (2, GeneratedPriority.High, GeneratedSmallKind.B, (GeneratedPriority?)GeneratedPriority.Low, "b"),
            (plainEnum.Id, plainEnum.Priority, plainEnum.Kind, plainEnum.MaybePriority, plainEnum.Label)
        );
        Assert.Equal((3, "c"), (plainAttributed.Id, plainAttributed.Name));
        Assert.Equal((4, "d"), (plainChild.ParentId, plainChild.Value));
        Assert.Equal(4, plainParent.Id);
        Assert.Same(plainChild, Assert.Single(plainParent.Children));
    }



    [Fact]
    public void DescriptorConformance_bulkcopyable_fixtures_members_round_trip()
    {
        var attributed = new BulkCopyableAttributedFixture { Id = 1, Name = "a" };
        var child = new BulkCopyableChildFixture { ParentId = 2, Value = "b" };
        var parent = new BulkCopyableParentFixture { Id = 2, Children = new[] { child } };

        Assert.Equal((1, "a"), (attributed.Id, attributed.Name));
        Assert.Equal((2, "b"), (child.ParentId, child.Value));
        Assert.Equal(2, parent.Id);
        Assert.Same(child, Assert.Single(parent.Children));
    }



    [Fact]
    public void Rejected_shape_records_members_round_trip()
    {
        var duplicate = new DuplicateColumnRecord { First = "a", Second = "b" };
        var dualType = new InvalidDualAttributeRecord { Id = 1 };
        var dualProperty = new InvalidPropertyDualAttributeRecord { Id = 2, Name = "c" };
        var noMappable = new NoMappablePropertiesRecord { Id = 3, Name = "d" };
        var notMappedType = new NotMappedTypeRecord { Id = 4 };

        Assert.Equal(("a", "b"), (duplicate.First, duplicate.Second));
        Assert.Equal(1, dualType.Id);
        Assert.Equal((2, "c"), (dualProperty.Id, dualProperty.Name));
        Assert.Equal((3, "d"), (noMappable.Id, noMappable.Name));
        Assert.Equal(4, notMappedType.Id);
    }



    [Fact]
    public void Getter_probes_members_round_trip()
    {
        var derived = new InheritedGetterDerivedProbe { Inherited = 1, Own = 2 };
        var nonPublic = new NonPublicGetterProbe { Id = 3, Secret = "s" };
        var nestedChild = new NonPublicNestedChild { ChildId = 5 };
        var nonPublicNested = new NonPublicNestedGetterProbe
        {
            Id = 4,
            Children = new List<NonPublicNestedChild> { nestedChild }
        };

        Assert.Equal((1, 2), (derived.Inherited, derived.Own));
        Assert.Equal(3, nonPublic.Id);
        Assert.Equal(4, nonPublicNested.Id);
        Assert.Equal(5, nestedChild.ChildId);
    }



    [Fact]
    public void Mangle_collision_probes_members_round_trip()
    {
        var dotted = new TestModels.MangleProbe_A.B { Id = 1 };
        var underscored = new TestModels.MangleProbe.A_B { Id = 2 };

        Assert.Equal((1, 2), (dotted.Id, underscored.Id));
    }



    [Fact]
    public void Child_collection_parents_members_round_trip()
    {
        var child = new ChildRecord { ChildId = 1, Description = "a" };
        var enumerableParent = new ParentWithIEnumerableChildren
        {
            ParentId = 1,
            Children = new[] { child }
        };
        var notMappedChild = new NotMappedChild { Id = 2 };
        var notMappedParent = new ParentWithNotMappedChildren
        {
            ParentId = 2,
            Children = new List<NotMappedChild> { notMappedChild }
        };

        Assert.Equal(1, enumerableParent.ParentId);
        Assert.Same(child, enumerableParent.Children.Single());
        Assert.Equal(2, notMappedParent.ParentId);
        Assert.Equal(2, Assert.Single(notMappedParent.Children).Id);
    }



    [Fact]
    public void Scalar_records_members_round_trip()
    {
        var when = new DateTime(2026, 10, 4, 0, 0, 0, DateTimeKind.Utc);
        var nullable = new NullablePropertiesRecord
        {
            Id = 1,
            NullableInt = 2,
            NullableDateTime = when,
            NullableString = "a"
        };
        var simple = new SimpleRecord { Id = 3, Value = "b" };
        var unsupportedEnum = new UnsupportedEnumRecord { Id = 4, Unsigned = UnsignedBackedKind.First };

        Assert.Equal
        (
            (1, (int?)2, (DateTime?)when, "a"),
            (nullable.Id, nullable.NullableInt, nullable.NullableDateTime, nullable.NullableString)
        );
        Assert.Equal((3, "b"), (simple.Id, simple.Value));
        Assert.Equal((4, UnsignedBackedKind.First), (unsupportedEnum.Id, unsupportedEnum.Unsigned));
    }
}
