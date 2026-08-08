using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class NestedTableMapTests
{
    [Fact]
    public void Constructor_when_propertyInfo_is_null_throws_ArgumentNullException()
    {
        var childMap = TypeMap.Create(typeof(ChildRecord));

        Assert.Throws<ArgumentNullException>
        (
            () => new NestedTableMap(null!, childMap)
        );
    }



    [Fact]
    public void Constructor_when_childTypeMap_is_null_throws_ArgumentNullException()
    {
        var prop = typeof(ParentRecord).GetProperty(nameof(ParentRecord.Children))!;

        Assert.Throws<ArgumentNullException>
        (
            () => new NestedTableMap(prop, null!)
        );
    }



    [Fact]
    public void PropertyName_returns_property_name()
    {
        var prop = typeof(ParentRecord).GetProperty(nameof(ParentRecord.Children))!;
        var childMap = TypeMap.Create(typeof(ChildRecord));

        var sut = new NestedTableMap(prop, childMap);

        Assert.Equal("Children", sut.PropertyName);
    }



    [Fact]
    public void ChildTypeMap_returns_provided_type_map()
    {
        var prop = typeof(ParentRecord).GetProperty(nameof(ParentRecord.Children))!;
        var childMap = TypeMap.Create(typeof(ChildRecord));

        var sut = new NestedTableMap(prop, childMap);

        Assert.Same(childMap, sut.ChildTypeMap);
    }



    [Fact]
    public void GetValues_returns_collection_items()
    {
        var prop = typeof(ParentRecord).GetProperty(nameof(ParentRecord.Children))!;
        var childMap = TypeMap.Create(typeof(ChildRecord));
        var sut = new NestedTableMap(prop, childMap);
        var parent = new ParentRecord
        {
            ParentId = 1,
            Name = "P1",
            Children = new List<ChildRecord>
            {
                new ChildRecord { ChildId = 10, Description = "C10" },
                new ChildRecord { ChildId = 11, Description = "C11" }
            }
        };

        var values = sut.GetValues(parent).ToList();

        Assert.Equal(2, values.Count);
    }



    [Fact]
    public void GetValues_when_parentInstance_is_null_throws_ArgumentNullException()
    {
        var prop = typeof(ParentRecord).GetProperty(nameof(ParentRecord.Children))!;
        var childMap = TypeMap.Create(typeof(ChildRecord));
        var sut = new NestedTableMap(prop, childMap);

        Assert.Throws<ArgumentNullException>
        (
            () => sut.GetValues(null!)
        );
    }



    [Fact]
    public void GetValues_when_collection_is_null_throws_InvalidOperationException()
    {
        var prop = typeof(ParentWithNullChildren).GetProperty(nameof(ParentWithNullChildren.Children))!;
        var childMap = TypeMap.Create(typeof(ChildRecord));
        var sut = new NestedTableMap(prop, childMap);
        var parent = new ParentWithNullChildren { ParentId = 1, Children = null! };

        Assert.Throws<InvalidOperationException>
        (
            () => sut.GetValues(parent).ToList()
        );
    }



    [Fact]
    public void GetValues_with_empty_collection_returns_empty()
    {
        var prop = typeof(ParentRecord).GetProperty(nameof(ParentRecord.Children))!;
        var childMap = TypeMap.Create(typeof(ChildRecord));
        var sut = new NestedTableMap(prop, childMap);
        var parent = new ParentRecord { ParentId = 1, Name = "P1", Children = new List<ChildRecord>() };

        var values = sut.GetValues(parent).ToList();

        Assert.Empty(values);
    }



    [Fact]
    public void GetValues_with_array_property_returns_items()
    {
        var prop = typeof(ParentWithArrayChildren).GetProperty(nameof(ParentWithArrayChildren.Children))!;
        var childMap = TypeMap.Create(typeof(ChildRecord));
        var sut = new NestedTableMap(prop, childMap);
        var parent = new ParentWithArrayChildren
        {
            ParentId = 1,
            Name = "P1",
            Children = new[]
            {
                new ChildRecord { ChildId = 10, Description = "C10" },
                new ChildRecord { ChildId = 11, Description = "C11" }
            }
        };

        var values = sut.GetValues(parent).ToList();

        Assert.Equal(2, values.Count);
    }



    // --- Defensive fallback paths in CreateValuesGetter ---
    //
    // BuildNestedTableMaps's filter normally rejects these shapes (non-generic
    // IEnumerable element type, non-enumerable runtime value), so they're
    // exercised by constructing NestedTableMap directly with PropertyInfo
    // pointing at the contrived test types below.

    private sealed class ParentWithNonGenericEnumerable
    {
        // Property typed as non-generic IEnumerable. The runtime value is an
        // ArrayList — implements IEnumerable but not IEnumerable<object>, so
        // the typed-cast branch fails and the non-generic Cast<object>
        // branch is exercised.
        public IEnumerable Items { get; init; } = new ArrayList();
    }

    private sealed class ParentWithNonEnumerableObject
    {
        // Property typed as object so PropertyInfo.GetValue can return
        // something that is not IEnumerable at all (here, a boxed int).
        // Exercises the final "cannot be enumerated" throw.
        public object Items { get; init; } = 42;
    }



    [Fact]
    public void GetValues_when_value_is_non_generic_IEnumerable_returns_cast_items()
    {
        var propertyInfo = typeof(ParentWithNonGenericEnumerable).GetProperty(nameof(ParentWithNonGenericEnumerable.Items))!;
        var childMap = TypeMap.Create(typeof(ChildRecord));
        var sut = new NestedTableMap(propertyInfo, childMap);
        var parent = new ParentWithNonGenericEnumerable
        {
            Items = new ArrayList { "a", "b", "c" }
        };

        var values = sut.GetValues(parent).ToList();

        Assert.Equal(3, values.Count);
        Assert.Equal(new object[] { "a", "b", "c" }, values);
    }



    [Fact]
    public void GetValues_when_value_is_not_enumerable_throws_InvalidOperationException()
    {
        var propertyInfo = typeof(ParentWithNonEnumerableObject).GetProperty(nameof(ParentWithNonEnumerableObject.Items))!;
        var childMap = TypeMap.Create(typeof(ChildRecord));
        var sut = new NestedTableMap(propertyInfo, childMap);
        var parent = new ParentWithNonEnumerableObject { Items = 42 };

        var ex = Assert.Throws<InvalidOperationException>(() => sut.GetValues(parent).ToList());

        Assert.Contains("cannot be enumerated", ex.Message, StringComparison.Ordinal);
    }



    [Fact]
    public void Constructor_descriptor_when_no_generated_getter_registered_throws_InvalidOperationException()
    {
        // The descriptor-based ctor requires a generated getter for the
        // (parentType, propertyName) pair; a pair that was never registered is a
        // source-generator defect and must fail loudly.
        var childMap = TypeMap.Create(typeof(ChildRecord));

        var ex = Assert.Throws<InvalidOperationException>
        (
            () => new NestedTableMap(typeof(object), "__no_such_generated_getter__", childMap)
        );

        Assert.Contains("No source-generated getter", ex.Message, StringComparison.Ordinal);
    }
}
