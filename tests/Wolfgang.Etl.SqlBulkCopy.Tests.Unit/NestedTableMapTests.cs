using System;
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
}
