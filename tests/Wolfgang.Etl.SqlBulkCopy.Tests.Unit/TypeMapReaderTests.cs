using System;
using System.Collections.Generic;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class TypeMapReaderTests
{
    private static TypeMapReader CreateReader(IReadOnlyList<object> batch)
    {
        var typeMap = TypeMap.Create(typeof(TestRecord));
        return new TypeMapReader(batch, typeMap);
    }



    [Fact]
    public void Constructor_when_batch_is_null_throws_ArgumentNullException()
    {
        var typeMap = TypeMap.Create(typeof(TestRecord));

        Assert.Throws<ArgumentNullException>
        (
            () => new TypeMapReader(null!, typeMap)
        );
    }



    [Fact]
    public void Constructor_when_typeMap_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new TypeMapReader(Array.Empty<object>(), null!)
        );
    }



    [Fact]
    public void FieldCount_returns_column_count()
    {
        var reader = CreateReader(Array.Empty<object>());

        // TestRecord has Id, Name, Amount (Ignored is NotMapped)
        Assert.Equal(3, reader.FieldCount);
    }



    [Fact]
    public void Read_advances_through_batch()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 1, Name = "A", Amount = 10m },
            new TestRecord { Id = 2, Name = "B", Amount = 20m }
        };
        var reader = CreateReader(batch);

        Assert.True(reader.Read());
        Assert.True(reader.Read());
        Assert.False(reader.Read());
    }



    [Fact]
    public void GetValue_returns_correct_value()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 42, Name = "Test", Amount = 99.5m }
        };
        var reader = CreateReader(batch);
        reader.Read();

        Assert.Equal(42, reader.GetValue(0));
        Assert.Equal("Test", reader.GetValue(1));
        Assert.Equal(99.5m, reader.GetValue(2));
    }



    [Fact]
    public void GetValue_when_null_returns_DBNull()
    {
        var typeMap = TypeMap.Create(typeof(NullablePropertiesRecord));
        var batch = new object[]
        {
            new NullablePropertiesRecord { Id = 1, NullableInt = null }
        };
        var reader = new TypeMapReader(batch, typeMap);
        reader.Read();

        // NullableInt is the second column (ordinal 1)
        Assert.Equal(DBNull.Value, reader.GetValue(1));
    }



    [Fact]
    public void GetValue_when_ordinal_out_of_range_throws_ArgumentOutOfRangeException()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 1, Name = "A", Amount = 10m }
        };
        var reader = CreateReader(batch);
        reader.Read();

        Assert.Throws<ArgumentOutOfRangeException>
        (
            () => reader.GetValue(99)
        );
    }



    [Fact]
    public void IsDBNull_when_value_is_null_returns_true()
    {
        var typeMap = TypeMap.Create(typeof(NullablePropertiesRecord));
        var batch = new object[]
        {
            new NullablePropertiesRecord { Id = 1, NullableString = null }
        };
        var reader = new TypeMapReader(batch, typeMap);
        reader.Read();

        // NullableString is the last column (ordinal 3)
        Assert.True(reader.IsDBNull(3));
    }



    [Fact]
    public void IsDBNull_when_value_is_not_null_returns_false()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 1, Name = "A", Amount = 10m }
        };
        var reader = CreateReader(batch);
        reader.Read();

        Assert.False(reader.IsDBNull(0));
    }



    [Fact]
    public void GetName_returns_column_name()
    {
        var reader = CreateReader(Array.Empty<object>());

        Assert.Equal("Id", reader.GetName(0));
        Assert.Equal("FullName", reader.GetName(1));
        Assert.Equal("Amount", reader.GetName(2));
    }



    [Fact]
    public void GetOrdinal_returns_correct_ordinal()
    {
        var reader = CreateReader(Array.Empty<object>());

        Assert.Equal(0, reader.GetOrdinal("Id"));
        Assert.Equal(1, reader.GetOrdinal("FullName"));
        Assert.Equal(2, reader.GetOrdinal("Amount"));
    }



    [Fact]
    public void GetOrdinal_is_case_insensitive()
    {
        var reader = CreateReader(Array.Empty<object>());

        Assert.Equal(1, reader.GetOrdinal("fullname"));
    }



    [Fact]
    public void GetOrdinal_when_name_not_found_throws_ArgumentOutOfRangeException()
    {
        var reader = CreateReader(Array.Empty<object>());

        Assert.Throws<ArgumentOutOfRangeException>
        (
            () => reader.GetOrdinal("NonExistent")
        );
    }



    [Fact]
    public void HasRows_when_batch_has_items_returns_true()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 1, Name = "A", Amount = 10m }
        };
        var reader = CreateReader(batch);

        Assert.True(reader.HasRows);
    }



    [Fact]
    public void HasRows_when_batch_is_empty_returns_false()
    {
        var reader = CreateReader(Array.Empty<object>());

        Assert.False(reader.HasRows);
    }
}
