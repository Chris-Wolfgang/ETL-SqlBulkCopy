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

        var ordinal = reader.GetOrdinal("NullableInt");
        Assert.Equal(DBNull.Value, reader.GetValue(ordinal));
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
    public void GetValue_when_ordinal_is_negative_throws_from_the_ordinal_guard()
    {
        // -1 pins the lower bound of `ordinal < 0`. We must assert the exception
        // comes from ValidateOrdinal (ParamName "ordinal"), NOT the downstream
        // Columns[ordinal] list indexer — which also throws ArgumentOutOfRange
        // but with ParamName "index". Without the ParamName check, a mutation to
        // the `< 0` clause (or `||` -> `&&`) would still throw via the indexer and
        // escape detection.
        var batch = new object[]
        {
            new TestRecord { Id = 1, Name = "A", Amount = 10m }
        };
        var reader = CreateReader(batch);
        reader.Read();

        var ex = Assert.Throws<ArgumentOutOfRangeException>
        (
            () => reader.GetValue(-1)
        );

        Assert.Equal("ordinal", ex.ParamName);
    }



    [Fact]
    public void GetValue_when_ordinal_equals_field_count_throws_from_the_ordinal_guard()
    {
        // FieldCount (3 for TestRecord: Id, Name, Amount) is the first invalid
        // index — it pins the exact `ordinal >= _typeMap.Columns.Count` boundary
        // (`>= Count` vs `> Count`). As above, assert ParamName "ordinal" so the
        // downstream indexer (ParamName "index") can't mask a mutated guard.
        var batch = new object[]
        {
            new TestRecord { Id = 1, Name = "A", Amount = 10m }
        };
        var reader = CreateReader(batch);
        reader.Read();

        var ex = Assert.Throws<ArgumentOutOfRangeException>
        (
            () => reader.GetValue(reader.FieldCount)
        );

        Assert.Equal("ordinal", ex.ParamName);
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

        var ordinal = reader.GetOrdinal("NullableString");
        Assert.True(reader.IsDBNull(ordinal));
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



    [Fact]
    public void GetValue_before_Read_throws_InvalidOperationException()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 1, Name = "A", Amount = 10m }
        };
        var reader = CreateReader(batch);

        Assert.Throws<InvalidOperationException>
        (
            () => reader.GetValue(0)
        );
    }



    [Fact]
    public void GetValue_after_Read_returns_false_throws_InvalidOperationException()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 1, Name = "A", Amount = 10m }
        };
        var reader = CreateReader(batch);
        reader.Read();
        reader.Read(); // past end

        Assert.Throws<InvalidOperationException>
        (
            () => reader.GetValue(0)
        );
    }



    [Fact]
    public void IsDBNull_before_Read_throws_InvalidOperationException()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 1, Name = "A", Amount = 10m }
        };
        var reader = CreateReader(batch);

        Assert.Throws<InvalidOperationException>
        (
            () => reader.IsDBNull(0)
        );
    }



    [Fact]
    public void GetValue_with_enum_returns_underlying_int()
    {
        var typeMap = TypeMap.Create(typeof(EnumRecord));
        var batch = new object[]
        {
            new EnumRecord { Id = 1, Status = Status.Active }
        };
        var reader = new TypeMapReader(batch, typeMap);
        reader.Read();

        // Status is the second column (ordinal 1)
        var value = reader.GetValue(1);

        Assert.Equal(1, value);
    }



    [Fact]
    public void GetValue_with_byte_backed_enum_returns_boxed_byte()
    {
        var typeMap = TypeMap.Create(typeof(MultiUnderlyingEnumRecord));
        var batch = new object[]
        {
            new MultiUnderlyingEnumRecord
            {
                Id = 1,
                ByteValue = ByteKind.Hundred,
                ShortValue = ShortKind.Max,
                LongValue = LongKind.Huge
            }
        };
        var reader = new TypeMapReader(batch, typeMap);
        reader.Read();

        // Also read the non-enum Id column so the test exercises every
        // property on the model.
        Assert.Equal(1, reader.GetValue(reader.GetOrdinal("Id")));

        var value = reader.GetValue(reader.GetOrdinal("ByteValue"));

        Assert.IsType<byte>(value);
        Assert.Equal((byte)100, value);
    }



    [Fact]
    public void GetValue_with_short_backed_enum_returns_boxed_short_preserving_sign()
    {
        var typeMap = TypeMap.Create(typeof(MultiUnderlyingEnumRecord));
        var batch = new object[]
        {
            new MultiUnderlyingEnumRecord
            {
                Id = 1,
                ByteValue = ByteKind.Zero,
                ShortValue = ShortKind.Min,
                LongValue = LongKind.Zero
            }
        };
        var reader = new TypeMapReader(batch, typeMap);
        reader.Read();

        var value = reader.GetValue(reader.GetOrdinal("ShortValue"));

        Assert.IsType<short>(value);
        Assert.Equal((short)-30_000, value);
    }



    [Fact]
    public void GetValue_with_long_backed_enum_returns_boxed_long()
    {
        var typeMap = TypeMap.Create(typeof(MultiUnderlyingEnumRecord));
        var batch = new object[]
        {
            new MultiUnderlyingEnumRecord
            {
                Id = 1,
                ByteValue = ByteKind.Zero,
                ShortValue = ShortKind.Max,
                LongValue = LongKind.Huge
            }
        };
        var reader = new TypeMapReader(batch, typeMap);
        reader.Read();

        var value = reader.GetValue(reader.GetOrdinal("LongValue"));

        Assert.IsType<long>(value);
        Assert.Equal(9_000_000_000L, value);
    }



    [Fact]
    public void GetValue_with_negative_ordinal_throws_ArgumentOutOfRangeException()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 1, Name = "A", Amount = 10m }
        };
        var reader = CreateReader(batch);
        reader.Read();

        Assert.Throws<ArgumentOutOfRangeException>
        (
            () => reader.GetValue(-1)
        );
    }



    [Fact]
    public void Indexer_by_ordinal_returns_value()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 42, Name = "Test", Amount = 99.5m }
        };
        var reader = CreateReader(batch);
        reader.Read();

        Assert.Equal(42, reader[0]);
    }



    [Fact]
    public void Indexer_by_name_returns_value()
    {
        var batch = new object[]
        {
            new TestRecord { Id = 42, Name = "Test", Amount = 99.5m }
        };
        var reader = CreateReader(batch);
        reader.Read();

        Assert.Equal(42, reader["Id"]);
    }



    [Fact]
    public void Depth_returns_zero()
    {
        var reader = CreateReader(Array.Empty<object>());

        Assert.Equal(0, reader.Depth);
    }



    [Fact]
    public void IsClosed_returns_false()
    {
        var reader = CreateReader(Array.Empty<object>());

        Assert.False(reader.IsClosed);
    }



    [Fact]
    public void RecordsAffected_returns_negative_one()
    {
        var reader = CreateReader(Array.Empty<object>());

        Assert.Equal(-1, reader.RecordsAffected);
    }



    [Fact]
    public void NextResult_returns_false()
    {
        var reader = CreateReader(Array.Empty<object>());

        Assert.False(reader.NextResult());
    }



    [Fact]
    public void GetOrdinal_when_name_is_null_throws_ArgumentNullException()
    {
        var reader = CreateReader(Array.Empty<object>());

        Assert.Throws<ArgumentNullException>
        (
            () => reader.GetOrdinal(null!)
        );
    }



    // --- NotSupportedException overrides ---
    //
    // SqlBulkCopy never calls these typed accessors — it routes everything
    // through GetValue / IsDBNull. The overrides exist because DbDataReader
    // declares them as abstract, and they explicitly throw to make the
    // unsupported contract loud rather than returning garbage. The tests
    // below pin that contract so a future "let's just implement these"
    // refactor has to consciously delete the test.

    [Fact]
    public void GetBoolean_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetBoolean(0));
    }

    [Fact]
    public void GetByte_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetByte(0));
    }

    [Fact]
    public void GetBytes_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetBytes(0, 0L, buffer: null, bufferOffset: 0, length: 0));
    }

    [Fact]
    public void GetChar_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetChar(0));
    }

    [Fact]
    public void GetChars_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetChars(0, 0L, buffer: null, bufferOffset: 0, length: 0));
    }

    [Fact]
    public void GetDataTypeName_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetDataTypeName(0));
    }

    [Fact]
    public void GetDateTime_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetDateTime(0));
    }

    [Fact]
    public void GetDecimal_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetDecimal(0));
    }

    [Fact]
    public void GetDouble_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetDouble(0));
    }

    [Fact]
    public void GetFieldType_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetFieldType(0));
    }

    [Fact]
    public void GetFloat_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetFloat(0));
    }

    [Fact]
    public void GetGuid_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetGuid(0));
    }

    [Fact]
    public void GetInt16_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetInt16(0));
    }

    [Fact]
    public void GetInt32_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetInt32(0));
    }

    [Fact]
    public void GetInt64_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetInt64(0));
    }

    [Fact]
    public void GetString_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetString(0));
    }

    [Fact]
    public void GetValues_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetValues(new object[1]));
    }

    [Fact]
    public void GetEnumerator_throws_NotSupportedException()
    {
        var reader = CreateReader(Array.Empty<object>());
        Assert.Throws<NotSupportedException>(() => reader.GetEnumerator());
    }
}
