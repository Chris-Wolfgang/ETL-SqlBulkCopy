using System;
using System.Reflection;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class ReflectionHelpersTests
{
    private sealed class Sample
    {
        public int IntProp { get; init; }

        public int? NullableIntProp { get; init; }

        public string? RefProp { get; init; }

        public DateTime DateProp { get; init; }

        public string this[int i] => i.ToString(System.Globalization.CultureInfo.InvariantCulture);

        public string SetOnly
        {
            // ReSharper disable once UnusedMember.Local — covered by the
            // "no getter throws" test.
            set { _setOnlyBacking = value; }
        }
        private string _setOnlyBacking = string.Empty;
    }



    [Fact]
    public void CompilePropertyGetter_when_propertyInfo_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => ReflectionHelpers.CompilePropertyGetter(null!)
        );
    }



    [Fact]
    public void CompilePropertyGetter_when_property_has_no_getter_throws_ArgumentException()
    {
        var prop = typeof(Sample).GetProperty
        (
            nameof(Sample.SetOnly),
            BindingFlags.Public | BindingFlags.Instance
        )!;

        Assert.Throws<ArgumentException>
        (
            () => ReflectionHelpers.CompilePropertyGetter(prop)
        );
    }



    [Fact]
    public void CompilePropertyGetter_when_property_is_indexer_throws_ArgumentException()
    {
        // Sample exposes `string this[int i]` — an indexer that PropertyInfo
        // can find via the indexer's compiler-emitted name. Compiling it
        // would fail deep inside Expression.Property; the helper short-
        // circuits with a clearer ArgumentException.
        var indexer = typeof(Sample).GetProperty
        (
            "Item",
            BindingFlags.Public | BindingFlags.Instance
        )!;

        Assert.Throws<ArgumentException>
        (
            () => ReflectionHelpers.CompilePropertyGetter(indexer)
        );
    }



    [Fact]
    public void CompilePropertyGetter_reads_value_type_property_as_boxed_object()
    {
        var prop = typeof(Sample).GetProperty(nameof(Sample.IntProp))!;
        var getter = ReflectionHelpers.CompilePropertyGetter(prop);
        var instance = new Sample { IntProp = 42 };

        var value = getter(instance);

        Assert.Equal(42, value);
        Assert.IsType<int>(value);
    }



    [Fact]
    public void CompilePropertyGetter_reads_nullable_value_type_property_when_value_present()
    {
        var prop = typeof(Sample).GetProperty(nameof(Sample.NullableIntProp))!;
        var getter = ReflectionHelpers.CompilePropertyGetter(prop);
        var instance = new Sample { NullableIntProp = 7 };

        var value = getter(instance);

        Assert.Equal(7, value);
    }



    [Fact]
    public void CompilePropertyGetter_reads_nullable_value_type_property_when_null()
    {
        var prop = typeof(Sample).GetProperty(nameof(Sample.NullableIntProp))!;
        var getter = ReflectionHelpers.CompilePropertyGetter(prop);
        var instance = new Sample { NullableIntProp = null };

        var value = getter(instance);

        Assert.Null(value);
    }



    [Fact]
    public void CompilePropertyGetter_reads_reference_type_property()
    {
        var prop = typeof(Sample).GetProperty(nameof(Sample.RefProp))!;
        var getter = ReflectionHelpers.CompilePropertyGetter(prop);
        var instance = new Sample { RefProp = "hello" };

        var value = getter(instance);

        Assert.Equal("hello", value);
    }



    [Fact]
    public void CompilePropertyGetter_reads_reference_type_property_when_null_returns_null()
    {
        var prop = typeof(Sample).GetProperty(nameof(Sample.RefProp))!;
        var getter = ReflectionHelpers.CompilePropertyGetter(prop);
        var instance = new Sample { RefProp = null };

        var value = getter(instance);

        Assert.Null(value);
    }



    [Fact]
    public void CompilePropertyGetter_reads_DateTime_property()
    {
        var prop = typeof(Sample).GetProperty(nameof(Sample.DateProp))!;
        var getter = ReflectionHelpers.CompilePropertyGetter(prop);
        var instance = new Sample { DateProp = new DateTime(2026, 5, 19, 0, 0, 0, DateTimeKind.Utc) };

        var value = getter(instance);

        Assert.Equal(new DateTime(2026, 5, 19, 0, 0, 0, DateTimeKind.Utc), value);
    }



    // --- CompileEnumToUnderlyingConverter ---

    private enum IntBacked
    {
        Zero = 0,
        One = 1,
        Big = 1_000_000
    }

    private enum ByteBacked : byte
    {
        Zero = 0,
        Hundred = 100,
        Max = 255
    }

    private enum LongBacked : long
    {
        Zero = 0,
        Big = 9_000_000_000L
    }

    private enum ShortBacked : short
    {
        Negative = -32_000,
        Positive = 32_000
    }



    [Fact]
    public void CompileEnumToUnderlyingConverter_when_enumType_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => ReflectionHelpers.CompileEnumToUnderlyingConverter(null!)
        );
    }



    [Fact]
    public void CompileEnumToUnderlyingConverter_when_type_is_not_enum_throws_ArgumentException()
    {
        Assert.Throws<ArgumentException>
        (
            () => ReflectionHelpers.CompileEnumToUnderlyingConverter(typeof(int))
        );
    }



    [Fact]
    public void CompileEnumToUnderlyingConverter_for_int_backed_enum_returns_boxed_int()
    {
        var converter = ReflectionHelpers.CompileEnumToUnderlyingConverter(typeof(IntBacked));

        var value = converter(IntBacked.Big);

        Assert.IsType<int>(value);
        Assert.Equal(1_000_000, value);
    }



    [Fact]
    public void CompileEnumToUnderlyingConverter_for_byte_backed_enum_returns_boxed_byte()
    {
        var converter = ReflectionHelpers.CompileEnumToUnderlyingConverter(typeof(ByteBacked));

        var value = converter(ByteBacked.Hundred);

        Assert.IsType<byte>(value);
        Assert.Equal((byte)100, value);
    }



    [Fact]
    public void CompileEnumToUnderlyingConverter_for_long_backed_enum_returns_boxed_long()
    {
        var converter = ReflectionHelpers.CompileEnumToUnderlyingConverter(typeof(LongBacked));

        var value = converter(LongBacked.Big);

        Assert.IsType<long>(value);
        Assert.Equal(9_000_000_000L, value);
    }



    [Fact]
    public void CompileEnumToUnderlyingConverter_for_short_backed_enum_preserves_sign()
    {
        var converter = ReflectionHelpers.CompileEnumToUnderlyingConverter(typeof(ShortBacked));

        var value = converter(ShortBacked.Negative);

        Assert.IsType<short>(value);
        Assert.Equal((short)-32_000, value);
    }
}
