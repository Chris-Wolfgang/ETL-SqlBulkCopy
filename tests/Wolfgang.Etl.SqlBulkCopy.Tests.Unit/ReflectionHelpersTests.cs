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
}
