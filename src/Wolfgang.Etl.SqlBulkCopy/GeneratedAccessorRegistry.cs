using System;
using System.Collections.Concurrent;
using System.ComponentModel;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Infrastructure for source-generated property accessors. This type is not
/// intended to be called directly from application code — the
/// <c>Wolfgang.Etl.SqlBulkCopy</c> source generator emits calls to
/// <see cref="Register"/> from a module initializer for each type it generates
/// accessors for.
/// </summary>
/// <remarks>
/// <para>
/// When a generated getter is registered for a property, <see cref="ColumnMap"/>
/// uses it instead of compiling one at runtime with
/// <see cref="System.Linq.Expressions"/>. A generated getter is ordinary C#
/// emitted at compile time, so it keeps the per-row hot path free of runtime IL
/// generation — which is what makes it Native-AOT clean — while preserving the
/// throughput of the compiled-getter approach. When no generated getter is
/// registered, <see cref="ColumnMap"/> falls back to the runtime
/// expression-compiled getter, which is behaviourally identical but emits IL at
/// runtime and is therefore not AOT-safe. See ADR 0006.
/// </para>
/// <para>
/// The registration entry point is <see langword="public"/> because generated
/// code runs in the consumer's assembly, which has no <c>InternalsVisibleTo</c>
/// access to this one; it is hidden from IntelliSense via
/// <see cref="EditorBrowsableAttribute"/> to signal it is not part of the
/// intended application-facing surface.
/// </para>
/// </remarks>
[EditorBrowsable(EditorBrowsableState.Never)]
public static class GeneratedAccessorRegistry
{
    private static readonly ConcurrentDictionary<(Type DeclaringType, string PropertyName), Func<object, object?>> Getters = new();

    private static readonly ConcurrentDictionary<Type, Func<object, object>> EnumConverters = new();



    /// <summary>
    /// Registers a source-generated getter for the property
    /// <paramref name="propertyName"/> declared on <paramref name="declaringType"/>.
    /// Intended to be called only by generated code.
    /// </summary>
    /// <param name="declaringType">The type that declares the property.</param>
    /// <param name="propertyName">The name of the property the getter reads.</param>
    /// <param name="getter">
    /// A delegate equivalent to <c>obj =&gt; (object?)((T)obj).PropertyName</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when any argument is <see langword="null"/>.
    /// </exception>
    public static void Register
    (
        Type declaringType,
        string propertyName,
        Func<object, object?> getter
    )
    {
        if (declaringType is null)
        {
            throw new ArgumentNullException(nameof(declaringType));
        }

        if (propertyName is null)
        {
            throw new ArgumentNullException(nameof(propertyName));
        }

        if (getter is null)
        {
            throw new ArgumentNullException(nameof(getter));
        }

        Getters[(declaringType, propertyName)] = getter;
    }



    /// <summary>
    /// Registers a source-generated converter that maps a boxed value of the
    /// enum <paramref name="enumType"/> to its underlying integral type (also
    /// boxed). Intended to be called only by generated code.
    /// </summary>
    /// <param name="enumType">The enum type the converter accepts.</param>
    /// <param name="converter">
    /// A delegate equivalent to <c>boxed =&gt; (object)(TUnderlying)(TEnum)boxed</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when any argument is <see langword="null"/>.
    /// </exception>
    public static void RegisterEnumConverter
    (
        Type enumType,
        Func<object, object> converter
    )
    {
        if (enumType is null)
        {
            throw new ArgumentNullException(nameof(enumType));
        }

        if (converter is null)
        {
            throw new ArgumentNullException(nameof(converter));
        }

        EnumConverters[enumType] = converter;
    }



    /// <summary>
    /// Attempts to retrieve a registered source-generated getter for the
    /// property <paramref name="propertyName"/> on <paramref name="declaringType"/>.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when a generated getter has been registered;
    /// otherwise <see langword="false"/>.
    /// </returns>
    internal static bool TryGetGetter
    (
        Type declaringType,
        string propertyName,
        out Func<object, object?> getter
    )
    {
        return Getters.TryGetValue((declaringType, propertyName), out getter!);
    }



    /// <summary>
    /// Attempts to retrieve a registered source-generated enum-to-underlying
    /// converter for <paramref name="enumType"/>.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when a generated converter has been registered;
    /// otherwise <see langword="false"/>.
    /// </returns>
    internal static bool TryGetEnumConverter
    (
        Type enumType,
        out Func<object, object> converter
    )
    {
        return EnumConverters.TryGetValue(enumType, out converter!);
    }
}
