using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Describes how a .NET type maps to a SQL Server table, including column mappings
/// and nested collection-to-table mappings.
/// </summary>
/// <remarks>
/// Built from reflection over the type's public instance properties, using
/// <see cref="TableAttribute"/>, <see cref="ColumnAttribute"/>, and
/// <see cref="NotMappedAttribute"/> from <c>System.ComponentModel.DataAnnotations.Schema</c>.
/// </remarks>
internal sealed class TypeMap
{
    private static readonly ConcurrentDictionary<(Type Type, string? SchemaName, string? TableName), TypeMap> Cache = new();

    /// <summary>
    /// The set of CLR types that can be mapped directly to SQL Server columns.
    /// </summary>
    /// <remarks>
    /// Private to prevent test pollution and accidental external mutation. The set is a
    /// closed list — extending support requires editing this collection in source.
    /// </remarks>
    private static readonly HashSet<Type> SupportedColumnTypes = new()
    {
        typeof(bool),
        typeof(byte),
        typeof(char),
        typeof(short),
        typeof(int),
        typeof(float),
        typeof(long),
        typeof(double),
        typeof(string),
        typeof(byte[]),
        typeof(Guid),
        typeof(decimal),
        typeof(DateTime),
        typeof(DateTimeOffset),
        typeof(TimeSpan),
#if NET6_0_OR_GREATER
        typeof(DateOnly),
        typeof(TimeOnly),
#endif
    };



    private TypeMap
    (
        string? schemaName,
        string tableName,
        IReadOnlyList<ColumnMap> columns,
        IReadOnlyList<NestedTableMap> nestedTables,
        bool isMappedToTable
    )
    {
        SchemaName = schemaName;
        TableName = tableName;
        Columns = columns;
        NestedTables = nestedTables;
        IsMappedToTable = isMappedToTable;
    }



    /// <summary>
    /// Gets the schema name, or <c>null</c> if no schema was specified.
    /// </summary>
    public string? SchemaName { get; }



    /// <summary>
    /// Gets the table name.
    /// </summary>
    public string TableName { get; }



    /// <summary>
    /// Gets the fully qualified table name in <c>[Schema].[Table]</c> format.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this type is not mapped to a table.
    /// </exception>
    public string QualifiedTableName
    {
        get
        {
            if (!IsMappedToTable)
            {
                throw new InvalidOperationException
                (
                    "This type is not mapped to a table in the database."
                );
            }

            return SchemaName is not null
                ? $"[{EscapeIdentifier(SchemaName)}].[{EscapeIdentifier(TableName)}]"
                : $"[{EscapeIdentifier(TableName)}]";
        }
    }



    /// <summary>
    /// Gets the ordered list of column mappings for this type.
    /// </summary>
    public IReadOnlyList<ColumnMap> Columns { get; }



    /// <summary>
    /// Gets the list of nested collection-to-table mappings.
    /// </summary>
    public IReadOnlyList<NestedTableMap> NestedTables { get; }



    /// <summary>
    /// Gets a value indicating whether this type is mapped to a database table.
    /// </summary>
    public bool IsMappedToTable { get; }



    /// <summary>
    /// Creates or retrieves a cached <see cref="TypeMap"/> for the specified type.
    /// </summary>
    /// <param name="type">The type to map.</param>
    /// <param name="schemaName">Optional schema name override.</param>
    /// <param name="tableName">Optional table name override.</param>
    /// <returns>A <see cref="TypeMap"/> describing the mapping.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="type"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a circular type reference is detected (e.g., a self-referential
    /// or mutually-recursive collection property).
    /// </exception>
    internal static TypeMap Create
    (
        Type type,
        string? schemaName = null,
        string? tableName = null
    )
    {
        if (type is null)
        {
            throw new ArgumentNullException(nameof(type));
        }

        // Normalize whitespace/empty overrides to null so the cache treats
        // (null), ("") and ("   ") as the same key and matches the
        // "no override" semantics applied later in BuildTypeMap.
        var normalizedSchema = string.IsNullOrWhiteSpace(schemaName) ? null : schemaName;
        var normalizedTable = string.IsNullOrWhiteSpace(tableName) ? null : tableName;

        return Create(type, normalizedSchema, normalizedTable, typesInProgress: new HashSet<Type>());
    }



    /// <summary>
    /// Internal recursive entry point. The <paramref name="typesInProgress"/> set is
    /// passed explicitly down the call chain (instead of via <c>[ThreadStatic]</c>) so
    /// cycle detection is correct under concurrency and async continuations.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a circular type reference is detected (e.g., a self-referential
    /// or mutually-recursive collection property).
    /// </exception>
    private static TypeMap Create
    (
        Type type,
        string? schemaName,
        string? tableName,
        HashSet<Type> typesInProgress
    )
    {
        var cacheKey = (type, schemaName, tableName);

        if (Cache.TryGetValue(cacheKey, out var cached))
        {
            return cached;
        }

        if (!typesInProgress.Add(type))
        {
            throw new InvalidOperationException
            (
                $"Circular type reference detected involving '{type.Name}'. " +
                "Self-referential or mutually-recursive collection properties are not supported."
            );
        }

        try
        {
            var map = BuildTypeMap(type, schemaName, tableName, typesInProgress);
            // GetOrAdd, not TryAdd: if another thread built and cached the
            // same key concurrently, return that thread's instance so a
            // given cache key always resolves to a single shared TypeMap.
            return Cache.GetOrAdd(cacheKey, map);
        }
        finally
        {
            typesInProgress.Remove(type);
        }
    }



    private static TypeMap BuildTypeMap
    (
        Type type,
        string? schemaName,
        string? tableName,
        HashSet<Type> typesInProgress
    )
    {
        var tableAttribute = type.GetCustomAttribute<TableAttribute>(inherit: false);
        var notMappedAttribute = type.GetCustomAttribute<NotMappedAttribute>(inherit: false);

        ValidateTypeAttributes(type, tableAttribute, notMappedAttribute, schemaName, tableName);
        ValidatePropertyAttributes(type);

        var isMapped = notMappedAttribute is null;
        var resolvedNames = ResolveTableName(type, tableAttribute, isMapped, schemaName, tableName);
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        var columns = isMapped
            ? BuildColumnMaps(type, properties)
            : Array.Empty<ColumnMap>();

        var nestedTables = BuildNestedTableMaps(properties, typesInProgress);

        if (isMapped && columns.Length == 0 && nestedTables.Length == 0)
        {
            throw new InvalidOperationException
            (
                $"Type '{type.Name}' has no properties that were mapped to columns " +
                "and is not marked with NotMappedAttribute. Either add at least one " +
                "property that maps to a column in the table or add the NotMappedAttribute to the type."
            );
        }

        return new TypeMap
        (
            resolvedNames.Schema,
            resolvedNames.Table,
            columns,
            nestedTables,
            isMapped
        );
    }



    private static void ValidateTypeAttributes
    (
        Type type,
        TableAttribute? tableAttribute,
        NotMappedAttribute? notMappedAttribute,
        string? schemaName,
        string? tableName
    )
    {
        if (notMappedAttribute is not null && tableAttribute is not null)
        {
            throw new InvalidOperationException
            (
                $"Type '{type.Name}' cannot have both TableAttribute and NotMappedAttribute."
            );
        }

        if (notMappedAttribute is not null
            && (!string.IsNullOrWhiteSpace(schemaName) || !string.IsNullOrWhiteSpace(tableName)))
        {
            throw new InvalidOperationException
            (
                "Cannot specify schemaName or tableName when type is marked with NotMappedAttribute."
            );
        }
    }



    private static (string? Schema, string Table) ResolveTableName
    (
        Type type,
        TableAttribute? tableAttribute,
        bool isMapped,
        string? schemaName,
        string? tableName
    )
    {
        if (!isMapped)
        {
            return (null, type.Name);
        }

        var resolvedSchemaName = string.IsNullOrWhiteSpace(schemaName)
            ? tableAttribute?.Schema
            : schemaName;

        var resolvedTableName = !string.IsNullOrWhiteSpace(tableName)
            ? tableName!
            : tableAttribute?.Name ?? type.Name;

        return (resolvedSchemaName, resolvedTableName);
    }



    private static void ValidatePropertyAttributes(Type type)
    {
        var invalidProperty = type
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault
            (
                p => p.GetCustomAttribute<NotMappedAttribute>(inherit: false) is not null
                     && p.GetCustomAttribute<ColumnAttribute>(inherit: false) is not null
            );

        if (invalidProperty is not null)
        {
            throw new InvalidOperationException
            (
                $"Property '{invalidProperty.Name}' on '{type.Name}' " +
                "has both NotMappedAttribute and ColumnAttribute."
            );
        }
    }



    private static ColumnMap[] BuildColumnMaps(Type type, PropertyInfo[] properties)
    {
        var ordinal = 0;

        var columns = properties
            .Where
            (
                p => IsReadableInstanceProperty(p)
                     && p.GetCustomAttribute<NotMappedAttribute>(inherit: false) is null
                     && IsSupportedColumnType(p.PropertyType)
            )
            .Select(p => new ColumnMap(p, ordinal++))
            .ToArray();

        // SqlBulkCopy column matching is case-insensitive on the source side,
        // so two properties resolving to the same ColumnName (case-insensitive)
        // would create an ambiguous mapping. Reject it early with a clear error.
        var duplicate = columns
            .GroupBy(c => c.ColumnName, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault(g => g.Count() > 1);

        if (duplicate is not null)
        {
            var propertyNames = string.Join(", ", duplicate.Select(c => $"'{c.PropertyName}'"));
            throw new InvalidOperationException
            (
                $"Type '{type.Name}' has multiple properties mapping to column " +
                $"'{duplicate.Key}': {propertyNames}. Column names must be unique " +
                "(case-insensitive). Use [Column(\"...\")] to disambiguate."
            );
        }

        return columns;
    }



    private static NestedTableMap[] BuildNestedTableMaps
    (
        PropertyInfo[] properties,
        HashSet<Type> typesInProgress
    )
    {
        return properties
            .Where
            (
                p => IsReadableInstanceProperty(p)
                     && p.GetCustomAttribute<NotMappedAttribute>(inherit: false) is null
                     && typeof(IEnumerable).IsAssignableFrom(p.PropertyType)
                     && p.PropertyType != typeof(string)
                     && p.PropertyType != typeof(byte[])
            )
            .Select
            (
                p => new
                {
                    PropertyInfo = p,
                    ElementType = GetEnumerableElementType(p.PropertyType)
                }
            )
            .Where
            (
                pair => pair.ElementType is not null
                        && pair.ElementType.IsClass
                        && !SupportedColumnTypes.Contains(pair.ElementType)
                        && pair.ElementType.GetCustomAttribute<NotMappedAttribute>(inherit: false) is null
            )
            .Select
            (
                pair => new NestedTableMap
                (
                    pair.PropertyInfo,
                    Create(pair.ElementType!, schemaName: null, tableName: null, typesInProgress)
                )
            )
            .ToArray();
    }



    private static bool IsSupportedColumnType(Type propertyType)
    {
        var type = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

        if (SupportedColumnTypes.Contains(type))
        {
            return true;
        }

        // Enums are mapped via their underlying integral type. Only accept enums
        // whose underlying type is itself in SupportedColumnTypes — sbyte, ushort,
        // uint, and ulong are not supported by SqlBulkCopy mapping, so rejecting
        // them up-front gives a clearer error than failing at write time.
        if (type.IsEnum)
        {
            return SupportedColumnTypes.Contains(Enum.GetUnderlyingType(type));
        }

        return false;
    }



    /// <summary>
    /// Returns <c>true</c> when the property can be read by <see cref="PropertyInfo.GetValue(object)"/>
    /// without throwing — i.e. it has a public getter and takes no index parameters.
    /// Indexer and write-only properties would throw <see cref="TargetParameterCountException"/>
    /// or <see cref="ArgumentException"/> at read time and must be excluded from both
    /// column maps and nested-table maps.
    /// </summary>
    private static bool IsReadableInstanceProperty(PropertyInfo property)
    {
        return property.GetMethod is not null
               && property.GetIndexParameters().Length == 0;
    }



    /// <summary>
    /// Returns the element type for an array, <see cref="IEnumerable{T}"/>, or any
    /// type that implements <see cref="IEnumerable{T}"/>. Non-generic
    /// <see cref="IEnumerable"/> types (e.g., <c>ArrayList</c>) return <c>null</c>
    /// and are silently skipped during nested-table mapping — there is no element
    /// type to reflect over, so they cannot be mapped to a child table.
    /// </summary>
    private static Type? GetEnumerableElementType(Type type)
    {
        // Check for array element type first
        if (type.IsArray)
        {
            return type.GetElementType();
        }

        // The type itself may be IEnumerable<T> (e.g., property declared as IEnumerable<Foo>).
        // GetInterfaces() does not include the type itself, so check it directly.
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
        {
            return type.GenericTypeArguments[0];
        }

        // Find the IEnumerable<T> interface and return T
        var enumerableInterface = type
            .GetInterfaces()
            .FirstOrDefault
            (
                i => i.IsGenericType
                     && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)
            );

        return enumerableInterface?.GenericTypeArguments.FirstOrDefault();
    }



    private static string EscapeIdentifier(string identifier)
    {
        return identifier.Replace("]", "]]");
    }
}
