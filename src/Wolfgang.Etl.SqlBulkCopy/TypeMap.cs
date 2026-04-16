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
    private static readonly ConcurrentDictionary<string, TypeMap> Cache = new();

    /// <summary>
    /// The set of CLR types that can be mapped directly to SQL Server columns.
    /// </summary>
    internal static readonly HashSet<Type> SupportedColumnTypes = new()
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
                ? $"[{SchemaName}].[{TableName}]"
                : $"[{TableName}]";
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

        var cacheKey = $"{type.FullName}|{schemaName ?? ""}|{tableName ?? ""}";

        return Cache.GetOrAdd(cacheKey, _ => BuildTypeMap(type, schemaName, tableName));
    }



    private static TypeMap BuildTypeMap
    (
        Type type,
        string? schemaName,
        string? tableName
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
            ? BuildColumnMaps(properties)
            : Array.Empty<ColumnMap>();

        var nestedTables = BuildNestedTableMaps(properties);

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

        return
        (
            schemaName ?? tableAttribute?.Schema,
            tableName ?? tableAttribute?.Name ?? type.Name
        );
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



    private static ColumnMap[] BuildColumnMaps(PropertyInfo[] properties)
    {
        var ordinal = 0;

        return properties
            .Where
            (
                p => p.GetCustomAttribute<NotMappedAttribute>(inherit: false) is null
                     && IsSupportedColumnType(p.PropertyType)
            )
            .Select(p => new ColumnMap(p, ordinal++))
            .ToArray();
    }



    private static NestedTableMap[] BuildNestedTableMaps(PropertyInfo[] properties)
    {
        return properties
            .Where
            (
                p => p.GetCustomAttribute<NotMappedAttribute>(inherit: false) is null
                     && typeof(IEnumerable).IsAssignableFrom(p.PropertyType)
                     && p.PropertyType != typeof(string)
                     && p.PropertyType != typeof(byte[])
            )
            .Select
            (
                p => new
                {
                    PropertyInfo = p,
                    ElementType = p.PropertyType.GenericTypeArguments.FirstOrDefault()
                                  ?? p.PropertyType.GetElementType()
                }
            )
            .Where
            (
                pair => pair.ElementType is not null
                        && pair.ElementType.IsClass
                        && !SupportedColumnTypes.Contains(pair.ElementType)
            )
            .Select
            (
                pair => new NestedTableMap
                (
                    pair.PropertyInfo,
                    Create(pair.ElementType!)
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

        // Support enum types by mapping to their underlying integral type
        if (type.IsEnum)
        {
            return true;
        }

        return false;
    }
}
