using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Wolfgang.Etl.SqlBulkCopy.SourceGenerator;

/// <summary>
/// Emits, for every type marked <c>[BulkCopyable]</c>: compile-time property
/// getters and enum→underlying converters (registered with
/// <c>GeneratedAccessorRegistry</c>), and — for fully-generatable types (the
/// type and its entire nested-table graph are <c>[BulkCopyable]</c> and
/// eligible) — a full type descriptor <em>including any nested tables</em>
/// (registered with <c>GeneratedTypeMapRegistry</c>) so the runtime builds the
/// map without reflecting over the type. A type that is not fully generatable
/// still gets generated getters but falls back to the reflection map. Together
/// these keep a marked type's mapping and per-row hot path free of runtime
/// reflection and IL emission, which is what makes it Native-AOT clean while
/// preserving compiled-getter throughput. See ADR 0006.
/// </summary>
/// <remarks>
/// Registration uses <c>[ModuleInitializer]</c> (net5.0+); the emitted file is
/// wrapped in <c>#if NET5_0_OR_GREATER</c> so earlier target frameworks compile
/// it to nothing and fall back to the reflection path.
/// </remarks>
[Generator(LanguageNames.CSharp)]
public sealed class BulkCopyAccessorGenerator : IIncrementalGenerator
{
    private const string BulkCopyableAttributeFullName = "Wolfgang.Etl.SqlBulkCopy.BulkCopyableAttribute";
    private const string NotMappedAttributeFullName = "System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute";
    private const string ColumnAttributeFullName = "System.ComponentModel.DataAnnotations.Schema.ColumnAttribute";
    private const string TableAttributeFullName = "System.ComponentModel.DataAnnotations.Schema.TableAttribute";

    // Field delimiters for the value-equatable model (control characters that
    // cannot appear in the C# identifiers / attribute strings being encoded).
    private const char ColumnSeparator = (char)31;
    private const char FieldSeparator = (char)30;
    private const char HeaderSeparator = (char)29;
    private const char GroupSeparator = (char)28;



    /// <inheritdoc/>
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var models = context.SyntaxProvider
            .ForAttributeWithMetadataName
            (
                BulkCopyableAttributeFullName,
                static (node, _) => node is TypeDeclarationSyntax,
                static (ctx, _) => Extract(ctx)
            )
            .Where(static m => m is not null)
            .Select(static (m, _) => m!);

        context.RegisterSourceOutput(models, static (spc, model) => Emit(spc, model));
    }



    private static AccessorModel? Extract(GeneratorAttributeSyntaxContext ctx)
    {
        if (ctx.TargetSymbol is not INamedTypeSymbol type
            || type.IsGenericType
            || !IsAccessibleToGeneratedCode(type))
        {
            return null;
        }

        var properties = GetGeneratedProperties(type);
        if (properties.Count == 0)
        {
            return null;
        }

        var fullyQualified = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        return new AccessorModel
        (
            fullyQualified,
            Mangle(fullyQualified),
            string.Join(";", properties.Select(static p => p.Name)),
            string.Join(";", CollectEnumConverters(properties)),
            EncodeDescriptor(type)
        );
    }



    // ---- property + enum accessor collection (unchanged getter behaviour) ----

    private static List<IPropertySymbol> GetGeneratedProperties(INamedTypeSymbol type)
    {
        var properties = new List<IPropertySymbol>();
        var seen = new HashSet<string>();

        for (var current = type; current is not null; current = current.BaseType)
        {
            foreach (var property in current.GetMembers().OfType<IPropertySymbol>())
            {
                if (!IsAccessorEmittableProperty(property))
                {
                    continue;
                }

                if (seen.Add(property.Name))
                {
                    properties.Add(property);
                }
            }
        }

        return properties;
    }



    private static List<string> CollectEnumConverters(List<IPropertySymbol> properties)
    {
        var entries = new List<string>();
        var seen = new HashSet<string>();

        foreach (var property in properties)
        {
            if (UnwrapNullable(property.Type) is not INamedTypeSymbol effective
                || effective.TypeKind != TypeKind.Enum
                || effective.EnumUnderlyingType is null)
            {
                continue;
            }

            var enumFullyQualified = effective.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            if (!seen.Add(enumFullyQualified))
            {
                continue;
            }

            var underlyingFullyQualified = effective.EnumUnderlyingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            entries.Add($"{enumFullyQualified},{underlyingFullyQualified},{Mangle(enumFullyQualified)}");
        }

        return entries;
    }



    // ---- descriptor encoding (mirrors TypeMap's column-mapping rules) ----

    /// <summary>
    /// Encodes the type's generated descriptor (columns and any nested tables),
    /// or returns an empty string when the type is not fully generatable in this
    /// pass (a nested-table child that is not itself a generatable
    /// <c>[BulkCopyable]</c> type, inherits mapped properties, has no mappable
    /// columns, has duplicate column names, or is <c>[NotMapped]</c>). Those
    /// types fall back to the reflection path, which produces the identical map.
    /// </summary>
    private static string EncodeDescriptor(INamedTypeSymbol type)
    {
        if (!IsFullyGeneratable(type, new HashSet<INamedTypeSymbol>(SymbolEqualityComparer.Default)))
        {
            return string.Empty;
        }

        var columns = new List<string>();
        var nested = new List<string>();
        var ordinal = 0;

        foreach (var property in type.GetMembers().OfType<IPropertySymbol>())
        {
            if (!IsMappableProperty(property))
            {
                continue;
            }

            if (IsNestedTableProperty(property))
            {
                nested.Add
                (
                    string.Join
                    (
                        FieldSeparator.ToString(),
                        property.Name,
                        GetEnumerableElementType(property.Type)!.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
                    )
                );

                continue;
            }

            var effective = UnwrapNullable(property.Type);
            if (!IsSupportedColumnType(effective))
            {
                continue;
            }

            columns.Add
            (
                string.Join
                (
                    FieldSeparator.ToString(),
                    property.Name,
                    GetColumnName(property) ?? property.Name,
                    effective.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    IsNullableProperty(property) ? "1" : "0",
                    ordinal.ToString(System.Globalization.CultureInfo.InvariantCulture)
                )
            );

            ordinal++;
        }

        var (schema, table) = ResolveTableName(type);
        var header = string.Join(HeaderSeparator.ToString(), schema ?? string.Empty, table);
        return header
            + GroupSeparator + string.Join(ColumnSeparator.ToString(), columns)
            + GroupSeparator + string.Join(ColumnSeparator.ToString(), nested);
    }



    /// <summary>
    /// Returns <see langword="true"/> when the entire object graph reachable from
    /// <paramref name="type"/> can be mapped without reflection: the type and
    /// every nested child element type is <c>[BulkCopyable]</c>, mappable (at
    /// least one column, no duplicate column names, no inherited mapped
    /// properties, not <c>[NotMapped]</c>), and the graph is acyclic. If any part
    /// fails, the whole type falls back to the reflection path so the map stays
    /// complete and consistent.
    /// </summary>
    private static bool IsFullyGeneratable(INamedTypeSymbol type, HashSet<INamedTypeSymbol> inProgress)
    {
        if (!inProgress.Add(type))
        {
            return false;
        }

        try
        {
            if (!HasAttribute(type, BulkCopyableAttributeFullName)
                || HasAttribute(type, NotMappedAttributeFullName)
                || type.IsGenericType
                || !IsAccessibleToGeneratedCode(type)
                || InheritsMappedProperty(type))
            {
                return false;
            }

            var columnCount = 0;
            var columnNames = new HashSet<string>(System.StringComparer.OrdinalIgnoreCase);

            foreach (var property in type.GetMembers().OfType<IPropertySymbol>())
            {
                if (!IsMappableProperty(property))
                {
                    continue;
                }

                if (IsNestedTableProperty(property))
                {
                    if (GetEnumerableElementType(property.Type) is not INamedTypeSymbol child
                        || !IsFullyGeneratable(child, inProgress))
                    {
                        return false;
                    }

                    continue;
                }

                if (!IsSupportedColumnType(UnwrapNullable(property.Type)))
                {
                    continue;
                }

                if (!columnNames.Add(GetColumnName(property) ?? property.Name))
                {
                    return false;
                }

                columnCount++;
            }

            return columnCount > 0;
        }
        finally
        {
            inProgress.Remove(type);
        }
    }



    private static bool InheritsMappedProperty(INamedTypeSymbol type)
    {
        for (var b = type.BaseType; b is not null && b.SpecialType != SpecialType.System_Object; b = b.BaseType)
        {
            foreach (var property in b.GetMembers().OfType<IPropertySymbol>())
            {
                if (IsReadableInstanceProperty(property))
                {
                    return true;
                }
            }
        }

        return false;
    }



    /// <summary>
    /// Returns <c>true</c> for an instance property with a getter — the generator's
    /// equivalent of <c>PropertyInfo</c> readability. Static properties, indexers, and
    /// write-only properties are excluded. Mirrors <c>TypeMap.IsReadableInstanceProperty</c>
    /// on the reflection path so both providers filter the same members.
    /// </summary>
    private static bool IsReadableInstanceProperty(IPropertySymbol property)
    {
        return !property.IsStatic
               && !property.IsIndexer
               && property.GetMethod is not null;
    }



    /// <summary>
    /// Returns <c>true</c> when <paramref name="property"/> maps to a column: a readable
    /// instance property (<see cref="IsReadableInstanceProperty"/>) that is not marked
    /// <c>[NotMapped]</c>.
    /// </summary>
    private static bool IsMappableProperty(IPropertySymbol property)
    {
        return IsReadableInstanceProperty(property)
               && !HasAttribute(property, NotMappedAttributeFullName);
    }



    /// <summary>
    /// Returns <c>true</c> when a strongly-typed accessor can be emitted for
    /// <paramref name="property"/>: a readable instance property whose type is not a
    /// ref-like (<c>ref struct</c>) type and whose getter is reachable from generated code.
    /// </summary>
    private static bool IsAccessorEmittableProperty(IPropertySymbol property)
    {
        return IsReadableInstanceProperty(property)
               && !property.Type.IsRefLikeType
               && property.GetMethod is { } getter
               && IsReachableAccessibility(getter.DeclaredAccessibility);
    }



    private static bool IsNestedTableProperty(IPropertySymbol property)
    {
        var type = property.Type;

        if (type.SpecialType == SpecialType.System_String || IsByteArray(type) || !ImplementsIEnumerable(type))
        {
            return false;
        }

        var element = GetEnumerableElementType(type);
        return element is { TypeKind: TypeKind.Class }
               && !IsSupportedColumnType(element)
               && !HasAttribute(element, NotMappedAttributeFullName);
    }



    private static bool IsSupportedColumnType(ITypeSymbol type)
    {
        var effective = UnwrapNullable(type);

        if (IsByteArray(effective))
        {
            return true;
        }

        switch (effective.SpecialType)
        {
            case SpecialType.System_Boolean:
            case SpecialType.System_Byte:
            case SpecialType.System_Char:
            case SpecialType.System_Int16:
            case SpecialType.System_Int32:
            case SpecialType.System_Single:
            case SpecialType.System_Int64:
            case SpecialType.System_Double:
            case SpecialType.System_String:
            case SpecialType.System_Decimal:
            case SpecialType.System_DateTime:
                return true;
        }

        if (effective.TypeKind == TypeKind.Enum && effective is INamedTypeSymbol enumType)
        {
            return enumType.EnumUnderlyingType?.SpecialType is
                SpecialType.System_Byte or SpecialType.System_Int16
                or SpecialType.System_Int32 or SpecialType.System_Int64;
        }

        return effective.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) switch
        {
            "global::System.Guid" => true,
            "global::System.DateTimeOffset" => true,
            "global::System.TimeSpan" => true,
            "global::System.DateOnly" => true,
            "global::System.TimeOnly" => true,
            _ => false,
        };
    }



    private static bool IsNullableProperty(IPropertySymbol property)
    {
        if (property.Type is INamedTypeSymbol named
            && named.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
        {
            return true;
        }

        return !property.Type.IsValueType;
    }



    private static string? GetColumnName(IPropertySymbol property)
    {
        var attribute = property.GetAttributes().FirstOrDefault(a => MatchesAttribute(a, ColumnAttributeFullName));
        if (attribute is null)
        {
            return null;
        }

        if (attribute.ConstructorArguments.Length > 0 && attribute.ConstructorArguments[0].Value is string ctorName)
        {
            return ctorName;
        }

        foreach (var named in attribute.NamedArguments)
        {
            if (string.Equals(named.Key, "Name", System.StringComparison.Ordinal) && named.Value.Value is string namedName)
            {
                return namedName;
            }
        }

        return null;
    }



    private static (string? Schema, string Table) ResolveTableName(INamedTypeSymbol type)
    {
        var attribute = type.GetAttributes().FirstOrDefault(a => MatchesAttribute(a, TableAttributeFullName));
        if (attribute is null)
        {
            return (null, type.Name);
        }

        string? table = null;
        if (attribute.ConstructorArguments.Length > 0 && attribute.ConstructorArguments[0].Value is string ctorName)
        {
            table = ctorName;
        }

        string? schema = null;
        foreach (var named in attribute.NamedArguments)
        {
            if (string.Equals(named.Key, "Schema", System.StringComparison.Ordinal) && named.Value.Value is string schemaValue && schemaValue.Length > 0)
            {
                schema = schemaValue;
            }
        }

        return (schema, string.IsNullOrEmpty(table) ? type.Name : table!);
    }



    // ---- small symbol helpers ----

    private static bool HasAttribute(ISymbol symbol, string attributeFullName)
    {
        return symbol.GetAttributes().Any(a => MatchesAttribute(a, attributeFullName));
    }



    private static bool MatchesAttribute(AttributeData attribute, string attributeFullName)
    {
        return string.Equals
        (
            attribute.AttributeClass?.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            "global::" + attributeFullName,
            System.StringComparison.Ordinal
        );
    }



    private static bool IsByteArray(ITypeSymbol type)
    {
        return type is IArrayTypeSymbol array && array.ElementType.SpecialType == SpecialType.System_Byte;
    }



    private static bool ImplementsIEnumerable(ITypeSymbol type)
    {
        if (type is IArrayTypeSymbol)
        {
            return true;
        }

        if (type.SpecialType == SpecialType.System_Collections_IEnumerable)
        {
            return true;
        }

        return type.AllInterfaces.Any(static i =>
            i.SpecialType == SpecialType.System_Collections_IEnumerable
            || i.OriginalDefinition.SpecialType == SpecialType.System_Collections_Generic_IEnumerable_T);
    }



    private static ITypeSymbol? GetEnumerableElementType(ITypeSymbol type)
    {
        if (type is IArrayTypeSymbol array)
        {
            return array.ElementType;
        }

        if (type is INamedTypeSymbol named
            && named.OriginalDefinition.SpecialType == SpecialType.System_Collections_Generic_IEnumerable_T
            && named.TypeArguments.Length == 1)
        {
            return named.TypeArguments[0];
        }

        var enumerableInterface = type.AllInterfaces.FirstOrDefault(static i =>
            i.OriginalDefinition.SpecialType == SpecialType.System_Collections_Generic_IEnumerable_T);

        return enumerableInterface?.TypeArguments.FirstOrDefault();
    }



    private static ITypeSymbol UnwrapNullable(ITypeSymbol type)
    {
        if (type is INamedTypeSymbol named
            && named.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T
            && named.TypeArguments.Length == 1)
        {
            return named.TypeArguments[0];
        }

        return type;
    }



    private static bool IsAccessibleToGeneratedCode(INamedTypeSymbol type)
    {
        for (INamedTypeSymbol? current = type; current is not null; current = current.ContainingType)
        {
            if (!IsReachableAccessibility(current.DeclaredAccessibility))
            {
                return false;
            }
        }

        return true;
    }



    private static bool IsReachableAccessibility(Accessibility accessibility)
    {
        return accessibility is Accessibility.Public
            or Accessibility.Internal
            or Accessibility.ProtectedOrInternal;
    }



    private static string Mangle(string fullyQualifiedName)
    {
        var builder = new StringBuilder(fullyQualifiedName.Length);

        foreach (var c in fullyQualifiedName)
        {
            builder.Append(char.IsLetterOrDigit(c) ? c : '_');
        }

        return builder.ToString();
    }



    // ---- emission ----

    private static void Emit(SourceProductionContext context, AccessorModel model)
    {
        var properties = model.PropertyNamesJoined.Split(';');
        var enumConverters = model.EnumConvertersJoined.Length == 0
            ? System.Array.Empty<string>()
            : model.EnumConvertersJoined.Split(';');

        var builder = new StringBuilder();
        builder.AppendLine("// <auto-generated/>");
        builder.AppendLine("#nullable enable");
        builder.AppendLine("#if NET5_0_OR_GREATER");
        builder.AppendLine("namespace Wolfgang.Etl.SqlBulkCopy.Generated");
        builder.AppendLine("{");
        builder.AppendLine($"    internal static class BulkCopyAccessors_{model.MangledName}");
        builder.AppendLine("    {");

        AppendGetterMethods(builder, model.FullyQualifiedName, properties);
        AppendEnumConverterMethods(builder, enumConverters);
        AppendModuleInitializer(builder, model, properties, enumConverters);

        builder.AppendLine("    }");
        builder.AppendLine("}");
        builder.AppendLine("#endif");

        context.AddSource($"BulkCopyAccessors_{model.MangledName}.g.cs", builder.ToString());
    }



    private static void AppendGetterMethods(StringBuilder builder, string typeFullyQualified, string[] properties)
    {
        foreach (var property in properties)
        {
            builder.AppendLine
            (
                $"        internal static object? Get_{property}(object instance) => (({typeFullyQualified})instance).{property};"
            );
        }
    }



    private static void AppendEnumConverterMethods(StringBuilder builder, string[] enumConverters)
    {
        foreach (var entry in enumConverters)
        {
            var parts = entry.Split(',');

            builder.AppendLine
            (
                $"        internal static object ConvertEnum_{parts[2]}(object boxed) => (object)({parts[1]})({parts[0]})boxed;"
            );
        }
    }



    private static void AppendModuleInitializer
    (
        StringBuilder builder,
        AccessorModel model,
        string[] properties,
        string[] enumConverters
    )
    {
        builder.AppendLine();
        builder.AppendLine("        [global::System.Runtime.CompilerServices.ModuleInitializer]");
        builder.AppendLine("        internal static void Initialize()");
        builder.AppendLine("        {");

        foreach (var property in properties)
        {
            builder.AppendLine
            (
                $"            global::Wolfgang.Etl.SqlBulkCopy.GeneratedAccessorRegistry.Register(typeof({model.FullyQualifiedName}), \"{property}\", Get_{property});"
            );
        }

        foreach (var entry in enumConverters)
        {
            var parts = entry.Split(',');

            builder.AppendLine
            (
                $"            global::Wolfgang.Etl.SqlBulkCopy.GeneratedAccessorRegistry.RegisterEnumConverter(typeof({parts[0]}), ConvertEnum_{parts[2]});"
            );
        }

        AppendDescriptorRegistration(builder, model);

        builder.AppendLine("        }");
    }



    private static void AppendDescriptorRegistration(StringBuilder builder, AccessorModel model)
    {
        if (model.DescriptorEncoded.Length == 0)
        {
            return;
        }

        var groups = model.DescriptorEncoded.Split(GroupSeparator);
        var header = groups[0].Split(HeaderSeparator);
        var schemaLiteral = header[0].Length == 0 ? "null" : $"\"{Escape(header[0])}\"";
        var columns = groups[1].Length == 0 ? System.Array.Empty<string>() : groups[1].Split(ColumnSeparator);
        var nested = groups[2].Length == 0 ? System.Array.Empty<string>() : groups[2].Split(ColumnSeparator);

        builder.AppendLine();
        builder.AppendLine($"            global::Wolfgang.Etl.SqlBulkCopy.GeneratedTypeMapRegistry.Register(typeof({model.FullyQualifiedName}), new global::Wolfgang.Etl.SqlBulkCopy.GeneratedTypeDescriptor");
        builder.AppendLine("            (");
        builder.AppendLine($"                {schemaLiteral},");
        builder.AppendLine($"                \"{Escape(header[1])}\",");
        builder.AppendLine("                new global::Wolfgang.Etl.SqlBulkCopy.GeneratedColumnDescriptor[]");
        builder.AppendLine("                {");

        foreach (var column in columns)
        {
            var fields = column.Split(FieldSeparator);
            var isNullable = string.Equals(fields[3], "1", System.StringComparison.Ordinal) ? "true" : "false";

            builder.AppendLine
            (
                $"                    new global::Wolfgang.Etl.SqlBulkCopy.GeneratedColumnDescriptor(\"{Escape(fields[0])}\", \"{Escape(fields[1])}\", typeof({fields[2]}), {isNullable}, {fields[4]}),"
            );
        }

        builder.AppendLine("                },");
        builder.AppendLine("                new global::Wolfgang.Etl.SqlBulkCopy.GeneratedNestedTableDescriptor[]");
        builder.AppendLine("                {");

        foreach (var entry in nested)
        {
            var fields = entry.Split(FieldSeparator);

            builder.AppendLine
            (
                $"                    new global::Wolfgang.Etl.SqlBulkCopy.GeneratedNestedTableDescriptor(\"{Escape(fields[0])}\", typeof({fields[1]})),"
            );
        }

        builder.AppendLine("                }));");
    }



    private static string Escape(string value)
    {
        return value.Replace("\\", "\\\\").Replace("\"", "\\\"");
    }



    private sealed record AccessorModel
    (
        string FullyQualifiedName,
        string MangledName,
        string PropertyNamesJoined,
        string EnumConvertersJoined,
        string DescriptorEncoded
    );
}
