using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Wolfgang.Etl.SqlBulkCopy.SourceGenerator;

/// <summary>
/// Emits compile-time property getters (and enum→underlying converters) for
/// every type marked <c>[BulkCopyable]</c> and registers them with
/// <c>Wolfgang.Etl.SqlBulkCopy.GeneratedAccessorRegistry</c> from a module
/// initializer. This lets the bulk-copy hot path read property values without
/// compiling a getter at runtime (no <c>System.Linq.Expressions</c> IL
/// emission), which is what makes a marked type's hot path Native-AOT clean
/// while preserving compiled-getter throughput. See ADR 0006.
/// </summary>
/// <remarks>
/// Registration uses <c>[ModuleInitializer]</c>, available on
/// <c>net5.0</c>+. The emitted registration is wrapped in
/// <c>#if NET5_0_OR_GREATER</c> so that on earlier target frameworks the file
/// compiles to nothing and the runtime falls back to the expression-compiled
/// getter — correct on those JIT-only targets, and avoiding any
/// <c>ModuleInitializerAttribute</c> polyfill collision.
/// </remarks>
[Generator(LanguageNames.CSharp)]
public sealed class BulkCopyAccessorGenerator : IIncrementalGenerator
{
    private const string BulkCopyableAttributeFullName = "Wolfgang.Etl.SqlBulkCopy.BulkCopyableAttribute";



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
        if (ctx.TargetSymbol is not INamedTypeSymbol type)
        {
            return null;
        }

        // An open generic type has no single closed CLR type to key a
        // typeof(T) registration on; skip it (it falls back to reflection).
        if (type.IsGenericType)
        {
            return null;
        }

        // The registration class references the type from a sibling top-level
        // class in the same assembly. Private/protected nested types are not
        // reachable from there; skip them (they fall back to reflection).
        if (!IsAccessibleToGeneratedCode(type))
        {
            return null;
        }

        var properties = GetGeneratedProperties(type);
        if (properties.Count == 0)
        {
            return null;
        }

        var propertyNames = properties.Select(static p => p.Name).ToList();
        var enumConverters = CollectEnumConverters(properties);

        var fullyQualified = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        return new AccessorModel
        (
            fullyQualified,
            Mangle(fullyQualified),
            string.Join(";", propertyNames),
            string.Join(";", enumConverters)
        );
    }



    /// <summary>
    /// Returns the properties the generator will emit getters for: instance,
    /// non-indexer properties with a get accessor reachable from generated code
    /// in the same assembly. Inherited public/internal properties are included;
    /// a name is emitted once (most-derived wins).
    /// </summary>
    /// <remarks>
    /// This intentionally does not replicate the runtime column-mapping rules
    /// (supported types, <c>[NotMapped]</c>, <c>[Column]</c> renames). The
    /// registry is keyed by property name and the runtime only ever looks up
    /// the properties it decides to map, so emitting a superset is harmless and
    /// keeps the duplicated attribute-reading surface to a minimum — the
    /// mapping decisions stay single-sourced in the runtime <c>TypeMap</c>.
    /// </remarks>
    private static List<IPropertySymbol> GetGeneratedProperties(INamedTypeSymbol type)
    {
        var properties = new List<IPropertySymbol>();
        var seen = new HashSet<string>();

        for (var current = type; current is not null; current = current.BaseType)
        {
            foreach (var property in current.GetMembers().OfType<IPropertySymbol>())
            {
                if (property.IsStatic
                    || property.IsIndexer
                    || property.GetMethod is null
                    || property.Type.IsRefLikeType)
                {
                    continue;
                }

                if (!IsReachableAccessibility(property.GetMethod.DeclaredAccessibility))
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



    /// <summary>
    /// Produces one <c>enumFqn,underlyingFqn,mangledEnum</c> entry per distinct
    /// enum type appearing (directly or as <see cref="System.Nullable{T}"/>)
    /// among the generated properties. Mirrors the runtime, which converts enum
    /// column values to their underlying integral type before writing.
    /// </summary>
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
        // Reachable from another top-level class in the same (consumer)
        // assembly: public, internal, or protected internal. Private,
        // protected, and private-protected are not.
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
        AppendModuleInitializer(builder, model.FullyQualifiedName, properties, enumConverters);

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
        string typeFullyQualified,
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
                $"            global::Wolfgang.Etl.SqlBulkCopy.GeneratedAccessorRegistry.Register(typeof({typeFullyQualified}), \"{property}\", Get_{property});"
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

        builder.AppendLine("        }");
    }



    private sealed record AccessorModel
    (
        string FullyQualifiedName,
        string MangledName,
        string PropertyNamesJoined,
        string EnumConvertersJoined
    );
}
