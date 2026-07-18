using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Wolfgang.Etl.SqlBulkCopy.SourceGenerator;

/// <summary>
/// Emits compile-time property getters for every type marked
/// <c>[BulkCopyable]</c> and registers them with
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

        var propertyNames = GetGeneratedGetterPropertyNames(type);
        if (propertyNames.Count == 0)
        {
            return null;
        }

        var fullyQualified = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        return new AccessorModel
        (
            fullyQualified,
            Mangle(fullyQualified),
            string.Join(";", propertyNames)
        );
    }



    /// <summary>
    /// Returns the names of the properties the generator will emit getters for:
    /// instance, non-indexer properties with a get accessor reachable from
    /// generated code in the same assembly. Inherited public/internal
    /// properties are included; a name is emitted once (most-derived wins).
    /// </summary>
    /// <remarks>
    /// This intentionally does not replicate the runtime column-mapping rules
    /// (supported types, <c>[NotMapped]</c>, <c>[Column]</c> renames). The
    /// registry is keyed by property name and the runtime only ever looks up
    /// the properties it decides to map, so emitting a superset is harmless and
    /// keeps the duplicated attribute-reading surface to a minimum — the
    /// mapping decisions stay single-sourced in the runtime <c>TypeMap</c>.
    /// </remarks>
    private static List<string> GetGeneratedGetterPropertyNames(INamedTypeSymbol type)
    {
        var names = new List<string>();
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
                    names.Add(property.Name);
                }
            }
        }

        return names;
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

        var builder = new StringBuilder();
        builder.AppendLine("// <auto-generated/>");
        builder.AppendLine("#nullable enable");
        builder.AppendLine("#if NET5_0_OR_GREATER");
        builder.AppendLine("namespace Wolfgang.Etl.SqlBulkCopy.Generated");
        builder.AppendLine("{");
        builder.AppendLine($"    internal static class BulkCopyAccessors_{model.MangledName}");
        builder.AppendLine("    {");

        foreach (var property in properties)
        {
            builder.AppendLine
            (
                $"        internal static object? Get_{property}(object instance) => (({model.FullyQualifiedName})instance).{property};"
            );
        }

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

        builder.AppendLine("        }");
        builder.AppendLine("    }");
        builder.AppendLine("}");
        builder.AppendLine("#endif");

        context.AddSource($"BulkCopyAccessors_{model.MangledName}.g.cs", builder.ToString());
    }



    private sealed record AccessorModel
    (
        string FullyQualifiedName,
        string MangledName,
        string PropertyNamesJoined
    );
}
