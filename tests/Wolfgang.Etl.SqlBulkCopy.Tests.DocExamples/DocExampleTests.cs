using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.DocExamples;

/// <summary>
/// Guards against XML-doc example rot: every <c>&lt;example&gt;&lt;code&gt;</c>
/// snippet in the library source is extracted, wrapped in a synthetic harness,
/// and compiled with Roslyn. A snippet that no longer compiles (because the API
/// it demonstrates changed) fails the build.
/// </summary>
public class DocExampleTests
{
    [Fact]
    public void Every_xml_doc_example_snippet_compiles()
    {
        var srcDir = LocateLibrarySourceDirectory();
        var examples = ExtractExamples(srcDir).ToList();

        // Sanity: the scanner must actually find the source. (If the library
        // ever ships zero <example> blocks this still passes — there is simply
        // nothing to compile — but a broken scan that found no .cs files is a
        // real failure.)
        Assert.NotEmpty(Directory.GetFiles(srcDir, "*.cs", SearchOption.AllDirectories));

        var references = BuildMetadataReferences();

        foreach (var (file, snippet) in examples)
        {
            var source = WrapInHarness(snippet);
            var tree = CSharpSyntaxTree.ParseText(source);
            var compilation = CSharpCompilation.Create
            (
                assemblyName: "DocExampleSnippet",
                syntaxTrees: new[] { tree },
                references: references,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
            );

            var errors = compilation
                .GetDiagnostics()
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .Select(d => d.ToString())
                .ToList();

            Assert.True
            (
                errors.Count == 0,   // not Assert.Empty: the custom message below carries the harness source

                $"XML-doc <example> in {Path.GetFileName(file)} does not compile:\n" +
                string.Join("\n", errors) +
                "\n\n--- harness source ---\n" + source
            );
        }
    }

    // Walk up from the test binary's directory to the repo root (identified by
    // the src project folder). Deliberately avoids [CallerFilePath], which
    // resolves to a deterministic '/_/...' path under CI and can't be read.
    private static string LocateLibrarySourceDirectory()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "src", "Wolfgang.Etl.SqlBulkCopy");
            if (Directory.Exists(candidate))
            {
                return candidate;
            }

            dir = dir.Parent;
        }

        throw new DirectoryNotFoundException
        (
            "Could not locate src/Wolfgang.Etl.SqlBulkCopy by walking up from " + AppContext.BaseDirectory
        );
    }

    private static readonly Regex ExampleBlock = new
    (
        @"<example>\s*(?:<code>)?(?<code>.*?)(?:</code>)?\s*</example>",
        RegexOptions.Singleline | RegexOptions.Compiled
    );

    private static IEnumerable<(string File, string Snippet)> ExtractExamples(string srcDir)
    {
        foreach (var file in Directory.GetFiles(srcDir, "*.cs", SearchOption.AllDirectories))
        {
            var text = File.ReadAllText(file);
            if (!text.Contains("<example>", StringComparison.Ordinal))
            {
                continue;
            }

            // Strip the leading `///` doc-comment markers so we recover the raw
            // snippet text, then un-escape the XML entities the doc comment used.
            var stripped = string.Join
            (
                "\n",
                text.Split('\n').Select(line => Regex.Replace(line, @"^\s*///\s?", string.Empty))
            );

            foreach (Match m in ExampleBlock.Matches(stripped))
            {
                var code = m.Groups["code"].Value
                    .Replace("&lt;", "<", StringComparison.Ordinal)
                    .Replace("&gt;", ">", StringComparison.Ordinal)
                    .Replace("&amp;", "&", StringComparison.Ordinal)
                    .Trim();

                if (code.Length > 0)
                {
                    yield return (file, code);
                }
            }
        }
    }

    // Synthetic harness supplying the context these examples assume (a domain
    // record, an open connection, an async source, a token). Extend this as new
    // snippets introduce new contextual symbols.
    private static string WrapInHarness(string snippet)
    {
        var sb = new StringBuilder();
        sb.AppendLine("using System;");
        sb.AppendLine("using System.Collections.Generic;");
        sb.AppendLine("using System.Threading;");
        sb.AppendLine("using System.Threading.Tasks;");
        sb.AppendLine("using Microsoft.Data.SqlClient;");
        sb.AppendLine("using Wolfgang.Etl.SqlBulkCopy;");
        sb.AppendLine("internal static class DocExampleHarness");
        sb.AppendLine("{");
        sb.AppendLine("    internal sealed record Person { public int Id { get; init; } public string Name { get; init; } = string.Empty; }");
        sb.AppendLine("    internal static async Task RunAsync(SqlConnection connection, IAsyncEnumerable<Person> items, IReadOnlyList<IAsyncEnumerable<Person>> files, CancellationToken cancellationToken)");
        sb.AppendLine("    {");
        sb.AppendLine(snippet);
        sb.AppendLine("    }");
        sb.AppendLine("}");
        return sb.ToString();
    }

    private static IReadOnlyList<MetadataReference> BuildMetadataReferences()
    {
        var refs = new List<MetadataReference>();

        // The shared framework (System.*, netstandard, etc.).
        var tpa = (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") ?? string.Empty;
        foreach (var path in tpa.Split(Path.PathSeparator).Where(p => p.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)))
        {
            refs.Add(MetadataReference.CreateFromFile(path));
        }

        // The library + Microsoft.Data.SqlClient (both used by the snippets),
        // resolved from the loaded assemblies so versions match the build.
        refs.Add(MetadataReference.CreateFromFile(typeof(SqlBulkCopyLoader<>).Assembly.Location));
        refs.Add(MetadataReference.CreateFromFile(typeof(Microsoft.Data.SqlClient.SqlConnection).Assembly.Location));

        return refs;
    }
}
