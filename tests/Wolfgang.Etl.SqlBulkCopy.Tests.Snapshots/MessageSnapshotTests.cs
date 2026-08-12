using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Runtime.CompilerServices;
using VerifyTests;
using VerifyXunit;
using Wolfgang.Etl.SqlBulkCopy;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Snapshots;

/// <summary>
/// Snapshot coverage for the library's inspectable text output: the SQL command
/// text it generates and the exception messages it surfaces.
/// </summary>
/// <remarks>
/// <para>
/// These are the outputs a targeted assertion tends to under-specify. A unit test
/// asserting <c>Contains("TRUNCATE TABLE")</c> passes even if the identifier
/// quoting or schema qualification silently changes; a snapshot pins the whole
/// string.
/// </para>
/// <para>
/// This also covers a gap mutation testing could not: the Stryker pass under #163
/// left 43 surviving <c>String</c> mutants — exception and message text that no
/// unit test distinguished. Snapshots are the right tool for that shape of
/// output, which is why #92 pairs with #163 rather than duplicating it.
/// </para>
/// </remarks>
public class MessageSnapshotTests
{
    // Redirect Verify's snapshot files into Snapshots/ per the #92 AC.
    //
    // Internal (not public) so xUnit1013 does not flag it as an unmarked test
    // method. Mirrors ETL-DbClient.Tests.Snapshots, including the reason it
    // ignores Verify's own `sourceFile` ([CallerFilePath]) argument: CI sets
    // ContinuousIntegrationBuild + <PathMap>, which rewrites embedded source
    // paths to a fictional `/_/...` root, and Directory.CreateDirectory would
    // then fail trying to create `/_` under the filesystem root.
    // AppContext.BaseDirectory is the real runtime output directory and is
    // never rewritten by PathMap.
    [ModuleInitializer]
    internal static void Init() => Verifier.DerivePathInfo
    (
        (_, _, type, method) => new PathInfo
        (
            directory: Path.Combine(ResolveProjectDirectory(), "Snapshots"),
            typeName: type.Name,
            methodName: method.Name
        )
    );



    private static string ResolveProjectDirectory()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            if (dir.GetFiles("*.csproj").Length > 0)
            {
                return dir.FullName;
            }
        }

        throw new InvalidOperationException
        (
            $"Could not locate the Tests.Snapshots project directory by walking up from '{AppContext.BaseDirectory}'."
        );
    }



    [Table("Customers", Schema = "dbo")]
    private sealed class Customer
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }



    [Table("Weird]Name")]
    private sealed class BracketedTable
    {
        public int Id { get; set; }
    }



    private sealed class NoTableAttribute
    {
        public int Id { get; set; }
    }



    private sealed class Validatable
    {
        public int Id { get; set; }

        [Required]
        public string Name { get; set; } = string.Empty;

        [Range(1, 100)]
        public int Quantity { get; set; }
    }



    [Fact]
    public Task Qualified_table_names_render_consistently()
    {
        // Pins schema qualification, bracket quoting, and the ]]-escaping of a
        // bracket inside an identifier — the last of which is a SQL-injection-
        // adjacent detail that a Contains() assertion would not notice changing.
        var rendered = string.Join
        (
            Environment.NewLine,
            "with schema:      " + TypeMap.Create(typeof(Customer)).QualifiedTableName,
            "no schema:        " + TypeMap.Create(typeof(NoTableAttribute)).QualifiedTableName,
            "bracket in name:  " + TypeMap.Create(typeof(BracketedTable)).QualifiedTableName,
            "schema override:  " + TypeMap.Create(typeof(Customer), "other", "Renamed").QualifiedTableName
        );

        return Verifier.Verify(rendered);
    }



    [Fact]
    public Task Validation_exception_message_renders_consistently()
    {
        var results = new List<ValidationResult>
        {
            new("The Name field is required.", new[] { nameof(Validatable.Name) }),
            new("The field Quantity must be between 1 and 100.", new[] { nameof(Validatable.Quantity) })
        };

        var single = new SqlBulkCopyValidationException
        (
            new Validatable { Id = 1 },
            new List<ValidationResult> { results[0] }
        );

        var multiple = new SqlBulkCopyValidationException(new Validatable { Id = 2 }, results);

        // Both arities: the message pluralizes, and pluralization is exactly the
        // kind of formatting that drifts unnoticed.
        var rendered = string.Join
        (
            Environment.NewLine,
            "one failure:   " + single.Message,
            "two failures:  " + multiple.Message
        );

        return Verifier.Verify(rendered);
    }



    [Fact]
    public Task Validation_exception_message_is_culture_invariant()
    {
        // Guards the same property CultureInvarianceTests asserts for column
        // mappings, but for the message text: a Turkish locale must not change
        // casing or number formatting in the rendered message.
        var original = CultureInfo.CurrentCulture;

        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");

            var exception = new SqlBulkCopyValidationException
            (
                new Validatable { Id = 1 },
                new List<ValidationResult> { new("The Name field is required.") }
            );

            return Verifier.Verify(exception.Message);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }
}
