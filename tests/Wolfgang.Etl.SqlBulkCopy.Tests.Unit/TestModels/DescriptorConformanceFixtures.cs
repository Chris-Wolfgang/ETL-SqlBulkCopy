using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

// Plain (unmarked → reflection-path) twins of the [BulkCopyable] fixtures.
// The descriptor conformance test asserts the generated map equals the
// reflection map for a structurally-identical type.

/// <summary>Reflection-path twin of <see cref="BulkCopyableFixture"/>.</summary>
public sealed class PlainFixture
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
}



/// <summary>Reflection-path twin of <see cref="BulkCopyableEnumFixture"/>.</summary>
public sealed class PlainEnumFixture
{
    public int Id { get; set; }

    public GeneratedPriority Priority { get; set; }

    public GeneratedSmallKind Kind { get; set; }

    public GeneratedPriority? MaybePriority { get; set; }

    public string Label { get; set; } = string.Empty;
}



/// <summary>
/// Marked fixture exercising <c>[Table]</c> schema/name and a <c>[Column]</c>
/// rename through the generated descriptor.
/// </summary>
[BulkCopyable]
[Table("Widgets", Schema = "dbo")]
public sealed class BulkCopyableAttributedFixture
{
    [Column("widget_id")]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
}



/// <summary>Reflection-path twin of <see cref="BulkCopyableAttributedFixture"/>.</summary>
[Table("Widgets", Schema = "dbo")]
public sealed class PlainAttributedFixture
{
    [Column("widget_id")]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
}
