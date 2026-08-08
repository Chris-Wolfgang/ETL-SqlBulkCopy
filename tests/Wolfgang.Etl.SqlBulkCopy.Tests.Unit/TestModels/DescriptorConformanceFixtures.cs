using System.Collections.Generic;
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



// Nested-table graph: a marked parent with a collection of a marked child. Both
// the parent and child must be [BulkCopyable] for the descriptor path to apply
// (recursive eligibility), so the whole graph maps reflection-free.

/// <summary>Marked child element type for the nested-table conformance test.</summary>
[BulkCopyable]
[Table("Children")]
public sealed class BulkCopyableChildFixture
{
    public int ParentId { get; set; }

    public string Value { get; set; } = string.Empty;
}



/// <summary>Marked parent with a nested collection of <see cref="BulkCopyableChildFixture"/>.</summary>
[BulkCopyable]
[Table("Parents")]
public sealed class BulkCopyableParentFixture
{
    public int Id { get; set; }

    public IEnumerable<BulkCopyableChildFixture> Children { get; set; } = new List<BulkCopyableChildFixture>();
}



/// <summary>Reflection-path twin of <see cref="BulkCopyableChildFixture"/>.</summary>
[Table("Children")]
public sealed class PlainChildFixture
{
    public int ParentId { get; set; }

    public string Value { get; set; } = string.Empty;
}



/// <summary>Reflection-path twin of <see cref="BulkCopyableParentFixture"/>.</summary>
[Table("Parents")]
public sealed class PlainParentFixture
{
    public int Id { get; set; }

    public IEnumerable<PlainChildFixture> Children { get; set; } = new List<PlainChildFixture>();
}
