using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

// Fixtures for #601: public properties whose type is not a supported column type
// are skipped, and both mapping paths report their names.

/// <summary>
/// A <c>[BulkCopyable]</c> type with two unsupported properties (a <c>uint</c> and a
/// <see cref="Uri"/>), a <c>[NotMapped]</c> one that must not be reported, and a
/// collection of a struct that is neither a column nor a nested table.
/// </summary>
[BulkCopyable]
[Table("SkippedProperties")]
[ExcludeFromCodeCoverage]
public sealed class BulkCopyableSkippedPropertyFixture
{
    public int Id { get; set; }

    public uint Counter { get; set; }

    public Uri? Link { get; set; }

    [NotMapped]
    public Uri? IgnoredLink { get; set; }

    public List<int> Numbers { get; set; } = new();
}



/// <summary>Reflection-path twin of <see cref="BulkCopyableSkippedPropertyFixture"/>.</summary>
[Table("SkippedProperties")]
[ExcludeFromCodeCoverage]
public sealed class PlainSkippedPropertyFixture
{
    public int Id { get; set; }

    public uint Counter { get; set; }

    public Uri? Link { get; set; }

    [NotMapped]
    public Uri? IgnoredLink { get; set; }

    public List<int> Numbers { get; set; } = new();
}



/// <summary>A nested child with one unsupported property.</summary>
[Table("SkippedChildren")]
[ExcludeFromCodeCoverage]
public sealed class SkippedPropertyChild
{
    public int ParentId { get; set; }

    public ulong Big { get; set; }
}



/// <summary>
/// A parent that is fully supported itself and holds two collections of the same
/// child type, so the child warning must be logged once, not once per collection.
/// </summary>
[Table("SkippedParents")]
[ExcludeFromCodeCoverage]
public sealed class SkippedPropertyParent
{
    public int Id { get; set; }

    public List<SkippedPropertyChild> First { get; set; } = new();

    public List<SkippedPropertyChild> Second { get; set; } = new();
}



/// <summary>
/// A <c>[NotMapped]</c> container: its properties are never columns, so nothing on it
/// is reported as skipped.
/// </summary>
[NotMapped]
[ExcludeFromCodeCoverage]
public sealed class NotMappedContainerWithScalar
{
    public uint Ignored { get; set; }

    public List<SkippedPropertyChild> Children { get; set; } = new();
}
