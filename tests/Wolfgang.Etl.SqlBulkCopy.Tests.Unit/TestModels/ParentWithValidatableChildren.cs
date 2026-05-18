using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Root type for nested-child validation tests. The child collection
/// element type carries DataAnnotation attributes so individual children
/// can be invalid independently of the root.
/// </summary>
[ExcludeFromCodeCoverage]
[Table("Parents")]
public record ParentWithValidatableChildren
{
    public int Id { get; init; }

    public IList<ValidatableChild> Children { get; init; } = new List<ValidatableChild>();
}



[ExcludeFromCodeCoverage]
[Table("Children")]
public record ValidatableChild
{
    public int Id { get; init; }

    [Required]
    [StringLength(50)]
    public string Name { get; init; } = string.Empty;

    [Range(0, 100)]
    public int Quantity { get; init; }

    public IList<ValidatableGrandchild> Grandchildren { get; init; } = new List<ValidatableGrandchild>();
}



[ExcludeFromCodeCoverage]
[Table("Grandchildren")]
public record ValidatableGrandchild
{
    public int Id { get; init; }

    [Required]
    public string Label { get; init; } = string.Empty;
}
