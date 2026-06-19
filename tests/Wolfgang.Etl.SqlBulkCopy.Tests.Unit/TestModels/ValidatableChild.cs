using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Nested child of <see cref="ParentWithValidatableChildren"/>. Carries
/// DataAnnotation attributes so a child can fail validation on its own, and
/// has its own nested collection (<see cref="ValidatableGrandchild"/>) to
/// exercise recursive nested-child validation.
/// </summary>
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
