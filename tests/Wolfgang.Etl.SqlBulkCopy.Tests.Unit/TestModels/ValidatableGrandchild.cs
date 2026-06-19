using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Second-level nested child of <see cref="ValidatableChild"/>, used to
/// verify nested-child validation recurses beyond the first level.
/// </summary>
[Table("Grandchildren")]
public record ValidatableGrandchild
{
    public int Id { get; init; }

    [Required]
    public string Label { get; init; } = string.Empty;
}
