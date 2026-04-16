using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("ValidatableRecords")]
public record ValidatableRecord
{
    public int Id { get; init; }

    [Required]
    [StringLength(100)]
    public string Name { get; init; } = string.Empty;

    [Range(0, 1000)]
    public int Quantity { get; init; }
}
