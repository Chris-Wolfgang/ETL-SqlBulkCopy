using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A type with a property that has both NotMapped and Column attributes — should throw.
/// </summary>
[ExcludeFromCodeCoverage]
[Table("ShouldFail")]
public record InvalidPropertyDualAttributeRecord
{
    [NotMapped]
    [Column("BadColumn")]
    public int Id { get; init; }

    public string Name { get; init; } = string.Empty;
}
