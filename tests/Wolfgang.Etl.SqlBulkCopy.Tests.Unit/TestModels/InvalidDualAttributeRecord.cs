using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A type with both Table and NotMapped — should throw on mapping.
/// </summary>
[ExcludeFromCodeCoverage]
[Table("ShouldFail")]
[NotMapped]
public record InvalidDualAttributeRecord
{
    public int Id { get; init; }
}
