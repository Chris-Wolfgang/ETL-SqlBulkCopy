using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A child type explicitly marked NotMapped — collections of this type
/// should NOT be treated as nested tables.
/// </summary>
[NotMapped]
public record NotMappedChild
{
    public int Id { get; init; }
}
