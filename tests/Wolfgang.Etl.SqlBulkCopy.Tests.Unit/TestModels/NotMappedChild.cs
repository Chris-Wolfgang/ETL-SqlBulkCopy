using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A child type explicitly marked NotMapped — collections of this type
/// should NOT be treated as nested tables.
/// </summary>
[ExcludeFromCodeCoverage]
[NotMapped]
public record NotMappedChild
{
    public int Id { get; init; }
}
