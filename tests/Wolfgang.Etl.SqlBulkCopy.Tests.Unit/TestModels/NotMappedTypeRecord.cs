using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A type marked with NotMapped — should not map to any table.
/// </summary>
[ExcludeFromCodeCoverage]
[NotMapped]
public record NotMappedTypeRecord
{
    public int Id { get; init; }
}
