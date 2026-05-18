using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A type marked with NotMapped — should not map to any table.
/// </summary>
[NotMapped]
public record NotMappedTypeRecord
{
    public int Id { get; init; }
}
