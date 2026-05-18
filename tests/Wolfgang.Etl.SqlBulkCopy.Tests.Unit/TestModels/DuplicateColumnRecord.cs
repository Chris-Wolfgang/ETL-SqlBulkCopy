using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Two properties resolving to the same column name (case-insensitive) so the
/// duplicate detection in <c>BuildColumnMaps</c> can be exercised.
/// </summary>
public record DuplicateColumnRecord
{
    [Column("Name")]
    public string First { get; init; } = string.Empty;

    [Column("NAME")]
    public string Second { get; init; } = string.Empty;
}
