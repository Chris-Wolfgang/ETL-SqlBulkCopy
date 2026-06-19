using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[Table("GrandchildRecords")]
public record GrandchildRecord
{
    public int GrandchildId { get; init; }

    public string Note { get; init; } = string.Empty;
}
