using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[Table("ChildRecords")]
public record ChildRecord
{
    public int ChildId { get; init; }

    public string Description { get; init; } = string.Empty;
}
