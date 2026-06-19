using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[Table("EnumRecords")]
public record EnumRecord
{
    public int Id { get; init; }

    public Status Status { get; init; }
}
