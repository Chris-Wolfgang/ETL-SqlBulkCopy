using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

public enum Status
{
    Active = 1,
    Inactive = 2
}

[ExcludeFromCodeCoverage]
[Table("EnumRecords")]
public record EnumRecord
{
    public int Id { get; init; }

    public Status Status { get; init; }
}
