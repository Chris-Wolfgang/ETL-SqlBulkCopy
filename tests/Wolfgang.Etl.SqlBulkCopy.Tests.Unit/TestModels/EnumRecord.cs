using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("EnumRecords")]
public record EnumRecord
{
    public int Id { get; init; }

    public Status Status { get; init; }
}
