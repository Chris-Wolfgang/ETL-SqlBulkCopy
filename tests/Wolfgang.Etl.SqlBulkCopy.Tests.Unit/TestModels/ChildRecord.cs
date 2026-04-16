using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("ChildRecords")]
public record ChildRecord
{
    public int ChildId { get; init; }

    public string Description { get; init; } = string.Empty;
}
