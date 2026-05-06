using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("GrandchildRecords")]
public record GrandchildRecord
{
    public int GrandchildId { get; init; }

    public string Note { get; init; } = string.Empty;
}
