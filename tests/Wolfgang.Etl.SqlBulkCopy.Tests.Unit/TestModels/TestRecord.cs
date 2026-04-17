using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("TestRecords", Schema = "dbo")]
public record TestRecord
{
    public int Id { get; init; }

    [Column("FullName")]
    public string Name { get; init; } = string.Empty;

    public decimal Amount { get; init; }

    [NotMapped]
    public string Ignored { get; init; } = string.Empty;
}
