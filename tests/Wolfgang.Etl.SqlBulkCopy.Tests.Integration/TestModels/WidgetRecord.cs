using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration.TestModels;

[ExcludeFromCodeCoverage]
[Table("Widgets", Schema = "dbo")]
public record WidgetRecord
{
    public int Id { get; init; }

    [Column("WidgetName")]
    public string Name { get; init; } = string.Empty;

    public decimal Price { get; init; }
}
