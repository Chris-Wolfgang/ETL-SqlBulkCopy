using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration.TestModels;

[Table("Widgets", Schema = "dbo")]
public record WidgetRecord
{
    public int Id { get; init; }

    [Column("WidgetName")]
    public string Name { get; init; } = string.Empty;

    public decimal Price { get; init; }
}
