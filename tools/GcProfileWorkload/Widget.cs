using System.ComponentModel.DataAnnotations.Schema;

namespace GcProfileWorkload;

/// <summary>The record shape loaded by the GC-profile workload.</summary>
[Table("Widgets", Schema = "dbo")]
public sealed class Widget
{
    public int Id { get; set; }

    [Column("WidgetName")]
    public string WidgetName { get; set; } = string.Empty;

    public decimal Price { get; set; }
}
