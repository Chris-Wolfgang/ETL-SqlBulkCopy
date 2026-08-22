using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.ShadowWorkloads;

// ReSharper disable UnusedAutoPropertyAccessor.Global -- WidgetRecord's
// property getters are read by the loader via SqlBulkCopy reflection,
// not from source in this assembly.

/// <summary>
/// Sample record mapped to <c>dbo.Widgets</c> — mirrors the integration-test
/// model so shadow workloads exercise the same attribute-driven mapping path.
/// </summary>
[Table("Widgets", Schema = "dbo")]
public record WidgetRecord
{
    /// <summary>Gets the widget identifier.</summary>
    public int Id { get; init; }



    /// <summary>Gets the widget name (maps to the <c>WidgetName</c> column).</summary>
    [Column("WidgetName")]
    public string Name { get; init; } = string.Empty;



    /// <summary>Gets the widget price.</summary>
    public decimal Price { get; init; }
}

// ReSharper restore UnusedAutoPropertyAccessor.Global
