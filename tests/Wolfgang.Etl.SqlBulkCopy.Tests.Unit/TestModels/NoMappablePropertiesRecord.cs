using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A mapped type where all properties are NotMapped — should throw on Create.
/// </summary>
[ExcludeFromCodeCoverage]
[Table("ShouldFail")]
public record NoMappablePropertiesRecord
{
    [NotMapped]
    public int Id { get; init; }

    [NotMapped]
    public string Name { get; init; } = string.Empty;
}
