using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("ParentWithArrayChildren")]
public record ParentWithArrayChildren
{
    public int ParentId { get; init; }

    public string Name { get; init; } = string.Empty;

    public ChildRecord[] Children { get; init; } = System.Array.Empty<ChildRecord>();
}
