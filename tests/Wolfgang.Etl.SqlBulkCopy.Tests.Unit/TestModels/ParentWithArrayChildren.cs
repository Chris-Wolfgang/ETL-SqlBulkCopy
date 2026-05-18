using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[Table("ParentWithArrayChildren")]
public record ParentWithArrayChildren
{
    public int ParentId { get; init; }

    public string Name { get; init; } = string.Empty;

    public ChildRecord[] Children { get; init; } = System.Array.Empty<ChildRecord>();
}
