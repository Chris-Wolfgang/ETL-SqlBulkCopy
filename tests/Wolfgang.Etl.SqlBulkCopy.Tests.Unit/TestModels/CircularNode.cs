using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Self-referential type — should be rejected by cycle detection in TypeMap.Create.
/// </summary>
[Table("CircularNodes")]
public record CircularNode
{
    public int Id { get; init; }

    public IList<CircularNode> Children { get; init; } = new List<CircularNode>();
}
