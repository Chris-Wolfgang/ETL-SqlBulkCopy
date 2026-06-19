using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[Table("ParentsWithNotMappedChildren")]
public record ParentWithNotMappedChildren
{
    public int ParentId { get; init; }

    public IList<NotMappedChild> Children { get; init; } = new List<NotMappedChild>();
}
