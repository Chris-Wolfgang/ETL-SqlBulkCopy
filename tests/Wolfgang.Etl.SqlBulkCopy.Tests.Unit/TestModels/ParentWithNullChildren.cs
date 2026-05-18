using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[Table("ParentWithNullChildren")]
public class ParentWithNullChildren
{
    public int ParentId { get; set; }

    public IList<ChildRecord> Children { get; set; } = null!;
}
