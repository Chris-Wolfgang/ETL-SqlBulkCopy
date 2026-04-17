using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("ParentWithNullChildren")]
public class ParentWithNullChildren
{
    public int ParentId { get; set; }

    public List<ChildRecord> Children { get; set; } = null!;
}
