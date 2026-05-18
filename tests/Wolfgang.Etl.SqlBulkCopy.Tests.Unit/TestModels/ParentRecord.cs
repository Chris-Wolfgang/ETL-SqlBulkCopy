using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[Table("ParentRecords")]
public record ParentRecord
{
    public int ParentId { get; init; }

    public string Name { get; init; } = string.Empty;

    public IList<ChildRecord> Children { get; init; } = new List<ChildRecord>();
}
