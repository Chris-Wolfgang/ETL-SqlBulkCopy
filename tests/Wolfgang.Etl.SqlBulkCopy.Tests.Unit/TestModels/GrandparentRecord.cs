using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[Table("GrandparentRecords")]
public record GrandparentRecord
{
    public int GrandparentId { get; init; }

    public IList<IntermediateRecord> Children { get; init; } = new List<IntermediateRecord>();
}
