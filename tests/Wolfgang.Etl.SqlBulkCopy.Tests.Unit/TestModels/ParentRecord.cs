using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("ParentRecords")]
public record ParentRecord
{
    public int ParentId { get; init; }

    public string Name { get; init; } = string.Empty;

    public IList<ChildRecord> Children { get; init; } = new List<ChildRecord>();
}
