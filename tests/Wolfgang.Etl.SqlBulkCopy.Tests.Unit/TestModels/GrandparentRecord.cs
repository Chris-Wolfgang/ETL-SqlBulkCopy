using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("GrandparentRecords")]
public record GrandparentRecord
{
    public int GrandparentId { get; init; }

    public IList<IntermediateRecord> Children { get; init; } = new List<IntermediateRecord>();
}
