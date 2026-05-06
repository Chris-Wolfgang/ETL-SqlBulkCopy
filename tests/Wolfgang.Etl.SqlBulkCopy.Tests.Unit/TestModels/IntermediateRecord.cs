using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("IntermediateRecords")]
public record IntermediateRecord
{
    public int IntermediateId { get; init; }

    public IList<GrandchildRecord> Grandchildren { get; init; } = new List<GrandchildRecord>();
}
