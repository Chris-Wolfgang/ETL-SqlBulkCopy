using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A type marked NotMapped — has nested table children but no direct columns.
/// </summary>
[ExcludeFromCodeCoverage]
[NotMapped]
public record NotMappedWithChildrenRecord
{
    [NotMapped]
    public int Id { get; init; }

    public IList<ChildRecord> Children { get; init; } = new List<ChildRecord>();
}
