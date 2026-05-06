using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Parent with a property typed directly as <see cref="IEnumerable{T}"/>
/// (not <see cref="List{T}"/>). Tests that nested-table detection finds
/// the element type when the property type IS the IEnumerable interface.
/// </summary>
[ExcludeFromCodeCoverage]
[Table("ParentsWithIEnumerable")]
public record ParentWithIEnumerableChildren
{
    public int ParentId { get; init; }

    public IEnumerable<ChildRecord> Children { get; init; } = new List<ChildRecord>();
}
