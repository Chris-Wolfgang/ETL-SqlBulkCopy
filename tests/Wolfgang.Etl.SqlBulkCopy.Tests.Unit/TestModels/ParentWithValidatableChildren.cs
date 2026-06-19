using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Root type for nested-child validation tests. The child collection
/// element type (<see cref="ValidatableChild"/>) carries DataAnnotation
/// attributes so individual children can be invalid independently of the root.
/// </summary>
[Table("Parents")]
public record ParentWithValidatableChildren
{
    public int Id { get; init; }

    public IList<ValidatableChild> Children { get; init; } = new List<ValidatableChild>();
}
