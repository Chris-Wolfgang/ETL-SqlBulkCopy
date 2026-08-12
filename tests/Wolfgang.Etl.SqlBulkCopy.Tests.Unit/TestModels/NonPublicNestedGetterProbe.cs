using System.Collections.Generic;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A <c>[BulkCopyable]</c> type whose nested collection property has a private
/// getter. The generator cannot emit an accessor for it, so it must not receive
/// a descriptor — otherwise the descriptor's nested entry would reference a
/// getter that is never registered and <c>NestedTableMap</c> would throw on
/// first load. The nested-table counterpart of <see cref="NonPublicGetterProbe"/>.
/// </summary>
[BulkCopyable]
public sealed class NonPublicNestedGetterProbe
{
    /// <summary>Gets or sets the identifier — an ordinary public column.</summary>
    public int Id { get; set; }

    /// <summary>
    /// Publicly settable, but the getter is private, so generated code cannot
    /// enumerate the children.
    /// </summary>
    public IList<NonPublicNestedChild> Children { private get; set; } = new List<NonPublicNestedChild>();
}



/// <summary>Child row type for <see cref="NonPublicNestedGetterProbe"/>.</summary>
[BulkCopyable]
public sealed class NonPublicNestedChild
{
    /// <summary>Gets or sets the child identifier.</summary>
    public int ChildId { get; set; }
}
