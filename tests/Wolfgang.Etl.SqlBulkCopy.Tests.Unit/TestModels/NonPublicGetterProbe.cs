namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A <c>[BulkCopyable]</c> type with a property whose getter is <c>private</c>.
/// The generator cannot emit an accessor for it (a private getter is not
/// reachable from generated code), so the type must not receive a generated
/// descriptor either — otherwise the descriptor would carry a column with no
/// registered getter and <c>ColumnMap</c> would throw on first load. Refs #47.
/// </summary>
[BulkCopyable]
public sealed class NonPublicGetterProbe
{
    /// <summary>Gets or sets the identifier — an ordinary public property.</summary>
    public int Id { get; set; }

    /// <summary>
    /// Publicly settable, but the getter is private, so generated code cannot read it.
    /// </summary>
    public string Secret { private get; set; } = string.Empty;
}
