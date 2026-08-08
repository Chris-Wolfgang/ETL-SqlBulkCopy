namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Derived type used to verify <c>ColumnMap</c> resolves the generated getter
/// registered under the mapped (derived) type for an <em>inherited</em>
/// property — not under the property's base <c>DeclaringType</c>.
/// </summary>
public sealed class InheritedGetterDerivedProbe : InheritedGetterBaseProbe
{
    public int Own { get; set; }
}
