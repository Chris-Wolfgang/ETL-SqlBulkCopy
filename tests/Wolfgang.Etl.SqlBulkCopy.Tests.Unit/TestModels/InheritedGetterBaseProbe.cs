namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Base type whose <see cref="Inherited"/> property is inherited by
/// <see cref="InheritedGetterDerivedProbe"/> — so the property's
/// <c>DeclaringType</c> is this base, not the mapped (derived) type.
/// </summary>
public class InheritedGetterBaseProbe
{
    public int Inherited { get; set; }
}
