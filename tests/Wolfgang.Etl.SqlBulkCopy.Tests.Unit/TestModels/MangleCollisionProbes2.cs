// See MangleCollisionProbes.cs — this is the colliding counterpart:
// namespace MangleProbe, type A_B.

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels.MangleProbe;

/// <summary>
/// Type <c>A_B</c> in namespace <c>MangleProbe</c>.
/// </summary>
[BulkCopyable]
public sealed class A_B
{
    /// <summary>Gets or sets the identifier.</summary>
    public int Id { get; set; }
}
