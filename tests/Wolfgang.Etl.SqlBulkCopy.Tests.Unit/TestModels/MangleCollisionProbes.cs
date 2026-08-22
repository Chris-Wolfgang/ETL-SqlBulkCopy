// Two [BulkCopyable] types whose fully-qualified names collide under a naive
// "replace every non-alphanumeric character with _" mangling:
//
//   ...TestModels.MangleProbe_A.B   -> ..._TestModels_MangleProbe_A_B
//   ...TestModels.MangleProbe.A_B   -> ..._TestModels_MangleProbe_A_B
//
// Before the hash suffix was added to Mangle, both produced the same AddSource
// hint name AND the same generated class name, so this file failing to compile
// IS the regression test — a duplicate hint name breaks the consumer's build.
// Refs #47.

// ReSharper disable once CheckNamespace -- deliberate sub-namespace under
// TestModels; the test's whole point is the mangling-collision surface.
namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels.MangleProbe_A;

/// <summary>
/// Type <c>B</c> in namespace <c>MangleProbe_A</c>.
/// </summary>
[BulkCopyable]
public sealed class B
{
    /// <summary>Gets or sets the identifier.</summary>
    public int Id { get; set; }
}
