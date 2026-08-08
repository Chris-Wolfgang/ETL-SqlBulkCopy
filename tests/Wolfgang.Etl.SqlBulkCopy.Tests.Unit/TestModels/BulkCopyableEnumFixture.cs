namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>Int-backed enum, exercised by the generated enum converter.</summary>
public enum GeneratedPriority
{
    Low,
    Medium,
    High,
}



/// <summary>Byte-backed enum — verifies the converter emits the correct
/// underlying integral type (tinyint), not a hard-coded int.</summary>
public enum GeneratedSmallKind : byte
{
    A = 1,
    B = 2,
}



/// <summary>
/// A <c>[BulkCopyable]</c> fixture with enum and nullable-enum columns, so the
/// generator emits enum→underlying converters in addition to property getters.
/// Consumed by the generator and conformance tests.
/// </summary>
[BulkCopyable]
public sealed class BulkCopyableEnumFixture
{
    public int Id { get; set; }

    public GeneratedPriority Priority { get; set; }

    public GeneratedSmallKind Kind { get; set; }

    public GeneratedPriority? MaybePriority { get; set; }

    public string Label { get; set; } = string.Empty;
}
