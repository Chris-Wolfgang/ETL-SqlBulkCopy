namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A <see cref="short"/>-backed enum with negative members, used to verify
/// TypeMapReader returns a boxed <see cref="short"/> and preserves sign.
/// </summary>
public enum ShortKind : short
{
    Min = -30_000,
    Max = 30_000
}
