namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A <see cref="long"/>-backed enum with a member outside the <see cref="int"/>
/// range, used to verify TypeMapReader returns a boxed <see cref="long"/>.
/// </summary>
public enum LongKind : long
{
    Zero = 0,
    Huge = 9_000_000_000L
}
