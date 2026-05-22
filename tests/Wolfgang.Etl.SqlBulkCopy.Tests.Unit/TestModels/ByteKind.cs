namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A <see cref="byte"/>-backed enum, used to verify TypeMapReader returns
/// a boxed <see cref="byte"/> for byte-backed enum columns.
/// </summary>
public enum ByteKind : byte
{
    Zero = 0,
    Hundred = 100
}
