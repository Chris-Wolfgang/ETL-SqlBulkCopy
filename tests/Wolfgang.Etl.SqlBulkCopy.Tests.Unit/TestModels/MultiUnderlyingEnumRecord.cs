using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Record with enum columns whose underlying integral types vary
/// (byte / short / long), so TypeMapReader's enum conversion can be
/// verified to return the correct boxed underlying type — not just int.
/// </summary>
[Table("MultiUnderlyingEnumRecords")]
public record MultiUnderlyingEnumRecord
{
    public int Id { get; init; }

    public ByteEnum ByteValue { get; init; }

    public ShortEnum ShortValue { get; init; }

    public LongEnum LongValue { get; init; }
}



public enum ByteEnum : byte
{
    Zero = 0,
    Hundred = 100
}



public enum ShortEnum : short
{
    Min = -30_000,
    Max = 30_000
}



public enum LongEnum : long
{
    Zero = 0,
    Huge = 9_000_000_000L
}
