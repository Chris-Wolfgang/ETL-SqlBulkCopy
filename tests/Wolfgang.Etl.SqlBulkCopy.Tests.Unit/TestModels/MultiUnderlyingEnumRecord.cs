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

    public ByteKind ByteValue { get; init; }

    public ShortKind ShortValue { get; init; }

    public LongKind LongValue { get; init; }
}
