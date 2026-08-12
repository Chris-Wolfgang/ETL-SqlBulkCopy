using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// An unsigned-backed enum. SQL Server has no unsigned integer types, so
/// <c>TypeMap</c> deliberately omits <c>sbyte</c>/<c>ushort</c>/<c>uint</c>/<c>ulong</c>
/// from its supported column types.
/// </summary>
public enum UnsignedBackedKind : uint
{
    /// <summary>The default value.</summary>
    None = 0,

    /// <summary>A non-default value.</summary>
    First = 1
}



/// <summary>
/// Pairs a mappable column with one whose enum has an unsupported underlying
/// type, so a test can prove the underlying-type gate rejects only the latter.
/// </summary>
[Table("UnsupportedEnumRecords")]
public sealed class UnsupportedEnumRecord
{
    /// <summary>Gets or sets the identifier — maps normally.</summary>
    public int Id { get; set; }

    /// <summary>Gets or sets a uint-backed enum — must not map to a column.</summary>
    public UnsignedBackedKind Unsigned { get; set; }
}
