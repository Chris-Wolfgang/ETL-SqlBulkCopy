namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// An enum deliberately never marked <c>[BulkCopyable]</c> and never registered
/// with <c>GeneratedAccessorRegistry</c>, so the descriptor-based
/// <c>ColumnMap</c> constructor reaches its "no source-generated enum converter"
/// defensive branch.
/// </summary>
public enum UnregisteredProbeEnum
{
    None = 0,
    One = 1
}
