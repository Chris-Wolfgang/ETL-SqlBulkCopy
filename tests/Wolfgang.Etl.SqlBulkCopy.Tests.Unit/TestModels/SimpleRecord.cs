namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A simple type with no Table attribute — uses type name as table name.
/// </summary>
public record SimpleRecord
{
    public int Id { get; init; }

    public string Value { get; init; } = string.Empty;
}
