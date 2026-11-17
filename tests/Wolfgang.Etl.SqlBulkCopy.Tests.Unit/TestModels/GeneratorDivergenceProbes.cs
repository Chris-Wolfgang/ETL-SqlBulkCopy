using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

// Probes for generator/reflection divergence (#600, #609).

[BulkCopyable]
public sealed class InternalPropertyProbe
{
    public int Id { get; set; }

    internal string Audit { get; set; } = string.Empty;
}



public sealed class PlainInternalPropertyProbe
{
    public int Id { get; set; }

    internal string Audit { get; set; } = string.Empty;
}



[BulkCopyable]
public sealed class DualAttributePropertyProbe
{
    public int Id { get; set; }

    [NotMapped]
    [Column("renamed")]
    public string Name { get; set; } = string.Empty;
}



[BulkCopyable]
public sealed class KeywordNamedPropertyProbe
{
    public int Id { get; set; }

    public string @class { get; set; } = string.Empty;
}
