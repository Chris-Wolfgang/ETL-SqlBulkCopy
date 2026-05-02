using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

[ExcludeFromCodeCoverage]
[Table("NullableRecords")]
public record NullablePropertiesRecord
{
    public int Id { get; init; }

    public int? NullableInt { get; init; }

    public DateTime? NullableDateTime { get; init; }

    public string? NullableString { get; init; }
}
