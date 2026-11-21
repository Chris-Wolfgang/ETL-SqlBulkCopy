using System.ComponentModel.DataAnnotations.Schema;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

// Used only by MutationHardeningTests5. TypeMap caches per (type, schema, table),
// so a map another test built earlier would hide a mutant of the schema
// resolution. Types no other test touches make each test build its own map.

/// <summary>Marked probe: the schema comes from the generated descriptor.</summary>
[BulkCopyable]
[Table("Gadgets", Schema = "sales")]
public sealed class GeneratedSchemaProbe
{
    public int Id { get; set; }
}



/// <summary>Marked probe for the schema-override path of the generated descriptor.</summary>
[BulkCopyable]
[Table("Gadgets", Schema = "sales")]
public sealed class GeneratedSchemaOverrideProbe
{
    public int Id { get; set; }
}



/// <summary>Unmarked probe: the schema comes from <c>[Table]</c> by reflection.</summary>
[Table("Gizmos", Schema = "ops")]
public sealed class ReflectedSchemaProbe
{
    public int Id { get; set; }
}
