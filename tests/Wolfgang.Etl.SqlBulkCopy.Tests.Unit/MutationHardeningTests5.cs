using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Fifth batch of mutation-hardening tests (#414): schema resolution from <c>[Table]</c> on
/// the generated-descriptor and reflection paths. Each test uses a probe type no other
/// test maps, so the <see cref="TypeMap"/> cache cannot hand back a map built before the
/// mutant was active.
/// </summary>
public class MutationHardeningTests5
{
    [Fact]
    public void Create_when_type_has_Table_schema_and_no_override_uses_the_attribute_schema()
    {
        var map = TypeMap.Create(typeof(GeneratedSchemaProbe));

        Assert.Equal("sales", map.SchemaName);
        Assert.True(map.IsMappedToTable);
        Assert.Equal("[sales].[Gadgets]", map.QualifiedTableName);
    }



    [Fact]
    public void Create_when_schema_override_is_given_prefers_it_over_the_attribute_schema()
    {
        var map = TypeMap.Create(typeof(GeneratedSchemaOverrideProbe), schemaName: "archive");

        Assert.Equal("archive", map.SchemaName);
        Assert.Equal("[archive].[Gadgets]", map.QualifiedTableName);
    }



    [Fact]
    public void Create_when_unmarked_type_has_Table_schema_and_no_override_uses_the_attribute_schema()
    {
        var map = TypeMap.Create(typeof(ReflectedSchemaProbe));

        Assert.Equal("ops", map.SchemaName);
        Assert.Equal("[ops].[Gizmos]", map.QualifiedTableName);
    }
}
