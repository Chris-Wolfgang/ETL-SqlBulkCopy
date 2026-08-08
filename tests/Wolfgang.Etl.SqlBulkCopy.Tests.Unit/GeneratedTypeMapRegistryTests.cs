using System;
using System.Collections.Generic;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class GeneratedTypeMapRegistryTests
{
    [Fact]
    public void Register_when_type_is_null_throws_ArgumentNullException()
    {
        var descriptor = new GeneratedTypeDescriptor
        (
            schemaName: null,
            tableName: "T",
            columns: new List<GeneratedColumnDescriptor>(),
            nestedTables: new List<GeneratedNestedTableDescriptor>()
        );

        var ex = Assert.Throws<ArgumentNullException>
        (
            () => GeneratedTypeMapRegistry.Register(null!, descriptor)
        );

        Assert.Equal("type", ex.ParamName);
    }



    [Fact]
    public void Register_when_descriptor_is_null_throws_ArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>
        (
            () => GeneratedTypeMapRegistry.Register(typeof(object), null!)
        );

        Assert.Equal("descriptor", ex.ParamName);
    }
}
