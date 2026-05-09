using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging.Abstractions;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Tests for the auto-generated record members of <see cref="PostLoadActionParameters"/>.
/// </summary>
public class PostLoadActionParametersTests
{
    private static PostLoadActionParameters CreateSut()
    {
        var idProp = typeof(TestRecord).GetProperty(nameof(TestRecord.Id))!;
        var columns = new List<ColumnMap> { new(idProp, ordinal: 0) };

        return new PostLoadActionParameters
        (
            Connection: null!,
            Transaction: null,
            SchemaName: "dbo",
            TableName: "Test",
            CommandTimeout: 30,
            ColumnMappings: columns,
            Logger: NullLogger.Instance,
            CancellationToken: CancellationToken.None
        );
    }



    [Fact]
    public void Equals_returns_true_when_all_values_match()
    {
        var a = CreateSut();
        var b = a with { };

        Assert.Equal(a, b);
        Assert.True(a == b);
    }



    [Fact]
    public void Equals_returns_false_when_value_differs()
    {
        var a = CreateSut();
        var b = a with { TableName = "Different" };

        Assert.NotEqual(a, b);
        Assert.True(a != b);
    }



    [Fact]
    public void GetHashCode_is_consistent_for_equal_instances()
    {
        var a = CreateSut();
        var b = a with { };

        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }



    [Fact]
    public void ToString_returns_non_empty_string_with_record_name()
    {
        var sut = CreateSut();

        var text = sut.ToString();

        Assert.False(string.IsNullOrWhiteSpace(text));
        Assert.Contains(nameof(PostLoadActionParameters), text, StringComparison.Ordinal);
    }



    [Fact]
    public void With_expression_creates_modified_copy()
    {
        var a = CreateSut();
        var b = a with { CommandTimeout = 120 };

        Assert.Equal(30, a.CommandTimeout);
        Assert.Equal(120, b.CommandTimeout);
        Assert.Equal(a.TableName, b.TableName);
    }



    [Fact]
    public void Properties_return_constructor_values()
    {
        var sut = CreateSut();

        Assert.Equal("dbo", sut.SchemaName);
        Assert.Equal("Test", sut.TableName);
        Assert.Equal(30, sut.CommandTimeout);
        Assert.Single(sut.ColumnMappings);
        Assert.Same(NullLogger.Instance, sut.Logger);
        Assert.Equal(CancellationToken.None, sut.CancellationToken);
        Assert.Null(sut.Transaction);
    }
}
