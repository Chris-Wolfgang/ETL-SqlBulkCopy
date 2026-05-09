using System;
using Microsoft.Data.SqlClient;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration;

/// <summary>
/// Tests for <see cref="SqlBulkCopyWrapper"/> that don't require a live SQL Server.
/// Lives in the Integration project because it relies on instantiating
/// <see cref="SqlConnection"/>, which has dependency quirks on older
/// .NET Framework TFMs that the Unit project supports.
/// Uses the parameterless <c>SqlConnection</c> constructor — no insecure
/// connection-string defaults are encoded in the repo.
/// </summary>
public class SqlBulkCopyWrapperTests
{
    [Fact]
    public void Constructor_when_connection_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyWrapper(null!, SqlBulkCopyOptions.Default, transaction: null)
        );
    }



    [Fact]
    public void DestinationTableName_get_returns_set_value()
    {
        using var connection = new SqlConnection();
        using var sut = new SqlBulkCopyWrapper(connection, SqlBulkCopyOptions.Default, transaction: null);

        sut.DestinationTableName = "[dbo].[Widgets]";

        Assert.Equal("[dbo].[Widgets]", sut.DestinationTableName);
    }



    [Fact]
    public void BatchSize_get_returns_set_value()
    {
        using var connection = new SqlConnection();
        using var sut = new SqlBulkCopyWrapper(connection, SqlBulkCopyOptions.Default, transaction: null);

        sut.BatchSize = 5000;

        Assert.Equal(5000, sut.BatchSize);
    }



    [Fact]
    public void BulkCopyTimeout_get_returns_set_value()
    {
        using var connection = new SqlConnection();
        using var sut = new SqlBulkCopyWrapper(connection, SqlBulkCopyOptions.Default, transaction: null);

        sut.BulkCopyTimeout = 120;

        Assert.Equal(120, sut.BulkCopyTimeout);
    }



    [Fact]
    public void Dispose_does_not_throw()
    {
        using var connection = new SqlConnection();
        var sut = new SqlBulkCopyWrapper(connection, SqlBulkCopyOptions.Default, transaction: null);

        sut.Dispose();
    }
}
