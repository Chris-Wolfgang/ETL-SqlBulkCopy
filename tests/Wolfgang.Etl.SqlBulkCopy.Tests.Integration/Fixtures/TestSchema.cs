using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures;

/// <summary>
/// Helpers for creating, querying, and dropping test tables.
/// </summary>
internal static class TestSchema
{
    public static async Task ExecuteAsync(SqlConnection connection, string commandText)
    {
        using var command = connection.CreateCommand();
        command.CommandText = commandText;
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }



    public static async Task<int> CountRowsAsync(SqlConnection connection, string qualifiedTableName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {qualifiedTableName}";
        var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
        return (int)result!;
    }



    public static async Task DropIfExistsAsync(SqlConnection connection, string qualifiedTableName)
    {
        await ExecuteAsync(connection, $"IF OBJECT_ID(N'{qualifiedTableName}', N'U') IS NOT NULL DROP TABLE {qualifiedTableName}").ConfigureAwait(false);
    }
}
