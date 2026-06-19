using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures;

/// <summary>
/// Marker collection that shares a single <see cref="SqlServerFixture"/>
/// across all tests decorated with <c>[Collection("SqlServer")]</c>.
/// </summary>
[CollectionDefinition("SqlServer")]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>
{
}
