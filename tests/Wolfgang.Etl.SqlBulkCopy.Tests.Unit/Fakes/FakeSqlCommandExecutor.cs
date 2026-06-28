using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;

/// <summary>
/// Test fake for <see cref="ISqlCommandExecutor"/> that records every
/// command text + timeout pair without touching a SQL Server. Lets unit
/// tests verify the loader's pre/post-action orchestration (switch on
/// <c>PreAction</c> / <c>PostAction</c>, command-text construction) without
/// the integration-test rig.
/// </summary>
internal sealed class FakeSqlCommandExecutor : ISqlCommandExecutor
{
    private readonly List<(string CommandText, int CommandTimeout)> _executedCommands = new();



    /// <summary>
    /// Every command issued to this executor, in the order it was issued.
    /// </summary>
    public IReadOnlyList<(string CommandText, int CommandTimeout)> ExecutedCommands => _executedCommands;



    /// <inheritdoc />
    public Task ExecuteNonQueryAsync(string commandText, int commandTimeout, CancellationToken cancellationToken)
    {
        _executedCommands.Add((commandText, commandTimeout));
        return Task.CompletedTask;
    }
}
