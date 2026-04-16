using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Wolfgang.Etl.Abstractions;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Loads items of type <typeparamref name="TRecord"/> into a SQL Server table
/// using <c>SqlBulkCopy</c> for high-throughput bulk insert operations.
/// </summary>
/// <typeparam name="TRecord">The type of items to load. Must be <c>notnull</c>.</typeparam>
/// <remarks>
/// <para>
/// Maps .NET types to SQL Server tables using <c>System.ComponentModel.DataAnnotations.Schema</c>
/// attributes: <c>[Table]</c>, <c>[Column]</c>, and <c>[NotMapped]</c>.
/// </para>
/// <para>
/// Supports nested collection properties that map to separate child tables,
/// optional pre/post-load actions, and opt-in data validation.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var loader = new SqlBulkCopyLoader&lt;Person&gt;(connection)
/// {
///     BatchSize = 5000,
///     BulkCopyTimeout = 60
/// };
/// await loader.LoadAsync(items, cancellationToken);
/// </code>
/// </example>
public sealed class SqlBulkCopyLoader<TRecord> : LoaderBase<TRecord, SqlBulkCopyReport>
    where TRecord : notnull
{
    private static readonly string OperationName = $"SQL bulk copy loading of {typeof(TRecord).Name}";
    private readonly SqlConnection? _connection;
    private readonly SqlBulkCopyOptions _options;
    private readonly SqlTransaction? _transaction;
    private readonly ILogger _logger;
    private readonly IProgressTimer? _progressTimer;
    private readonly ISqlBulkCopyWrapperFactory? _wrapperFactory;
    private int _progressTimerWired;
    private int _batchSize = 10_000;
    private int _bulkCopyTimeout = 30;
    private int _batchCount;



    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBulkCopyLoader{TRecord}"/> class.
    /// </summary>
    /// <param name="connection">The SQL Server connection.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="connection"/> is <c>null</c>.
    /// </exception>
    public SqlBulkCopyLoader
    (
        SqlConnection connection
    )
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _logger = NullLogger.Instance;
        _options = SqlBulkCopyOptions.Default;
    }



    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBulkCopyLoader{TRecord}"/> class
    /// with diagnostic logging.
    /// </summary>
    /// <param name="connection">The SQL Server connection.</param>
    /// <param name="logger">The logger instance for diagnostic output.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="connection"/> or <paramref name="logger"/> is <c>null</c>.
    /// </exception>
    public SqlBulkCopyLoader
    (
        SqlConnection connection,
        ILogger<SqlBulkCopyLoader<TRecord>> logger
    )
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = SqlBulkCopyOptions.Default;
    }



    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBulkCopyLoader{TRecord}"/> class
    /// with full configuration.
    /// </summary>
    /// <param name="connection">The SQL Server connection.</param>
    /// <param name="options">The bulk copy options.</param>
    /// <param name="transaction">An optional external transaction.</param>
    /// <param name="logger">An optional logger instance for diagnostic output.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="connection"/> is <c>null</c>.
    /// </exception>
    public SqlBulkCopyLoader
    (
        SqlConnection connection,
        SqlBulkCopyOptions options,
        SqlTransaction? transaction,
        ILogger<SqlBulkCopyLoader<TRecord>>? logger = null
    )
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _options = options;
        _transaction = transaction;
        _logger = logger ?? (ILogger)NullLogger.Instance;
    }



    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBulkCopyLoader{TRecord}"/> class
    /// with an injected wrapper factory and progress timer for testing.
    /// </summary>
    /// <param name="wrapperFactory">The factory for creating bulk copy wrappers.</param>
    /// <param name="logger">An optional logger instance.</param>
    /// <param name="timer">The progress timer to inject.</param>
    internal SqlBulkCopyLoader
    (
        ISqlBulkCopyWrapperFactory wrapperFactory,
        ILogger? logger,
        IProgressTimer timer
    )
    {
        _wrapperFactory = wrapperFactory ?? throw new ArgumentNullException(nameof(wrapperFactory));
        _logger = logger ?? (ILogger)NullLogger.Instance;
        _progressTimer = timer ?? throw new ArgumentNullException(nameof(timer));
        _options = SqlBulkCopyOptions.Default;
    }



    /// <summary>
    /// Gets or sets the number of rows in each batch sent to the server.
    /// </summary>
    /// <value>The default is 10,000.</value>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when the value is less than 1.
    /// </exception>
    public int BatchSize
    {
        get => _batchSize;
        set
        {
            if (value < 1)
            {
                throw new ArgumentOutOfRangeException
                (
                    nameof(value),
                    value,
                    "BatchSize must be at least 1."
                );
            }

            _batchSize = value;
        }
    }



    /// <summary>
    /// Gets or sets the timeout in seconds for each bulk copy operation.
    /// A value of 0 means no timeout.
    /// </summary>
    /// <value>The default is 30 seconds.</value>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when the value is negative.
    /// </exception>
    public int BulkCopyTimeout
    {
        get => _bulkCopyTimeout;
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException
                (
                    nameof(value),
                    value,
                    "BulkCopyTimeout must be 0 or greater."
                );
            }

            _bulkCopyTimeout = value;
        }
    }



    /// <summary>
    /// Gets or sets an optional destination table name override.
    /// When <c>null</c>, the table name is derived from the <c>[Table]</c> attribute
    /// or the type name.
    /// </summary>
    public string? DestinationTableName { get; set; }



    /// <summary>
    /// Gets or sets an optional destination schema name override.
    /// When <c>null</c>, the schema is derived from the <c>[Table]</c> attribute.
    /// </summary>
    public string? DestinationSchemaName { get; set; }



    /// <summary>
    /// Gets or sets a value indicating whether to validate each item using
    /// <see cref="System.ComponentModel.DataAnnotations"/> attributes before loading.
    /// </summary>
    /// <value>The default is <c>false</c>.</value>
    /// <remarks>
    /// Enabling validation adds per-item overhead. Items failing validation are skipped
    /// and counted in <see cref="LoaderBase{TDestination, TProgress}.CurrentSkippedItemCount"/>.
    /// </remarks>
    public bool EnableDataValidation { get; set; }



    /// <summary>
    /// Gets or sets an optional callback invoked when an item fails validation.
    /// </summary>
    /// <remarks>
    /// Only invoked when <see cref="EnableDataValidation"/> is <c>true</c>.
    /// The callback receives the item and the collection of validation errors.
    /// </remarks>
    public Action<TRecord, ICollection<ValidationResult>>? OnValidationFailed { get; set; }



    /// <summary>
    /// Gets or sets the action to execute before loading begins.
    /// </summary>
    /// <value>The default is <see cref="SqlBulkCopy.PreAction.None"/>.</value>
    public PreAction PreAction { get; set; }



    /// <summary>
    /// Gets or sets the custom delegate to invoke when
    /// <see cref="PreAction"/> is <see cref="SqlBulkCopy.PreAction.CustomAction"/>.
    /// </summary>
    public Func<PreLoadActionParameters, Task>? PreLoadCustomAction { get; set; }



    /// <summary>
    /// Gets or sets the action to execute after loading completes.
    /// </summary>
    /// <value>The default is <see cref="SqlBulkCopy.PostAction.None"/>.</value>
    public PostAction PostAction { get; set; }



    /// <summary>
    /// Gets or sets the custom delegate to invoke when
    /// <see cref="PostAction"/> is <see cref="SqlBulkCopy.PostAction.CustomAction"/>.
    /// </summary>
    public Func<PostLoadActionParameters, Task>? PostLoadCustomAction { get; set; }



    /// <inheritdoc />
    protected override async Task LoadWorkerAsync
    (
        IAsyncEnumerable<TRecord> items,
        CancellationToken token
    )
    {
        SqlBulkCopyLogMessages.StartingOperation(_logger, OperationName, exception: null);

        var typeMap = TypeMap.Create
        (
            typeof(TRecord),
            DestinationSchemaName,
            DestinationTableName
        );

        ValidateActionConfiguration(typeMap);

        await ExecutePreActionAsync(typeMap, token).ConfigureAwait(false);

        _batchCount = 0;
        var skipCounter = 0;
        var batch = new List<TRecord>(_batchSize);
        var factory = _wrapperFactory ?? CreateFactory();

        await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
        {
            token.ThrowIfCancellationRequested();

            if (skipCounter < SkipItemCount)
            {
                skipCounter++;
                IncrementCurrentSkippedItemCount();
                SqlBulkCopyLogMessages.SkippedItem(_logger, skipCounter, SkipItemCount, exception: null);
                continue;
            }

            if (CurrentItemCount >= MaximumItemCount)
            {
                SqlBulkCopyLogMessages.ReachedMaximumItemCount(_logger, MaximumItemCount, exception: null);
                break;
            }

            if (EnableDataValidation && !ValidateItem(item))
            {
                continue;
            }

            batch.Add(item);
            IncrementCurrentItemCount();

            if (batch.Count >= _batchSize)
            {
                await WriteBatchAsync(batch, typeMap, factory, token).ConfigureAwait(false);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            await WriteBatchAsync(batch, typeMap, factory, token).ConfigureAwait(false);
        }

        await ExecutePostActionAsync(typeMap, token).ConfigureAwait(false);

        SqlBulkCopyLogMessages.BulkCopyCompleted(_logger, CurrentItemCount, CurrentSkippedItemCount, exception: null);
    }



    /// <inheritdoc />
    protected override SqlBulkCopyReport CreateProgressReport() =>
        new
        (
            CurrentItemCount,
            CurrentSkippedItemCount,
            _batchCount
        );



    /// <inheritdoc />
    protected override IProgressTimer CreateProgressTimer(IProgress<SqlBulkCopyReport> progress)
    {
        if (_progressTimer is not null)
        {
            if (Interlocked.CompareExchange(ref _progressTimerWired, 1, 0) == 0)
            {
                _progressTimer.Elapsed += () => progress.Report(CreateProgressReport());
            }

            return _progressTimer;
        }

        return base.CreateProgressTimer(progress);
    }



    private async Task WriteBatchAsync
    (
        List<TRecord> batch,
        TypeMap typeMap,
        ISqlBulkCopyWrapperFactory factory,
        CancellationToken token
    )
    {
        _batchCount++;

        if (typeMap.IsMappedToTable)
        {
            await WriteToTableAsync(batch.Cast<object>().ToList(), typeMap, factory, token)
                .ConfigureAwait(false);

            SqlBulkCopyLogMessages.BatchWritten(_logger, _batchCount, batch.Count, exception: null);
        }

        foreach (var nestedMap in typeMap.NestedTables)
        {
            var childItems = batch
                .SelectMany(parent => nestedMap.GetValues(parent))
                .ToList();

            if (childItems.Count > 0)
            {
                await WriteToTableAsync(childItems, nestedMap.ChildTypeMap, factory, token)
                    .ConfigureAwait(false);

                SqlBulkCopyLogMessages.NestedTableBatchWritten
                (
                    _logger,
                    nestedMap.ChildTypeMap.QualifiedTableName,
                    childItems.Count,
                    exception: null
                );
            }
        }
    }



    private async Task WriteToTableAsync
    (
        IReadOnlyList<object> items,
        TypeMap typeMap,
        ISqlBulkCopyWrapperFactory factory,
        CancellationToken token
    )
    {
        using var wrapper = factory.Create();

        wrapper.DestinationTableName = typeMap.QualifiedTableName;
        wrapper.BatchSize = items.Count;
        wrapper.BulkCopyTimeout = _bulkCopyTimeout;

        foreach (var column in typeMap.Columns)
        {
            wrapper.AddColumnMapping(column.ColumnName, column.ColumnName);
        }

        using var reader = new TypeMapReader(items, typeMap);
        await wrapper.WriteToServerAsync(reader, token).ConfigureAwait(false);
    }



    private bool ValidateItem(TRecord item)
    {
        var context = new ValidationContext(item);
        var results = new List<ValidationResult>();

        if (Validator.TryValidateObject(item, context, results, validateAllProperties: true))
        {
            return true;
        }

        var position = (CurrentItemCount + CurrentSkippedItemCount).ToString();
        SqlBulkCopyLogMessages.ValidationFailed(_logger, position, results.Count, exception: null);

        OnValidationFailed?.Invoke(item, results);
        IncrementCurrentSkippedItemCount();

        return false;
    }



    private void ValidateActionConfiguration(TypeMap typeMap)
    {
        switch (PreAction)
        {
            case PreAction.None:
                break;

            case PreAction.CustomAction:
                if (PreLoadCustomAction is null)
                {
                    throw new InvalidOperationException
                    (
                        "PreAction is CustomAction but PreLoadCustomAction is null."
                    );
                }
                break;

            case PreAction.DeleteAllRecords:
            case PreAction.TruncateTable:
                if (!typeMap.IsMappedToTable)
                {
                    throw new InvalidOperationException
                    (
                        $"PreAction is {PreAction} but the type is not mapped to a table."
                    );
                }
                break;

            default:
                throw new ArgumentOutOfRangeException
                (
                    nameof(PreAction),
                    PreAction,
                    "Unknown PreAction value."
                );
        }

        switch (PostAction)
        {
            case PostAction.None:
                break;

            case PostAction.CustomAction:
                if (PostLoadCustomAction is null)
                {
                    throw new InvalidOperationException
                    (
                        "PostAction is CustomAction but PostLoadCustomAction is null."
                    );
                }
                break;

            default:
                throw new ArgumentOutOfRangeException
                (
                    nameof(PostAction),
                    PostAction,
                    "Unknown PostAction value."
                );
        }
    }



    private async Task ExecutePreActionAsync(TypeMap typeMap, CancellationToken token)
    {
        if (PreAction == PreAction.None)
        {
            return;
        }

        SqlBulkCopyLogMessages.ExecutingPreAction(_logger, PreAction.ToString(), exception: null);

        switch (PreAction)
        {
            case PreAction.DeleteAllRecords:
                await ExecuteSqlCommandAsync
                (
                    $"DELETE FROM {typeMap.QualifiedTableName}",
                    token
                ).ConfigureAwait(false);
                break;

            case PreAction.TruncateTable:
                await ExecuteSqlCommandAsync
                (
                    $"TRUNCATE TABLE {typeMap.QualifiedTableName}",
                    token
                ).ConfigureAwait(false);
                break;

            case PreAction.CustomAction:
                EnsureConnectionAvailable("PreAction.CustomAction");
                var parameters = new PreLoadActionParameters
                (
                    _connection!,
                    _transaction,
                    typeMap.SchemaName,
                    typeMap.TableName,
                    _bulkCopyTimeout,
                    typeMap.Columns,
                    _logger,
                    token
                );
                await PreLoadCustomAction!(parameters).ConfigureAwait(false);
                break;
        }
    }



    private async Task ExecutePostActionAsync(TypeMap typeMap, CancellationToken token)
    {
        if (PostAction == PostAction.None)
        {
            return;
        }

        SqlBulkCopyLogMessages.ExecutingPostAction(_logger, PostAction.ToString(), exception: null);

        switch (PostAction)
        {
            case PostAction.CustomAction:
                EnsureConnectionAvailable("PostAction.CustomAction");
                var parameters = new PostLoadActionParameters
                (
                    _connection!,
                    _transaction,
                    typeMap.SchemaName,
                    typeMap.TableName,
                    _bulkCopyTimeout,
                    typeMap.Columns,
                    _logger,
                    token
                );
                await PostLoadCustomAction!(parameters).ConfigureAwait(false);
                break;
        }
    }



    private async Task ExecuteSqlCommandAsync(string commandText, CancellationToken token)
    {
        EnsureConnectionAvailable("SQL command execution");

        using var command = _connection!.CreateCommand();
        command.CommandText = commandText;
        command.CommandTimeout = _bulkCopyTimeout;

        if (_transaction is not null)
        {
            command.Transaction = _transaction;
        }

        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }



    private void EnsureConnectionAvailable(string operation)
    {
        if (_connection is null)
        {
            throw new InvalidOperationException
            (
                $"Cannot perform '{operation}' without a SqlConnection. " +
                "Use a constructor that accepts a SqlConnection."
            );
        }
    }



    private ISqlBulkCopyWrapperFactory CreateFactory()
    {
        EnsureConnectionAvailable("bulk copy");

        return new SqlBulkCopyWrapperFactory(_connection!, _options, _transaction);
    }
}
