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
public sealed class SqlBulkCopyLoader<TRecord> : LoaderBase<TRecord, SqlBulkCopyReport>, ISupportDryRun
    where TRecord : notnull
{
    private static readonly string OperationName = $"SQL bulk copy loading of {typeof(TRecord).Name}";
    private readonly SqlConnection? _connection;
    private readonly SqlBulkCopyOptions _options;
    private readonly SqlTransaction? _transaction;
    private readonly ILogger _logger;
    private readonly IProgressTimer? _progressTimer;
    private readonly ISqlBulkCopyWrapperFactory? _wrapperFactory;
    private readonly ISqlCommandExecutor? _commandExecutor;
    private Action? _progressTimerHandler;
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
        _wrapperFactory = new SqlBulkCopyWrapperFactory(connection, _options, transaction: null);
        _commandExecutor = new SqlConnectionCommandExecutor(connection, transaction: null);
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
        _wrapperFactory = new SqlBulkCopyWrapperFactory(connection, _options, transaction: null);
        _commandExecutor = new SqlConnectionCommandExecutor(connection, transaction: null);
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
        _wrapperFactory = new SqlBulkCopyWrapperFactory(connection, options, transaction);
        _commandExecutor = new SqlConnectionCommandExecutor(connection, transaction);
    }



    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBulkCopyLoader{TRecord}"/> class
    /// with an injected wrapper factory and progress timer for testing.
    /// </summary>
    /// <param name="wrapperFactory">The factory for creating bulk copy wrappers.</param>
    /// <param name="logger">An optional logger instance.</param>
    /// <param name="timer">An optional progress timer to inject. When <c>null</c>, the
    /// base class creates a <c>SystemProgressTimer</c>.</param>
    /// <param name="commandExecutor">
    /// Optional SQL command executor for pre/post actions. When <c>null</c>,
    /// any call into the SQL path (PreAction = DeleteAllRecords / TruncateTable,
    /// PostAction = ...) throws <see cref="InvalidOperationException"/>. Tests
    /// that exercise SQL-issuing pre/post actions must supply a fake.
    /// </param>
    internal SqlBulkCopyLoader
    (
        ISqlBulkCopyWrapperFactory wrapperFactory,
        ILogger? logger,
        IProgressTimer? timer,
        ISqlCommandExecutor? commandExecutor = null
    )
    {
        _wrapperFactory = wrapperFactory ?? throw new ArgumentNullException(nameof(wrapperFactory));
        _logger = logger ?? NullLogger.Instance;
        _progressTimer = timer;
        _commandExecutor = commandExecutor;
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
    /// Enabling validation adds per-item overhead. Validation is applied recursively
    /// to root <typeparamref name="TRecord"/> instances and to every level of
    /// nested-collection children. How a failure is handled is controlled by
    /// <see cref="ValidationFailureBehavior"/> — the default is to throw a
    /// <see cref="SqlBulkCopyValidationException"/>, which fails loudly and
    /// stops the load. Set <see cref="ValidationFailureBehavior"/> to
    /// <see cref="Wolfgang.Etl.SqlBulkCopy.ValidationFailureBehavior.Skip"/>
    /// to tolerate dirty data and drop only the failing items.
    /// </remarks>
    public bool EnableDataValidation { get; set; }



    /// <summary>
    /// Gets or sets a value indicating whether the load runs as a dry run —
    /// validating the pipeline against real data without writing to SQL Server.
    /// </summary>
    /// <value>The default is <c>false</c>.</value>
    /// <remarks>
    /// When <c>true</c>, the loader still enumerates the source, applies
    /// <c>SkipItemCount</c> / <c>MaximumItemCount</c>, runs data
    /// validation, increments the progress counters, and logs as usual — but
    /// performs <b>no</b> SQL side effects: the <see cref="PreAction"/> /
    /// <see cref="PostAction"/> (e.g. truncate / delete) and the bulk insert are
    /// all skipped. This lets a caller confirm a pipeline runs end-to-end and
    /// surfaces mapping / validation errors without touching the destination.
    /// </remarks>
    public bool IsDryRun { get; set; }



    /// <summary>
    /// Gets or sets how the loader reacts to a validation failure when
    /// <see cref="EnableDataValidation"/> is <c>true</c>.
    /// </summary>
    /// <value>
    /// The default is
    /// <see cref="Wolfgang.Etl.SqlBulkCopy.ValidationFailureBehavior.Throw"/>.
    /// </value>
    /// <remarks>
    /// See <see cref="Wolfgang.Etl.SqlBulkCopy.ValidationFailureBehavior"/>
    /// for the semantics of each option. The same setting applies to both
    /// root <typeparamref name="TRecord"/> instances and nested-collection
    /// children.
    /// </remarks>
    public ValidationFailureBehavior ValidationFailureBehavior { get; set; } = ValidationFailureBehavior.Throw;



    /// <summary>
    /// Gets or sets an optional callback invoked when a root
    /// <typeparamref name="TRecord"/> fails validation.
    /// </summary>
    /// <remarks>
    /// Only invoked when <see cref="EnableDataValidation"/> is <c>true</c>.
    /// The callback runs <em>before</em> the configured
    /// <see cref="ValidationFailureBehavior"/> takes effect, so consumers
    /// can log / inspect the failing item from a single hook regardless of
    /// whether the loader then throws or skips. The callback receives the
    /// failing root item and the collection of validation errors. For
    /// nested-collection children, see <see cref="OnNestedValidationFailed"/>.
    /// </remarks>
    public Action<TRecord, ICollection<ValidationResult>>? OnValidationFailed { get; set; }



    /// <summary>
    /// Gets or sets an optional callback invoked when a nested-collection
    /// child instance fails validation.
    /// </summary>
    /// <remarks>
    /// Only invoked when <see cref="EnableDataValidation"/> is <c>true</c>.
    /// The callback runs <em>before</em> the configured
    /// <see cref="ValidationFailureBehavior"/> takes effect. The child is
    /// passed as <see cref="object"/> because the child type is resolved at
    /// load time, not at <typeparamref name="TRecord"/> definition. For
    /// root-item validation, see <see cref="OnValidationFailed"/>.
    /// </remarks>
    public Action<object, ICollection<ValidationResult>>? OnNestedValidationFailed { get; set; }



    /// <summary>
    /// Gets or sets the action to execute before loading begins.
    /// </summary>
    /// <value>The default is <see cref="Wolfgang.Etl.SqlBulkCopy.PreAction.None"/>.</value>
    public PreAction PreAction { get; set; }



    /// <summary>
    /// Gets or sets the custom delegate to invoke when
    /// <see cref="PreAction"/> is <see cref="Wolfgang.Etl.SqlBulkCopy.PreAction.CustomAction"/>.
    /// </summary>
    public Func<PreLoadActionParameters, Task>? PreLoadCustomAction { get; set; }



    /// <summary>
    /// Gets or sets the action to execute after loading completes.
    /// </summary>
    /// <value>The default is <see cref="Wolfgang.Etl.SqlBulkCopy.PostAction.None"/>.</value>
    public PostAction PostAction { get; set; }



    /// <summary>
    /// Gets or sets the custom delegate to invoke when
    /// <see cref="PostAction"/> is <see cref="Wolfgang.Etl.SqlBulkCopy.PostAction.CustomAction"/>.
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

        // Dry run: skip all SQL side effects (pre-action, bulk insert, post-action)
        // but still enumerate, validate, count, and report below.
        if (!IsDryRun)
        {
            await ExecutePreActionAsync(typeMap, token).ConfigureAwait(false);
        }

        Volatile.Write(ref _batchCount, 0); // paired with Volatile.Read in CreateProgressReport
        var skipCounter = 0;
        var batch = new List<TRecord>(_batchSize);
        var factory = _wrapperFactory!; // every constructor sets this

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

        await FinalizeLoadAsync(batch, typeMap, factory, token).ConfigureAwait(false);
    }



    private async Task FinalizeLoadAsync
    (
        List<TRecord> batch,
        TypeMap typeMap,
        ISqlBulkCopyWrapperFactory factory,
        CancellationToken token
    )
    {
        if (batch.Count > 0)
        {
            await WriteBatchAsync(batch, typeMap, factory, token).ConfigureAwait(false);
        }

        if (!IsDryRun)
        {
            await ExecutePostActionAsync(typeMap, token).ConfigureAwait(false);
        }

        SqlBulkCopyLogMessages.BulkCopyCompleted(_logger, CurrentItemCount, CurrentSkippedItemCount, exception: null);
    }



    /// <inheritdoc />
    protected override SqlBulkCopyReport CreateProgressReport() =>
        new
        (
            CurrentItemCount,
            CurrentSkippedItemCount,
            // CreateProgressReport runs on the progress-timer thread, so read
            // _batchCount with Volatile.Read to reliably observe the latest
            // value written by Interlocked.Increment on the loader thread.
            Volatile.Read(ref _batchCount)
        );



    /// <inheritdoc />
    protected override IProgressTimer CreateProgressTimer(IProgress<SqlBulkCopyReport> progress)
    {
        if (_progressTimer is not null)
        {
            // Detach the handler from the prior LoadAsync call (if any) before
            // wiring the new IProgress instance, so reuse of a single loader
            // across multiple LoadAsync calls routes progress to the current
            // caller — not the first one — and the previous IProgress is no
            // longer invoked after its LoadAsync has returned.
            if (_progressTimerHandler is not null)
            {
                _progressTimer.Elapsed -= _progressTimerHandler;
            }

            Action handler = () => progress.Report(CreateProgressReport());
            _progressTimer.Elapsed += handler;
            _progressTimerHandler = handler;

            return _progressTimer;
        }

        return base.CreateProgressTimer(progress);
    }



    private Task WriteBatchAsync
    (
        List<TRecord> batch,
        TypeMap typeMap,
        ISqlBulkCopyWrapperFactory factory,
        CancellationToken token
    )
    {
        // Use covariance to avoid an extra full-batch copy for reference TRecord types.
        // For value types, fall back to materializing as object[].
        IReadOnlyList<object> rootItems = typeof(TRecord).IsValueType
            ? batch.Cast<object>().ToArray()
            : (IReadOnlyList<object>)batch;

        return WriteRecursiveAsync(rootItems, typeMap, factory, isRoot: true, token);
    }



    private async Task WriteRecursiveAsync
    (
        IReadOnlyList<object> items,
        TypeMap typeMap,
        ISqlBulkCopyWrapperFactory factory,
        bool isRoot,
        CancellationToken token
    )
    {
        if (typeMap.IsMappedToTable)
        {
            // Enforce BatchSize for both the root batch and any nested table writes.
            for (var offset = 0; offset < items.Count; offset += _batchSize)
            {
                var chunkSize = Math.Min(_batchSize, items.Count - offset);
                var chunk = SliceList(items, offset, chunkSize);

                await WriteToTableAsync(chunk, typeMap, factory, token).ConfigureAwait(false);
                // Interlocked: CreateProgressReport() may read _batchCount from the
                // progress-timer thread.
                var batchCount = Interlocked.Increment(ref _batchCount);

                if (isRoot)
                {
                    SqlBulkCopyLogMessages.BatchWritten(_logger, batchCount, chunk.Count, exception: null);
                }
                else
                {
                    SqlBulkCopyLogMessages.NestedTableBatchWritten
                    (
                        _logger,
                        typeMap.QualifiedTableName,
                        chunk.Count,
                        exception: null
                    );
                }
            }
        }

        // Recurse so grandchildren and deeper nested collections are also written.
        foreach (var nestedMap in typeMap.NestedTables)
        {
            await WriteNestedTableStreamingAsync(items, nestedMap, factory, token)
                .ConfigureAwait(false);
        }
    }



    /// <summary>
    /// Streams a nested collection into <see cref="WriteRecursiveAsync"/> in
    /// fixed-size chunks. We do this even when the parent type is not mapped,
    /// since a [NotMapped] type can still expose collections of mapped children.
    /// </summary>
    /// <remarks>
    /// Bounded by BatchSize per nesting level. The naive
    /// <c>items.SelectMany(GetValues).ToList()</c> would allocate every child
    /// of every parent up front — a 10,000-parent batch with 100 children
    /// each would pin 1,000,000 object references before any write fires.
    /// </remarks>
    private async Task WriteNestedTableStreamingAsync
    (
        IReadOnlyList<object> parents,
        NestedTableMap nestedMap,
        ISqlBulkCopyWrapperFactory factory,
        CancellationToken token
    )
    {
        var buffer = new List<object>(_batchSize);

        foreach (var parent in parents)
        {
            // Observe cancellation at the parent boundary so callers don't have
            // to wait for buffer to fill before a canceled token takes effect
            // when traversing deep or wide nested collections.
            token.ThrowIfCancellationRequested();

            foreach (var child in nestedMap.GetValues(parent))
            {
                token.ThrowIfCancellationRequested();

                if (EnableDataValidation && !ValidateNestedItem(child, nestedMap.ChildTypeMap))
                {
                    // Drop the failing child without breaking the streaming
                    // batch boundary — buffer fill rate is unaffected by
                    // skipped children, so cancellation/timeout semantics
                    // remain consistent with the un-validated path.
                    continue;
                }

                buffer.Add(child);

                if (buffer.Count >= _batchSize)
                {
                    await WriteRecursiveAsync(buffer, nestedMap.ChildTypeMap, factory, isRoot: false, token)
                        .ConfigureAwait(false);
                    buffer = new List<object>(_batchSize);
                }
            }
        }

        if (buffer.Count > 0)
        {
            await WriteRecursiveAsync(buffer, nestedMap.ChildTypeMap, factory, isRoot: false, token)
                .ConfigureAwait(false);
        }
    }



    internal static IReadOnlyList<object> SliceList(IReadOnlyList<object> source, int offset, int count)
    {
        if (offset == 0 && count == source.Count)
        {
            return source;
        }

        var slice = new object[count];
        for (var i = 0; i < count; i++)
        {
            slice[i] = source[offset + i];
        }
        return slice;
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
        // Chunking to the user-configured _batchSize is already done upstream in
        // WriteRecursiveAsync, so each call here writes exactly one chunk. Setting
        // SqlBulkCopy.BatchSize to items.Count tells the underlying SqlBulkCopy
        // to send the whole chunk in a single batch (no further sub-batching).
        wrapper.BatchSize = items.Count;
        wrapper.BulkCopyTimeout = _bulkCopyTimeout;

#pragma warning disable S3267 // Side-effecting loop is clearer than LINQ for void methods
        foreach (var column in typeMap.Columns)
        {
            wrapper.AddColumnMapping(column.ColumnName, column.ColumnName);
        }
#pragma warning restore S3267

        using var reader = new TypeMapReader(items, typeMap);
        if (IsDryRun)
        {
            // Dry run: don't write, but still pull the reader so the per-row
            // getter + enum-conversion (mapping) path runs and any mapping /
            // value-extraction error surfaces — just without touching the server.
            await DrainReaderAsync(reader, token).ConfigureAwait(false);
        }
        else
        {
            await wrapper.WriteToServerAsync(reader, token).ConfigureAwait(false);
        }
    }



    private static async Task DrainReaderAsync(TypeMapReader reader, CancellationToken token)
    {
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                _ = reader.GetValue(i);
            }
        }
    }



    /// <summary>
    /// Validates a root <typeparamref name="TRecord"/>. Returns <c>true</c>
    /// if the item passed (continue processing) or <c>false</c> if the item
    /// failed and the loader is configured to skip
    /// (<see cref="ValidationFailureBehavior.Skip"/>).
    /// </summary>
    /// <exception cref="SqlBulkCopyValidationException">
    /// Thrown when the item fails validation and
    /// <see cref="ValidationFailureBehavior"/> is
    /// <see cref="Wolfgang.Etl.SqlBulkCopy.ValidationFailureBehavior.Throw"/>
    /// (the default).
    /// </exception>
    private bool ValidateItem(TRecord item)
    {
        var context = new ValidationContext(item);
        var results = new List<ValidationResult>();

        if (Validator.TryValidateObject(item, context, results, validateAllProperties: true))
        {
            return true;
        }

        var position = (CurrentItemCount + CurrentSkippedItemCount).ToString(System.Globalization.CultureInfo.InvariantCulture);
        SqlBulkCopyLogMessages.ValidationFailed(_logger, position, results.Count, exception: null);

        // Run the consumer's hook before deciding throw vs. skip so a single
        // callback works for both modes.
        OnValidationFailed?.Invoke(item, results);

        if (ValidationFailureBehavior == ValidationFailureBehavior.Throw)
        {
            throw new SqlBulkCopyValidationException(item, results);
        }

        IncrementCurrentSkippedItemCount();
        return false;
    }



    /// <summary>
    /// Validates a nested-collection child instance. Mirrors <see cref="ValidateItem"/>
    /// but routes failures to <see cref="OnNestedValidationFailed"/>, logs a
    /// nested-specific message that carries the child table name (so the
    /// log line identifies which nested table a failing child belonged to),
    /// honors <see cref="ValidationFailureBehavior"/> the same way as root
    /// validation, and (when skipping) does not touch
    /// <see cref="LoaderBase{TDestination, TProgress}.CurrentSkippedItemCount"/>
    /// — the source-level skip counter only reflects root items.
    /// </summary>
    /// <exception cref="SqlBulkCopyValidationException">
    /// Thrown when the child fails validation and
    /// <see cref="ValidationFailureBehavior"/> is
    /// <see cref="Wolfgang.Etl.SqlBulkCopy.ValidationFailureBehavior.Throw"/>
    /// (the default).
    /// </exception>
    private bool ValidateNestedItem(object item, TypeMap childTypeMap)
    {
        var context = new ValidationContext(item);
        var results = new List<ValidationResult>();

        if (Validator.TryValidateObject(item, context, results, validateAllProperties: true))
        {
            return true;
        }

        SqlBulkCopyLogMessages.NestedValidationFailed
        (
            _logger,
            childTypeMap.QualifiedTableName,
            results.Count,
            exception: null
        );

        OnNestedValidationFailed?.Invoke(item, results);

        if (ValidationFailureBehavior == ValidationFailureBehavior.Throw)
        {
            throw new SqlBulkCopyValidationException(item, results);
        }

        return false;
    }



    private void ValidateActionConfiguration(TypeMap typeMap)
    {
        ValidatePreActionConfiguration(typeMap);
        ValidatePostActionConfiguration();
    }



    private void ValidatePreActionConfiguration(TypeMap typeMap)
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
#pragma warning disable S3928, MA0015 // PreAction is a property, not a parameter
                throw new ArgumentOutOfRangeException
                (
                    nameof(PreAction),
                    PreAction,
                    "Unknown PreAction value."
                );
#pragma warning restore S3928, MA0015
        }
    }



    private void ValidatePostActionConfiguration()
    {
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
#pragma warning disable S3928, MA0015 // PostAction is a property, not a parameter
                throw new ArgumentOutOfRangeException
                (
                    nameof(PostAction),
                    PostAction,
                    "Unknown PostAction value."
                );
#pragma warning restore S3928, MA0015
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



    private Task ExecuteSqlCommandAsync(string commandText, CancellationToken token)
    {
        if (_commandExecutor is null)
        {
            // Mirrors EnsureConnectionAvailable's contract for CustomAction:
            // a SQL-issuing pre/post action requires either a public-ctor
            // SqlConnection (which constructs the executor) or — in tests —
            // an explicitly injected ISqlCommandExecutor on the internal
            // ctor. Public-API callers can never see this path because every
            // public constructor populates _commandExecutor.
            throw new InvalidOperationException
            (
                "Cannot execute SQL command without a SqlConnection or ISqlCommandExecutor. " +
                "Use a constructor that accepts a SqlConnection."
            );
        }

        return _commandExecutor.ExecuteNonQueryAsync(commandText, _bulkCopyTimeout, token);
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



}
