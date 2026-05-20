using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// A <see cref="DbDataReader"/> implementation that reads from an in-memory
/// batch of objects using a <see cref="TypeMap"/> for column metadata.
/// </summary>
/// <remarks>
/// <para>
/// This reader is designed to feed <c>SqlBulkCopy.WriteToServerAsync</c>
/// without the overhead of <c>DataTable</c> / <c>DataRow</c> conversion.
/// </para>
/// <para>
/// Implementation policy:
/// </para>
/// <list type="bullet">
/// <item><description>Methods actually called by <c>SqlBulkCopy</c>
/// (<c>Read</c>, <c>GetValue</c>, <c>IsDBNull</c>, <c>GetName</c>,
/// <c>GetOrdinal</c>, <c>FieldCount</c>) are fully implemented.</description></item>
/// <item><description>Lightweight state properties (<c>Depth</c>,
/// <c>HasRows</c>, <c>IsClosed</c>, <c>RecordsAffected</c>) and
/// <c>NextResult</c> return sensible defaults appropriate to a
/// single-result, in-memory reader.</description></item>
/// <item><description>The remaining <c>Get*</c>-typed accessor overrides
/// throw <see cref="NotSupportedException"/> because <c>SqlBulkCopy</c>
/// never calls them.</description></item>
/// </list>
/// </remarks>
internal sealed class TypeMapReader : DbDataReader
{
    private readonly IReadOnlyList<object> _batch;
    private readonly TypeMap _typeMap;
    private readonly Dictionary<string, int> _ordinalLookup;
    private int _currentIndex = -1;



    /// <summary>
    /// Initializes a new instance of the <see cref="TypeMapReader"/> class.
    /// </summary>
    /// <param name="batch">The batch of objects to read.</param>
    /// <param name="typeMap">The type map describing column metadata.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="batch"/> or <paramref name="typeMap"/> is <c>null</c>.
    /// </exception>
    internal TypeMapReader
    (
        IReadOnlyList<object> batch,
        TypeMap typeMap
    )
    {
        _batch = batch ?? throw new ArgumentNullException(nameof(batch));
        _typeMap = typeMap ?? throw new ArgumentNullException(nameof(typeMap));

        _ordinalLookup = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < _typeMap.Columns.Count; i++)
        {
            _ordinalLookup[_typeMap.Columns[i].ColumnName] = i;
        }
    }



    /// <inheritdoc />
    public override int FieldCount => _typeMap.Columns.Count;



    /// <inheritdoc />
    public override bool Read()
    {
        _currentIndex++;
        return _currentIndex < _batch.Count;
    }



    /// <inheritdoc />
    public override object GetValue(int ordinal)
    {
        ValidateReaderState();
        ValidateOrdinal(ordinal);

        var column = _typeMap.Columns[ordinal];
        var rawValue = column.GetValue(_batch[_currentIndex]);

        if (rawValue is null)
        {
            return DBNull.Value;
        }

        // Convert enum values to their underlying integral type. The
        // converter delegate is compiled once at ColumnMap construction (and
        // is `null` for non-enum columns) so the hot path doesn't reflect
        // on the runtime type per row. Read the delegate into a local so the
        // per-row path touches the property once.
        var enumConverter = column.EnumConverter;
        return enumConverter is not null
            ? enumConverter(rawValue)
            : rawValue;
    }



    /// <inheritdoc />
    public override bool IsDBNull(int ordinal)
    {
        ValidateReaderState();
        ValidateOrdinal(ordinal);

        var column = _typeMap.Columns[ordinal];
        return column.GetValue(_batch[_currentIndex]) is null;
    }



    /// <inheritdoc />
    public override string GetName(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _typeMap.Columns[ordinal].ColumnName;
    }



    /// <inheritdoc />
    public override int GetOrdinal(string name)
    {
        if (name is null)
        {
            throw new ArgumentNullException(nameof(name));
        }

        if (_ordinalLookup.TryGetValue(name, out var ordinal))
        {
            return ordinal;
        }

        throw new ArgumentOutOfRangeException(nameof(name), name, $"Column '{name}' was not found.");
    }



    /// <inheritdoc />
    public override object this[int ordinal] => GetValue(ordinal);



    /// <inheritdoc />
    public override object this[string name] => GetValue(GetOrdinal(name));



    // --- Required overrides that SqlBulkCopy does not call ---

    /// <inheritdoc />
    public override int Depth => 0;

    /// <inheritdoc />
    public override bool HasRows => _batch.Count > 0;

    /// <inheritdoc />
    public override bool IsClosed => false;

    /// <inheritdoc />
    public override int RecordsAffected => -1;



    /// <inheritdoc />

    public override bool GetBoolean(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override byte GetByte(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
        throw new NotSupportedException();

    /// <inheritdoc />

    public override char GetChar(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
        throw new NotSupportedException();

    /// <inheritdoc />

    public override string GetDataTypeName(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override double GetDouble(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override Type GetFieldType(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override float GetFloat(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override Guid GetGuid(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override short GetInt16(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override int GetInt32(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override long GetInt64(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override string GetString(int ordinal) => throw new NotSupportedException();

    /// <inheritdoc />

    public override int GetValues(object[] values) => throw new NotSupportedException();

    /// <inheritdoc />

    public override IEnumerator GetEnumerator() => throw new NotSupportedException();

    /// <inheritdoc />

    public override bool NextResult() => false;



    private void ValidateReaderState()
    {
        if (_currentIndex < 0)
        {
            throw new InvalidOperationException
            (
                "No current row. Call Read() before accessing data."
            );
        }

        if (_currentIndex >= _batch.Count)
        {
            throw new InvalidOperationException
            (
                "Reader has been exhausted. Read() returned false."
            );
        }
    }



    private void ValidateOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal >= _typeMap.Columns.Count)
        {
            throw new ArgumentOutOfRangeException
            (
                nameof(ordinal),
                ordinal,
                $"Ordinal {ordinal} is out of range. Column count is {_typeMap.Columns.Count}."
            );
        }
    }
}
