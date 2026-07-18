using System;
using System.Linq;
using Wolfgang.Etl.SqlBulkCopy;

namespace Wolfgang.Etl.SqlBulkCopy.AotSmoke;

/// <summary>Byte-backed enum — exercises the generated enum→underlying converter.</summary>
public enum SmokeKind : byte
{
    A = 1,
    B = 2,
}



/// <summary>
/// A <c>[BulkCopyable]</c> type whose accessors the source generator emits.
/// Reached only through <c>TypeMap</c> / <c>ColumnMap</c> below — never through
/// <c>SqlBulkCopyLoader</c> — so Microsoft.Data.SqlClient stays out of the
/// AOT-reachable graph and the publish signal is about this library's code.
/// </summary>
[BulkCopyable]
public sealed class SmokeRow
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public SmokeKind Kind { get; set; }
}



/// <summary>
/// Native-AOT smoke: builds the column map for a <c>[BulkCopyable]</c> type and
/// reads values through the source-generated getters and enum converter, under
/// a Native-AOT-published binary. Proves the generated hot path compiles AOT
/// warning-free (verified at publish) and runs correctly (this program's exit
/// code). Deeper generated-vs-reflection equivalence is covered by the JIT
/// conformance tests; this asserts the same holds once natively compiled.
/// </summary>
public static class Program
{
    public static int Main()
    {
        var map = TypeMap.Create(typeof(SmokeRow));
        var row = new SmokeRow { Id = 7, Name = "aot", Kind = SmokeKind.B };

        var idColumn = map.Columns.First(c => string.Equals(c.PropertyName, nameof(SmokeRow.Id), StringComparison.Ordinal));
        var nameColumn = map.Columns.First(c => string.Equals(c.PropertyName, nameof(SmokeRow.Name), StringComparison.Ordinal));
        var kindColumn = map.Columns.First(c => string.Equals(c.PropertyName, nameof(SmokeRow.Kind), StringComparison.Ordinal));

        // Source-generated property getters (no Expression.Compile under AOT).
        var idValue = idColumn.GetValue(row);
        var nameValue = nameColumn.GetValue(row);

        // Source-generated enum→underlying converter.
        var kindBoxed = kindColumn.GetValue(row);
        var kindUnderlying = kindColumn.EnumConverter is null || kindBoxed is null
            ? null
            : kindColumn.EnumConverter(kindBoxed);

        var ok = idValue is 7
                 && nameValue is string name && string.Equals(name, "aot", StringComparison.Ordinal)
                 && kindUnderlying is byte kind && kind == 2;

        Console.WriteLine
        (
            $"AOT smoke: Id={idValue}, Name={nameValue}, Kind(underlying)={kindUnderlying}, ok={ok}"
        );

        return ok ? 0 : 1;
    }
}
