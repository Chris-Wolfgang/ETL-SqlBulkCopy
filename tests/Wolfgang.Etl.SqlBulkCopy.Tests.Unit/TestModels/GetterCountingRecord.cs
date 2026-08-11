using System.ComponentModel.DataAnnotations.Schema;
using System.Threading;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A record whose <see cref="Counted"/> getter increments a counter, so a test
/// can assert how many times the mapping layer actually invoked it.
/// </summary>
/// <remarks>
/// Deliberately NOT <c>[BulkCopyable]</c>: the generated accessor would read the
/// backing field directly and bypass the counting getter, defeating the point.
/// The reflection path compiles a getter that calls the real property.
/// </remarks>
[Table("GetterCountingRecords")]
public sealed class GetterCountingRecord
{
    private static int _getCount;

    private string _counted = string.Empty;



    /// <summary>Gets the number of times <see cref="Counted"/> has been read.</summary>
    public static int GetCount => Volatile.Read(ref _getCount);



    /// <summary>Gets or sets the identifier.</summary>
    public int Id { get; set; }



    /// <summary>
    /// Gets or sets the counted value. Every read increments <see cref="GetCount"/>.
    /// </summary>
    public string Counted
    {
        get
        {
            Interlocked.Increment(ref _getCount);
            return _counted;
        }

        set => _counted = value;
    }



    /// <summary>Resets the counter. Call at the start of each test.</summary>
    public static void ResetCount()
    {
        Volatile.Write(ref _getCount, 0);
    }
}
