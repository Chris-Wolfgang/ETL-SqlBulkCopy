using System.Collections.Generic;
using System.Threading.Tasks;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures;

/// <summary>
/// Test helpers for converting synchronous collections to
/// <see cref="IAsyncEnumerable{T}"/> for feeding the loader.
/// </summary>
internal static class AsyncEnumerableHelpers
{
    /// <summary>
    /// Wraps a synchronous sequence as an <see cref="IAsyncEnumerable{T}"/>
    /// for tests that need to feed the loader without depending on
    /// System.Linq.Async's <c>ToAsyncEnumerable</c> extension.
    /// </summary>
    public static async IAsyncEnumerable<T> ToAsyncEnumerableAsync<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}
