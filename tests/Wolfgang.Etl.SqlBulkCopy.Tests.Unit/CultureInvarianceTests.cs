using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// The loader resolves column names case-insensitively
/// (<see cref="StringComparer.OrdinalIgnoreCase"/> in <c>TypeMap</c> and
/// <c>TypeMapReader</c>). "Case-insensitive" is culture-dependent unless it is
/// explicitly ordinal: under <c>tr-TR</c> the dotted/dotless-I fold changes
/// which strings compare equal, so a future switch to a culture-sensitive
/// comparer (e.g. <c>CurrentCultureIgnoreCase</c>) would silently change column
/// matching for any name containing <c>I</c>/<c>i</c> — but only when the host
/// runs under Turkish. These tests run the mapping path under hostile cultures
/// and assert the observable output is identical to the invariant run, locking
/// in the ordinal choice.
/// </summary>
public class CultureInvarianceTests
{
    // Cultures chosen for their classic string-handling traps:
    //   tr-TR — dotted/dotless-I case folding (the important one here)
    //   de-DE — decimal comma / grouping
    //   ja-JP — non-Latin script, different collation
    // en-US is the CI default and the invariance baseline.
    public static IEnumerable<object[]> HostileCultures()
    {
        yield return new object[] { "tr-TR" };
        yield return new object[] { "de-DE" };
        yield return new object[] { "ja-JP" };
        yield return new object[] { "en-US" };
    }



    [Theory]
    [MemberData(nameof(HostileCultures))]
    public async Task LoadAsync_resolves_column_mappings_identically_under_any_culture_Async(string cultureName)
    {
        var mappingsUnderCulture = await RunLoadAndCaptureMappingsAsync(cultureName).ConfigureAwait(true);

        // The mapping set must be exactly the invariant-culture result — the
        // property names Id / FullName / Amount all round-trip regardless of
        // the ambient culture's case-folding rules.
        Assert.Equal
        (
            new[] { ("Amount", "Amount"), ("FullName", "FullName"), ("Id", "Id") },
            mappingsUnderCulture
        );
    }



    [Fact]
    public async Task LoadAsync_column_mappings_are_stable_across_tr_TR_and_invariant_Async()
    {
        var underTurkish = await RunLoadAndCaptureMappingsAsync("tr-TR").ConfigureAwait(true);
        var underInvariant = await RunLoadAndCaptureMappingsAsync(CultureInfo.InvariantCulture.Name).ConfigureAwait(true);

        Assert.Equal(underInvariant, underTurkish);
    }



    private static async Task<IReadOnlyList<(string Source, string Destination)>> RunLoadAndCaptureMappingsAsync(string cultureName)
    {
        var originalCulture = CultureInfo.CurrentCulture;
        var originalUiCulture = CultureInfo.CurrentUICulture;

        try
        {
            var culture = CultureInfo.GetCultureInfo(cultureName);
            CultureInfo.CurrentCulture = culture;
            CultureInfo.CurrentUICulture = culture;

            var factory = new FakeSqlBulkCopyWrapperFactory();
            var timer = new ManualProgressTimer();
            var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer);

            await sut.LoadAsync(ToAsyncEnumerableAsync(CreateTestItems(1))).ConfigureAwait(true);

            return factory.CreatedWrappers[0].ColumnMappings
                .OrderBy(m => m.Source, StringComparer.Ordinal)
                .ToList();
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUiCulture;
        }
    }



    private static IReadOnlyList<TestRecord> CreateTestItems(int count)
    {
        return Enumerable.Range(1, count)
            .Select(i => new TestRecord { Id = i, Name = $"Item{i}", Amount = i * 10m })
            .ToList();
    }



    private static async IAsyncEnumerable<T> ToAsyncEnumerableAsync<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}
