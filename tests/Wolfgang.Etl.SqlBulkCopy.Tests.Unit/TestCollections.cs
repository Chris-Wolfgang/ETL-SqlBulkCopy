using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Collection names for tests that touch process-wide state and therefore
/// cannot run concurrently with each other or with unrelated tests.
/// </summary>
/// <remarks>
/// xUnit parallelizes across test classes by default. Two categories of test in
/// this assembly mutate state that is shared by the whole process, so they are
/// pinned into non-parallel collections rather than left to interleave.
/// </remarks>
internal static class TestCollections
{
    /// <summary>
    /// Tests that write into <see cref="GeneratedAccessorRegistry"/>, a static
    /// <c>ConcurrentDictionary</c> with no removal API. They register sentinel
    /// getters (e.g. <c>_ =&gt; 999</c>) that stay for the life of the process, so
    /// any test building a <see cref="ColumnMap"/> for the same probe type
    /// afterwards would silently read the sentinel instead of the real value.
    /// </summary>
    internal const string GeneratedAccessorRegistry = "GeneratedAccessorRegistry";

    /// <summary>
    /// Tests that swap <see cref="System.Globalization.CultureInfo.CurrentCulture"/>.
    /// The swap is undone in a <c>finally</c>, but it spans an <c>await</c> — so
    /// while it is in effect another test resuming on the same thread-pool thread
    /// would observe the foreign culture.
    /// </summary>
    internal const string AmbientCulture = "AmbientCulture";
}



/// <summary>
/// Serializes the tests that register sentinel getters in the shared accessor
/// registry.
/// </summary>
[CollectionDefinition(TestCollections.GeneratedAccessorRegistry, DisableParallelization = true)]
public sealed class GeneratedAccessorRegistryCollection
{
    // Marker type only — xUnit uses the attribute, not the class body.
}



/// <summary>
/// Serializes the tests that temporarily replace the ambient culture.
/// </summary>
[CollectionDefinition(TestCollections.AmbientCulture, DisableParallelization = true)]
public sealed class AmbientCultureCollection
{
    // Marker type only — xUnit uses the attribute, not the class body.
}
