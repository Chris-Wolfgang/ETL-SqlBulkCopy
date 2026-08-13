using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

/// <summary>
/// Collection names for tests that cannot safely run concurrently.
/// </summary>
/// <remarks>
/// Deliberately short. Serialization is the tool of last resort: it trades away
/// parallelism, so it is only justified when a test mutates state that cannot be
/// partitioned by key.
/// <para>
/// The registry tests are NOT here, and that is the point. They write into the
/// static <see cref="GeneratedAccessorRegistry"/>, but each writes its own
/// <c>(type, propertyName)</c> key on a dedicated probe type no other test reads
/// — so they are isolated by construction and stay parallel. Keying, not
/// serializing, is how shared-registry tests should be isolated here.
/// </para>
/// </remarks>
internal static class TestCollections
{
    /// <summary>
    /// Tests that replace <see cref="System.Globalization.CultureInfo.CurrentCulture"/>.
    /// Ambient thread state cannot be partitioned by key — there is only one
    /// current culture — and the swap spans an <c>await</c>, so another test
    /// resuming on the same thread-pool thread would observe the foreign culture.
    /// This is the case serialization genuinely fixes.
    /// </summary>
    internal const string AmbientCulture = "AmbientCulture";
}



/// <summary>
/// Serializes the tests that temporarily replace the ambient culture.
/// </summary>
[CollectionDefinition(TestCollections.AmbientCulture, DisableParallelization = true)]
public sealed class AmbientCultureCollection
{
    // Marker type only — xUnit uses the attribute, not the class body.
}
