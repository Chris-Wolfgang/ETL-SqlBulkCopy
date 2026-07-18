namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// A <c>[BulkCopyable]</c> fixture the source generator picks up at compile
/// time. It emits getters for these properties and registers them with
/// <c>GeneratedAccessorRegistry</c> from a module initializer (on net5.0+).
/// <c>GeneratedAccessorGeneratorTests</c> asserts that registration happened.
/// </summary>
[BulkCopyable]
public sealed class BulkCopyableFixture
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
}
