namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;

/// <summary>
/// Dedicated to <c>GeneratedAccessorRegistryTests</c>. The generated-accessor
/// registry is process-global static state, so registering a sentinel getter
/// against a shared model would leak into other tests. This type is mapped by
/// no other test, keeping that registration isolated.
/// </summary>
public sealed class GeneratedAccessorProbeRecord
{
    public int Registered { get; set; }

    public int Unregistered { get; set; }
}
