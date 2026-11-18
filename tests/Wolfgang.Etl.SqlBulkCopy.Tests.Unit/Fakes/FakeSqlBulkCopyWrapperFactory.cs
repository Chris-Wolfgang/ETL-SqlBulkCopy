using System.Collections.Generic;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;

internal sealed class FakeSqlBulkCopyWrapperFactory : ISqlBulkCopyWrapperFactory
{
    private readonly List<FakeSqlBulkCopyWrapper> _createdWrappers = new();
    private int _createCount;



    public IReadOnlyList<FakeSqlBulkCopyWrapper> CreatedWrappers => _createdWrappers;



    /// <summary>1-based number of the wrapper (one per written chunk) whose write should throw; 0 = never.</summary>
    public int FailOnWriteNumber { get; set; }



    public ISqlBulkCopyWrapper Create()
    {
        var wrapper = new FakeSqlBulkCopyWrapper();
        _createCount++;
        if (_createCount == FailOnWriteNumber)
        {
            wrapper.FailWith = new System.InvalidOperationException("simulated write failure");
        }

        _createdWrappers.Add(wrapper);
        return wrapper;
    }
}
