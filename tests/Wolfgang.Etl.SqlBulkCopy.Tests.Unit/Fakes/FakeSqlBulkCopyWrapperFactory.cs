using System.Collections.Generic;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;

internal sealed class FakeSqlBulkCopyWrapperFactory : ISqlBulkCopyWrapperFactory
{
    private readonly List<FakeSqlBulkCopyWrapper> _createdWrappers = new();



    public IReadOnlyList<FakeSqlBulkCopyWrapper> CreatedWrappers => _createdWrappers;



    public ISqlBulkCopyWrapper Create()
    {
        var wrapper = new FakeSqlBulkCopyWrapper();
        _createdWrappers.Add(wrapper);
        return wrapper;
    }
}
