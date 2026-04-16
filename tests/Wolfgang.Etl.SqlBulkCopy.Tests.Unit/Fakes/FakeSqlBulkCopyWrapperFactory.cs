using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;

[ExcludeFromCodeCoverage]
internal sealed class FakeSqlBulkCopyWrapperFactory : ISqlBulkCopyWrapperFactory
{
    private readonly List<FakeSqlBulkCopyWrapper> _createdWrappers = new();
    private readonly Exception? _throwOnWrite;



    internal FakeSqlBulkCopyWrapperFactory(Exception? throwOnWrite = null)
    {
        _throwOnWrite = throwOnWrite;
    }



    public IReadOnlyList<FakeSqlBulkCopyWrapper> CreatedWrappers => _createdWrappers;



    public ISqlBulkCopyWrapper Create()
    {
        var wrapper = new FakeSqlBulkCopyWrapper(_throwOnWrite);
        _createdWrappers.Add(wrapper);
        return wrapper;
    }
}
