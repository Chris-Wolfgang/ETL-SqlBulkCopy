type: feature

Add `EtlPipelineSqlBulkCopyExtensions.SqlBulkCopyLoader`, a sink terminator for the `EtlPipeline` fluent chain, so any source (`.From(...)`, a CSV/JSON/XML extractor, ...) can end in a SQL Server bulk insert: `EtlPipeline.Create().From(records).SqlBulkCopyLoader(connection, options).RunAsync()`.
