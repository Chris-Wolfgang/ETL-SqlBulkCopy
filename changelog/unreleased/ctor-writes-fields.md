type: internal

The options constructor assigns the stage's backing fields directly instead of going through the deprecated setters, so the `CS0618` suppressions that covered those writes are gone. The `BatchSize` (≥ 1) and `BulkCopyTimeout` (≥ 0) guards now also run on the record's init accessors; the two record tests that expected the loader constructor to throw now assert the throw at `new SqlBulkCopyLoaderOptions { … }` and no longer need a SqlConnection. (#345)
