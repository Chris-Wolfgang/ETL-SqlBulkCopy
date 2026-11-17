type: fix

The loader now stops pulling from the source as soon as `MaximumItemCount` is reached, and a null root item or null nested-collection element throws a clear `InvalidOperationException` instead of a `NullReferenceException` inside `SqlBulkCopy`.
